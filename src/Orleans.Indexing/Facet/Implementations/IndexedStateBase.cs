using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Indexing.Facet
{
    abstract class IndexedStateBase<TGrainState>(IServiceProvider sp, IIndexedStateConfiguration config, IGrainContext context)
        : IIndexedState<TGrainState> where TGrainState : class, new()
    {
        readonly IServiceProvider ServiceProvider = sp;
        protected readonly IIndexedStateConfiguration IndexedStateConfig = config;
        protected readonly IGrainContext grainActivationContext = context;
        protected Grain grain;
        protected IIndexableGrain iIndexableGrain;
        protected Func<Guid> getWorkflowIdFunc;
        protected GrainIndexes grainIndexes;
        bool _hasAnyUniqueIndex;

        // IndexManager (and therefore logger) cannot be set in ctor because Grain activation has not yet set base.Runtime.
        internal SiloIndexManager SiloIndexManager => IndexManager.GetSiloIndexManager(ref this.siloIndexManager, this.ServiceProvider);
        SiloIndexManager siloIndexManager;

        protected ILogger Logger => this.__logger ??= this.SiloIndexManager.LoggerFactory.CreateLoggerWithFullCategoryName(this.GetType());
        ILogger __logger;

        protected SiloAddress BaseSiloAddress => this.SiloIndexManager.SiloAddress;

        #region public API

        public abstract Task<TResult> PerformRead<TResult>(Func<TGrainState, TResult> readFunction);

        public abstract Task<TResult> PerformUpdate<TResult>(Func<TGrainState, TResult> updateFunction);

        #endregion public API

        #region Lifecycle management

        protected void Participate<TSubclass>(IGrainLifecycle lifecycle)
        {
            lifecycle.Subscribe<TSubclass>(GrainLifecycleStage.SetupState, onStart: _ => OnSetupStateAsync());
            lifecycle.Subscribe<TSubclass>(GrainLifecycleStage.Activate, onStart: ct => OnActivateAsync(ct), onStop: ct => OnDeactivateAsync(ct));
        }

        Task OnSetupStateAsync() => this.Initialize(this.grain);

        internal abstract Task OnActivateAsync(CancellationToken ct);

        internal abstract Task OnDeactivateAsync(CancellationToken ct);

        #endregion Lifecycle management

        Task Initialize(Grain grain)
        {
            if (this.grain != null) // If not already called
            {
                return Task.CompletedTask;
            }

            this.grain = grain;
            this.iIndexableGrain = this.grain.AsReference<IIndexableGrain>(this.SiloIndexManager);
            if (!GrainIndexes.CreateInstance(this.SiloIndexManager.IndexRegistry, this.grain.GetType(), out this.grainIndexes) || !this.grainIndexes.HasAnyIndexes)
            {
                throw new InvalidOperationException("IndexedState should not be used for a Grain class with no indexes");
            }
            this._hasAnyUniqueIndex = this.grainIndexes.HasAnyUniqueIndex;
            return Task.CompletedTask;
        }

        /// <summary>
        /// After some changes were made to the grain, and the grain is in a consistent state, this method is called to update the
        /// indexes defined on this grain type.
        /// </summary>
        /// <remarks>
        /// UpdateIndexes kicks off the sequence that eventually goes through virtual/overridden ApplyIndexUpdates, which in turn calls
        /// writeGrainStateFunc() appropriately to ensure that only the successfully persisted bits are indexed, and the indexes are updated
        /// concurrently while writeGrainStateFunc() is done.
        ///
        /// The only reason that this method can receive a negative result from a call to ApplyIndexUpdates is that the list of indexes
        /// might have changed. In this case, it updates the list of member update and tries again. In the case of a positive result
        /// from ApplyIndexUpdates, the list of before-images is replaced by the list of after-images.
        /// </remarks>
        /// <param name="updateReason">Determines whether this method is called upon activation, deactivation, or still-active state of this grain</param>
        /// <param name="onlyUpdateActiveIndexes">whether only active indexes should be updated</param>
        /// <param name="writeStateIfConstraintsAreNotViolated">whether to write back the state to the storage if no constraint is violated</param>
        protected async Task<InterfaceToUpdatesMap> UpdateIndexes(IndexUpdateReason updateReason, bool onlyUpdateActiveIndexes, bool writeStateIfConstraintsAreNotViolated)
        {
            // A flag to determine whether only unique indexes were updated
            var onlyUniqueIndexesWereUpdated = this._hasAnyUniqueIndex;

            // Gather the dictionary of indexes to their corresponding updates, grouped by interface
            var interfaceToUpdatesMap = this.GenerateMemberUpdates(updateReason, onlyUpdateActiveIndexes, out var updateIndexesEagerly, ref onlyUniqueIndexesWereUpdated, out var numberOfUniqueIndexesUpdated);

            // Apply the updates to the indexes defined on this grain
            await this.ApplyIndexUpdates(interfaceToUpdatesMap, updateIndexesEagerly, onlyUniqueIndexesWereUpdated, numberOfUniqueIndexesUpdated, writeStateIfConstraintsAreNotViolated);
            return interfaceToUpdatesMap;
        }

        /// <summary>
        /// Applies a set of updates to the indexes defined on the grain
        /// </summary>
        /// <param name="interfaceToUpdatesMap">the dictionary of indexes to their corresponding updates</param>
        /// <param name="updateIndexesEagerly">whether indexes should be updated eagerly or lazily</param>
        /// <param name="onlyUniqueIndexesWereUpdated">a flag to determine whether only unique indexes were updated</param>
        /// <param name="numberOfUniqueIndexesUpdated">determine the number of updated unique indexes</param>
        /// <param name="writeStateIfConstraintsAreNotViolated">whether writing back
        ///             the state to the storage should be done if no constraint is violated</param>
        protected abstract Task ApplyIndexUpdates(InterfaceToUpdatesMap interfaceToUpdatesMap,
                                                  bool updateIndexesEagerly, bool onlyUniqueIndexesWereUpdated,
                                                  int numberOfUniqueIndexesUpdated, bool writeStateIfConstraintsAreNotViolated);

        InterfaceToUpdatesMap GenerateMemberUpdates(IndexUpdateReason updateReason, bool onlyUpdateActiveIndexes, out bool updateIndexesEagerly, ref bool onlyUniqueIndexesWereUpdated, out int numberOfUniqueIndexesUpdated)
        {
            (string prevIndexName, var prevIndexIsEager) = (null, false);
            var numUniqueIndexes = 0;
            var onlyUniqueIndexes = true;
            var interfaceToUpdatesMap = new InterfaceToUpdatesMap(
                updateReason,
                this.getWorkflowIdFunc,
                this.grainIndexes.Select(kvp => (kvp.Key, generateNamedMemberUpdates(kvp.Key, indexes: kvp.Value)))
                );
            updateIndexesEagerly = prevIndexName != null && prevIndexIsEager;
            numberOfUniqueIndexesUpdated = numUniqueIndexes;
            onlyUniqueIndexesWereUpdated = onlyUniqueIndexes;
            return interfaceToUpdatesMap;

            IEnumerable<(string indexName, IMemberUpdate mu)> generateNamedMemberUpdates(Type interfaceType, InterfaceIndexes indexes)
            {
                var befImgs = indexes.BeforeImages.Value;
                foreach (var (indexName, indexInfo) in indexes.NamedIndexes
                             .Where(kvp => !onlyUpdateActiveIndexes || !kvp.Value.IndexInterface.IsTotalIndex())
                             .Select(kvp => (kvp.Key, kvp.Value)))
                {
                    var mu = updateReason == IndexUpdateReason.OnActivate
                        ? indexInfo.UpdateGenerator.CreateMemberUpdate(befImgs[indexName])
                        : indexInfo.UpdateGenerator.CreateMemberUpdate(updateReason == IndexUpdateReason.OnDeactivate ? null : indexes.Properties, befImgs[indexName]);
                    if (mu.OperationType != IndexOperationType.None)
                    {
                        if (prevIndexName != null && prevIndexIsEager != indexInfo.MetaData.IsEager)
                        {
                            throw new InvalidOperationException($"Inconsistent index eagerness specification on grain implementation {this.GetType().Name}," +
                                                                $" interface {interfaceType.Name}, properties {indexes.PropertiesType.FullName}." +
                                                                $" Prior indexes (most recently {prevIndexName}) specified {prevIndexIsEager} while" +
                                                                $" index {indexName} specified {indexInfo.MetaData.IsEager}. This misconfiguration should have been detected on silo startup.");
                        }
                        (prevIndexName, prevIndexIsEager) = (indexName, indexInfo.MetaData.IsEager);

                        if (indexInfo.MetaData.IsUniqueIndex)
                        {
                            // An update is a delete plus insert, so count it as two.
                            numUniqueIndexes += (mu.OperationType == IndexOperationType.Update) ? 2 : 1;
                        }
                        else
                        {
                            onlyUniqueIndexes = false;
                        }
                        yield return (indexName, mu);
                    }
                }
            }
        }

        // IIndexableGrain methods; these are overridden only by FaultTolerantWorkflowIndexedState.  TODO move to FT only
        public virtual Task<Immutable<HashSet<Guid>>> GetActiveWorkflowIdsSet() => throw new NotImplementedException("GetActiveWorkflowIdsSet");

        public virtual Task RemoveFromActiveWorkflowIds(HashSet<Guid> removedWorkflowIds) => throw new NotImplementedException("RemoveFromActiveWorkflowIds");
    }
}
