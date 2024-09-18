#nullable enable
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Core;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Transactions.Abstractions;

namespace Orleans.Indexing;

/// <summary>
/// Indexed state that enriches <typeparamref name="TState"/> with indexing pipeline metadata and implements a fault-tolerant
/// commit protocol that ensures the indexes are guaranteed to be updated eventually.
/// Based on an underlying <see cref="ITransactionalState{State}"/>
/// </summary>
/// <param name="sp"></param>
/// <param name="grainContext"></param>
/// <param name="options"></param>
/// <typeparam name="TState"></typeparam>
public class IndexedState<TState>(
    IServiceProvider sp,
    ILoggerFactory loggerFactory, 
    IGrainContext grainContext,
    IGrainFactory grainFactory,
    IndexedStateOptions options
    ) : ILifecycleParticipant<IGrainLifecycle>, 
        IIndexedState<TState> 
        where TState : class, new()
{
    readonly ILogger<IndexedState<TState>> Log = loggerFactory.CreateLogger<IndexedState<TState>>();
    
    NonTransactionalState<IndexedStateEnvelope<TState>>? state;
    NonTransactionalState<IndexedStateEnvelope<TState>> State => state.EnsureNotNull("state is not initialized!");

    IndexedStateEnvelope<TState> WrappedState => State.State;
    
    TState UserState => WrappedState.State;

    
    IndexedStateCache? indexCache;
    IndexedStateCache IndexCache => indexCache.EnsureNotNull("indexCache is not initialized!");
    
    
    IndexManager? indexManager;
    IndexManager IndexManager => indexManager ??= sp.GetRequiredService<IndexManager>();
    
    IndexRegistry IndexRegistry => IndexManager.Registry;

    
    IIndexableGrain? indexableGrain;

    IIndexableGrain IndexableGrain => indexableGrain.EnsureNotNull("indexableGrain is not initialized!");
    
    
    public void Participate(IGrainLifecycle observer)
    {
        observer.Subscribe<TState>(GrainLifecycleStage.SetupState, OnSetupState);
        observer.Subscribe<TState>(GrainLifecycleStage.Activate, OnActivate, OnDeactivate);
    }
    
    async Task OnActivate(CancellationToken ct)
    {
        var indexableGrainInterfaces = sp.GetRequiredService<IndexableGrainInterfaceRegistry>().GrainInterfaces;

        this.indexableGrain = (IIndexableGrain)grainFactory
            .GetGrain<IIndexableGrain>(grainContext.GrainId)
            .AsReference(indexableGrainInterfaces[0]); // TODO: generalize
        
        var grainStorage = sp.GetRequiredKeyedService<IGrainStorage>(serviceKey: options.StorageName);
        var storage = new StateStorageBridge<IndexedStateEnvelope<TState>>(name: options.StateName, grainContext, grainStorage);
        this.state = await NonTransactionalState<IndexedStateEnvelope<TState>>.CreateAsync(storage);        
        this.indexCache = IndexedStateCache.Create(IndexRegistry, indexableGrainInterfaces, state: UserState);
        if (WrappedState.ActiveIndexingActionIds.Count == 0)
        {
            WrappedState.ClearIndexingQueues();
            // TODO: insert into active index
        }
        else
        {
            // remove indexing queues for which the index cache doesn't have an entry (by grain type).
            WrappedState.IndexingQueues.RemoveWhere(x => !IndexCache.ContainsIndexableGrainInterface(indexableGrainInterface: x.Key));
            await HandlePendingIndexingActions();
        }
    }
    
    public async Task<TResult> PerformRead<TResult>(Func<TState, TResult> read)
    {
        return read(UserState);
    }

    /// <summary>
    /// Performs an updates on the indexed property object and 
    /// </summary>
    /// <param name="update"></param>
    /// <typeparam name="TResult"></typeparam>
    /// <returns></returns>
    public async Task<TResult> PerformUpdate<TResult>(Func<TState, TResult> update)
    {
        var res = update(UserState);
        await UpdateIndexes();
        return res;
    }
    
    /// <summary>
    /// Updates all indexes associated to the corresponding indexable grain in a fault-tolerant manner: <br/>
    /// 1. Gets index updates from <see cref="IndexCache"/>. <br/>
    /// 2. Durably enqueues updates in <see cref="IIndexingQueueService"/>, applying eagerly if a unique index update is present. <br/>
    /// 3. Adds to active indexing action ids <see cref="IndexedStateEnvelope{TProperties}.ActiveIndexingActionIds"/>. <br/>
    /// 4. Persists <see cref="UserState"/>. <br/>
    /// 5. Sets commited values in <see cref="IndexCache"/> based on the latest state.
    /// </summary>
    async Task UpdateIndexes()
    {
        var updates = IndexCache.PrepareUpdates(IndexUpdateReason.WriteState, UserState);
        var updateIds = await EnqueueIndexUpdates(updates);
        if (updates.HasUniqueIndexUpdate)
        {
            // Updates to unique indexes should be tentative so they are not visible before all uniqueness constraints are satisfied
            // and that the grain state persistence completes successfully.
            // any tentative records will be removed by the indexing queue service.
            await ApplyIndexUpdatesEagerly(updates, IndexUpdateApplicabilityMode.Unique);
        }        
        WrappedState.ActiveIndexingActionIds.AddRange(updateIds);
        await State.PerformUpdate();
        IndexCache.CommitUpdates(updates);
    }
    
    /// <summary>
    /// Handles the pending indexing actions across all queues <see cref="IIndexingQueueService"/>.
    /// </summary>
    async Task HandlePendingIndexingActions()
    {
        var prevQueues = WrappedState.GetIndexingQueuesSnapshot();
        var activeActionIds = WrappedState.ActiveIndexingActionIds;
     
        Log.LogTrace("handle_remaining_indexing_actions/started active_action_id_count={0} indexing_queue_count={1}", activeActionIds.Count, prevQueues.Count);
        
        var pendingActionIds = await prevQueues
            .Parallel(x =>
            {
                var newQueue = GetOrCreateIndexingQueue(x.GrainInterface);
                return HandlePendingIndexingActions(newQueue: newQueue, previousQueue: x.Queue, activeActionIds);
            })
            .Then(x => x.FlattenToReadOnlyList());

        if (WrappedState.ClearProcessedActionsExcept(pendingActionIds))
            await State.PerformUpdate();
        
        Log.LogTrace("handle_remaining_indexing_actions/completed");
    }

    /// <summary>
    /// Handles the pending indexing actions associated to the given grain type.
    /// </summary>
    /// <param name="newQueue"></param>
    /// <param name="previousQueue"></param>
    /// <param name="activeActionIds"></param>
    /// <returns>The set of pending action ids.</returns>
    async Task<HashSet<Guid>> HandlePendingIndexingActions(IIndexingQueueService newQueue, IIndexingQueueService previousQueue, HashSet<Guid> activeActionIds)
    {        
        if (newQueue.Equals(previousQueue))
        {
            var actions = await previousQueue.GetPending(activeActionIds);
            return actions.Value.Select(x => x.ActionId).ToHashSet();
        }
        
        Immutable<IReadOnlyList<IndexingAction>> pendingActions;
        try
        {
            pendingActions = await previousQueue.GetPending(activeActionIds);                
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "failed_to_access_previous_queue_now_trying_to_reincarnate");
            previousQueue = await GetReincarnatedQueue(previousQueue: previousQueue);
            pendingActions = await previousQueue.GetPending(activeActionIds);
        }

        var pendingActionIds = pendingActions.Value.Select(x => x.ActionId).ToHashSet();
        if (pendingActions.Value.Count > 0)
        {
            await newQueue.EnqueueBatch(pendingActions);
            await previousQueue.Dequeue(pendingActionIds); // TODO: ignore
        }
        
        return pendingActionIds;
    }
    
    async Task<IIndexingQueueService> GetReincarnatedQueue(IIndexingQueueService previousQueue)
    {
        var pk = previousQueue.GetPrimaryKeyString();
        var reincarnatedQueue = grainFactory.GetGrain<IIndexingQueueService>(pk);
        return reincarnatedQueue;
    }
    
    async Task OnDeactivate(CancellationToken ct)
    {
        // TODO: remove from active indexes
        return;
    }    
    
    Task OnSetupState(CancellationToken ct)
    {
        return Task.CompletedTask;
    }

    public async Task<Immutable<HashSet<Guid>>> GetActiveIndexingActionIds()
    {
        return WrappedState.ActiveIndexingActionIds.AsImmutable();
    }

    public Task RemoveFromActiveIndexingActions(HashSet<Guid> ids)
    {
        WrappedState.ActiveIndexingActionIds.RemoveWhere(ids.Contains);
        return Task.CompletedTask;
    }

    IIndexingQueueService GetOrCreateIndexingQueue(Type grainInterface) =>
        WrappedState.IndexingQueues.GetOrAddValue(grainInterface, () => MakeIndexingQueue(grainInterface));
    
    IIndexingQueueService MakeIndexingQueue(Type grainInterface) => 
        sp.GetRequiredService<IIndexingQueueServiceClient>().GetQueueByCallingGrain(grainContext.GrainId); // TODO: interface-based routing
    
    /// <summary>
    /// Adds index updates into the write-ahead queue, but does not wait for the updates to be applied.
    /// </summary>
    /// <param name="updates"></param>
    /// <returns>The generated indexing action ids.</returns>
    async Task<IReadOnlyList<Guid>> EnqueueIndexUpdates(IndexedPropertyUpdates updates)
    {
        var actionIds = new List<Guid>();
        await updates.UpdatesByGrain
            .Parallel(kvp =>
            {
                var grainInterface = kvp.Key;
                var updatesByIndex = kvp.Value;
                var queue = GetOrCreateIndexingQueue(grainInterface);
                var actionId = NewIndexingActionId();
                actionIds.Add(actionId);
                var action = new IndexingAction(Grain: IndexableGrain, InterfaceType: grainInterface, ActionId: actionId, UpdatesByIndex: updatesByIndex);
                return queue.Enqueue(action.AsImmutable());
            }, maxParallelism: options.EnqueueParallelism);
        return actionIds;
    }

    /// <summary>
    /// Applies tentative index updates eagerly and tentatively.
    /// </summary>
    /// <param name="updates"></param>
    /// <param name="updateApplicabilityMode"></param>
    async Task ApplyIndexUpdatesEagerly(IndexedPropertyUpdates updates, IndexUpdateApplicabilityMode updateApplicabilityMode = default)
    {
        await updates.UpdatesByGrain.Parallel(x => ApplyUpdate(updatesByIndexName: x.Value, indexInfos: IndexRegistry.GetIndexInfos(grainInterface: x.Key)));
        return;

        Task ApplyUpdate(Dictionary<string, IndexedPropertyUpdate> updatesByIndexName, IndexInfos indexInfos) => updatesByIndexName
            .Parallel(u =>
            {
                var indexName = u.Key;
                var indexInfo = indexInfos.ByIndexName[indexName];
                var tentativeUpdate = u.Value.WithTentativeVisibility().AsImmutable(); // TODO: always tentative?
                return indexInfo.Index.Update(grain: IndexableGrain, update: tentativeUpdate, metadata: indexInfo.Metadata);
            }, maxParallelism: options.EagerIndexUpdateParallelism);
    }
    
    Guid NewIndexingActionId() => Guid.NewGuid();    
}
