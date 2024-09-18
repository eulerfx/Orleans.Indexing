using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Storage;

#nullable enable

namespace Orleans.Indexing;

/// <summary>
/// A service hosting and processing an indexing log associated to an indexable grain interface type.
/// </summary>
/// <param name="id"></param>
/// <param name="sp"></param>
/// <param name="loggerFactory"></param>
[Reentrant]
public class IndexingQueueService(
    GrainId id,
    IServiceProvider sp,
    Silo silo,
    ILoggerFactory loggerFactory,
    IOptions<IndexingSystemOptions> options
    ) : GrainService(id, silo, loggerFactory), IIndexingQueueService
{
    readonly ILogger<IndexingQueueService> Log = loggerFactory.CreateLogger<IndexingQueueService>();
    readonly CancellationTokenSource cts = new();
    readonly Debounce debounce = new();
    readonly string stateName = $"IndexingQueueService_{id}";
    readonly GrainState<IndexingQueue> state = new(new IndexingQueue());
    readonly Channel<IReadOnlyList<IndexingAction>> ch = Channel.CreateBounded<IReadOnlyList<IndexingAction>>(capacity: options.Value.IndexingQueueInputBufferSize);
    IGrainStorage? storage;
    Task? queueProcess;
    
    IndexingQueue IndexingQueue => state.State;
    
    IndexManager? indexManager;
    IndexManager IndexManager => indexManager ??= sp.GetRequiredService<IndexManager>();    
    
    public override async Task Init(IServiceProvider serviceProvider)
    {
        Log.LogInformation("initialize_indexing_queue_service/started");
        var actions = IndexingQueue.GetPendingActions();
        if (actions.Count > 0)
        {
            Log.LogInformation("initialize_indexing_queue_service/resuming items={0}", actions.Count);
            await this.ch.Writer.WriteAsync(actions);
        }
        this.queueProcess = ProcessQueue(cts.Token);
    }

    public async Task Enqueue(Immutable<IndexingAction> action)
    {
        await this.ch.Writer.WriteAsync([action.Value]);
        await Persist(() => IndexingQueue.AddAction(action.Value));
    }
    
    public async Task EnqueueBatch(Immutable<IReadOnlyList<IndexingAction>> items)
    {
        await this.ch.Writer.WriteAsync(items.Value);
        await Persist(() => IndexingQueue.AddActions(items.Value));
    }
    
    async Task Persist(Action? action = default)
    {
        await this.debounce.Run(action, async () =>
        {
            if (this.storage is null)
            {
                Log.LogInformation("initialize_grain_storage/started provider={0} state={1}", options.Value.IndexingQueueStorageProviderName, this.stateName);
                this.storage = ActivationServices.GetRequiredKeyedService<IGrainStorage>(serviceKey: options.Value.IndexingQueueStorageProviderName);
                await this.storage.ReadStateAsync(stateName: this.stateName, GrainReference.GrainId, this.state);
                Log.LogInformation("initialize_grain_storage/completed provider={0} state={1}", options.Value.IndexingQueueStorageProviderName, this.stateName);
            }
            
            var etag = this.state.ETag;
            this.state.ETag = "*";
            try
            {
                await this.storage.WriteStateAsync(this.stateName, GrainReference.GrainId, this.state);
            }
            finally
            {
                if (this.state.ETag == "*")
                    this.state.ETag = etag;
            }
        });
    }
    
    class Debounce
    {
        readonly AsyncLock mutex = new();
        readonly HashSet<long> pendingWriteIds = [];
        long writeId;        
        
        /// <summary>
        /// Runs the given prepareAction under an async mutex and the given commit action, but debouncing to skip overlapping write requests.
        /// </summary>
        /// <param name="prepareAction">The prepare action to run under a mutex, if any.</param>
        /// <param name="commitAction">The commit action to run debounced.</param>
        public async Task Run(Action? prepareAction, Func<Task> commitAction)
        {
            var wid = Interlocked.Increment(ref writeId);
            pendingWriteIds.Add(wid);
            using (await mutex.LockAsync())
            {
                prepareAction?.Invoke();
                if (pendingWriteIds.Contains(wid))
                {
                    pendingWriteIds.Clear();
                    await commitAction();
                }
            }
        }
    }
    
    /// <summary>
    /// Processes the indexing write-ahead queue in a loop: <br/>
    /// 1. Buffers a batch of index updates from the channel. <br/>
    /// 2. Applies the index updates. <br/>
    /// 3. Clears the applied updates from the active indexing actions and the queue. <br/>
    /// </summary>
    /// <param name="ct"></param>
    async Task ProcessQueue(CancellationToken ct)
    {
        Log.LogInformation("process_indexing_queue/started grain_id={0}", GrainId);
        try
        {
            await foreach (var actionBatches in this.ch.Reader.ReadBufferByCountAndTime(count: options.Value.IndexingQueueOutputBufferSize, time: options.Value.IndexingQueueOutputBufferTimeOut, ct))
            {
                var actionBatch = actionBatches.FlattenToReadOnlyList();
                var activeActionsByGrain = await GetActiveIndexingActionsByGrains(actionBatch);
                var updatesByIndex = GetIndexUpdates(actionBatch, activeActionsByGrain);
                await ApplyIndexUpdates(updatesByIndex);
                await RemoveActiveIndexingActions(activeActionsByGrain);
                await Persist(() => IndexingQueue.DequeueActions(actionBatch));
            }
        }
        catch (Exception ex)
        {
            Log.LogCritical(ex, "process_indexing_queue/error grain_id={0}", GrainId);
            await this.DeactivateAsync(new DeactivationReason(DeactivationReasonCode.InternalFailure, ex, "processing_indexing_queue/error"), ct);
        }
    }

    /// <summary>
    /// Removes all active indexing actions via <see cref="IIndexableGrain.RemoveFromActiveIndexingActions"/>.
    /// </summary>
    /// <param name="activeIndexingActions"></param>
    /// <returns></returns>
    static Task RemoveActiveIndexingActions(Dictionary<IIndexableGrain, HashSet<Guid>> activeIndexingActions) =>
        activeIndexingActions.Parallel(x => x.Key.RemoveFromActiveIndexingActions(x.Value));    
    
    async Task<Dictionary<IIndexableGrain, HashSet<Guid>>> GetActiveIndexingActionsByGrains(IEnumerable<IndexingAction> actionBatch)
    {
        var activeActionTasksByGrain = new Dictionary<IIndexableGrain, Task<Immutable<HashSet<Guid>>>>();
        var currentActionIds = new HashSet<Guid>();
        foreach (var action in actionBatch)
        {
            currentActionIds.Add(action.ActionId);
            if (!activeActionTasksByGrain.ContainsKey(action.Grain))
            {
                activeActionTasksByGrain[action.Grain] = action.Grain.AsReference<IIndexableGrain>().GetActiveIndexingActionIds(); 
            }
        }
        await Task.WhenAll(activeActionTasksByGrain.Values);
        var res = activeActionTasksByGrain.ToDictionary(kvp => kvp.Key, kvp => new HashSet<Guid>(kvp.Value.Result.Value.Intersect(currentActionIds)));
        return res;
    }
     
    /// <summary>
    /// Gets the index updates in the given indexing actions with respect to the given set of active indexing actions.
    /// </summary>
    /// <param name="actions"></param>
    /// <param name="activeActionsByGrain"></param>
    /// <exception cref="InvalidOperationException">Unable to find index information for an indexable grain.</exception>
    /// <returns></returns>
    Dictionary<string, List<(IIndexableGrain Grain, IndexInfos Indexes, List<IndexedPropertyUpdate> Updates)>> GetIndexUpdates(IEnumerable<IndexingAction> actions, Dictionary<IIndexableGrain, HashSet<Guid>> activeActionsByGrain)
    {
        var updates = new Dictionary<string, List<(IIndexableGrain Grain, IndexInfos Indexes, List<IndexedPropertyUpdate> Updates)>>();
        foreach (var action in actions)
        {
            var indexInfos = IndexManager.Registry.GetIndexInfos(grainInterface: action.InterfaceType);
            var isActiveAction = activeActionsByGrain.TryGetValue(action.Grain)?.Contains(action.ActionId) is true;
            foreach (var (indexName, up) in action.GetUpdates())
            {
                var updatesByGrain = updates.GetOrAddValue(indexName, () => []);
                var ups = new List<IndexedPropertyUpdate>();
                if (isActiveAction)
                {
                    ups.Add(up);
                }
                else if (indexInfos.ByIndexName[indexName].Metadata.IsUnique)
                {
                    ups.Add(up.ReversedCrud());
                }
                updatesByGrain.Add((action.Grain, indexInfos, ups));
            }
        }
        return updates;
    }
    
    /// <summary>
    /// Applies the updates to the indexes.
    /// </summary>
    /// <param name="updatesByIndex"></param>
    /// <returns></returns>
    Task ApplyIndexUpdates(Dictionary<string, List<(IIndexableGrain Grain, IndexInfos Indexes, List<IndexedPropertyUpdate> Updates)>> updatesByIndex) =>
        updatesByIndex.Parallel(async kvp =>
        {
            var indexName = kvp.Key;
            var updates = kvp.Value;
            foreach (var (grain, indexInfos, ups) in updates)
            {
                var indexInfo = indexInfos.ByIndexName[indexName];
                var upsByGrain = new Dictionary<IIndexableGrain, IReadOnlyList<IndexedPropertyUpdate>> { { grain, ups } };
                await indexInfo.Index.UpdateBatch(
                    new Immutable<IReadOnlyDictionary<IIndexableGrain, IReadOnlyList<IndexedPropertyUpdate>>>(upsByGrain),
                    metadata: indexInfo.Metadata
                );
            }
        }, maxParallelism: options.Value.IndexUpdateParallelism);

    /// <summary>
    /// Gets all remaining and active indexing actions until punctuation.
    /// </summary>
    /// <param name="activeActionIds"></param>
    /// <returns></returns>
    public async Task<Immutable<IReadOnlyList<IndexingAction>>> GetPending(HashSet<Guid> activeActionIds) =>
        IndexingQueue.GetPendingActions(activeActionIds).AsImmutable();

    public Task Dequeue(HashSet<Guid> activeActionIds)
    {
        IndexingQueue.RemoveActions(activeActionIds);
        return Task.CompletedTask;
    }
}
