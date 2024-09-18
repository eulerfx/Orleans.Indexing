using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Concurrency;
using Orleans.Core;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Streams;
using Orleans.Transactions.Abstractions;
#nullable enable

namespace Orleans.Indexing;

/// <summary>
/// Base class for grains hosting a single bucket of an index.
/// This primary key of the grain will contain the index name, as well as the index type and a hash-code.
/// The bucket may 'spill over' to subsequent buckets via <see cref="IIndexState{TKey, TGrain}.NextBucket"/> chaining.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain">The type of indexable grain.</typeparam>
/// <typeparam name="TIndex">The type of index structure extending <see cref="IIndexState{TKey, TGrain}"/></typeparam>
/// <typeparam name="TBucket">The type of the index grain bucket (implemented by the superclass).</typeparam>
public abstract class SingleNodeIndexGrain<TKey, TGrain, TIndex, TBucket> : 
    Grain, 
    ISingleNodeIndexGrain<TKey, TGrain>
    where TGrain : IIndexableGrain
    where TBucket : IIndexGrain<TKey, TGrain>
    where TIndex : class, IIndexState<TKey, TGrain>, new()
{
    protected string IndexName => this.GetIndexNameFromIndexGrain();
    
    ITransactionalState<TIndex>? state;
    public ITransactionalState<TIndex> State => this.state.EnsureNotNull();
    
    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var storage = ServiceProvider.GetRequiredKeyedService<IGrainStorage>(serviceKey: IndexingConstants.IndexStorageProviderName);
        this.state = await NonTransactionalState<TIndex>.CreateAsync(new StateStorageBridge<TIndex>(name: "state", GrainContext, storage));
    }
    
    public virtual Task<IndexStatus> GetStatus() => Task.FromResult(IndexStatus.Available);

    /// <summary>
    /// Gets the next bucket associated to the given index grain.
    /// </summary>
    /// <returns></returns>
    protected TBucket GetNextBucket() =>
        GrainFactory.GetGrain<TBucket>(primaryKey: IndexingHelper.GetNextBucketIdInChain(this));
    
    /// <summary>
    /// Applies an update to the state of the grain index.
    /// </summary>
    /// <param name="indexableGrain"></param>
    /// <param name="update"></param>
    /// <param name="metadata"></param>
    /// <returns></returns>
    public async virtual Task<bool> Update(IIndexableGrain indexableGrain, Immutable<IndexedPropertyUpdate> update, IndexMetadata metadata)
    {
        IIndexGrain<TKey, TGrain>? nextBucket = null;
        
        var res = await State.PerformUpdate(s =>
        {
            var res = s.Update(indexableGrain, update.Value, metadata);
            if (!res.IsSuccess)
            {
                nextBucket = GetNextBucket();
                if (nextBucket is not null)
                    s.NextBucket = nextBucket.AsWeaklyTypedReference();
                if (res.FixIndexUnavailableOnDelete)
                {
                    // TODO: tombstone
                }
            }
            return res;
        });
        
        if (nextBucket is not null)
            return await nextBucket.Update(indexableGrain, update, metadata);
        
        return res.IsSuccess;
    }

    public virtual async Task Dispose()
    {
        await State.PerformUpdate(s => s.Dispose());
        this.DeactivateOnIdle();
    }
    
    public virtual Task<IReadOnlyList<IIndexableGrain>> LookupByKey(object? key, PageInfo page) =>
            LookupByKey((TKey?)key ?? throw new ArgumentException("Invalid key type!"), page).Then(x => x.Cast<IIndexableGrain>().ToReadOnlyList());
    
    public async Task<IReadOnlyList<TGrain>> LookupByKey(TKey key, PageInfo page)
    {
        IIndex<TKey, TGrain>? nextBucket = null;              
        
        var entry = await State.PerformRead(s =>
        {
            // if (s.IndexStatus is not IndexStatus.Available)
            // {
            //     throw new InvalidOperationException("The index is not currently available!");
            // }

            var value = s.TryGetValue(key);
            if (value is not null)
                return value;

            // if the key is not found and there's another bucket in the chain, move down the chain
            if (s.NextBucket is not null)
                nextBucket = GetNextBucket();
    
            return default;
        });
    
        if (entry is not null && !entry.IsTentative)
        {
            return entry.GetPage(page);
        }

        if (nextBucket is not null)
        {
            return await nextBucket.LookupByKey(key, page);
        }
        
        return Array.Empty<TGrain>();
    }
}
