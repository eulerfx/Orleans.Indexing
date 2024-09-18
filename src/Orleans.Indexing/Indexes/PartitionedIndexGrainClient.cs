#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Concurrency;

namespace Orleans.Indexing;

public class PartitionedIndexOptions
{
    public required string IndexName { get; init; }
    
    public required string? PartitionSchemeName { get; init; }

    public const string DefaultHashIndexPartitionSchemeName = "DefaultHashIndexPartitionSchemeName";
    
    public const string DefaultSortedIndexPartitionSchemeName = "DefaultSortedIndexPartitionSchemeName";
}

/// <summary>
/// Base class for index grain clients that partition an index across single-node index <typeparamref name="TBucket"/> grains.
/// </summary>
/// <param name="grainFactory"></param>
/// <param name="partitionScheme"></param>
/// <param name="options"></param>
/// <typeparam name="TBucket"></typeparam>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public abstract class PartitionedIndexGrainClient<TKey, TGrain, TBucket>(
    IGrainFactory grainFactory, 
    IServiceProvider sp,
    IIndexPartitionScheme partitionScheme, 
    PartitionedIndexOptions options
    ) : 
    IIndex<TKey, TGrain> 
    where TBucket : IIndexGrain
    where TGrain : IIndexableGrain
{
    protected IGrainFactory GrainFactory => grainFactory;
    
    protected string IndexName => options.IndexName;

    protected IIndexPartitionScheme PartitionScheme => partitionScheme;
    
    protected ISortedIndexPartitionScheme SortedPartitionScheme => PartitionScheme as ISortedIndexPartitionScheme 
        ?? throw new InvalidOperationException($"Partition scheme '{PartitionScheme}' is not a sorted index partition scheme!");
    
    public Task<IndexStatus> GetStatus() => IndexStatus.Available.ToCompletedTask();
    
    public Task Dispose()
    {
        return Task.CompletedTask;
    }
    
    public Task<IIndexableGrain?> LookupUniqueByKey(object? key)
    {
        throw new System.NotImplementedException();
    }    

    public Task<TGrain?> LookupUniqueByKey(TKey key) => LookupUniqueByKey((object?)key).Then(x => (TGrain?)x);

    public Task<IReadOnlyList<IIndexableGrain>> LookupByKey(object? key, PageInfo page) =>
        GetBucketByKey(key).LookupByKey(key, page);

    public Task<IReadOnlyList<TGrain>> LookupByKey(TKey key, PageInfo page) => 
        LookupByKey((object?)key, page).Then(x => x.Cast<TGrain>().ToReadOnlyList());    

    /// <summary>
    /// Updates the index by directing updates to all applicable bucket grains.
    /// </summary>
    /// <param name="grain"></param>
    /// <param name="update"></param>
    /// <param name="metadata"></param>
    /// <returns></returns>
    public virtual async Task<bool> Update(IIndexableGrain grain, Immutable<IndexedPropertyUpdate> update, IndexMetadata metadata)
    {
        var up = update.Value;
        
        if (up.CrudType is IndexUpdateCrudType.Update)
        {
            var beforeValueHash = GetPartitionByKey(up.BeforeValue);
            var afterValueHash = GetPartitionByKey(up.AfterValue);
            var beforeBucket = GetBucket(beforeValueHash);
            if (beforeValueHash == afterValueHash)
            {
                return await beforeBucket.Update(grain, update, metadata);
            }
            var afterBucket = GetBucket(afterValueHash);
            var (res1, res2) = await TaskHelper.Parallel(
                beforeBucket.Update(grain, up.WithCrudType(IndexUpdateCrudType.Delete).AsImmutable(), metadata),
                afterBucket.Update(grain, up.WithCrudType(IndexUpdateCrudType.Insert).AsImmutable(), metadata)
            );
            return res1 && res2;
        }
        
        if (up.CrudType is IndexUpdateCrudType.Insert)
        {
            var afterBucket = GetBucketByKey(up.AfterValue);
            return await afterBucket.Update(grain, up.AsImmutable(), metadata);
        }
        
        if (up.CrudType is IndexUpdateCrudType.Delete)
        {
            var beforeBucket = GetBucketByKey(up.BeforeValue);
            return await beforeBucket.Update(grain, up.AsImmutable(), metadata);
        }

        return false;
    }
    
    /// <summary>
    /// Gets the hash-code of the partition associated to the given property value.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    protected string GetPartitionByKey(object? key) => PartitionScheme.GetPartitionByKey(key);

    /// <summary>
    /// Gets a grain by computing a primary key based on the index name, the partition interface and the given hash-code.
    /// </summary>
    /// <param name="hashCode"></param>
    /// <returns></returns>
    protected TBucket GetBucket(string hashCode)
    {
        var pk = IndexingHelper.GetIndexGrainBucketPrimaryKeyByHashCode(grainInterfaceType: typeof(TBucket), indexName: IndexName, hashCode: hashCode);
        return GrainFactory.GetGrain<TBucket>(pk);
    }
    
    /// <summary>
    /// Gets a grain whose primary key is based on the type of partition, the index name and a hash code of the given value.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    protected TBucket GetBucketByKey(object? key) => GetBucket(hashCode: GetPartitionByKey(key: key));
}
