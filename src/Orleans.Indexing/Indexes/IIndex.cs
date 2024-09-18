#nullable enable
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Streams;

namespace Orleans.Indexing;

/// <summary>
/// An index.
/// </summary>
/// <remarks>
/// Indexes are hosted by grains.
/// Partitioned indexes are not actually grains themselves since they are instantiated directly, but they act as clients to bucket grains.
/// </remarks>
[Unordered]
public interface IIndex
{
    /// <summary>
    /// Gets the current status of the index.
    /// </summary>
    /// <returns></returns>
    [ReadOnly]
    [AlwaysInterleave]
    Task<IndexStatus> GetStatus();
    
    /// <summary>
    /// Updates the index.
    /// </summary>
    /// <param name="grain">The indexable grain being indexed.</param>
    /// <param name="update">The index update to apply.</param>
    /// <param name="metadata">Index metadata.</param>
    /// <returns></returns>
    [AlwaysInterleave]
    [Transaction(TransactionOption.Supported)]
    Task<bool> Update(IIndexableGrain grain, Immutable<IndexedPropertyUpdate> update, IndexMetadata metadata);
    
    /// <summary>
    /// Updates the index.
    /// </summary>
    /// <param name="updates"></param>
    /// <param name="metadata"></param>
    /// <returns></returns>
    [AlwaysInterleave]
    [Transaction(TransactionOption.Supported)]
    Task<bool> UpdateBatch(Immutable<IReadOnlyDictionary<IIndexableGrain, IReadOnlyList<IndexedPropertyUpdate>>> updates, IndexMetadata metadata) => 
        updates.Value
            .Parallel(ups => ups.Value
                .Select(x => this.Update(ups.Key, new Immutable<IndexedPropertyUpdate>(x), metadata))
                .Parallel()
                .Then(xs => xs.All(x => x)), 1)
            .Then(xs => xs.All(x => x));

    /// <summary>
    /// Performs a lookup on the index.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="page"></param>
    /// <returns></returns>
    [ReadOnly]
    [AlwaysInterleave]
    [Transaction(TransactionOption.Supported)]
    Task<IReadOnlyList<IIndexableGrain>> LookupByKey(object? key, PageInfo page);
    
    /// <summary>
    /// Clears the index.
    /// </summary>
    /// <returns></returns>
    [AlwaysInterleave]
    Task Dispose();
}

/// <summary>
/// An index, to be hosted by a grain.
/// </summary>
/// <typeparam name="TKey">The index key type.</typeparam>
/// <typeparam name="TGrain">The indexable grain type.</typeparam>
public interface IIndex<TKey, TGrain> : IIndex
{
    /// <summary>
    /// Performs a lookup on the index.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="page"></param>
    /// <returns>The collection of grains with that key.</returns>
    [ReadOnly]
    [AlwaysInterleave]
    [Transaction(TransactionOption.Supported)]
    Task<IReadOnlyList<TGrain>> LookupByKey(TKey key, PageInfo page);
}

/// <summary>
/// A hash index.
/// </summary>
/// <typeparam name="TKey">The hash index key type.</typeparam>
/// <typeparam name="TGrain">The indexable grain type.</typeparam>
public interface IHashIndex<TKey, TGrain> : IIndex<TKey, TGrain>
{
    /// <summary>
    /// Performs a lookup of a unique value on the index.
    /// </summary>
    /// <param name="key"></param>
    /// <returns>The unique grain with the given key.</returns>
    [ReadOnly]
    [AlwaysInterleave]
    [Transaction(TransactionOption.Supported)]
    Task<TGrain?> LookupUniqueByKey(TKey key);
}


/// <summary>
/// A marker interface for a grain that hosts an index.
/// </summary>
public interface IIndexGrain : 
    IIndex, 
    IGrainWithStringKey {}

/// <summary>
/// A marker interface for a grain that hosts an index.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public interface IIndexGrain<TKey, TGrain> : 
    IIndex<TKey, TGrain>,
    IIndexGrain {}


/// <summary>
/// A marker interface for a single-node index grain.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public interface ISingleNodeIndexGrain<TKey, TGrain> : 
    IIndexGrain<TKey, TGrain> {}

/// <summary>
/// A marker interface for a grain that hosts a hash index.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public interface IHashIndexGrain<TKey, TGrain> : 
    IIndexGrain<TKey, TGrain>, 
    IHashIndex<TKey, TGrain> {}

/// <summary>
/// A marker interface for a grain that hosts a single-node hash index.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public interface IHashIndexSingleNode<TKey, TGrain> : 
    IHashIndexGrain<TKey, TGrain>,
    ISingleNodeIndexGrain<TKey, TGrain>
    where TKey : notnull {}

/// <summary>
/// A marker interface for a hash-index partitioned by key.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public interface IHashIndexPartitionedByKey<TKey, TGrain> : 
    IHashIndexGrain<TKey, TGrain>
    where TKey : notnull {}

/// <summary>
/// A sorted index supporting range queries.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public interface ISortedIndex<TKey, TGrain> : IIndex<TKey, TGrain>
{
    /// <summary>
    /// Performs a lookup on the index by range.
    /// </summary>
    /// <param name="start"></param>
    /// <param name="end"></param>
    /// <param name="page"></param>
    /// <returns>The collection of grains within the given range.</returns>
    [ReadOnly]
    [AlwaysInterleave]
    [Transaction(TransactionOption.Supported)]
    Task<IReadOnlyList<TGrain>> LookupRange(TKey? start, TKey? end, [Immutable] PageInfo page);

    /// <summary>
    /// Gets the overlap with the given range.
    /// </summary>
    /// <param name="start"></param>
    /// <param name="end"></param>
    /// <returns></returns>
    [ReadOnly]
    [AlwaysInterleave]
    [Transaction(TransactionOption.Supported)]
    Task<RangeOverlapType> GetRangeOverlap(TKey start, TKey end);
}

/// <summary>
/// Marker interface for a grain that hosts a sorted index.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public interface ISortedIndexGrain<TKey, TGrain> : 
    ISortedIndex<TKey, TGrain>, 
    IIndexGrain<TKey, TGrain>  {}

/// <summary>
/// Marker interface for a grain hosting a single node of a sorted index.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public interface ISortedIndexSingleNode<TKey, TGrain> : 
    ISortedIndexGrain<TKey, TGrain>,
    ISingleNodeIndexGrain<TKey, TGrain>
    where TKey : notnull {} 

/// <summary>
/// Marker interface for a grain hosting an index partitioned by key, wherein lookups and updates are forwarded based on hash.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public interface ISortedIndexPartitionedByKey<TKey, TGrain> : 
    ISortedIndexGrain<TKey, TGrain> {}

/// <summary>
/// The status of an index.
/// </summary>
public enum IndexStatus
{
    UnderConstruction,
    Available,
    Disposed
}

public enum IndexUpdateReason
{
    /// <summary>
    /// The index grain is activating.
    /// </summary>
    OnActivate,
    
    /// <summary>
    /// The index grain is deactivating.
    /// </summary>
    OnDeactivate,
    
    /// <summary>
    /// Indexed state is being written.
    /// </summary>
    WriteState,
}
