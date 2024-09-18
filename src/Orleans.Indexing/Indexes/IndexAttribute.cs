#nullable enable
using System;

namespace Orleans.Indexing;

/// <summary>
/// An attribute that indicates that a property should be indexed.
/// </summary>
/// <param name="IndexType"></param>
/// <param name="IsUnique"></param>
/// <param name="IsEager"></param>
/// <param name="MaxBucketSize"></param>
/// <param name="PartitionSchemeName"></param>
[AttributeUsage(AttributeTargets.Property)]
public class IndexAttribute(Type IndexType, bool IsUnique = false, bool IsEager = false, int MaxBucketSize = 0, string? PartitionSchemeName = null) : Attribute
{
    /// <summary>
    /// The index type <see cref="IIndex{TKey,TGrain}"/>.
    /// </summary>
    public Type IndexType { get; } = IndexType;

    /// <summary>
    /// Does the index have a uniqueness constraint?
    /// </summary>
    public bool IsUnique { get; } = IsUnique;

    /// <summary>
    /// Should the index be updated eagerly or lazily?
    /// </summary>
    public bool IsEager { get; } = IsEager;
    
    /// <summary>
    /// The maximum number of items in a grain-level index bucket, after which a chain is formed via <see cref="IIndexState{TKey,TGrain}.NextBucket"/>.
    /// </summary>
    public int MaxBucketSize { get; } = MaxBucketSize;
    
    /// <summary>
    /// The name of the partition scheme <see cref="IIndexPartitionScheme"/> to use.
    /// </summary>
    public string? PartitionSchemeName { get; init; } = PartitionSchemeName;
}

/// <summary>
/// Indicates that a property should have a total hash index.
/// </summary>
/// <param name="IndexType"></param>
/// <param name="IsUnique"></param>
/// <param name="IsEager"></param>
/// <param name="MaxBucketSize"></param>
/// <param name="PartitionSchemeName"></param>
[AttributeUsage(AttributeTargets.Property)]
public sealed class HashIndexAttribute(HashIndexType IndexType = HashIndexType.PartitionedByKey, bool IsUnique = false, bool IsEager = false, int MaxBucketSize = 0, string PartitionSchemeName = PartitionedIndexOptions.DefaultHashIndexPartitionSchemeName) 
    : IndexAttribute(GetIndexType(IndexType), IsUnique: IsUnique, IsEager: IsEager, MaxBucketSize: MaxBucketSize, PartitionSchemeName: PartitionSchemeName)
{
    static Type GetIndexType(HashIndexType type) => type switch
    {
        HashIndexType.SingleNode => typeof(IHashIndexSingleNode<,>),
        
        // This uses the class, not an interface, because there is no underlying grain implementation for per-key indexes
        // themselves (unlike for their buckets).
        HashIndexType.PartitionedByKey => typeof(HashIndexPartitionedByKey<,>),
        
        _ => throw new ArgumentException("Unknown total index type!")
    };
}

/// <summary>
/// A type of total index.
/// </summary>
public enum HashIndexType
{
    /// <summary>
    /// Represents a hash-index that comprises a single bucket.<br/><br/>
    /// This type of index is not distributed and should be used with caution.
    /// The whole index should not have many entries, because it should be maintainable in a single grain on a single silo.
    /// </summary>
    SingleNode = 0,

    /// <summary>
    /// Represents a distributed hash-index, and each bucket maintains a single value for the hash of the key.
    /// </summary>
    PartitionedByKey = 1
}

/// <summary>
/// An attribute indicating that a property should have a sorted index that supports range queries.
/// </summary>
/// <param name="IsEager"></param>
/// <param name="MaxBucketSize"></param>
/// <param name="PartitionSchemeName"></param>
[AttributeUsage(AttributeTargets.Property)]
public sealed class SortedIndexAttribute(bool IsEager = false, int MaxBucketSize = 0, string PartitionSchemeName = PartitionedIndexOptions.DefaultSortedIndexPartitionSchemeName) 
    : IndexAttribute(typeof(SortedIndexPartitionedByKey<,>), IsEager: IsEager, MaxBucketSize: MaxBucketSize, PartitionSchemeName: PartitionSchemeName)
{
}
