#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Indexing;

/// <summary>
/// Index information stored in the <see cref="IndexRegistry"/>.
/// </summary>
public class IndexInfo
{
    /// <summary>
    /// The index grain.
    /// </summary>
    public required IIndex Index { get; init; }
    
    /// <summary>
    /// Index metadata.
    /// </summary>
    public required IndexMetadata Metadata { get; init; }
    
    /// <summary>
    /// Generates updates to the indexed property (which are then enqueued to be applied).
    /// </summary>
    public required IndexUpdateGenerator UpdateGenerator { get; init; }
}

/// <summary>
/// Index information for an indexed property class stored in the <see cref="IndexRegistry"/>.
/// </summary>
/// <param name="indexedStateClass"></param>
public class IndexInfos(Type indexedStateClass)
{
    /// <summary>
    /// The type of class containing indexed properties.
    /// </summary>
    public Type IndexedStateClass => indexedStateClass;

    /// <summary>
    /// Indexes by name.
    /// </summary>
    public Dictionary<string, IndexInfo> ByIndexName { get; } = [];
    
    /// <summary>
    /// Indicates whether there's an eager index.
    /// </summary>
    public bool HasEagerIndex => ByIndexName.Values.Any(x => x.Metadata.IsEager);
    
    /// <summary>
    /// Indicates whether there's a unique index.
    /// </summary>
    public bool HasUniqueIndex => ByIndexName.Values.Any(x => x.Metadata.IsUnique);
}

/// <summary>
/// Index metadata stored in the <see cref="IndexRegistry"/>.
/// </summary>
[GenerateSerializer]
public class IndexMetadata
{
    /// <summary>
    /// The type of <see cref="IIndex"/>.
    /// </summary>
    [Id(0)] 
    public required Type IndexType { get; init; }
    
    /// <summary>
    /// The name of the index.
    /// </summary>
    [Id(1)]
    public required string IndexName { get; init; }
    
    /// <summary>
    /// Indicates whether the index has a uniqueness constraint.
    /// </summary>
    [Id(2)]
    public required bool IsUnique { get; init; }
        
    /// <summary>
    /// Indicates whether the index should be updated eagerly.
    /// </summary>
    [Id(3)]
    public required bool IsEager { get; init; }
    
    /// <summary>
    /// Indicates whether the index supports bucket chaining, which spreads the data across multiple buckets forming a chain via <see cref="IIndexState{TKey,TGrain}.NextBucket"/>
    /// </summary>
    [Id(4)]
    public bool IsChainedBuckets { get; init; }
        
    /// <summary>
    /// The maximum size of a bucket, after which it is chained.
    /// </summary>
    [Id(5)]
    public required int MaxBucketSize { get; init; }

    /// <summary>
    /// Determines whether a new bucket should be added to the bucket chain.
    /// </summary>
    /// <param name="indexSize"></param>
    /// <returns></returns>
    public bool ShouldCreateNewBucketInChain(int indexSize) => IsChainedBuckets && indexSize >= MaxBucketSize;
}
