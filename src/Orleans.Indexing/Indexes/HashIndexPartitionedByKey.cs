#nullable enable
using System;
using System.Threading.Tasks;
using Orleans.Streams;

namespace Orleans.Indexing;

/// <summary>
/// A hash index partitioned by key with respect to <see cref="IndexingSystemOptions.DefaultMaxHashIndexPartitions"/> implementing <see cref="IHashIndexPartitionedByKey{TKey,TGrain}"/>. 
/// Acts as a client into the individual partition grains of type <see cref="IHashIndexSingleNode{TKey,TGrain}"/>.
/// </summary>
/// <param name="grainFactory"></param>
/// <param name="options"></param>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public class HashIndexPartitionedByKey<TKey, TGrain>(
    IGrainFactory grainFactory, 
    IServiceProvider sp,
    IHashIndexPartitionScheme partitionScheme, 
    PartitionedIndexOptions options
    ) : PartitionedIndexGrainClient<TKey, TGrain, IHashIndexSingleNode<TKey, TGrain>>(grainFactory, sp, partitionScheme, options), 
        IHashIndexPartitionedByKey<TKey, TGrain> 
        where TKey : notnull
        where TGrain : IIndexableGrain
{}

