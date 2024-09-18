using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Streams;
#nullable enable

namespace Orleans.Indexing;

/// <summary>
/// An implementation of <see cref="ISortedIndexPartitionedByKey{TKey, TGrain}"/>.
/// </summary>
/// <param name="partitionScheme"></param>
/// <param name="options"></param>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public class SortedIndexPartitionedByKey<TKey, TGrain>(
    IGrainFactory grainFactory, 
    IServiceProvider sp,
    ISortedIndexPartitionScheme partitionScheme, 
    PartitionedIndexOptions options
    ) : PartitionedIndexGrainClient<TKey, TGrain, ISortedIndexSingleNode<TKey, TGrain>>(grainFactory, sp, partitionScheme, options), 
        ISortedIndexPartitionedByKey<TKey, TGrain> 
        where TKey : notnull 
        where TGrain : IIndexableGrain
{
    public async Task<IReadOnlyList<TGrain>> LookupRange(TKey? start, TKey? end, PageInfo page)
    {
        ArgumentNullException.ThrowIfNull(start, nameof(start));
        ArgumentNullException.ThrowIfNull(end, nameof(end));
        
        var partitions = SortedPartitionScheme.GetPartitionsByRange(start, end);
        var results = new List<TGrain>();
        foreach (var partition in partitions)
        {
            var bucket = GetBucket(partition);
            var overlap = await bucket.GetRangeOverlap(start, end);
            if (overlap.HasOverlap())
            {
                var res = await bucket.LookupRange(start, end, page);
                results.AddRange(res);
                if (results.Count >= page.Size)
                    break;
                if (overlap is RangeOverlapType.PartialLessThan)
                    break;
                if (overlap is RangeOverlapType.Superset)
                    break;
            }
            else if (overlap is RangeOverlapType.LessThan)
            {
                break;
            }
        }

        return results;
    }

    public Task<RangeOverlapType> GetRangeOverlap(TKey? start, TKey? end)
    {
        throw new NotImplementedException();
    }
}
