using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Streams;
#nullable enable

namespace Orleans.Indexing;

/// <summary>
/// An implementation of <see cref="ISortedIndexSingleNode{TKey,TGrain}"/>.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public class SortedIndex<TKey, TGrain> : 
    SingleNodeIndexGrain<TKey, TGrain, SortedIndexState<TKey, TGrain>, ISortedIndexSingleNode<TKey, TGrain>>, 
    ISortedIndexSingleNode<TKey, TGrain>
    where TGrain : IIndexableGrain
    where TKey : notnull
{
    public async Task<IReadOnlyList<TGrain>> LookupRange(TKey? start, TKey? end, PageInfo page)
    {
        ArgumentNullException.ThrowIfNull(start, nameof(start));
        ArgumentNullException.ThrowIfNull(end, nameof(end));
        ISortedIndexSingleNode<TKey, TGrain>? nextBucket = null;
        
        var res = await State.PerformRead(s =>
        {
            if (s.NextBucket is not null)                
                nextBucket = GetNextBucket(); 
            return s.GetByRange(start, end, page);
        });

        if (res?.Count > 0)
            return res;
        
        if (nextBucket is not null)
            return await nextBucket.LookupRange(start, end, page);
        
        return Array.Empty<TGrain>();
    }

    public Task<RangeOverlapType> GetRangeOverlap(TKey start, TKey end) =>
        State.PerformRead(s => s.GetRangeOverlap(start, end));
}
