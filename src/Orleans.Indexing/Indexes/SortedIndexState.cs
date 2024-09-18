using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Streams;

#nullable enable

namespace Orleans.Indexing;

/// <summary>
/// The state of a range index on a single node.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
[GenerateSerializer]
public class SortedIndexState<TKey, TGrain> : IndexState<TKey, TGrain> where TKey : notnull
{
    public override IDictionary<TKey, IndexEntry<TGrain>> Dictionary => Index;
    
    [Id(5)]
    public SortedList<TKey, IndexEntry<TGrain>> Index { get; } = [];

    public RangeOverlapType GetRangeOverlap(TKey start, TKey end) => 
        Index.GetRangeOverlap(start, end);
    
    /// <summary>
    /// Gets items in the given key range.
    /// </summary>
    /// <param name="start"></param>
    /// <param name="end"></param>
    /// <param name="page"></param>
    /// <returns></returns>
    public IReadOnlyList<TGrain> GetByRange(TKey start, TKey end, PageInfo page)
    {
        var res = new List<IndexEntry<TGrain>>();
        GetByRange(start, end, page, res);
        return res.SelectMany(x => x.Values).ToReadOnlyList();
    }

    public int GetByRange(TKey start, TKey end, PageInfo page, ICollection<IndexEntry<TGrain>> result) => 
        Index.GetValuesInRange(start, end, offset: page.Offset, size: page.Size, result: result);
}


