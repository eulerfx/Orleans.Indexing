#nullable enable
using System;
using System.Threading.Tasks;

namespace Orleans.Indexing;

/// <summary>
/// A single-node <see cref="IHashIndex{TKey,TGrain}"/> storing <see cref="HashIndexState{TKey,TGrain}"/>.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public class HashIndex<TKey, TGrain> : 
    SingleNodeIndexGrain<TKey, TGrain, HashIndexState<TKey, TGrain>, IHashIndexSingleNode<TKey, TGrain>>, 
    IHashIndexSingleNode<TKey, TGrain>
    where TGrain : IIndexableGrain 
    where TKey : notnull
{
    public Task<TGrain?> LookupUniqueByKey(TKey key)
    {
        throw new NotImplementedException();
    }
}
