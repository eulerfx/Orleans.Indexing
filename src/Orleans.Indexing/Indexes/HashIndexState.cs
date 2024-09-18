#nullable enable
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Orleans.Runtime;

namespace Orleans.Indexing;

/// <summary>
/// The state of a hash index <see cref="IHashIndex{TKey,TGrain}"/> on a single-node.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
[GenerateSerializer]
public class HashIndexState<TKey, TGrain> : IndexState<TKey, TGrain> where TKey : notnull
{
    /// <summary>
    /// The index.
    /// </summary>
    [Id(5)]
    public readonly Dictionary<TKey, IndexEntry<TGrain>> Index = [];
    
    public override IDictionary<TKey, IndexEntry<TGrain>> Dictionary => Index;
    
}

public readonly struct HashIndexStateUpdateResultType(bool IsSuccess, bool FixIndexUnavailableOnDelete)
{
    public bool IsSuccess { get; } = IsSuccess;
    
    public bool FixIndexUnavailableOnDelete { get; } = FixIndexUnavailableOnDelete;
}
