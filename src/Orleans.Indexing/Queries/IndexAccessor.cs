using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Streams;
#nullable enable

namespace Orleans.Indexing;

/// <summary>
/// Provides access to query and update <see cref="IIndex"/>.
/// </summary>
public interface IIndexAccessor
{
    /// <summary>
    /// Gets an index associated to an indexable grain.
    /// </summary>
    /// <param name="grainInterface"></param>
    /// <param name="indexName">The name of the index.</param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">The index could not be found.</exception>
    IIndex GetIndex(Type grainInterface, string indexName);

    /// <summary>
    /// Gets an index associated to an indexable grain.
    /// </summary>
    /// <param name="grainInterface"></param>
    /// <param name="indexName"></param>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TGrain"></typeparam>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">The index could not be found.</exception>
    IIndex<TKey, TGrain> GetIndex<TKey, TGrain>(Type grainInterface, string indexName) where TGrain : IIndexableGrain;
    
    /// <summary>
    /// Gets an index associated to an indexable grain.
    /// </summary>
    /// <param name="grainInterface"></param>
    /// <param name="propertyName"></param>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TGrain"></typeparam>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">The index could not be found.</exception>
    IIndex<TKey, TGrain> GetIndexByProperty<TKey, TGrain>(Type grainInterface, string propertyName) where TGrain : IIndexableGrain;    
}

public class IndexAccessor(IndexManager indexManager) : IIndexAccessor
{
    public IIndex GetIndex(Type grainInterface, string indexName)
    {
        var indexInfos = indexManager.Registry.GetIndexInfos(grainInterface: grainInterface);
        var index = indexInfos.ByIndexName.TryGetValue(indexName) ?? throw new KeyNotFoundException($"No index named '{indexName}' was found.");
        return index.Index;
    }    

    public IIndex<TKey, TGrain> GetIndex<TKey, TGrain>(Type grainInterface, string indexName) where TGrain : IIndexableGrain => 
        (IIndex<TKey, TGrain>)GetIndex(grainInterface: grainInterface, indexName);
    
    public IIndex<TKey, TGrain> GetIndexByProperty<TKey, TGrain>(Type grainInterface, string propertyName) where TGrain : IIndexableGrain => 
        GetIndex<TKey, TGrain>(grainInterface, indexName: IndexingHelper.PropertyNameToIndexName(propertyName));   
}

public static class IndexAccessorExtensions
{
    public static ISortedIndex<TKey, TGrain> GetSortedIndexByProperty<TKey, TGrain>(this IIndexAccessor indexAccessor, Type grainInterface, string propertyName) where TGrain : IIndexableGrain => 
        (ISortedIndex<TKey, TGrain>)indexAccessor.GetIndexByProperty<TKey, TGrain>(grainInterface, propertyName: propertyName);
}

