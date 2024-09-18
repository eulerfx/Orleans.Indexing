#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Orleans.Indexing;

/// <summary>
/// In-memory index information of type <see cref="IndexInfos"/> by indexable grain interface type.
/// </summary>
public class IndexRegistry
{
    IndexRegistry() { }
    
    readonly Dictionary<Type, IndexInfos> indexesByIndexableGrainInterfaceType = [];

    public int IndexableGrainInterfaceCount => indexesByIndexableGrainInterfaceType.Count;

    /// <summary>
    /// Gets the indexes on the given indexable grain interface type.
    /// </summary>
    /// <param name="grainInterface"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public IndexInfos GetIndexInfos(Type grainInterface) =>
        indexesByIndexableGrainInterfaceType.TryGetValue(grainInterface) 
        ?? throw new InvalidOperationException($"Unable to find index information for grain interface '{grainInterface}'!");

    /// <summary>
    /// Gets the index on the given indexable grain interface type and with the given name.
    /// </summary>
    /// <param name="grainInterfaceType"></param>
    /// <param name="indexName"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public IndexInfo GetIndexInfo(Type grainInterfaceType, string indexName) => GetIndexInfos(grainInterfaceType).ByIndexName[indexName];

    /// <summary>
    /// Creates the index registry based on the given indexable grain interface types.
    /// </summary>
    /// <param name="indexManager"></param>
    /// <param name="grainInterfaceRegistry"></param>
    /// <exception cref="InvalidOperationException">Invalid index definition</exception>
    /// <returns></returns>
    public static IndexRegistry CreateIndexRegistry(IndexManager indexManager, IndexableGrainInterfaceRegistry grainInterfaceRegistry)
    {
        var registry = new IndexRegistry();
        foreach (var (grainInterface, stateClass) in grainInterfaceRegistry.GrainInterfacesWithStates)
        {
            var indexInfos = new IndexInfos(indexedStateClass: stateClass);
            foreach (var indexedProperty in stateClass.GetProperties())
            {
                var indexAttrs = indexedProperty.GetCustomAttributes<IndexAttribute>();
                var indexName = IndexingHelper.PropertyNameToIndexName(indexedProperty: indexedProperty.Name);
                if (indexInfos.ByIndexName.ContainsKey(indexName))
                    throw new InvalidOperationException($"An index named '{indexName}' already exists!");
                foreach (var indexAttr in indexAttrs)
                {
                    var indexType = indexAttr.IndexType;
                    if (indexType.IsGenericType)
                        indexType = indexType.MakeGenericType(typeArguments: [indexedProperty.PropertyType, grainInterface]);
                    indexInfos.ByIndexName[indexName] = indexManager.CreateIndex(indexType, indexName, indexedProperty, indexAttr);
                }
            }
            registry.indexesByIndexableGrainInterfaceType[grainInterface] = indexInfos;
        }
        return registry;
    }
}
