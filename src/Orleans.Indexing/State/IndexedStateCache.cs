using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using Orleans.Concurrency;

#nullable enable

namespace Orleans.Indexing;

/// <summary>
/// The grain index cache contains an in-memory cache (by grain type) of property indexes,
/// as well as the current state of the corresponding indexed state object.
/// </summary>
public class IndexedStateCache
{
    IndexedStateCache(IndexRegistry indexRegistry, IReadOnlyList<Type> indexableGrainInterfaces, object state)
    {
        this.state = state;
        this.indexCachesByGrainInterface = indexableGrainInterfaces.ToDictionary(x => x, x => new IndexInfosCache(this, indexRegistry.GetIndexInfos(x)));        
        indexCachesByGrainInterface.ForEach(kvp => kvp.Value.Initialize());
    }

    readonly Dictionary<Type, IndexInfosCache> indexCachesByGrainInterface;
    object state;

    public object State => this.state;
    
    /// <summary>
    /// Creates an instance of <see cref="IndexedStateCache"/> associated to the given indexable grain interface types <see cref="IIndexableGrain{TState}"/>.
    /// </summary>
    /// <param name="indexRegistry"></param>
    /// <param name="indexableGrainInterfaces"></param>
    /// <param name="state"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static IndexedStateCache Create(IndexRegistry indexRegistry, IReadOnlyList<Type> indexableGrainInterfaces, object state)
    {
        var cache = new IndexedStateCache(indexRegistry, indexableGrainInterfaces, state);
        if (cache.indexCachesByGrainInterface.Count == 0)
            throw new ArgumentException("No interfaces registered!");
        return cache;
    }

    /// <summary>
    /// Determines whether the store manages a index caches for the given indexable grain interface type.
    /// </summary>
    /// <param name="indexableGrainInterface"></param>
    /// <returns></returns>
    public bool ContainsIndexableGrainInterface(Type indexableGrainInterface) => indexCachesByGrainInterface.ContainsKey(indexableGrainInterface);    
    
    /// <summary>
    /// Indicates whether any index is a unique index.
    /// </summary>
    public bool HasUniqueIndex => indexCachesByGrainInterface.Values.Any(x => x.IndexInfos.HasUniqueIndex);

    /// <summary>
    /// Creates property updates based on the current state of the state object and before values.
    /// </summary>
    /// <param name="reason"></param>
    /// <param name="s"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public IndexedPropertyUpdates PrepareUpdates(IndexUpdateReason reason, object s)
    {
        if (indexCachesByGrainInterface.Count == 0)
            throw new InvalidOperationException("No grain interfaces have been registered!");
     
        CaptureState(s);
        
        var uniqueIndexCount = 0;
        var onlyUniqueIndexes = true;
        
        var ups = indexCachesByGrainInterface.ToDictionary(x => x.Key, x =>
        {
            var ups = x.Value.GetUpdates(reason);
            uniqueIndexCount += ups.UniqueIndexCount;
            onlyUniqueIndexes = onlyUniqueIndexes && ups.OnlyUniqueIndexes;
            return ups.UpdatesByIndexName;
        });
        
        return new(
            reason: reason,
            updatesByGrain: ups,
            uniqueIndexCount: uniqueIndexCount,
            onlyUniqueIndexes: onlyUniqueIndexes
            );
    }
    
    /// <summary>
    /// Maps property values from the given grain state object to the underlying <see cref="IndexInfosCache"/>.<see cref="IndexInfosCache.State"/>.
    /// </summary>
    /// <param name="s"></param>
    /// <exception cref="InvalidOperationException"></exception>
    [MemberNotNull(nameof(state))] 
    void CaptureState(object? s)
    {
        ArgumentNullException.ThrowIfNull(s);
        if (this.state is not null && !this.state.GetType().IsInstanceOfType(s))
        {
            throw new InvalidOperationException($"Can't capture state of type {this.state.GetType()} from an incompatible type {s.GetType()}!");
        }
        this.state = s;
    }
    
    /// <summary>
    /// Updates the before values of all indexed properties on all indexable grain interfaces managed by this cache.
    /// </summary>
    /// <param name="updates"></param>
    public void CommitUpdates(IndexedPropertyUpdates updates)
    {
        updates.UpdatesByGrain.ForEach(kvp => indexCachesByGrainInterface[kvp.Key].CommitUpdates(kvp.Value));
    }
    
    /// <summary>
    /// An in-memory cache of <see cref="IndexInfos"/> along with an instance of the corresponding properties object in <see cref="State"/> as
    /// well as prior property values in <see cref="CommittedByIndexName"/>.
    /// </summary>
    /// <param name="indexInfos"></param>
    class IndexInfosCache(IndexedStateCache cache, IndexInfos indexInfos)
    {
        /// <summary>
        /// Index information by index name.
        /// </summary>
        public IndexInfos IndexInfos { get; } = indexInfos;

        /// <summary>
        /// The indexed state object instance of type <see cref="Indexing.IndexInfos.IndexedStateClass"/>.
        /// </summary>
        object State => cache.State;

        /// <summary>
        /// The committed values of indexed state properties.
        /// </summary>
        Dictionary<string, object?> CommittedByIndexName { get; set; } = [];        
    
        /// <summary>
        /// Commits updates by placing all the indexed properties in the before-values cache.
        /// </summary>
        /// <param name="updates"></param>
        internal void CommitUpdates(IReadOnlyDictionary<string, IndexedPropertyUpdate> updates)
        {
            var committedByIndexName = new Dictionary<string, object?>(CommittedByIndexName);
            foreach (var (indexName, op) in updates.Select(u => (u.Key, UpdateType: u.Value.CrudType)))
            {
                if (op is IndexUpdateCrudType.Update or IndexUpdateCrudType.Insert)
                {
                    committedByIndexName[indexName] = IndexInfos.ByIndexName[indexName].UpdateGenerator.GetValue(State);
                }
                else if (op is IndexUpdateCrudType.Delete)
                {
                    committedByIndexName[indexName] = null;
                }
            }
            CommittedByIndexName = committedByIndexName;
        }
    
        internal void Initialize()
        {
            CommittedByIndexName = IndexInfos.ByIndexName.ToDictionary(x => x.Key, x => x.Value.UpdateGenerator.GetValue(State));
        }        
        
        /// <summary>
        /// Generates property updates based on <see cref="CommittedByIndexName"/> and the current state of <see cref="State"/>, for non-total indexes.
        /// </summary>
        /// <param name="reason"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        internal IndexedPropertyUpdatesByGrain GetUpdates(IndexUpdateReason reason)
        {
            var uniqueIndexCount = 0;
            var onlyUniqueIndex = true;
            var committedByIndexName = CommittedByIndexName;
            var state = State ?? throw new InvalidOperationException(message: "The state object not be null during update!");
            var ups = new Dictionary<string, IndexedPropertyUpdate>();
            foreach (var (indexName, indexInfo) in IndexInfos.ByIndexName)
            {
                var committedValue = committedByIndexName[indexName];
                
                var update = reason is IndexUpdateReason.OnActivate 
                    ? indexInfo.UpdateGenerator.CreateUpdate(currentValue: committedValue, IndexUpdateVisibilityMode.NonTentative)
                    : indexInfo.UpdateGenerator.CreateUpdate(state: state, beforeValue: committedValue, IndexUpdateVisibilityMode.NonTentative);
                
                if (update.CrudType is not IndexUpdateCrudType.None) // TODO: verify eagerness consistency
                {
                    if (indexInfo.Metadata.IsUnique)
                        uniqueIndexCount++;
                    else
                        onlyUniqueIndex = false;
    
                    ups.Add(indexName, update);
                }
            }
            return new(ups, UniqueIndexCount: uniqueIndexCount, OnlyUniqueIndexes: onlyUniqueIndex);
        }
    }
}

/// <summary>
/// Represents a batch of updates to property indexes across multiple indexable grain types.
/// </summary>
/// <param name="reason"></param>
/// <param name="updatesByGrain"></param>
/// <param name="uniqueIndexCount"></param>
/// <param name="onlyUniqueIndexes"></param>
public class IndexedPropertyUpdates(
    IndexUpdateReason reason,
    Dictionary<Type, Dictionary<string, IndexedPropertyUpdate>> updatesByGrain,
    int uniqueIndexCount,
    bool onlyUniqueIndexes
)
{
    /// <summary>
    /// The updates indexed by grain interface.
    /// </summary>
    public Dictionary<Type, Dictionary<string, IndexedPropertyUpdate>> UpdatesByGrain => updatesByGrain;    
    
    /// <summary>
    /// Indicates whether there are updates to unique indexes.
    /// </summary>
    public bool HasUniqueIndexUpdate => uniqueIndexCount > 0;

    /// <summary>
    /// The number of unique indexes updated.
    /// </summary>
    public int UniqueIndexCount => uniqueIndexCount;
    
    /// <summary>
    /// Indicates whether there are any <see cref="IndexUpdateCrudType.Delete"/> operations in this batch.
    /// </summary>
    public bool HasDeletes => UpdatesByGrain.Values.Any(x => x.Values.Any(y => y.CrudType is IndexUpdateCrudType.Delete));
}

/// <summary>
/// A collection of updates from a single indexable grain.
/// </summary>
/// <param name="UpdatesByIndexName"></param>
/// <param name="UniqueIndexCount"></param>
/// <param name="OnlyUniqueIndexes"></param>
internal record IndexedPropertyUpdatesByGrain(
    Dictionary<string, IndexedPropertyUpdate> UpdatesByIndexName,
    int UniqueIndexCount,
    bool OnlyUniqueIndexes
);
