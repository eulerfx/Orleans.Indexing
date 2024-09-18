#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Indexing;

/// <summary>
/// An envelope for indexed state that contains indexing system metadata.
/// </summary>
/// <typeparam name="TState">The properties class.</typeparam>
[GenerateSerializer]
public class IndexedStateEnvelope<TState>
{
    /// <summary>
    /// The indexed state instance.
    /// </summary>
    [Id(0)]
    public TState State { get; set; } = Activator.CreateInstance<TState>();

    /// <summary>
    /// The currently active indexing actions associated to the corresponding indexed state instance.
    /// </summary>
    [Id(1)]
    public HashSet<Guid> ActiveIndexingActionIds { get; } = [];

    /// <summary>
    /// Indexing queue by indexable grain interface.
    /// </summary>
    [Id(2)]
    public Dictionary<Type, IIndexingQueueService> IndexingQueues { get; } = [];

    /// <summary>
    /// Returns a snapshot of the indexing queues by grain type.
    /// </summary>
    /// <returns></returns>
    public IReadOnlyList<(Type GrainInterface, IIndexingQueueService Queue)> GetIndexingQueuesSnapshot() =>
        IndexingQueues.Select(x => (x.Key, x.Value)).ToReadOnlyList();

    /// <summary>
    /// Clears the cache of indexing queues.
    /// </summary>
    public void ClearIndexingQueues() => IndexingQueues.Clear();
    
    /// <summary>
    /// Clears all active indexing actions except those in the given list <paramref name="remainingActionIds"/>.
    /// </summary>
    /// <param name="remainingActionIds"></param>
    /// <returns>true of the list changed; false otherwise</returns>
    public bool ClearProcessedActionsExcept(IEnumerable<Guid> remainingActionIds)
    {
        var initialSize = ActiveIndexingActionIds.Count;
        ActiveIndexingActionIds.Clear();
        ActiveIndexingActionIds.UnionWith(remainingActionIds);
        return ActiveIndexingActionIds.Count != initialSize;
    }
}
