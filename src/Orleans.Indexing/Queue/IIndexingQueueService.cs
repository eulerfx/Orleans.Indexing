#nullable enable
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Services;

namespace Orleans.Indexing;

/// <summary>
/// A service hosting and processing the write-ahead <see cref="IndexingQueue"/>.
/// </summary>
[Unordered]
public interface IIndexingQueueService : IGrainService, IGrainWithStringKey
{
    /// <summary>
    /// Adds an indexing action to the queue and persists it.
    /// </summary>
    /// <param name="action"></param>
    /// <returns></returns>
    Task Enqueue(Immutable<IndexingAction> action);
    
    /// <summary>
    /// Adds a batch of indexing actions to the queue and persists it.
    /// </summary>
    /// <param name="actions"></param>
    /// <returns></returns>
    Task EnqueueBatch(Immutable<IReadOnlyList<IndexingAction>> actions);
    
    /// <summary>
    /// Dequeues the given action ids from the log.
    /// </summary>
    /// <param name="activeActionIds"></param>
    /// <returns></returns>
    Task Dequeue([Immutable] HashSet<Guid> activeActionIds);
    
    /// <summary>
    /// Returns the list of actions that are not completely processed but still active (pending).
    /// </summary>
    /// <param name="activeActionIds">the set of active indexing actions</param>
    /// <returns>the pending indexing actions that are in the given active set</returns>
    [ReadOnly] 
    Task<Immutable<IReadOnlyList<IndexingAction>>> GetPending([Immutable] HashSet<Guid> activeActionIds);
}
