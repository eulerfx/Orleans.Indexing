#nullable enable
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace Orleans.Indexing;

/// <summary>
/// An entry in the indexing write-ahead queue.
/// </summary>
[GenerateSerializer]
[Immutable]
public class IndexingQueueEntry
{
    /// <summary>
    /// The indexing action, unless this is a punctuation.
    /// </summary>
    [Id(0)]
    public IndexingAction? Action { get; init; }

    /// <summary>
    /// A punctuation causes the write-ahead queue to be 'flushed'.
    /// </summary>
    [Id(1)]
    [MemberNotNullWhen(false, nameof(Action))]
    public bool IsPunctuation { get; init; }
    
    public static IndexingQueueEntry CreatePunctuation() => new() { IsPunctuation = true };
    
    public static IndexingQueueEntry CreateAction(IndexingAction action) => new() { Action = action };
}

/// <summary>
/// The FIFO write-ahead queue.
/// </summary>
[GenerateSerializer]
public class IndexingQueue
{
    [Id(0)]
    public LinkedList<IndexingQueueEntry> Items { get; } = [];
    
    /// <summary>
    /// The number of items in the queue, including punctuation entries.
    /// </summary>
    public int Count => Items.Count;    
    
    public IEnumerable<LinkedListNode<IndexingQueueEntry>> EnumerateEntriesUntilPunctuation()
    {
        var node = Items.First;
        while (node is not null)
        {
            if (node.Value.IsPunctuation)
                break;
            yield return node;
            node = node.Next;
        }
    }
    
    public IEnumerable<IndexingAction> EnumerateActionsUntilPunctuation()
    {
        foreach (var entry in EnumerateEntriesUntilPunctuation())
        {
            var action = entry.Value.Action;
            if (action is not null)
                yield return action;
        }
    }
    
    /// <summary>
    /// Gets all pending actions in the queue that are in the given list (if given).
    /// </summary>
    /// <param name="activeActionIds"></param>
    /// <returns></returns>
    public IReadOnlyList<IndexingAction> GetPendingActions(HashSet<Guid>? activeActionIds = null) => 
        EnumerateActionsUntilPunctuation()
            .Where(x => activeActionIds is null || activeActionIds.Contains(x.ActionId))
            .ToReadOnlyList();
    
    /// <summary>
    /// Dequeues all indexing actions until punctuation.
    /// </summary>
    /// <param name="actions"></param>
    public void DequeueActions(IEnumerable<IndexingAction> actions)
    {        
        var ids = actions.Select(x => x.ActionId).ToHashSet();
        foreach (var entry in EnumerateEntriesUntilPunctuation())
        {
            if (entry.Value.IsPunctuation)
                break;
            if (ids.Contains(entry.Value.Action.ActionId))
                Items.Remove(entry);
        }
        Punctuate();
    }
    
    /// <summary>
    /// Adds an action to the end of the queue.
    /// </summary>
    /// <param name="action"></param>
    public void AddAction(IndexingAction action) => Items.AddLast(IndexingQueueEntry.CreateAction(action));

    /// <summary>
    /// Adds actions to the end of the queue.
    /// </summary>
    /// <param name="actions"></param>
    public void AddActions(IEnumerable<IndexingAction> actions) => actions.ForEach(AddAction);
        
    /// <summary>
    /// Adds a punctuation entry to the log, forcing the processor to flush.
    /// </summary>
    public void Punctuate() => Items.AddLast(IndexingQueueEntry.CreatePunctuation());

    /// <summary>
    /// Removes the given actions from the log of actions until punctuation.
    /// </summary>
    /// <param name="actionIds"></param>
    /// <returns>The number of items removed.</returns>
    public int RemoveActions(ISet<Guid> actionIds)
    {
        var removed = 0;
        foreach (var entry in EnumerateEntriesUntilPunctuation())
        {
            if (entry.Value.Action?.ActionId is not null && actionIds.Contains(entry.Value.Action.ActionId))
            {
                Items.Remove(entry);
                removed++;
            }
        }
        if (removed > 0)
        {
            if (Items.First?.Value.IsPunctuation is true)
            {
                Items.RemoveFirst();
            }
        }
        return removed;
    }
}


