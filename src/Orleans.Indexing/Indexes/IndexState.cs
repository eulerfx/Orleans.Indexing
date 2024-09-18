#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Runtime;

namespace Orleans.Indexing;

/// <summary>
/// The state of an individual index.
/// Implemented by <see cref="HashIndexState{TKey,TGrain}"/> and <see cref="SortedIndexState{TKey,TGrain}"/>.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public interface IIndexState<TKey, TGrain>
{
    /// <summary>
    /// The status.
    /// </summary>
    IndexStatus IndexStatus { get; }
    
    /// <summary>
    /// The number of items in the index.
    /// </summary>
    int Count { get; }    
    
    /// <summary>
    /// Tries to get a value from the index.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    IndexEntry<TGrain>? TryGetValue(TKey key);

    /// <summary>
    /// Gets a page of items.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="page"></param>
    /// <returns></returns>
    IReadOnlyList<TGrain> GetPage(TKey key, PageInfo page);
    
    /// <summary>
    /// Adds a new entry to the index.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    void Add(TKey key, IndexEntry<TGrain> value);
    
    /// <summary>
    /// Gets a reference to the grain hosting the next bucket in the chain, if any.
    /// </summary>
    GrainReference? NextBucket { get; set; }

    /// <summary>
    /// Sets the status to available.
    /// </summary>
    void SetAvailable();

    /// <summary>
    /// Clears the index.
    /// </summary>
    void Dispose();

    /// <summary>
    /// Updates the index.
    /// </summary>
    /// <param name="grain"></param>
    /// <param name="update"></param>
    /// <param name="metadata"></param>
    /// <returns></returns>
    HashIndexStateUpdateResultType Update(IIndexableGrain grain, IndexedPropertyUpdate update, IndexMetadata metadata);
}

/// <summary>
/// Base class for <see cref="IIndexState{TKey,TGrain}"/> implementations based on a dictionary.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TGrain"></typeparam>
public abstract class IndexState<TKey, TGrain> : IIndexState<TKey, TGrain>
{
    public abstract IDictionary<TKey, IndexEntry<TGrain>> Dictionary { get; }
    
    [Id(0)]
    public IndexStatus IndexStatus { get; protected set; }

    [Id(1)]
    public GrainReference? NextBucket { get; set; }
    
    [Id(2)]
    public int Count => Dictionary.Count;

    [Id(3)]
    public IndexStatistics Stats { get; set; }

    public IndexEntry<TGrain>? TryGetValue(TKey key) => Dictionary.TryGetValue(key);

    public IReadOnlyList<TGrain> GetPage(TKey key, PageInfo page)
    {
        var entry = Dictionary.TryGetValue(key);
        if (entry is null)
            return Array.Empty<TGrain>();
        var results = new List<TGrain>(page.Size);
        var skipped = 0;
        foreach (var value in entry.Values)
        {
            if (++skipped > page.Offset)
            {
                results.Add(value);
                if (results.Count >= page.Size)
                    break;
            }
        }

        return results;
    }
    
    public virtual void Add(TKey key, IndexEntry<TGrain> value)
    {
        Dictionary.Add(key, value);
        Stats.IncrementTotalItems(value.Count);
    }

    public void SetAvailable() => IndexStatus = IndexStatus.Available;

    public virtual void Dispose()
    {
        Dictionary.Clear();
        Stats.ResetTotalItems();
        IndexStatus = IndexStatus.Disposed;
    }
    
    /// <summary>
    /// Updates the index.
    /// </summary>
    /// <param name="grain"></param>
    /// <param name="update"></param>
    /// <param name="metadata"></param>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TGrain"></typeparam>
    /// <returns></returns>
    public HashIndexStateUpdateResultType Update(IIndexableGrain grain, IndexedPropertyUpdate update, IndexMetadata metadata) 
        => Update((TGrain)grain, update, metadata);
    
    /// <summary>
    /// Updates the index.
    /// </summary>
    /// <param name="grain"></param>
    /// <param name="update"></param>
    /// <param name="metadata"></param>
    /// <returns></returns>
    /// <exception cref="UniquenessConstraintViolatedException"></exception>
    HashIndexStateUpdateResultType Update(TGrain grain, IndexedPropertyUpdate update, IndexMetadata metadata) 
    {
        var success = Update(grain, update, metadata, out var fixIndexUnavailableOnDelete);
        return new(IsSuccess: success, FixIndexUnavailableOnDelete: fixIndexUnavailableOnDelete);
    }

    /// <summary>
    /// Applies an update to the state of this index.
    /// </summary>
    /// <param name="grain">the updated grain that is being indexed</param>
    /// <param name="update">the update information</param>
    /// <param name="metadata">the index metadata</param>
    /// <param name="fixIndexUnavailableOnDelete">output parameter: this variable determines whether the index was still unavailable when we received a delete operation</param>
    /// <exception cref="UniquenessConstraintViolatedException"></exception>
    /// <returns>true if the update succeeds and false otherwise, or if the bucket is full</returns>
    bool Update(TGrain grain, IndexedPropertyUpdate update, IndexMetadata metadata, out bool fixIndexUnavailableOnDelete)
    {
        fixIndexUnavailableOnDelete = false;
        if (update.CrudType is IndexUpdateCrudType.Update)
        {
            var afterValue = (TKey?)update.AfterValue ??
                throw new InvalidOperationException($"The after value cannot be null in an update on index '{metadata.IndexName}'!");
            var beforeValue = (TKey?)update.BeforeValue ??
                throw new InvalidOperationException($"The before value cannot be null in an update on index '{metadata.IndexName}'!");
            var beforeEntry = TryGetValue(beforeValue);
            if (beforeEntry?.Contains(grain) is true)
            {
                var afterEntry = TryGetValue(afterValue);
                if (afterEntry is not null)
                {
                    if (afterEntry.Contains(grain))
                    {
                        if (update.Visibility is IndexUpdateVisibilityMode.Tentative)
                        {
                            afterEntry.SetTentativeInsert();
                        }
                        else
                        {
                            afterEntry.ClearTentativeFlag();
                            beforeEntry.Remove(grain, update.Visibility, metadata.IsUnique);
                        }
                    }
                    else
                    {
                        if (metadata.IsUnique && afterEntry.Count > 0)
                        {
                            throw new UniquenessConstraintViolatedException(
                                $"The uniqueness property of index {metadata.IndexName} is would be violated for an update operation for before-value = {beforeValue}, after-value = {afterValue} and grain = {grain}");
                        }

                        beforeEntry.Remove(grain, update.Visibility, metadata.IsUnique);
                        afterEntry.Add(grain, update.Visibility, metadata.IsUnique);
                    }
                }
                else
                {
                    afterEntry = new IndexEntry<TGrain>();
                    beforeEntry.Remove(grain, update.Visibility, isUniqueIndex: metadata.IsUnique);
                    afterEntry.Add(grain, update.Visibility, isUniqueIndex: metadata.IsUnique);
                    Add(afterValue, afterEntry);
                }
            }
            else
            {
                // Insert-only, because Delete was not found. If there's a NextBucket so return false to search it;
                // otherwise, the desired Delete result is met (the previous value is not present), so proceed to Insert.
                if (metadata.IsChainedBuckets && NextBucket != null)
                {
                    return false;
                }

                if (!DoInsert(afterValue, out bool uniquenessViolation))
                {
                    return uniquenessViolation
                        ? throw new UniquenessConstraintViolatedException(
                            $"The uniqueness property of index {metadata.IndexName} would be violated for an update operation" +
                            $" for (not found before-image = {beforeValue}), after-image = {afterValue} and grain = {grain}")
                        : false; // The shard is full
                }
            }
        }
        else if (update.CrudType is IndexUpdateCrudType.Insert)
        {
            if (!DoInsert((TKey?)update.AfterValue, out bool uniquenessViolation))
            {
                return uniquenessViolation
                    ? throw new UniquenessConstraintViolatedException(
                        $"The uniqueness property of index {metadata.IndexName} would be violated for an insert operation for after-value = {(TKey?)update.AfterValue} and grain = {grain}")
                    : false;
            }
        }
        else if (update.CrudType is IndexUpdateCrudType.Delete)
        {
            var beforeValue = (TKey?)update.BeforeValue ??
                throw new InvalidOperationException("The before value cannot be null on a delete operation!");
            var beforeEntry = TryGetValue(beforeValue);
            if (beforeEntry?.Contains(grain) is true)
            {
                beforeEntry.Remove(grain, update.Visibility, isUniqueIndex: metadata.IsUnique);
                if (IndexStatus != IndexStatus.Available)
                {
                    fixIndexUnavailableOnDelete = true;
                }
            }
            else if (metadata.IsChainedBuckets)
            {
                // Not found in this shard. If there's a NextShard, return false to search it;
                // otherwise, the desired Delete result is met (the value is not present), so return true.
                return NextBucket is null;
            }
        }

        return true;

        bool DoInsert(TKey? afterValue, out bool uniquenessViolation)
        {
            if (afterValue is null)
                throw new InvalidOperationException("The after value cannot be null on an insert!");

            uniquenessViolation = false;
            var afterEntry = TryGetValue(afterValue);
            if (afterEntry is not null)
            {
                if (!afterEntry.Contains(grain))
                {
                    if (metadata.IsUnique && afterEntry.Count > 0)
                    {
                        uniquenessViolation = true;
                        return false;
                    }

                    afterEntry.Add(grain, update.Visibility, isUniqueIndex: metadata.IsUnique);
                }
                else if (update.Visibility is IndexUpdateVisibilityMode.Tentative)
                {
                    afterEntry.SetTentativeInsert();
                }
                else
                {
                    afterEntry.ClearTentativeFlag();
                }

                return true;
            }

            if (metadata.ShouldCreateNewBucketInChain(indexSize: Count))
            {
                return false; // the bucket is full
            }

            afterEntry = new IndexEntry<TGrain>();
            afterEntry.Add(grain, update.Visibility, isUniqueIndex: metadata.IsUnique);
            Add(afterValue, afterEntry);
            return true;
        }
    }
}

public enum IndexStateUpdateResultType
{
    Success    = 0,
    NextBucket = 1,
    Failure    = 2
}


internal static class TentativeIndexOps
{
    public const byte None = 0;
    public const byte Delete = 1;
    public const byte Insert = 2;
}
