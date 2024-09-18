#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Indexing;

/// <summary>
/// An entry in an index associated to a single key.
/// For unique indexes, entry contains a single value.
/// </summary>
/// <typeparam name="T"></typeparam>
[GenerateSerializer]
public class IndexEntry<T>
{
    /// <summary>
    /// The values in the index entry associated to a single key.
    /// </summary>
    [Id(0)]
    public HashSet<T> Values = [];
    
    /// <summary>
    /// A flag indicating the presence of a tentative operation on a value in this index entry.
    /// </summary>
    [Id(1)]
    public byte TentativeOperationType = TentativeIndexOps.None;

    /// <summary>
    /// Determines whether the index entry contains the given value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public bool Contains(T value) => Values.Contains(value);

    /// <summary>
    /// Returns the number of items in the index entry.
    /// </summary>
    public int Count => Values.Count;

    /// <summary>
    /// Gets a page of results from this entry.
    /// </summary>
    /// <param name="page"></param>
    /// <returns></returns>
    public IReadOnlyList<T> GetPage(PageInfo page) => Values.Skip(page.Offset).Take(page.Size).ToList();
    
    /// <summary>
    /// Adds an item to this hash index entry under the given visibility mode.
    /// </summary>
    /// <param name="item"></param>
    /// <param name="visibility"></param>
    /// <param name="isUniqueIndex"></param>
    /// <exception cref="UniquenessConstraintViolatedException"></exception>
    /// <returns>true if the item was added</returns>
    public bool Add(T item, IndexUpdateVisibilityMode visibility, bool isUniqueIndex)
    {
        var added = Values.Add(item);
        if (!added && isUniqueIndex)
            throw new UniquenessConstraintViolatedException($"The item {item} already exists!");
        
        if (visibility is IndexUpdateVisibilityMode.Tentative)
        {
            SetTentativeInsert();
            return added;
        }

        // No condition check is necessary: if the flag is set, we will unset it, and if it's unset, we will unset it again, which is a no-op.
        ClearTentativeFlag();
        return added;
    }
    
    /// <summary>
    /// Removes and item from the index entry.
    /// </summary>
    /// <param name="item"></param>
    /// <param name="visibility"></param>
    /// <param name="isUniqueIndex"></param>
    /// <returns>true if the value was removed</returns>
    public bool Remove(T item, IndexUpdateVisibilityMode visibility, bool isUniqueIndex)
    {
        if (visibility == IndexUpdateVisibilityMode.Tentative)
        {
            SetTentativeDelete();
            return false;
        }

        // In order to make the index update operations idempotent, non-transactional unique indexes must only do their action if the index entry
        // is still marked as tentative. Otherwise, it means that tentative flag was removed by an earlier attempt and should not be done again.
        // There is no concern about non-unique indexes, because they cannot affect the operations among different grains and therefore
        // cannot fail the operations on other grains.
        if (!isUniqueIndex || visibility is IndexUpdateVisibilityMode.Transactional || IsTentative)
        {
            ClearTentativeFlag();
            return Values.Remove(item);
        }
        
        return false;
    }

    internal void SetTentativeInsert() => TentativeOperationType = TentativeIndexOps.Insert;
    
    /// <summary>
    /// Clears the tentative flag, making the values visible.
    /// </summary>
    internal void ClearTentativeFlag() => TentativeOperationType = TentativeIndexOps.None;
    
    /// <summary>
    /// Indicates whether there's a tentative operation in progress.
    /// </summary>
    public bool IsTentative => IsTentativeDelete || IsTentativeInsert;

    bool IsTentativeDelete => TentativeOperationType is TentativeIndexOps.Delete;

    bool IsTentativeInsert => TentativeOperationType is TentativeIndexOps.Insert;

    void SetTentativeDelete() => TentativeOperationType = TentativeIndexOps.Delete;
}
