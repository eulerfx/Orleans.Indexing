#nullable enable
using System;
using System.Reflection;

namespace Orleans.Indexing;

/// <summary>
/// An update to an indexed property.
/// </summary>
/// <param name="beforeValue"></param>
/// <param name="afterValue"></param>
/// <param name="crudType"></param>
/// <param name="visibility"></param>
[GenerateSerializer]
[Immutable]
public class IndexedPropertyUpdate(object? beforeValue, object? afterValue, IndexUpdateCrudType crudType, IndexUpdateVisibilityMode visibility)
{
    [Id(0)]
    public object? BeforeValue { get; } = beforeValue;
    
    [Id(1)]
    public object? AfterValue { get; } = afterValue;
    
    [Id(2)]
    public IndexUpdateCrudType CrudType { get; } = crudType;
    
    [Id(3)]
    public IndexUpdateVisibilityMode Visibility { get; } = visibility;

    static IndexUpdateCrudType GetCrudType(object? before, object? after) => (before, after) switch
    {
        (null, null) => IndexUpdateCrudType.None,
        (null, {} _) => IndexUpdateCrudType.Insert,
        ({} _, null) => IndexUpdateCrudType.Delete,
        ({} b, {} a) => b.Equals(a) ? IndexUpdateCrudType.None : IndexUpdateCrudType.Update
    };

    public static IndexedPropertyUpdate Create(object? beforeValue, object? afterValue, IndexUpdateVisibilityMode visibilityMode)
    {
        var crud = GetCrudType(beforeValue, afterValue);
        beforeValue = crud is IndexUpdateCrudType.Update or IndexUpdateCrudType.Delete ? beforeValue : null;
        afterValue = crud is IndexUpdateCrudType.Update or IndexUpdateCrudType.Insert ? afterValue : null;
        return new IndexedPropertyUpdate(beforeValue, afterValue, crud, visibilityMode);
    }
}

public static class IndexedPropertyUpdateHelper
{
    /// <summary>
    /// Overrides the update visibility mode to <see cref="IndexUpdateVisibilityMode.Tentative"/>.
    /// </summary>
    /// <param name="update"></param>
    /// <param name="visibility"></param>
    /// <returns></returns>
    static IndexedPropertyUpdate OverrideVisibilityMode(this IndexedPropertyUpdate update, IndexUpdateVisibilityMode visibility) =>
        new(beforeValue: update.BeforeValue, afterValue: update.AfterValue, update.CrudType, visibility);

    /// <summary>
    /// Sets the index update mode visibility to tentative.
    /// </summary>
    /// <param name="update"></param>
    /// <returns></returns>
    public static IndexedPropertyUpdate WithTentativeVisibility(this IndexedPropertyUpdate update) =>
        update.OverrideVisibilityMode(IndexUpdateVisibilityMode.Tentative);

    /// <summary>
    /// Reverses the <see cref="IndexUpdateCrudType"/> reversing inserts and deletes.
    /// </summary>
    /// <param name="update"></param>
    /// <returns></returns>
    public static IndexedPropertyUpdate ReversedCrud(this IndexedPropertyUpdate update) => update.WithCrudType(update.CrudType.Reverse());
    
    /// <summary>
    /// Creates a new update with a modified CRUD type.
    /// </summary>
    /// <param name="update"></param>
    /// <param name="crud"></param>
    /// <returns></returns>
    public static IndexedPropertyUpdate WithCrudType(this IndexedPropertyUpdate update, IndexUpdateCrudType crud) =>
        new(beforeValue: update.BeforeValue, afterValue: update.AfterValue, crud, update.Visibility);

    static IndexUpdateCrudType Reverse(this IndexUpdateCrudType op) => op switch
    {
        IndexUpdateCrudType.Delete => IndexUpdateCrudType.Insert,
        IndexUpdateCrudType.Insert => IndexUpdateCrudType.Delete,
        _ => op
    };
}


/// <summary>
/// The type of CRUD update to an index.
/// </summary>
public enum IndexUpdateCrudType
{
    None, 
    Insert, 
    Update, 
    Delete
}


/// <summary>
/// Indicates whether an update should apply exclusively to unique or non-unique indexes.
/// </summary>
[Flags]
internal enum IndexUpdateApplicabilityMode
{
    None = 0,
    Unique = 1,
    NonUnique = 2,
    Both = Unique | NonUnique
}


/// <summary>
/// The index update visibility mode controls visibility of concurrent index updates.
/// </summary>
public enum IndexUpdateVisibilityMode
{
    /// <summary>
    /// For unique indexes, a two-step workflow is used to both block visibility during synchronization and block concurrent constraint violation (such as uniqueness).
    /// </summary>
    Tentative,
    
    /// <summary>
    /// Makes any tentative update permanent.
    /// </summary>
    NonTentative,
    
    /// <summary>
    /// A transactional update that is always permanent if the transaction completes.
    /// </summary>
    Transactional
}

/// <summary>
/// Generates updates on indexed properties.
/// </summary>
/// <param name="indexedProperty"></param>
public class IndexUpdateGenerator(PropertyInfo indexedProperty)
{
    /// <summary>
    /// The property.
    /// </summary>
    public PropertyInfo IndexedProperty => indexedProperty;
    
    /// <summary>
    /// Gets the value of the indexed property <see cref="IndexedProperty"/>.
    /// </summary>
    /// <param name="state">The object containing the indexed properties.</param>
    /// <returns>The value of the indexed property.</returns>
    public object? GetValue(object state) => IndexedProperty.GetValue(state);

    /// <summary>
    /// Creates a member update on the given properties object (un-applied).
    /// </summary>
    /// <param name="state"></param>
    /// <param name="beforeValue"></param>
    /// <param name="visibilityMode"></param>
    /// <returns></returns>
    public IndexedPropertyUpdate CreateUpdate(object state, object? beforeValue, IndexUpdateVisibilityMode visibilityMode) =>
        IndexedPropertyUpdate.Create(beforeValue: beforeValue, afterValue: GetValue(state), visibilityMode);

    /// <summary>
    /// Creates a member update using the current image of the grain during activation.
    /// </summary>
    /// <param name="currentValue"></param>
    /// <param name="visibilityMode"></param>
    /// <returns></returns>
    public IndexedPropertyUpdate CreateUpdate(object? currentValue, IndexUpdateVisibilityMode visibilityMode) => 
        IndexedPropertyUpdate.Create(beforeValue: null, afterValue: currentValue, visibilityMode);
}

