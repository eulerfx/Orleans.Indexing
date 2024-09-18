#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Indexing;

/// <summary>
/// An index update action.
/// </summary>
/// <param name="Grain">The indexable grain being updated.</param>
/// <param name="InterfaceType">The indexable grain interface.</param>
/// <param name="ActionId">The id of the action in the indexing pipeline.</param>
/// <param name="UpdatesByIndex">The property updates, by index name.</param>
[GenerateSerializer]
[Immutable]
public record IndexingAction(
    [property: Id(0)] IIndexableGrain Grain,
    [property: Id(1)] Type InterfaceType,
    [property: Id(2)] Guid ActionId,
    [property: Id(3)] IDictionary<string, IndexedPropertyUpdate> UpdatesByIndex
)
{
    /// <summary>
    /// Gets updates that are not <see cref="IndexUpdateCrudType.None"/>.
    /// </summary>
    /// <returns></returns>
    public IEnumerable<KeyValuePair<string, IndexedPropertyUpdate>> GetUpdates() => 
        UpdatesByIndex.Where(x => x.Value.CrudType is not IndexUpdateCrudType.None);
    
    public override int GetHashCode() => ActionId.GetInvariantHashCode();
}
