#nullable enable
namespace Orleans.Indexing;

/// <summary>
/// Index statistics to be used for query optimization.
/// </summary>
/// <param name="TotalItems"></param>
[GenerateSerializer]
public record struct IndexStatistics(
    [property: Id(0)] long TotalItems = 0
)
{
    public void ResetTotalItems() => TotalItems = 0;
    
    public void IncrementTotalItems(int increment = 1) => TotalItems += increment;
}
