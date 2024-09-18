#nullable enable
namespace Orleans.Indexing;

/// <summary>
/// Options for <see cref="IndexedState{TProperties}"/>.
/// </summary>
public class IndexedStateOptions
{
    public required string StateName { get; init; }
    
    public required string StorageName { get; init; }
    
    public int EagerIndexUpdateParallelism { get; init; } = 10;
    
    public int EnqueueParallelism { get; init; } = 10;
}
