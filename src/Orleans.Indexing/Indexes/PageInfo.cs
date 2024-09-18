#nullable enable
namespace Orleans.Indexing;

[GenerateSerializer]
[Immutable]
public readonly record struct PageInfo(
    [property: Id(0)] int Offset = 0, 
    [property: Id(1)] int Size = 100
);
