#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Orleans.Indexing;

[GenerateSerializer]
public class TestIndexedProcessStateQuery
{
    [Id(0)]
    public string? ProcessType { get; set; }
    
    [Id(1)]
    public string? Status { get; set; }
    
    [Id(2)]
    public DateTimeOffset? StartedOnStart { get; set; }
    
    [Id(3)]
    public DateTimeOffset? StartedOnEnd { get; set; }
    
    [Id(4)]
    public PageInfo Page { get; init; } = new(Offset: 0, Size: 10);
}


[GenerateSerializer]
public class TestIndexedProcessState
{
    [Id(0)]
    [HashIndex(IsUnique: true)] 
    public string? ProcessId { get; set; }
    
    [Id(1)]
    [HashIndex]
    public string? ProcessType { get; set; }
    
    [Id(2)]
    [HashIndex]
    public string? Status { get; set; }
    
    [Id(3)]
    [SortedIndex]
    public DateTimeOffset? StartedOn { get; set;  }
        
    [Id(4)]
    [HashIndex]
    public bool? IsCompleted { get; set; }

    public void Start(TestIndexedProcessStartRequest req)
    {
        ProcessId = req.ProcessId;
        StartedOn = DateTimeOffset.UtcNow;
        ProcessType = req.ProcessType;
        Status = "Started";
    }
}

[GenerateSerializer]
public class TestIndexedProcessStartRequest
{
    [Id(1)]
    public required string ProcessId { get; init; } = Guid.NewGuid().ToString("N");
    
    [Id(2)] 
    public required string ProcessType { get; init; }
}

public interface ITestIndexableProcessGrain : IIndexableGrain<TestIndexedProcessState>, IGrainWithStringKey
{
    Task<TestIndexedProcessState> Start(TestIndexedProcessStartRequest req);

    [ReadOnly]
    Task<TestIndexedProcessState?> GetState();
    
    Task<TestIndexedProcessState?> Error(string error);

    [ReadOnly]
    Task<IReadOnlyList<TestIndexedProcessState>> Query(TestIndexedProcessStateQuery query);
}

public class TestIndexableProcessGrain(
    [IndexedState(StorageName: TestProcessManagerConstants.IndexedProcessStateStorage, StateName: "state")] 
    IIndexedState<TestIndexedProcessState> State,
    IndexAccessor Indexes
) : Grain, ITestIndexableProcessGrain
{
    readonly IIndex<string, ITestIndexableProcessGrain> processTypeIndex = 
        Indexes.GetIndexByProperty<string, ITestIndexableProcessGrain>(
            grainInterface: typeof(ITestIndexableProcessGrain), propertyName: nameof(TestIndexedProcessState.ProcessType));    
    
    readonly ISortedIndex<DateTimeOffset?, ITestIndexableProcessGrain> startedOnIndex = 
        Indexes.GetSortedIndexByProperty<DateTimeOffset?, ITestIndexableProcessGrain>(
            grainInterface: typeof(ITestIndexableProcessGrain), propertyName: nameof(TestIndexedProcessState.StartedOn));
    
    readonly IIndex<string, ITestIndexableProcessGrain> statusIndex = 
        Indexes.GetIndexByProperty<string, ITestIndexableProcessGrain>(
            grainInterface: typeof(ITestIndexableProcessGrain), propertyName: nameof(TestIndexedProcessState.Status));
    
    public async Task<TestIndexedProcessState> Start(TestIndexedProcessStartRequest req)
    {
        return await State.PerformUpdate(s =>
        {
            s.Start(req);
            return s;
        });
    }
    
    public async Task<TestIndexedProcessState?> Error(string error)
    {
        return await State.PerformUpdate(s =>
        {
            s.Status = error;
            return s;
        });
    }
    
    public async Task<IReadOnlyList<TestIndexedProcessState>> Query(TestIndexedProcessStateQuery query)
    {
        var res = new HashSet<ITestIndexableProcessGrain>();

        if (query.StartedOnStart is not null && query.StartedOnEnd is not null)
            Join(await startedOnIndex.LookupRange(start: query.StartedOnStart, end: query.StartedOnEnd, query.Page));
        
        if (query.Status is not null)
            Join(await statusIndex.LookupByKey(key: query.Status, query.Page));

        if (query.ProcessType is not null)
            Join(await processTypeIndex.LookupByKey(key: query.ProcessType, query.Page));        

        var states = await res.Select(x => x.GetState()).Parallel(maxParallelism: Environment.ProcessorCount);

        return states.WhereNotNull().ToArray();

        void Join(IEnumerable<ITestIndexableProcessGrain> items)
        {
            if (res.Count == 0) res.UnionWith(items);
            else res.IntersectWith(items);
        }
    }

    public async Task<TestIndexedProcessState?> GetState() => await State.PerformRead(s => s);
    
    public Task<Immutable<HashSet<Guid>>> GetActiveIndexingActionIds() => State.GetActiveIndexingActionIds();
    
    public Task RemoveFromActiveIndexingActions(HashSet<Guid> ids) => State.RemoveFromActiveIndexingActions(ids);
    
}
