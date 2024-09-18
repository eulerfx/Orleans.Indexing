#nullable enable
using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;

namespace Orleans.Indexing;

/// <summary>
/// A silo-level service hosting the <see cref="Registry"/>.
/// </summary>
/// <param name="Log"></param>
/// <param name="grainFactory"></param>
/// <param name="options"></param>
/// <param name="grainInterfaceRegistry"></param>
public class IndexManager(
    ILogger<IndexManager> Log,
    IServiceProvider sp,
    IGrainFactory grainFactory,
    IOptions<IndexingSystemOptions> options,
    IndexableGrainInterfaceRegistry grainInterfaceRegistry,
    IGrainContextAccessor grainContextAccessor,
    ILoggerFactory loggerFactory,
    IOptions<IndexingSystemOptions> systemOptions
    ) : ILifecycleParticipant<IClusterClientLifecycle>, ILifecycleParticipant<ISiloLifecycle>
{
    public IGrainFactory GrainFactory { get; } = grainFactory;

    IndexRegistry? registry;

    /// <summary>
    /// The index registry.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    public IndexRegistry Registry => registry.EnsureNotNull("The index registry has not been initialized!");

    public IndexingSystemOptions Options => options.Value;
    
    public void Participate(ISiloLifecycle observer)
    {
        observer.Subscribe(observerName: GetType().FullName, ServiceLifecycleStage.ApplicationServices, onStart: OnSiloStart);
    }
    
    public void Participate(IClusterClientLifecycle observer)
    {
        observer.Subscribe(observerName: GetType().FullName, ServiceLifecycleStage.ApplicationServices, onStart: OnSiloStart);
    }
    
    [MemberNotNull(nameof(registry))]
    async Task OnSiloStart(CancellationToken ct)
    {
        Log.LogInformation("create_index_registry/start");
        registry = IndexRegistry.CreateIndexRegistry(this, grainInterfaceRegistry);
        Log.LogInformation("create_index_registry/complete interface_count={0}", registry.IndexableGrainInterfaceCount);
    }

    /// <summary>
    /// Creates or activates an index of a property of the given type.
    /// </summary>
    /// <param name="indexType"></param>
    /// <param name="indexName"></param>
    /// <param name="indexedProperty"></param>
    /// <param name="indexAttr"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public IndexInfo CreateIndex(Type indexType, string indexName, PropertyInfo indexedProperty, IndexAttribute indexAttr)
    {
        var ops = new PartitionedIndexOptions { IndexName = indexName, PartitionSchemeName = indexAttr.PartitionSchemeName };
        var indexGrain = GetIndexGrain(indexType, indexedProperty, ops);
        return new IndexInfo
        {
            Index = indexGrain, 
            Metadata = new() { IndexType = indexType, IsUnique = indexAttr.IsUnique, IsEager = indexAttr.IsEager, IndexName = indexName, MaxBucketSize = indexAttr.MaxBucketSize },
            UpdateGenerator = new IndexUpdateGenerator(indexedProperty)
        };
    }

    IIndex GetIndexGrain(Type indexType, PropertyInfo indexedProperty, PartitionedIndexOptions ops)
    {
        var grainInterfaceType = IndexingHelper.GetIndexableGrainInterfaceType(indexType: indexType, indexedProperty: indexedProperty);
        var grainPrimaryKey = IndexingHelper.GetIndexGrainPrimaryKey(grainInterfaceType: grainInterfaceType, indexName: ops.IndexName);
        var indexGrain = indexType.IsInterface
            ? (IIndex)GrainFactory.GetGrain(grainInterfaceType: indexType, grainPrimaryKey: grainPrimaryKey)
            : CreateIndexGrain(indexType, ops);
        return indexGrain;
    }

    IIndex CreateIndexGrain(Type indexType, PartitionedIndexOptions ops)
    {
        if (!indexType.IsClass)
            throw new InvalidOperationException($"Unable to create index '{ops.IndexName}'!");
        return (IIndex)ActivatorUtilities.CreateInstance(sp, instanceType: indexType, parameters: [ops]);
    }
    
    public IIndexedState<TState> CreateIndexedState<TState>(IndexedStateOptions ops) where TState : class, new()
    {
        var state = new IndexedState<TState>(sp, loggerFactory, grainContextAccessor.GrainContext, GrainFactory, ops);
        state.Participate(grainContextAccessor.GrainContext.ObservableLifecycle);
        return state;
    }
}
