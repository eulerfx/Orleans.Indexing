using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Serialization.TypeSystem;
using Orleans.Services;
using Orleans.Transactions.Abstractions;

#nullable enable

namespace Orleans.Indexing;

public class IndexingSystemOptions
{
    public static string SectionName => "IndexingSystem";

    /// <summary>
    /// The static number of partitions in a hash index.
    /// </summary>
    /// <remarks>
    /// When set to 0, this will result in a unique partition grain for each computed hash-code.
    /// Otherwise, the modulus of the hash-code with respect to the max number of partitions is computed.
    /// </remarks>
    public int DefaultMaxHashIndexPartitions { get; init; } = 0;

    /// <summary>
    /// The bin type for partitions on datetime values.
    /// </summary>
    public DateTimePartitionBinType? DefaultDateTimePartitionBin { get; init; } = DateTimePartitionBinType.Year;
    
    /// <summary>
    /// The name of the storage provider to be used for the <see cref="IndexingQueue"/>.
    /// </summary>
    public string? IndexingQueueStorageProviderName { get; set; }

    public int IndexUpdateParallelism { get; init; } = 10;
    
    public int IndexingQueueInputBufferSize { get; init; } = 10;
        
    public int IndexingQueueOutputBufferSize { get; init; } = 10;
        
    public TimeSpan IndexingQueueOutputBufferTimeOut { get; init; } = TimeSpan.FromMilliseconds(100);
}

public static class IndexingSystemRegistration
{
    public static void UseIndexing(this ISiloBuilder silo, IConfiguration cfg, Action<IndexingSystemOptions>? configureOptions = null)
    {
        var options = new IndexingSystemOptions();
        cfg.GetSection(IndexingSystemOptions.SectionName).Bind(options);
        configureOptions?.Invoke(options);
        
        var registry = IndexableGrainInterfaceRegistry.Create(Assembly.GetCallingAssembly());
        
        silo.ConfigureServices(s => s.UseIndexing(options, registry));
        silo.RegisterIndexingQueueServices(registry);
        silo.UseTransactions();
    }
    
    public static void UseIndexing(this IServiceCollection s, IndexingSystemOptions options, IndexableGrainInterfaceRegistry registry)
    {
        s.AddSingleton(registry);
        s.AddSingleton(Options.Create(options));
        s.AddSingleton<IndexManager>();
        s.AddSingleton<IndexAccessor>();
        s.AddSingleton<ILifecycleParticipant<ISiloLifecycle>>(sp => sp.GetRequiredService<IndexManager>());
        s.AddSingleton<IIndexingQueueServiceClient, IndexingQueueServiceClient>();
        
        s.AddKeyedSingleton<IHashIndexPartitionScheme>(
            PartitionedIndexOptions.DefaultHashIndexPartitionSchemeName,  
            (_, _) => new HashIndexPartitionScheme(MaxPartitions: options.DefaultMaxHashIndexPartitions)
            );
        s.AddSingleton<IHashIndexPartitionScheme>(sp => sp.GetRequiredKeyedService<IHashIndexPartitionScheme>(PartitionedIndexOptions.DefaultHashIndexPartitionSchemeName));
        
        s.AddKeyedSingleton<ISortedIndexPartitionScheme>(
            PartitionedIndexOptions.DefaultSortedIndexPartitionSchemeName, 
            (_,_) => new DateTimePartitionScheme(Bin: options.DefaultDateTimePartitionBin)
            );
        s.AddSingleton<ISortedIndexPartitionScheme>(sp => sp.GetRequiredKeyedService<ISortedIndexPartitionScheme>(PartitionedIndexOptions.DefaultSortedIndexPartitionSchemeName));
        
        s.AddSingleton(typeof(IAttributeToFactoryMapper<IndexedStateAttribute>), typeof(IndexedStateAttributeMapper));
    }

    /// <summary>
    /// Registers instances <see cref="IndexingQueueService"/> of each indexable grain interface type.
    /// </summary>
    /// <param name="silo"></param>
    /// <param name="registry"></param>
    /// <returns></returns>
    static ISiloBuilder RegisterIndexingQueueServices(this ISiloBuilder silo, IndexableGrainInterfaceRegistry registry)
    {
        silo.AddGrainService<IndexingQueueService>();
        return silo;
    }    
}
