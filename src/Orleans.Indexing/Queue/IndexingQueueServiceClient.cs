#nullable enable
using System;
using Orleans.Runtime;
using Orleans.Runtime.Services;
using Orleans.Services;

namespace Orleans.Indexing;

/// <summary>
/// Provides access to the indexing queue service for an indexable grain.
/// </summary>
public interface IIndexingQueueServiceClient : IGrainServiceClient<IIndexingQueueService>
{
    IIndexingQueueService GetQueueByCallingGrain(GrainId callingGrainId);

    IIndexingQueueService GetQueueByInterface(Type grainInterfaceType);
}

public class IndexingQueueServiceClient(IServiceProvider sp) : GrainServiceClient<IIndexingQueueService>(sp), IIndexingQueueServiceClient
{
    public IIndexingQueueService GetQueueByCallingGrain(GrainId callingGrainId) => 
        GetGrainService(callingGrainId: callingGrainId);

    public IIndexingQueueService GetQueueBySilo(SiloAddress destination) => 
        GetGrainService(destination: destination);
    
    public IIndexingQueueService GetQueueByInterface(Type grainInterfaceType) => 
        GetGrainService(key: IndexingHelper.GetGrainClassTypeCode(grainInterfaceType));
}
