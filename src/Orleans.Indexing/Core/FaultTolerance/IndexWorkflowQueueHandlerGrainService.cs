using Orleans.Concurrency;
using Orleans.Runtime;
using System;
using System.Threading.Tasks;

namespace Orleans.Indexing
{
    [Reentrant]
    internal class IndexWorkflowQueueHandlerGrainService : GrainService, IIndexWorkflowQueueHandler
    {
        private IIndexWorkflowQueueHandler _base;

        internal IndexWorkflowQueueHandlerGrainService(SiloIndexManager sim, Type grainInterfaceType, int queueSeqNum, bool isDefinedAsFaultTolerantGrain)
            : base(IndexWorkflowQueueHandlerBase.CreateIndexWorkflowQueueHandlerGrainReference(sim, grainInterfaceType, queueSeqNum, sim.SiloAddress).GrainId,
                                                                                               sim.Silo, sim.LoggerFactory)
        {
            _base = new IndexWorkflowQueueHandlerBase(sim, grainInterfaceType, queueSeqNum, sim.SiloAddress, isDefinedAsFaultTolerantGrain,
                                                      () => base.GrainReference);  // lazy is needed because the runtime isn't attached until Registered
        }

        public Task HandleWorkflowsUntilPunctuation(Immutable<IndexWorkflowRecordNode> workflowRecordsHead)
            => _base.HandleWorkflowsUntilPunctuation(workflowRecordsHead);

        public Task Initialize(IIndexWorkflowQueue oldParentGrainService)
            => throw new NotSupportedException();
    }
}
