using Orleans.Runtime;
using System;

namespace Orleans.Indexing
{
    /// <summary>
    /// All the information stored for a single <see cref="IndexWorkflowQueueGrainService"/>
    /// </summary>
    [Serializable]
    [GenerateSerializer]
    internal class IndexWorkflowQueueEntry
    {
        /// <summary>
        /// The sequence of updates to be applied to indices.
        /// </summary>
        internal IndexWorkflowRecordNode WorkflowRecordsHead;
    }
}
