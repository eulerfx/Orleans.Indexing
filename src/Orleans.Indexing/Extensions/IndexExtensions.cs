using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Concurrency;

namespace Orleans.Indexing
{
    internal static class IndexExtensions
    {
        /// <summary>
        /// An extension method to intercept the workflow queue's calls to DirectApplyIndexUpdateBatch on an Index,
        /// so that for a PerSilo index, it can obtain the GrainService of that index on the silo of the indexed grain.
        /// </summary>
        public static Task<bool> ApplyIndexUpdateBatch(this IIndexInterface index, SiloIndexManager siloIndexManager,
                                                        Immutable<IDictionary<IIndexableGrain, IList<IMemberUpdate>>> iUpdates,
                                                        bool isUniqueIndex, IndexMetaData idxMetaData, SiloAddress siloAddress = null)
        {
            if (index is IActiveHashIndexPartitionedPerSilo)
            {
                var grainReference = GetActiveHashIndexPartitionedPerSiloGrainReference(
                    siloIndexManager,
                    IndexUtils.GetIndexNameFromIndexGrain((IAddressable)index), index.GetType().GetGenericArguments()[1],
                    siloAddress);
                var bucketInCurrentSilo = siloIndexManager.GetGrainService<IActiveHashIndexPartitionedPerSiloBucket>(grainReference);
                return bucketInCurrentSilo.DirectApplyIndexUpdateBatch(iUpdates, isUniqueIndex, idxMetaData/*, siloAddress*/);
            }
            return index.DirectApplyIndexUpdateBatch(iUpdates, isUniqueIndex, idxMetaData, siloAddress);
        }

        /// <summary>
        /// An extension method to intercept the calls to DirectApplyIndexUpdate on an Index,
        /// so that for a PerSilo index, it can obtain the GrainService of that index on the silo of the indexed grain.
        /// </summary>
        internal static Task<bool> ApplyIndexUpdate(this IIndexInterface index, SiloIndexManager siloIndexManager,
                                                    IIndexableGrain updatedGrain, Immutable<IMemberUpdate> update,
                                                    IndexMetaData idxMetaData, SiloAddress siloAddress = null)
        {
            if (index is IActiveHashIndexPartitionedPerSilo)
            {
                var grainReference = GetActiveHashIndexPartitionedPerSiloGrainReference(
                    siloIndexManager,
                    IndexUtils.GetIndexNameFromIndexGrain((IAddressable)index), index.GetType().GetGenericArguments()[1],
                    siloAddress);
                var bucketInCurrentSilo = siloIndexManager.GetGrainService<IActiveHashIndexPartitionedPerSiloBucket>(grainReference);
                return bucketInCurrentSilo.DirectApplyIndexUpdate(updatedGrain, update, idxMetaData.IsUniqueIndex, idxMetaData/*, siloAddress*/);
            }
            return index.DirectApplyIndexUpdate(updatedGrain, update, idxMetaData.IsUniqueIndex, idxMetaData, siloAddress);
        }


        static GrainReference GetActiveHashIndexPartitionedPerSiloGrainReference(SiloIndexManager siloIndexManager, string indexName, Type grainInterfaceType, SiloAddress siloAddress) =>
            siloIndexManager.MakeGrainServiceGrainReference(
                typeData: IndexingConstants.HASH_INDEX_PARTITIONED_PER_SILO_BUCKET_GRAIN_SERVICE_TYPE_CODE,
                systemGrainId: IndexUtils.GetIndexGrainPrimaryKey(grainInterfaceType, indexName),
                siloAddress: siloAddress);
    }
}
