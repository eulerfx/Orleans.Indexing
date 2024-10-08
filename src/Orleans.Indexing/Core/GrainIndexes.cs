using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Orleans.Concurrency;
using Orleans.Indexing.Facet;

namespace Orleans.Indexing
{
    internal class GrainIndexes : IEnumerable<KeyValuePair<Type, InterfaceIndexes>>
    {
        /// <summary>
        /// An immutable cached version of IndexInfo (containing IIndexUpdateGenerator) instances for the current indexes on the grain,
        /// keyed by interface.
        /// </summary>
        IDictionary<Type, InterfaceIndexes> interfaceToIndexMap = new Dictionary<Type, InterfaceIndexes>();

        internal InterfaceIndexes this[Type interfaceType] => this.interfaceToIndexMap[interfaceType];
        internal bool ContainsInterface(Type interfaceType) => this.interfaceToIndexMap.ContainsKey(interfaceType);
        internal IReadOnlyDictionary<string, object> PropertyNullValues { get; }


        GrainIndexes(IndexRegistry registry, IEnumerable<Type> indexedInterfaceTypes, IReadOnlyDictionary<string, object> propertyNullValues)
        {
            this.PropertyNullValues = propertyNullValues;
            this.interfaceToIndexMap = indexedInterfaceTypes.ToDictionary(itf => itf, itf => new InterfaceIndexes(registry[itf]));
        }

        internal static bool CreateInstance(IndexRegistry registry, Type grainType, out GrainIndexes grainIndexes)
        {
            grainIndexes = registry.TryGetGrainIndexedInterfaces(grainType, out Type[] indexedInterfaces)
                            ? new GrainIndexes(registry, indexedInterfaces, registry.GetNullPropertyValuesForGrain(grainType))
                            : null;
            return grainIndexes != null;
        }

        internal bool HasAnyIndexes => this.interfaceToIndexMap.Count > 0;

        internal bool HasAnyUniqueIndex => this.interfaceToIndexMap.Any(indexes => indexes.Value.NamedIndexes.HasAnyUniqueIndex);
        internal bool HasAnyTotalIndex => this.interfaceToIndexMap.Any(indexes => indexes.Value.NamedIndexes.HasAnyTotalIndex);

        internal void MapStateToProperties(object state)
        {
            var grainStateType = state.GetType();
            this.interfaceToIndexMap.ForEach(kvp => createOrUpdatePropertiesFromState(kvp.Value));
            return;

            void createOrUpdatePropertiesFromState(InterfaceIndexes indexes)
            {
                indexes.Properties = indexes.PropertiesType.IsAssignableFrom(grainStateType) ? state : mapStateToProperties();
                return;

                object mapStateToProperties()
                {
                    // Copy named property values from this.State to indexes.Properties. The set of property names will not change.
                    // Note: TProperties is specified on IIndexableGrain<TProperties> with a "where TProperties: new()" constraint.
                    var properties = indexes.Properties ?? Activator.CreateInstance(indexes.PropertiesType);
                    indexes.PropertiesType
                        .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                        .ForEach(p => p.SetValue(properties, grainStateType.GetProperty(p.Name).GetValue(state)));
                    return properties;
                }
            }
        }

        /// <summary>
        /// This method checks the list of cached indexes, and if any index does not have a before-image, it will create
        /// one for it. As before-images are stored as an immutable field, a new map is created in this process.
        ///
        /// This method is called on activation of the grain, and when the UpdateIndexes method detects an inconsistency
        /// between the indexes in the index handler and the cached indexes of the current grain.
        /// </summary>
        internal void AddMissingBeforeImages(object state) => UpdateBeforeImages(state, force: false);

        internal void UpdateBeforeImages(object state, bool force)
        {
            void addMissingBeforeImages(InterfaceIndexes indexes)
            {
                var oldBefImgs = indexes.BeforeImages.Value;

                object getImage(string indexName, IIndexUpdateGenerator upGen)
                    => !force && oldBefImgs.TryGetValue(indexName, out var value) ? value : upGen.ExtractIndexImage(indexes.Properties);

                indexes.BeforeImages = (indexes.NamedIndexes
                                               .ToDictionary(kvp => kvp.Key, kvp => getImage(kvp.Key, kvp.Value.UpdateGenerator)) as IDictionary<string, object>)
                                               .AsImmutable();
            }

            MapStateToProperties(state);
            this.interfaceToIndexMap.ForEach(kvp => addMissingBeforeImages(kvp.Value));
        }

        /// <summary>
        /// This method assumes that a set of changes is applied to the indexes, and then it replaces the current before-images
        /// with after-images produced by the update.
        /// </summary>
        /// <param name="interfaceToUpdatesMap">the member updates that were successfully applied to the current indexes</param>
        internal void UpdateBeforeImages(InterfaceToUpdatesMap interfaceToUpdatesMap)
        {
            void updateBeforeImages(InterfaceIndexes indexes, IReadOnlyDictionary<string, IMemberUpdate> updates)
            {
                IDictionary<string, object> befImgs = new Dictionary<string, object>(indexes.BeforeImages.Value);
                foreach (var (indexName, opType) in updates.Select(u => (u.Key, u.Value.OperationType)))
                {
                    if (opType == IndexOperationType.Update || opType == IndexOperationType.Insert)
                    {
                        befImgs[indexName] = indexes.NamedIndexes[indexName].UpdateGenerator.ExtractIndexImage(indexes.Properties);
                    }
                    else if (opType == IndexOperationType.Delete)
                    {
                        befImgs[indexName] = null;
                    }
                }
                indexes.BeforeImages = befImgs.AsImmutable();
            }

            // Note that there may not be an index update for all interfaces; thus, iterate the updates list.
            interfaceToUpdatesMap.ForEach(kvp => updateBeforeImages(interfaceToIndexMap[kvp.Key], kvp.Value));
        }

        internal bool HasIndexImages => this.interfaceToIndexMap.Values.Any(itf => itf.HasIndexImages);

        #region <KeyValuePair<Type, InterfaceIndexes>>
        public IEnumerator<KeyValuePair<Type, InterfaceIndexes>> GetEnumerator() => this.interfaceToIndexMap.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => this.interfaceToIndexMap.GetEnumerator();
        #endregion <KeyValuePair<Type, InterfaceIndexes>>
    }
}
