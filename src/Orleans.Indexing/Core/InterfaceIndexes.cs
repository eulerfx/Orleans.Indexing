using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Concurrency;

namespace Orleans.Indexing
{
    internal class InterfaceIndexes
    {
        /// <summary>
        /// The indexes defined on the indexable object.
        /// </summary>
        internal NamedIndexMap NamedIndexes { get; }

        /// <summary>
        /// The indexed properties object.
        /// </summary>
        internal object Properties { get; set; }

        /// <summary>
        /// The type of the indexed properties object.
        /// </summary>
        internal Type PropertiesType => this.NamedIndexes.PropertiesClassType;

        /// <summary>
        /// An immutable copy of before-images of the indexed fields
        /// </summary>
        internal Immutable<IDictionary<string, object>> BeforeImages = new Dictionary<string, object>().AsImmutable<IDictionary<string, object>>();

        internal InterfaceIndexes(NamedIndexMap indexes) => this.NamedIndexes = indexes;

        internal bool HasIndexImages => this.BeforeImages.Value.Values.Any(obj => obj != null);
    }
}
