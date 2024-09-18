using System;
using System.Runtime.Serialization;

namespace Orleans.Indexing
{
    /// <summary>
    /// This exception is thrown when an indexing operation exception is encountered.
    /// </summary>
    [Serializable]
    [GenerateSerializer]
    public class IndexOperationException : IndexException
    {
        public IndexOperationException(string message) : base(message)
        {
        }

        [Obsolete]
        protected IndexOperationException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
