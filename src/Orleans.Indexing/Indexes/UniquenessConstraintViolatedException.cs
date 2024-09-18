#nullable enable
using System;

namespace Orleans.Indexing;

/// <summary>
/// Indicates that a uniqueness constraint on an index with IsUnique=true is violated during an attempted index update.
/// </summary>
/// <param name="message"></param>
/// <param name="inner"></param>
[Serializable]
public class UniquenessConstraintViolatedException(string message, Exception? inner = default) 
    : Exception(message, innerException: inner)
{
}
