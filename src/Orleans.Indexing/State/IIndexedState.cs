#nullable enable
using System;
using System.Threading.Tasks;

namespace Orleans.Indexing;

/// <summary>
/// Indexed property state.
/// </summary>
/// <typeparam name="TState">The indexed properties type.</typeparam>
public interface IIndexedState<TState> : IIndexingPipelineParticipant
{
    /// <summary>
    /// Performs a read operation and returns the result, without modifying the state.
    /// </summary>
    /// <typeparam name="TResult">The type of the return value</typeparam>
    /// <param name="read">A function that reads the state and returns the result. MUST NOT modify the state.</param>
    Task<TResult> PerformRead<TResult>(Func<TState, TResult> read);

    /// <summary>
    /// Performs an update operation and returns the result.
    /// </summary>
    /// <typeparam name="TResult">The type of the return value</typeparam>
    /// <param name="update">A function that can read and update the state, and return a result</param>
    Task<TResult> PerformUpdate<TResult>(Func<TState, TResult> update);
}
