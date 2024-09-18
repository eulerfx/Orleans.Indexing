#nullable enable
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Orleans.Indexing;

/// <summary>
/// An indexable grain uses <see cref="IIndexedState{TState}"/>.
/// </summary>
public interface IIndexableGrain : IGrain, IIndexingPipelineParticipant {}

/// <summary>
/// An indexable grain with indexed property type <typeparamref name="TState"/> uses <see cref="IIndexedState{TState}"/>.
/// </summary>
/// <typeparam name="TState"></typeparam>
public interface IIndexableGrain<TState> : IIndexableGrain {}

/// <summary>
/// Implemented by indexable states and the indexable grains that host them.
/// </summary>
public interface IIndexingPipelineParticipant
{
    /// <summary>
    /// Gets the active indexing actions associated to the grain.
    /// </summary>
    /// <returns></returns>
    Task<Immutable<HashSet<Guid>>> GetActiveIndexingActionIds();
    
    /// <summary>
    /// Removes actions from the list of active indexing actions associated to the grain.
    /// </summary>
    /// <param name="ids"></param>
    /// <returns></returns>
    Task RemoveFromActiveIndexingActions(HashSet<Guid> ids);
}
