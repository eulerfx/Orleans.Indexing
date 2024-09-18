using System;
using System.Threading.Tasks;
using Orleans.Core;
using Orleans.Storage;
using Orleans.Transactions.Abstractions;

namespace Orleans.Indexing;

/// <summary>
/// A noop adapter between <see cref="IStorage{TGrainState}"/> and <see cref="ITransactionalState{TGrainState}"/>
/// </summary>
/// <typeparam name="TState"></typeparam>
public class NonTransactionalState<TState>(IStorage<TState> storage) : ITransactionalState<TState>
    where TState : class, new()
{
    public static async Task<NonTransactionalState<TState>> CreateAsync(IStorage<TState> storage)
    {
        await storage.ReadStateAsync();
        return new NonTransactionalState<TState>(storage);
    }

    internal TState State => storage.State;

    public void Initialize(TState state)
    {
        if (storage.State is null)
            storage.State = state;
    }

    public Task<TResult> PerformRead<TResult>(System.Func<TState, TResult> readFunction)
        => Task.FromResult(readFunction(State));

    public Task PerformUpdate() => PerformUpdate(_ => true);

    public Task<TResult> PerformUpdate<TResult>(Func<TState, TResult> updateFunction) => PerformUpdateWithRetry(updateFunction);

    async Task<TResult> PerformUpdateWithRetry<TResult>(Func<TState, TResult> updateFunction, int maxRetries = 10)
    {
        var retries = 0;
        while (true)
        {       
            var result = updateFunction(State);
            try
            {
                await storage.WriteStateAsync();
                return result;
            }
            catch (InconsistentStateException)
            {
                if (retries++ <= maxRetries)
                {
                    await Task.Delay(retries * 100);
                    await storage.ReadStateAsync();
                }
                else
                {
                    throw;
                }
            }
        }
    }
}
