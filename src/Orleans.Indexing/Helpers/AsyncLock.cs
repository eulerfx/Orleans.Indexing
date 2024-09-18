#nullable enable
using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Indexing;

/// <summary>
/// An async lock based on <see cref="SemaphoreSlim"/>.
/// </summary>
internal class AsyncLock
{
    readonly SemaphoreSlim semaphore = new(1);

    public Task<IDisposable> LockAsync()
    {
        var waitTask = semaphore.WaitAsync();
        IDisposable releaser = new LockReleaser(this);
        return waitTask.IsCompleted
            ? Task.FromResult(releaser)
            : waitTask.ContinueWith(_ => releaser, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
    }

    public async Task Execute(Action action)
    {
        using (await LockAsync())
            action();
    }

    class LockReleaser(AsyncLock? target) : IDisposable
    {
        AsyncLock? target = target;

        public void Dispose()
        {
            if (target is null)
                return;

            var tmp = target;
            target = null;
            try
            {
                tmp.semaphore.Release();
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }
}
