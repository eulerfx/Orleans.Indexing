#nullable enable
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Orleans.Indexing;

internal static class TaskHelper
{
    /// <summary>
    /// Evaluates the tasks sequentially and returns the results.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="items"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static async Task<T[]> Sequential<T>(this IEnumerable<Task<T>> items, CancellationToken ct = default)
    {
        var results = new List<T>();
        foreach (var item in items)
        {
            ct.ThrowIfCancellationRequested();
            results.Add(await item);
        }

        return results.ToArray();
    }

    /// <summary>
    /// Awaits the result of the specified task and applies on it the specified function.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="t"></param>
    /// <param name="f"></param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static async Task<TResult> Then<T, TResult>(this Task<T> t, Func<T, TResult> f) => f(await t);
    
    /// <summary>
    /// Wraps the value in a completed <see cref="Task{T}"/>
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Task<T> ToCompletedTask<T>(this T value) => Task.FromResult(value);
    
    /// <summary>
    /// Awaits the tasks in parallel and returns the results when available.
    /// </summary>
    /// <typeparam name="T1"></typeparam>
    /// <typeparam name="T2"></typeparam>
    /// <param name="task1"></param>        
    /// <param name="task2"></param>
    /// <exception cref="System.AggregateException">When any of the input tasks throws.</exception>
    /// <returns>A task that produces a tuple of values from the input tasks.</returns>
    public static async Task<(T1, T2)> Parallel<T1, T2>(Task<T1> task1, Task<T2> task2)
    {
        await WhenAllThrottled(maxParallelism: 10, task1, task2);
        return (task1.Result, task2.Result);
    }
    
    [StackTraceHidden]
    static Task WhenAllThrottled(int maxParallelism = 10, params Task[] tasks) => tasks.Parallel(maxParallelism: maxParallelism);
    
    /// <summary>
    /// Runs tasks in parallel using <see cref="Task.WhenAll"/>. Note that the number of concurrent tasks is unbounded and can result in performance degredation (due to increased contention).
    /// </summary>
    /// <param name="tasks"></param>
    /// <returns></returns>
    public static Task Parallel(params Task[] tasks) => Task.WhenAll(tasks.AsEnumerable());

    /// <summary>
    /// Creates a <see cref="Task"/> than runs all the tasks in the collection in parallel with the given degree of parallelism, and completes when all the child tasks complete,
    /// returning the results in the same order as the inputs. If any tasks error, the operation will short-circuit.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="tasks"></param>
    /// <param name="maxParallelism"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task<T[]> Parallel<T>(this IEnumerable<Task<T>> tasks, int maxParallelism = 10, CancellationToken ct = default) =>
        tasks.Parallel(x => x, maxParallelism: maxParallelism, ct: ct);

    /// <summary>
    /// Runs the specified tasks in parallel with the given degree of parallelism.
    /// </summary>
    /// <param name="tasks"></param>
    /// <param name="maxParallelism"></param>
    /// <param name="ct"></param>
    /// <returns></returns>
    public static Task Parallel(this IEnumerable<Task> tasks, int maxParallelism = 10, CancellationToken ct = default) =>
        tasks.Parallel(x => x, maxParallelism: maxParallelism, ct: ct);

    /// <summary>
    /// Creates a <see cref="Task"/> than runs all the tasks in the collection in parallel and completes when all the child tasks complete.
    /// If any tasks error, the operation will short-circuit.
    /// </summary>
    public static async Task<TResult[]> Parallel<T, TResult>(this IEnumerable<T> items, Func<T, Task<TResult>> func, int maxParallelism = 10, CancellationToken ct = default)
    {
        if (maxParallelism <= 0 || maxParallelism > 100)
            throw new ArgumentException($"Invalid {nameof(maxParallelism)} value: {maxParallelism}");

        T[] input = items.ToArray();
        int count = input.Length;
        var output = new TResult[count];
        int lastIndex = -1;
        var mainTaskSource = new TaskCompletionSource();
        var taskSources = new TaskCompletionSource[count];
        for (int i = 0; i < taskSources.Length; i++)
        {
            taskSources[i] = new TaskCompletionSource();
        }

        for (int i = 0; i < maxParallelism; i++)
        {
            ProcessNext();
        }

        Task allTasks = Task.WhenAll(taskSources.Select(x => x.Task));
        await Task.WhenAny(allTasks, mainTaskSource.Task);

        if (allTasks.IsCompletedSuccessfully)
            return output;

        await mainTaskSource.Task;
        throw new InvalidOperationException("Should not get here because mainTaskSource must throw");

        void ProcessNext()
        {
            int newIndex = Interlocked.Increment(ref lastIndex);
            if (newIndex >= count)
                return;

            if (ct.IsCancellationRequested)
            {
                mainTaskSource.TrySetCanceled(ct);
                return;
            }

            func(input[newIndex]).ContinueWith(t =>
            {
                TaskCompletionSource taskSource = taskSources[newIndex];

                if (t.IsCanceled)
                {
                    mainTaskSource.TrySetCanceled(CancellationToken.None);
                    return;
                }

                if (t.IsFaulted)
                {
                    mainTaskSource.TrySetException(t.Exception!.InnerException!);
                    return;
                }

                output[newIndex] = t.Result;
                taskSource.TrySetResult();

                ProcessNext();
            }, ct);
        }
    }

    /// <summary>
    /// An overload that accepts an action instead of a func.
    /// </summary>
    public static Task Parallel<T>(this IEnumerable<T> items, Func<T, Task> action, int maxParallelism = 10, CancellationToken ct = default) =>
        items.Parallel(async x =>
            {
                await action(x);
                return true;
            },
            maxParallelism: maxParallelism,
            ct);

    /// <summary>
    /// Calls <see cref="ChannelReader{T}.WaitToReadAsync"/> and waits up to the given timeout, returning null if the timeout expires.
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="timeout"></param>
    /// <param name="ct"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static async Task<bool?> WaitToReadAsync<T>(this ChannelReader<T> reader, TimeSpan timeout, CancellationToken ct = default)
    {
        try
        {
            if (timeout <= TimeSpan.Zero)
                return null;
            return await reader.WaitToReadAsync(ct).AsTask().WaitAsync(timeout: timeout, ct);
        }
        catch (TimeoutException)
        {
            return null;
        }
    }
    
    /// <summary>
    /// Reads from the channel in a buffering mode, yielding when the buffer size reaches a given size or a given interval elapses.
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="count"></param>
    /// <param name="time"></param>
    /// <param name="ct"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns>An async sequence of batches of items read from the channel up to the given size and duration.</returns>
    public static async IAsyncEnumerable<IReadOnlyList<T>> ReadBufferByCountAndTime<T>(this ChannelReader<T> reader, int count, TimeSpan time, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var buffer = new List<T>(count);
        var sw = Stopwatch.StartNew();
        while (!ct.IsCancellationRequested)
        {
            var res = await reader.WaitToReadAsync(timeout: time - sw.Elapsed, ct);
            if (res is true)
            {
                while (reader.TryRead(out var item))
                {
                    buffer.Add(item);
                    if (buffer.Count >= count || sw.Elapsed >= time)
                    {
                        yield return buffer;
                        buffer.Clear();
                        sw = Stopwatch.StartNew();
                    }
                }
            }
            else if (res is null)
            {
                sw = Stopwatch.StartNew();
                if (buffer.Count > 0)
                {
                    yield return buffer;
                    buffer.Clear();                    
                }
            }
            else
            {                
                break;
            }
        }

        if (buffer.Count > 0)
        {
            yield return buffer;
            buffer.Clear();
        }
    }
}
