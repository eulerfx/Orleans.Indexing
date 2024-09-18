using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Concurrency;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Task = System.Threading.Tasks.Task;

#nullable enable

namespace Orleans.Indexing;

internal static class TestProcessManagerConstants
{
    public const string IndexedProcessStateStorage = "IndexedProcessStateStorage";
    public const string IndexingQueueStateStorage = "IndexingQueueStateStorage";
}

[TestClass]
public class IndexingSystemTests
{
    IHost? host;
    IClusterClient client;

    [TestInitialize]
    public async Task Setup()
    {
        this.host = await Host
            .CreateDefaultBuilder()
            .UseEnvironment(Environments.Development)
            .ConfigureAppConfiguration((hostContext, config) =>
            {
                config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: false);
                config.AddEnvironmentVariables();
            })
            .ConfigureLogging(ops =>
            {
                ops.AddSimpleConsole();
            })
            .UseOrleans((hbc, silo) =>
            {
                silo.UseLocalhostClustering();
                silo.UseInMemoryReminderService();
                silo.ConfigureLogging(log =>
                {
                    log.SetMinimumLevel(LogLevel.Information);
                });
                silo.UseLocalhostClustering();
                silo.AddMemoryGrainStorage(name: TestProcessManagerConstants.IndexedProcessStateStorage);
                silo.AddMemoryGrainStorage(name: TestProcessManagerConstants.IndexingQueueStateStorage);
                silo.AddMemoryGrainStorage(name: IndexingConstants.IndexStorageProviderName);
                silo.AddMemoryGrainStorage(name: IndexingConstants.IndexingStreamProviderName);
                silo.AddMemoryStreams(name: IndexingConstants.IndexingStreamProviderName);

                silo.UseIndexing(hbc.Configuration, ops =>
                {
                    ops.IndexingQueueStorageProviderName = TestProcessManagerConstants.IndexingQueueStateStorage;
                });
            })
            .StartAsync();

        this.client = host.Services.GetRequiredService<IClusterClient>();
    }


    [TestMethod]
    public async Task ShouldIndexAndQueryAll()
    {
        var timeout = TimeSpan.FromSeconds(10);

        var ids = Enumerable
            .Range(1, 100)
            .Select(x => $"P{x}")
            .ToArray();

        await ids.Parallel(Start);
        //await Task.Delay(1000);
        await ids.Parallel(GetState);
        //await ids.Parallel(QueryAndError);

        var grain = this.client.GetGrain<ITestIndexableProcessGrain>(ids[0]);

        var query = new TestIndexedProcessStateQuery
        {
            ProcessType = "test",
            StartedOnStart = DateTimeOffset.UtcNow.AddMinutes(-5),
            StartedOnEnd = DateTimeOffset.UtcNow.AddMinutes(5),
            Page = new PageInfo(Offset: 0, Size: 10)
        };

        var res = await grain.Query(query).WaitAsync(timeout);

        Console.WriteLine(res.Count);

        async Task<TestIndexedProcessState?> GetState(string id)
        {
            var g = this.client.GetGrain<ITestIndexableProcessGrain>(id);
            return await g.GetState();
        }

        async Task Start(string id)
        {
            var g = this.client.GetGrain<ITestIndexableProcessGrain>(id);
            await g.Start(new TestIndexedProcessStartRequest { ProcessId = Guid.NewGuid().ToString("N"), ProcessType = "test" });
        }

        async Task QueryAndError(string id)
        {
            var g = this.client.GetGrain<ITestIndexableProcessGrain>(id);
            var query = new TestIndexedProcessStateQuery
            {
                ProcessType = "test",
                StartedOnStart = DateTimeOffset.UtcNow.AddMinutes(-5),
                StartedOnEnd = DateTimeOffset.UtcNow.AddMinutes(5),
                Page = new PageInfo(Offset: 0, Size: 10)
            };
            var res = await g.Query(query);
            await g.Error("Error");
        }
    }

    [TestMethod]
    public async Task ShouldIndexAndQuery()
    {
        var timeout = TimeSpan.FromSeconds(10);

        var grain = this.client.GetGrain<ITestIndexableProcessGrain>("hello");

        var state = await grain.Start(new TestIndexedProcessStartRequest { ProcessId = Guid.NewGuid().ToString("N"), ProcessType = "test" });
        Assert.AreEqual("test", state.ProcessType);

        await Task.Delay(200);

        var query = new TestIndexedProcessStateQuery
        {
            ProcessType = "test",
            StartedOnStart = DateTimeOffset.Parse("2024-10-03"),
            StartedOnEnd = DateTimeOffset.Parse("2025-10-04"),
            Page = new PageInfo(Offset: 0, Size: 10)
        };

        var res = await grain.Query(query).WaitAsync(timeout);
        Assert.IsTrue(res.Count > 0);

        state = await grain.Error("Error").WaitAsync(timeout);
        Assert.AreEqual("Error", state.Status);

        await Task.Delay(500);

        res = await grain.Query(new() { Status = "Error" }).WaitAsync(timeout);
        Assert.IsTrue(res.Count == 1);
    }
}

class TestIndexableGrain : IIndexableGrain
{
    public Task<Immutable<HashSet<Guid>>> GetActiveIndexingActionIds() => Task.FromResult(new HashSet<Guid>().AsImmutable());

    public Task RemoveFromActiveIndexingActions(HashSet<Guid> ids) => Task.CompletedTask;
}
