using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Serialization.Cloning;

namespace UnitTests.StorageTests
{
    public static class SiloBuilderExtensions
    {
        public static ISiloBuilder AddTestStorageProvider<T>(this ISiloBuilder builder, string name) where T : IGrainStorage
        {
            return builder.AddTestStorageProvider(name, (sp, n) => ActivatorUtilities.CreateInstance<T>(sp));
        }

        public static ISiloBuilder AddTestStorageProvider<T>(this ISiloBuilder builder, string name, Func<IServiceProvider, string, T> createInstance) where T : IGrainStorage
        {
            return builder.ConfigureServices(services =>
            {
                services.AddKeyedSingleton<IGrainStorage>(name, (sp, n) => createInstance(sp, n.ToString()));

                if (typeof(ILifecycleParticipant<ISiloLifecycle>).IsAssignableFrom(typeof(T)))
                {
                    services.AddKeyedSingleton<ILifecycleParticipant<ISiloLifecycle>>(name, (svc, n) => (ILifecycleParticipant<ISiloLifecycle>)svc.GetRequiredKeyedService<IGrainStorage>(name));
                }

                if (typeof(IControllable).IsAssignableFrom(typeof(T)))
                {
                    services.AddKeyedSingleton<IControllable>(name, (svc, n) => (IControllable)svc.GetRequiredKeyedService<IGrainStorage>(name));
                }
            });
        }
    }

    // [DebuggerDisplay("MockStorageProvider:{Name}")]
    // public class MockStorageProvider : IControllable, IGrainStorage
    // {
    //     public enum Commands
    //     {
    //         InitCount,
    //         SetValue,
    //         GetProvideState,
    //         SetErrorInjection,
    //         GetLastState,
    //         ResetHistory
    //     }
    //     [Serializable]
    //     public class StateForTest
    //     {
    //         public int InitCount { get; set; }
    //         public int CloseCount { get; set; }
    //         public int ReadCount { get; set; }
    //         public int WriteCount { get; set; }
    //         public int DeleteCount { get; set; }
    //     }
    //
    //     private static int _instanceNum;
    //     private readonly int _id;
    //
    //     private int initCount, closeCount, readCount, writeCount, deleteCount;
    //
    //     private readonly int numKeys;
    //     private ILocalDataStore StateStore;
    //     private IDeepCopierProvider serializationManager;
    //     private const string stateStoreKey = "State";
    //     private ILogger logger;
    //     public string LastId { get; private set; }
    //     public object LastState { get; private set; }
    //
    //     public string Name { get; private set; }
    //
    //     public MockStorageProvider(ILoggerFactory loggerFactory, IDeepCopierProvider serializationManager)
    //         : this(Guid.NewGuid().ToString(), 2, loggerFactory, serializationManager)
    //     { }
    //
    //     public MockStorageProvider(string name, ILoggerFactory loggerFactory, IDeepCopierProvider serializationManager)
    //         : this(name, 2, loggerFactory, serializationManager)
    //     { }
    //
    //     public MockStorageProvider(string name, int numKeys, ILoggerFactory loggerFactory, IDeepCopierProvider serializationManager)
    //     {
    //         _id = ++_instanceNum;
    //         this.numKeys = numKeys;
    //
    //         this.Name = name;
    //         this.logger = loggerFactory.CreateLogger(string.Format("Storage.{0}-{1}", this.GetType().Name, this._id));
    //
    //         this.logger.LogInformation(0, "Init Name={0}", name);
    //         this.serializationManager = serializationManager;
    //         Interlocked.Increment(ref initCount);
    //
    //         StateStore = new HierarchicalKeyStore(numKeys);
    //
    //         logger.LogInformation(0, "Finished Init Name={0}", name);
    //     }
    //
    //     public StateForTest GetProviderState()
    //     {
    //         var state = new StateForTest();
    //         state.InitCount = initCount;
    //         state.CloseCount = closeCount;
    //         state.DeleteCount = deleteCount;
    //         state.ReadCount = readCount;
    //         state.WriteCount = writeCount;
    //         return state;
    //     }
    //
    //     [Serializable]
    //     public class SetValueArgs
    //     {
    //         public Type StateType { get; set; }
    //         public string GrainType { get; set; }
    //         public GrainReference GrainReference { get; set; }
    //         public string Name { get; set; }
    //         public object Val { get; set; }
    //
    //     }
    //
    //     public void SetValue(SetValueArgs args)
    //     {
    //         SetValue(args.StateType, args.GrainType, args.GrainReference.GrainId, args.Name, args.Val);
    //     }
    //
    //     private void SetValue(Type stateType, string grainType, GrainId grainReference, string name, object val)
    //     {
    //         lock (StateStore)
    //         {
    //             this.logger.LogInformation("Setting stored value {0} for {1} to {2}", name, grainReference, val);
    //             var keys = MakeGrainStateKeys(grainType, grainReference);
    //             var storedDict = StateStore.ReadRow(keys);
    //             if (!storedDict.ContainsKey(stateStoreKey))
    //             {
    //                 storedDict[stateStoreKey] = Activator.CreateInstance(stateType);
    //             }
    //
    //             var storedState = storedDict[stateStoreKey];
    //             var field = storedState.GetType().GetProperty(name).GetSetMethod(true);
    //             field.Invoke(storedState, new[] { val });
    //             LastId = GetId(grainReference);
    //             LastState = storedState;
    //         }
    //     }
    //
    //     public object GetLastState()
    //     {
    //         return LastState;
    //     }
    //
    //     public T GetLastState<T>()
    //     {
    //         return (T) LastState;
    //     }
    //
    //     private T GetLastState<T>(string grainType, GrainId grainReference, IGrainState<T> grainState)
    //     {
    //         lock (StateStore)
    //         {
    //             var keys = MakeGrainStateKeys(grainType, grainReference);
    //             var storedStateRow = StateStore.ReadRow(keys);
    //             if (!storedStateRow.ContainsKey(stateStoreKey))
    //             {
    //                 storedStateRow[stateStoreKey] = Activator.CreateInstance(grainState.State.GetType());
    //             }
    //
    //             var storedState = storedStateRow[stateStoreKey];
    //             LastId = GetId(grainReference);
    //             LastState = storedState;
    //             return (T)storedState;
    //         }
    //     }
    //
    //     public virtual Task Close()
    //     {
    //         logger.LogInformation(0, "Close");
    //         Interlocked.Increment(ref closeCount);
    //         StateStore.Clear();
    //         return Task.CompletedTask;
    //     }
    //
    //     public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    //     {
    //         logger.LogInformation(0, "ReadStateAsync for {0} {1}", stateName, grainId);
    //         Interlocked.Increment(ref readCount);
    //         lock (StateStore)
    //         {
    //             var storedState = GetLastState(stateName, grainId, grainState);
    //
    //             grainState.State = this.serializationManager.GetDeepCopier<T>().DeepCopy(storedState, null); // Read current state data
    //         }
    //         return Task.CompletedTask;
    //     }
    //
    //
    //     public virtual Task WriteStateAsync<T>(string grainType, GrainReference grainReference, IGrainState<T> grainState)
    //     {
    //         logger.LogInformation(0, "WriteStateAsync for {0} {1}", grainType, grainReference);
    //         Interlocked.Increment(ref writeCount);
    //         lock (StateStore)
    //         {
    //             var storedState = this.serializationManager.GetDeepCopier<T>().DeepCopy(grainState.State, null); // Store current state data
    //             var stateStore = new Dictionary<string, object> {{ stateStoreKey, storedState }};
    //             StateStore.WriteRow(MakeGrainStateKeys(grainType, grainReference.GrainId), stateStore, grainState.ETag);
    //
    //             LastId = GetId(grainReference.GrainId);
    //             LastState = storedState;
    //         }
    //         return Task.CompletedTask;
    //     }
    //
    //     public virtual Task ClearStateAsync<T>(string grainType, GrainReference grainReference, IGrainState<T> grainState)
    //     {
    //         logger.LogInformation(0, "ClearStateAsync for {0} {1}", grainType, grainReference);
    //         Interlocked.Increment(ref deleteCount);
    //         var keys = MakeGrainStateKeys(grainType, grainReference.GrainId);
    //         lock (StateStore)
    //         {
    //             StateStore.DeleteRow(keys, grainState.ETag);
    //             LastId = GetId(grainReference.GrainId);
    //             LastState = null;
    //         }
    //         return Task.CompletedTask;
    //     }
    //
    //     private static string GetId(GrainId grainReference)
    //     {
    //         return grainReference.ToString();
    //         //.ToKeyString();
    //     }
    //     private static IList<Tuple<string, string>> MakeGrainStateKeys(string grainType, GrainId grainReference)
    //     {
    //         return new[]
    //         {
    //             Tuple.Create("GrainType", grainType),
    //             Tuple.Create("GrainId", GetId(grainReference))
    //         }.ToList();
    //     }
    //
    //     public void ResetHistory()
    //     {
    //         // initCount = 0;
    //         closeCount = readCount = writeCount = deleteCount = 0;
    //         LastId = null;
    //         LastState = null;
    //     }
    //
    //     /// <summary>
    //     /// A function to execute a control command.
    //     /// </summary>
    //     /// <param name="command">A serial number of the command.</param>
    //     /// <param name="arg">An opaque command argument</param>
    //     public virtual Task<object> ExecuteCommand(int command, object arg)
    //     {
    //         switch ((Commands)command)
    //         {
    //             case Commands.InitCount:
    //                 return Task.FromResult<object>(initCount);
    //             case Commands.SetValue:
    //                 SetValue((SetValueArgs) arg);
    //                 return Task.FromResult<object>(true);
    //             case Commands.GetProvideState:
    //                 return Task.FromResult<object>(GetProviderState());
    //             case Commands.GetLastState:
    //                 return Task.FromResult(GetLastState());
    //             case Commands.ResetHistory:
    //                 ResetHistory();
    //                 return Task.FromResult<object>(true);
    //             default:
    //                 return Task.FromResult<object>(true);
    //         }
    //     }
    //
    //     public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) => throw new NotImplementedException();
    //
    //     public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) => throw new NotImplementedException();
    // }
}
