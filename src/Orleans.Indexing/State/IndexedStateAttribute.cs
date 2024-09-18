#nullable enable
using System;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Indexing;

/// <summary>
/// Specifies <see cref="IndexedStateOptions"/> for an <see cref="IndexedState{TProperties}"/> instance.
/// </summary>
/// <param name="StorageName"></param>
/// <param name="StateName"></param>
[AttributeUsage(AttributeTargets.Property|AttributeTargets.Parameter)]
public class IndexedStateAttribute(string? StorageName = null, string? StateName = null) : Attribute, IFacetMetadata
{
    public string? StorageName { get; } = StorageName;

    public string? StateName { get; } = StateName;
    
    public IndexedStateOptions GetOptions() => new()
    {
        StorageName = StorageName.EnsureNotNull(), 
        StateName = StateName.EnsureNotNull()
    };
}


internal class IndexedStateAttributeMapper : IAttributeToFactoryMapper<IndexedStateAttribute>
{
    static readonly MethodInfo CreateMethod = typeof(IndexManager).GetMethod(nameof(IndexManager.CreateIndexedState))!;

    public Factory<IGrainContext, object> GetFactory(ParameterInfo parameter, IndexedStateAttribute options)
        => GetFactory(CreateMethod, parameter, options.GetOptions());
    
    Factory<IGrainContext, object> GetFactory(MethodInfo creator, ParameterInfo parameter, IndexedStateOptions indexingConfig)
    {
        var genericCreate = creator.MakeGenericMethod(parameter.ParameterType.GetGenericArguments());
        return context => Create(context, genericCreate, [ indexingConfig ]);
    }

    object Create(IGrainContext context, MethodInfo genericCreate, object[] args)
    {
        var factory = context.ActivationServices.GetRequiredService<IndexManager>();
        var state = genericCreate.Invoke(factory, args);
        return state.EnsureNotNull();
    }
}
