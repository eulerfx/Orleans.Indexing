#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Orleans.Indexing;

/// <summary>
/// A registry of all indexable grain classes.
/// </summary>
public class IndexableGrainInterfaceRegistry
{        
    public required IReadOnlyList<Type> GrainInterfaces { get; init; }
    
    public required IReadOnlyList<(Type, Type)> GrainInterfacesWithStates { get; init; }
    
    IndexableGrainInterfaceRegistry() {}

    public static IndexableGrainInterfaceRegistry Create(Assembly assembly)
    {
        var grainInterfaces = GetIndexableGrainInterfaces(assembly).ToArray();
        if (grainInterfaces.Length == 0)
            throw new InvalidOperationException("No indexable grain types found!");
        var registry = new IndexableGrainInterfaceRegistry()
        {
            GrainInterfaces = grainInterfaces, 
            GrainInterfacesWithStates = GetIndexableGrainInterfacesWithStateClasses(assembly, grainInterfaces).ToArray()
        };
        return registry;
    }

    static IEnumerable<Type> GetIndexableGrainInterfaces(Assembly assembly) =>
        assembly
            .GetTypes()
            .Where(type => typeof(IIndexableGrain).IsAssignableFrom(type) && type is { IsInterface: false, IsClass: true, IsAbstract: false })
            .SelectMany(type =>
            {
                var ifs = type.GetInterfaces().Where(x => typeof(IIndexableGrain).IsAssignableFrom(x) && x.IsInterface && x.Assembly == assembly).ToArray();
                return ifs;
            })
            .Distinct();
    
    static IEnumerable<(Type GrainInterface, Type StateClass)> GetIndexableGrainInterfacesWithStateClasses(Assembly assembly, IReadOnlyList<Type> grainInterfaces)
    {
        foreach (var grainInterface in grainInterfaces)
        {
            if (grainInterface.Assembly == assembly)
            {
                var stateClass = GetStateType(grainInterface);
                if (stateClass is not null)
                    yield return (grainInterface, stateClass);
            }
        }
    }

    static Type? GetStateType(Type indexableGrainInterface)
    {
        var stateType = indexableGrainInterface
            .GetInterfaces()
            .Where(type => type.IsGenericType && typeof(IIndexableGrain<>) == type.GetGenericTypeDefinition())
            .Select(x => x.GetGenericArguments()[0])
            .FirstOrDefault();

        if (stateType is not null)
            return stateType;
        
        return null;
    }
}
