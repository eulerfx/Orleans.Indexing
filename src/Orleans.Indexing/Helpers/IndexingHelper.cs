#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Serialization.TypeSystem;
using Orleans.Streams;

namespace Orleans.Indexing;

public static class IndexingConstants
{
    public const string IndexStorageProviderName = "HashIndexStorageName";    
    public const string IndexingStreamProviderName = "IndexingStreamProviderName";
}

internal static class IndexingHelper
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string GetFullTypeName(Type type) => RuntimeTypeNameFormatter.Format(type);
    
    /// <summary>
    /// Computes a stable hash-code for the given grain class.
    /// </summary>
    /// <param name="grainClass"></param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)] 
    public static uint GetGrainClassTypeCode(Type grainClass) => StableHash.ComputeHash(RuntimeTypeNameFormatter.Format(grainClass));
    
    
    internal static GrainReference AsWeaklyTypedReference(this IAddressable grain)
    {
        if (grain is GrainReference reference)
        {
            return reference;
        }

        if (grain is Grain grainBase)
        {
            return grainBase.GrainReference ?? throw new ArgumentException("WRONG_GRAIN_ERROR_MSG", nameof(grain));
        }

        return grain is GrainService grainService
            ? grainService.GrainReference
            : throw new ArgumentException($"AsWeaklyTypedReference has been called on an unexpected type: {grain.GetType().FullName}.", nameof(grain));
    }
    
    /// <summary>
    /// Gets the primary key of an index (host) grain.
    /// </summary>
    /// <param name="grainInterfaceType"></param>
    /// <param name="indexName"></param>
    /// <returns>{<paramref name="grainInterfaceType"/>}-{<paramref name="indexName"/>}</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string GetIndexGrainPrimaryKey(Type grainInterfaceType, string indexName) => $"{GetFullTypeName(grainInterfaceType)}-{indexName}";    
    
    /// <summary>
    /// Gets the primary key of an index bucket grain for the given indexable grain interface, index name, and value hash code.
    /// </summary>
    /// <param name="grainInterfaceType"></param>
    /// <param name="indexName"></param>
    /// <param name="hashCode">The hash code is appended at the end, after an underscore</param>
    /// <returns>'{<paramref name="grainInterfaceType"/>}-{<paramref name="indexName"/>}_{<paramref name="hashCode"/>}' where <paramref name="indexName"/> = '_{PropertyName}_' </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)] 
    public static string GetIndexGrainBucketPrimaryKeyByHashCode(Type grainInterfaceType, string indexName, string hashCode) =>
        GetIndexGrainPrimaryKey(grainInterfaceType: grainInterfaceType, indexName) + "_" + hashCode;
        
    /// <summary>
    /// Gets {indexName} from grain primary key string '{grainInterfaceType}-{indexName}_{hashCode}'. 
    /// </summary>
    /// <param name="grain"></param>
    /// <returns></returns>
    internal static string GetIndexNameFromIndexGrain(this IAddressable grain)
    {
        var key = grain.GetPrimaryKeyString();
        return key[(key.IndexOf('-') + 1)..key.LastIndexOf('-')];
    }
    
    /// <summary>
    /// Gets the name of an index based on an indexed property.
    /// </summary>
    /// <param name="indexedProperty"></param>
    /// <returns>_{<paramref name="indexedProperty"/>}</returns>
    public static string PropertyNameToIndexName(string indexedProperty) => $"_{indexedProperty}";
        
    /// <summary>
    /// Gets the type of the interface implemented by an indexable grain class.
    /// </summary>
    /// <param name="indexType"></param>
    /// <param name="indexedProperty"></param>
    /// <returns></returns>
    /// <exception cref="NotSupportedException"></exception>
    internal static Type GetIndexableGrainInterfaceType(Type indexType, PropertyInfo indexedProperty)
    {
        var indexHostType = indexType.GetGenericType(typeof(IIndex<,>))
            ?? throw new NotSupportedException("Adding an index that does not implement IIndex<TKey, TGrain> is not supported.");

        var indexTypeArgs = indexHostType.GetGenericArguments();
        var keyType = indexTypeArgs[0];
        var grainInterfaceType = indexTypeArgs[1];
        if (keyType != indexedProperty.PropertyType)
        {
            throw new NotSupportedException($"Index <{indexType}, TGrain> does not match property '{indexedProperty.PropertyType} {indexedProperty.Name}'");
        }

        return grainInterfaceType;
    }
    
    static Type? GetGenericType(this Type givenType, Type genericInterfaceType)
    {
        var interfaceTypes = givenType.GetInterfaces();

        foreach (var it in interfaceTypes)
        {
            if (it.IsGenericType && it.GetGenericTypeDefinition() == genericInterfaceType)
                return it;
        }

        if (givenType.IsGenericType && givenType.GetGenericTypeDefinition() == genericInterfaceType)
        {
            return givenType;
        }

        var baseType = givenType.BaseType;
        return baseType == null ? null : GetGenericType(baseType, genericInterfaceType);
    }
        
    internal static int GetInvariantHashCode(this object item) => item is string s ? GetInvariantStringHashCode(s) : item.GetHashCode();

    internal static int GetInvariantStringHashCode(this string item)
    {
        // NetCore randomizes string.GetHashCode() per-appdomain, to prevent hash flooding.
        // Therefore, it's important to verify for each call site that this isn't a concern.
        // This is a non-unsafe/unchecked version of (internal) string.GetLegacyNonRandomizedHashCode().
        unchecked
        {
            var hash1 = (5381 << 16) + 5381;
            var hash2 = hash1;
            for (var i = 0; i < item.Length; i += 2)
            {
                hash1 = ((hash1 << 5) + hash1) ^ item[i];
                if (i < item.Length - 1)
                {
                    hash2 = ((hash2 << 5) + hash2) ^ item[i + 1];
                }
            }
            return hash1 + (hash2 * 1566083941);
        }
    }
    
    /// <summary>
    /// Gets the id of the next index partition grain in a chain given a partition grain.
    /// </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    internal static string GetNextBucketIdInChain(IAddressable index)
    {
        var key = index.GetPrimaryKeyString();
        var next = 1;
        if (key.Split('-').Length == 3)
        {
            var lastDashIndex = key.LastIndexOf('-');
            next = int.Parse(key[(lastDashIndex + 1)..]) + 1;
            return key[..(lastDashIndex + 1)] + next;
        }
        return key + "-" + next;
    }
}
