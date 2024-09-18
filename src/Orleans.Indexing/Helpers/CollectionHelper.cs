#nullable enable
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Orleans.Indexing;

/// <summary>
/// The ways in which a range overlaps with a partition.
/// </summary>
public enum RangeOverlapType
{
    /// <summary>
    /// The range refers to a partition prior to this one.
    /// </summary>
    LessThan,
    
    /// <summary>
    /// The range overlaps partially, and refers to a partition prior to this one.
    /// </summary>
    PartialLessThan,
    
    /// <summary>
    /// The range is completely covered by the partition.
    /// The partition is a superset of the range.
    /// </summary>
    Superset,
    
    /// <summary>
    /// The range overlaps partially, and refers to a partition after this one.
    /// </summary>
    PartialGreaterThan,
    
    /// <summary>
    /// The range refers to a partition after this one.
    /// </summary>
    GreaterThan,
    
    /// <summary>
    /// The partition covers a proper subset of the range.
    /// </summary>
    Subset
}


/// <summary>
/// Helpers on collections.
/// </summary>
internal static class CollectionHelper
{
    /// <summary>
    /// Indicates whether there's any overlap between the range and the partition.
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    public static bool HasOverlap(this RangeOverlapType type) => 
        type is RangeOverlapType.Subset or RangeOverlapType.PartialGreaterThan or RangeOverlapType.Superset or RangeOverlapType.PartialLessThan;
    
    /// <summary>
    /// Adds a page of items from this list into the destination.  
    /// </summary>
    /// <param name="items"></param>
    /// <param name="destination"></param>
    /// <param name="offset">The starting offset. Must be non-negative.</param>
    /// <param name="limit">The page size. Must be positive.</param>
    /// <typeparam name="T"></typeparam>
    /// <exception cref="ArgumentOutOfRangeException"> <paramref name="offset"/> or <paramref name="limit"/> are invalid</exception>
    public static int GetPage<T>(this IReadOnlyList<T> items, int offset, int limit, ICollection<T> destination)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset, nameof(offset));
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1, nameof(limit));
        
        var added = 0;
        for (var i = offset; i < items.Count; i++)
        {
            destination.Add(items[i]);            
            if (++added >= limit)
            {
                break;
            }
        }

        return added;
    }
    
    /// <summary>
    /// Adds a page of items from this list into the destination.
    /// </summary>
    /// <param name="items"></param>
    /// <param name="offset"></param>
    /// <param name="limit"></param>
    /// <param name="destination"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    /// <exception cref="ArgumentOutOfRangeException"> <paramref name="offset"/> or <paramref name="limit"/> are invalid</exception>
    public static int GetPage<T>(this IList<T> items, int offset, int limit, ICollection<T> destination)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset, nameof(offset));
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1, nameof(limit));
        
        var added = 0;
        for (var i = offset; i < items.Count; i++)
        {
            destination.Add(items[i]);            
            if (++added >= limit)
            {
                break;
            }
        }

        return added;
    }
    
    /// <summary>
    /// Ensures that the value is not null, throwing <see cref="InvalidOperationException"/> otherwise.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="value"></param>
    /// <param name="message"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    [return: NotNull]
    public static T EnsureNotNull<T>(this T? value, string? message = null) =>
        value is null ? throw new InvalidOperationException(message ?? $"The value of type {typeof(T)} is required.") : value;
    
    /// <summary>
    /// Where(x => x is not null)
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="source"></param>
    /// <returns></returns>
    public static IEnumerable<T> WhereNotNull<T>(this IEnumerable<T?>? source) =>
        (source ?? []).Where(x => x is not null)!;
    
    /// <summary>
    /// Gets the value transformed by the given function if not null and null otherwise.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="value">The possibly null value.</param>
    /// <param name="f">The function to apply to a non-null value.</param>
    /// <returns>Null if the input is null, or the value transformed by the given function.</returns>
    public static TResult? GetValueOrDefault<T, TResult>(this T? value, Func<T, TResult> f) where T : struct where TResult : struct
    {
        if (value.HasValue)
        {
            return f(value.Value);
        }
        else
        {
            return default;
        }
    }
    
    /// <summary>
    /// Applies an action to each item in the collection.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="items"></param>
    /// <param name="action"></param>
    public static void ForEach<T>(this IEnumerable<T> items, Action<T> action)
    {
        foreach (var item in items)
        {
            action(item);
        }
    }
    
    /// <summary>
    /// Converts the collection to <see cref="IReadOnlyList{T}"/> casting if it is already of the required type,
    /// otherwise calling <see cref="Enumerable.ToArray{T}(IEnumerable{T})"/>.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="items"></param>
    /// <returns></returns>
    /// <remarks>The point of this is to avoid calling ToArray unless required.</remarks>
    public static IReadOnlyList<T> ToReadOnlyList<T>(this IEnumerable<T>? items) => items switch
    {
        null => Array.Empty<T>(),
        T[] a => a,
        List<T> a => a,
        IReadOnlyList<T> a => a,
        _ => items.ToArray()
    };
    
    /// <summary>
    /// Adds a range of values to a collection.
    /// </summary>
    /// <param name="collection"></param>
    /// <param name="items"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns>The number of items added.</returns>
    public static int AddRange<T>(this ICollection<T> collection, IEnumerable<T?> items)
    {
        var added = 0;
        foreach (var item in items)
        {
            if (item is not null)
            {
                collection.Add(item);
                added++;
            }
        }

        return added;
    }
    
    /// <summary>
    /// Gets a value with the specified key and returns it or adds a new value and returns it.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="dict"></param>
    /// <param name="key"></param>
    /// <param name="addValue"></param>
    /// <exception cref="ArgumentNullException"></exception>
    /// <returns></returns>
    public static TValue GetOrAddValue<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key, TValue addValue)
    {
        if (dict.TryGetValue(key, out TValue? value))
        {
            return value;
        }
        else
        {
            dict.Add(key, addValue);
            return addValue;
        }
    }
    
    /// <summary>
    /// Gets a value with the specified key and returns it or adds a new value and returns it.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="dict"></param>
    /// <param name="key"></param>
    /// <param name="addValue"></param>
    /// <exception cref="ArgumentNullException"></exception>
    /// <returns></returns>
    public static TValue GetOrAddValue<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key, Func<TValue> addValue)
    {
        if (dict.TryGetValue(key, out TValue? value))
        {
            return value;
        }
        else
        {
            var v = addValue();
            dict.Add(key, v);
            return v;
        }
    }
    
    /// <summary>
    /// Gets the value at <paramref name="key"/> or returns <paramref name="defaultValue"/>.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="dict"></param>
    /// <param name="key"></param>
    /// <param name="defaultValue"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentNullException"></exception>
    public static TValue? TryGetValue<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key, TValue? defaultValue = default) =>
        dict.TryGetValue(key, out TValue? value) ? value : defaultValue;
    
    /// <summary>
    /// Removes key-value pairs for which the predicate returns true.
    /// </summary>
    /// <param name="dictionary"></param>
    /// <param name="predicate"></param>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public static void RemoveWhere<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, Func<KeyValuePair<TKey, TValue>, bool> predicate)
    {
        foreach (var kvp in dictionary.ToArray())
        {
            if (predicate(kvp))
            {
                dictionary.Remove(kvp.Key);
            }
        }
    }
    
    /// <summary>
    /// Returns the index of the given value in the list. 
    /// </summary>
    /// <param name="array"></param>
    /// <param name="index"></param>
    /// <param name="length"></param>
    /// <param name="value"></param>
    /// <param name="comparer"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns>
    /// If the list does not contain the given value, the method returns a negative
    /// integer. The bitwise complement operator (~) can be applied to a
    /// negative result to produce the index of the first element (if any) that
    /// is larger than the given search value. This is also the index at which
    /// the search value should be inserted into the list in order for the list
    /// to remain sorted.
    /// </returns>
    internal static int InternalBinarySearch<T>(IList<T> array, int index, int length, T value, IComparer<T> comparer)
    {
        Debug.Assert(array != null, "Check the arguments in the caller!");
        Debug.Assert(index >= 0 && length >= 0 && (array.Count - index >= length), "Check the arguments in the caller!");

        int lo = index;
        int hi = index + length - 1;
        while (lo <= hi)
        {
            int i = lo + ((hi - lo) >> 1);
            int order = comparer.Compare(array[i], value);
            if (order == 0)
                return i;
            if (order < 0)
            {
                lo = i + 1;
            }
            else
            {
                hi = i - 1;
            }
        }

        return ~lo;
    }

    /// <summary>
    /// Gets all the values within the given range (inclusive).
    /// </summary>
    /// <param name="d"></param>
    /// <param name="start">The starting key.</param>
    /// <param name="end">The ending key.</param>
    /// <param name="offset">The page offset.</param>
    /// <param name="size">The page size.</param>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TKey"></typeparam>
    /// <returns></returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public static List<T> GetValuesInRange<TKey, T>(this SortedList<TKey, T> d, TKey start, TKey end, int offset = 0, int size = 10000) where TKey : notnull
    {
        var res = new List<T>(); 
        d.GetValuesInRange(start: start, end: end, offset: offset, size: size, res); 
        return res;
    }
    
    /// <summary>
    /// Gets the index of the given key, or the index of the next key in the sort order.
    /// The existing <see cref="SortedList{TKey,TValue}.IndexOfKey"/> does not return anything if there's no exact match.
    /// </summary>
    /// <param name="list"></param>
    /// <param name="key"></param>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="T"></typeparam>
    /// <returns>
    /// Returns -1 if the key is less than any key in the list.
    /// Returns <see cref="SortedList{TKey,TValue}.Count"/> + 1 if the key is greater than any key in the list.
    /// Returns the index of the key for an exact match, or the index of the next key on the list.
    /// </returns>
    public static int IndexOfKeyOrNext<TKey, T>(this SortedList<TKey, T> list, TKey key) where TKey : notnull
    {        
        var index = InternalBinarySearch(list.Keys, index: 0, length: list.Count, value: key, comparer: list.Comparer);
        if (index < 0)
        {
            index = ~index;
            if (index == 0)
                return -1;
        }
        return index;
    }

    /// <summary>
    /// Gets all the values within the given range (inclusive).
    /// </summary>
    /// <param name="list"></param>
    /// <param name="start"></param>
    /// <param name="end"></param>
    /// <param name="offset"></param>
    /// <param name="size"></param>
    /// <param name="result"></param>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static int GetValuesInRange<TKey, T>(this SortedList<TKey, T> list, TKey start, TKey end, int offset, int size, ICollection<T> result) where TKey : notnull
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(size, nameof(size));
        ArgumentOutOfRangeException.ThrowIfNegative(offset, nameof(offset));
        ArgumentOutOfRangeException.ThrowIfNegative(-list.Comparer.Compare(start, end), paramName: nameof(start));
        
        var startIndex = list.IndexOfKeyOrNext(key: start);
        if (startIndex < 0)
            startIndex = 0;
        startIndex += offset;
        
        var endIndex = list.IndexOfKeyOrNext(key: end);
        if (endIndex < 0)
            endIndex = list.Count - 1;
        
        var added = 0;
        for (var i = startIndex; i <= endIndex && i < list.Count && result.Count < size; i++)
        {
            result.Add(list.Values[i]);
            added++;
        }

        return added;
    }
    
    
    /// <summary>
    /// Gets the overlap type between the range specified by <paramref name="start"/> and <paramref name="end"/> with respect to the keys in the list.
    /// </summary>
    /// <param name="list"></param>
    /// <param name="start"></param>
    /// <param name="end"></param>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    /// <exception cref="ArgumentOutOfRangeException">The range is invalid</exception>
    public static RangeOverlapType GetRangeOverlap<TKey, T>(this SortedList<TKey, T> list, TKey start, TKey end) where TKey : notnull
    {
        var startIndex = list.IndexOfKeyOrNext(key: start);
        var endIndex = list.IndexOfKeyOrNext(key: end);
        ArgumentOutOfRangeException.ThrowIfLessThan(endIndex, startIndex, paramName: nameof(end));
        return (startIndex, endIndex) switch
        {
            (_,_) when startIndex >= 0 && startIndex < list.Count && endIndex >= 0 && endIndex < list.Count => RangeOverlapType.Superset,
            (_,_) when startIndex >= 0 && startIndex < list.Count && endIndex == list.Count => RangeOverlapType.PartialGreaterThan,
            (_,_) when startIndex == -1 && endIndex >= 0 && endIndex < list.Count => RangeOverlapType.PartialLessThan,
            (_,_) when startIndex == -1 && endIndex == list.Count => RangeOverlapType.Subset,
            (_,_) when startIndex == list.Count && endIndex == list.Count => RangeOverlapType.GreaterThan,
            (_,_) when startIndex == -1 && endIndex == -1 => RangeOverlapType.LessThan,
            _ => throw new ArgumentOutOfRangeException(paramName: nameof(end), message: "Invalid range!")
        };        
    }
    
    /// <summary>
    /// Flattens a list of lists.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="batches"></param>
    /// <returns></returns>
    public static IReadOnlyList<T> FlattenToReadOnlyList<T>(this IEnumerable<IEnumerable<T>> batches)
    {
        using var en = batches.GetEnumerator();
        List<T>? res = null;
        while (en.MoveNext())
        {
            using var en2 = en.Current.GetEnumerator();
            while (en2.MoveNext())
            {
                (res ??= []).Add(en2.Current);
            }
        }

        if (res is not null)
            return res;
        return Array.Empty<T>();
    }
}
