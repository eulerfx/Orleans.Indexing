#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Indexing;

/// <summary>
/// A partition scheme for a keyed index.
/// </summary>
public interface IIndexPartitionScheme
{
    /// <summary>
    /// Gets the partition in which the given value is contained.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    string GetPartitionByKey(object? key);
}

/// <summary>
/// A partition scheme for a hash index.
/// </summary>
public interface IHashIndexPartitionScheme : IIndexPartitionScheme {}

/// <summary>
/// A partition scheme for sorted indexes supporting range queries.
/// </summary>
public interface ISortedIndexPartitionScheme : IIndexPartitionScheme
{
    /// <summary>
    /// Gets the contiguous sequence of partitions that cover the given range.
    /// </summary>
    /// <param name="startValue"></param>
    /// <param name="endValue"></param>
    /// <returns></returns>
    IReadOnlyList<string> GetPartitionsByRange(object? startValue, object? endValue);
}

/// <summary>
/// A static hash-index partition scheme.
/// </summary>
/// <param name="MaxPartitions"></param>
public class HashIndexPartitionScheme(int MaxPartitions = 0) : IHashIndexPartitionScheme
{
    /// <summary>
    /// The maximum number of partitions.<br/>
    /// If set to 0, then there's a unique partition for each hash-code.
    /// </summary>
    public int MaxPartitions { get; init; } = MaxPartitions;
    
    public string GetPartitionByKey(object? key)
    {
        if (key is null) return string.Empty;        
        var hash = key.GetInvariantHashCode();
        var p = MaxPartitions > 0 ? hash % MaxPartitions : hash;
        return p.ToString();
    }
}

public enum DateTimePartitionBinType
{
    Year,
    Month
}

/// <summary>
/// A sorted index partition scheme based on <see cref="DateTime"/>.
/// </summary>
public class DateTimePartitionScheme(DateTimePartitionBinType? Bin = default) : ISortedIndexPartitionScheme
{
    public DateTimePartitionBinType Bin { get; init; } = Bin.GetValueOrDefault();
        
    PartitionInfo? partitionInfo;
    PartitionInfo Info => partitionInfo ??= GetPartitionInfo();
    
    public IReadOnlyList<string> GetPartitionIndexesByValueRange(DateTimeOffset? startValue, DateTimeOffset? endValue) =>
        GetPartitionIndexesByValueRange(startValue.GetValueOrDefault(x => x.DateTime), endValue.GetValueOrDefault(x => x.DateTime));
    
    public IReadOnlyList<string> GetPartitionIndexesByValueRange(DateTime? startValue, DateTime? endValue)
    {
        startValue = startValue.EnsureNotNull();
        endValue = endValue.EnsureNotNull();

        var info = Info;
        var startPartition = info.GetPartition(startValue.Value.ToUniversalTime());
        var endPartition = info.GetPartition(endValue.Value.ToUniversalTime());
        var range = endPartition - startPartition;        
        var partitionCount = (int)Math.Ceiling(range / info.Span) + 1;
        var partitions = new string[partitionCount];
        var prevPartition = info.GetPartition(startPartition);
        partitions[0] = info.GetPartitionString(prevPartition);
        for (var i = 1; i < partitionCount; i++)
        {
            prevPartition = info.AddSpan(prevPartition);
            partitions[i] = info.GetPartitionString(prevPartition);
        }
        return partitions;
    } 
    
    public IReadOnlyList<string> GetPartitionsByRange(object? startValue, object? endValue)
    {
        if (startValue is DateTimeOffset sd && endValue is DateTimeOffset ed) return GetPartitionIndexesByValueRange(sd, ed);
        if (startValue is DateTime sd2 && endValue is DateTime ed2) return GetPartitionIndexesByValueRange(sd2, ed2);
        throw new NotSupportedException();
    }

    public string GetPartitionByKey(object? key) => key switch
    {
        (DateTimeOffset dt) => Info.GetPartitionString(dt.DateTime),
        _ => throw new NotSupportedException($"The value type {key?.GetType()} is not supported.")
    };
    
    record PartitionInfo(DateTimePartitionBinType Bin, TimeSpan Span, string Format, Func<DateTime, DateTime> AddSpan)
    {
        public string GetPartitionString(DateTime value) => value.ToString(Format);

        public DateTime GetPartition(DateTime value) => Bin switch
        {
            DateTimePartitionBinType.Year => new DateTime(year: value.Year, month: 1, day: 1, hour: 0, minute: 0, second: 0),
            DateTimePartitionBinType.Month => new DateTime(year: value.Year, month: value.Month, day: 1, hour: 0, minute: 0, second: 0),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    PartitionInfo GetPartitionInfo() => Bin switch
    {
        DateTimePartitionBinType.Year => new PartitionInfo(Bin, TimeSpan.FromDays(365), Format: "yyyy", y => y.AddYears(1)),
        DateTimePartitionBinType.Month => new PartitionInfo(Bin, TimeSpan.FromDays(30), Format: "yyyyMM", y => y.AddMonths(1)),
        _ => throw new ArgumentOutOfRangeException()
    };
}
