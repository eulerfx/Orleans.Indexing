using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Orleans.Indexing;

[TestClass]
public class PartitionSchemeTests
{
    [TestMethod]
    public void ShouldGetPartitionsByRange()
    {
        var scheme = new DateTimePartitionScheme { Bin = DateTimePartitionBinType.Year };
        var partitions = scheme.GetPartitionIndexesByValueRange(startValue: DateTimeOffset.Parse("2021-10-09"), endValue: DateTimeOffset.Parse("2024-10-11"));
        string[] expected = ["2021", "2022", "2023", "2024"];
        Assert.IsTrue(expected.SequenceEqual(partitions));
    }
    
    [TestMethod]
    public void ShouldGetPartitionsByRange2()
    {
        var scheme = new DateTimePartitionScheme { Bin = DateTimePartitionBinType.Month };
        var partitions = scheme.GetPartitionIndexesByValueRange(startValue: DateTimeOffset.Parse("2021-10-09"), endValue: DateTimeOffset.Parse("2024-10-11"));
        Assert.AreEqual(38, partitions.Count);
    }
}
