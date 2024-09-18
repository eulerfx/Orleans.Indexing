using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Orleans.Indexing;

[TestClass]
public class SortedIndexTests
{
    [TestMethod]
    public void ShouldGetValuesInRange()
    {
        var d = new SortedList<int, string> 
        {
            { 1, "hello" }, 
            { 2, "world" },
            { 3, "foo" },
        };
        
        Assert.IsTrue(d.GetValuesInRange(-1, 1).SequenceEqual(["hello"]));
        Assert.IsTrue(d.GetValuesInRange(1, 1).SequenceEqual(["hello"]));
        Assert.IsTrue(d.GetValuesInRange(1, 2).SequenceEqual(["hello", "world"]));
        Assert.IsTrue(d.GetValuesInRange(1, 3).SequenceEqual(["hello", "world", "foo"]));
        Assert.IsTrue(d.GetValuesInRange(1, 4).SequenceEqual(["hello", "world", "foo"]));
        Assert.IsTrue(d.GetValuesInRange(0, 4).SequenceEqual(["hello", "world", "foo"]));
        Assert.IsTrue(d.GetValuesInRange(3, 3).SequenceEqual(["foo"]));        
        Assert.IsTrue(d.GetValuesInRange(4, 5).SequenceEqual([]));
        Assert.IsTrue(d.GetValuesInRange(0, 1).SequenceEqual(["hello"]));
        Assert.IsTrue(d.GetValuesInRange(2, 5).SequenceEqual(["world", "foo"]));
        Assert.ThrowsException<ArgumentOutOfRangeException>(() => d.GetValuesInRange(2, 1));
    }
    
    [TestMethod]
    public void ShouldGetValuesInRangeWithOffset()
    {
        var d = new SortedList<int, string> 
        {
            { 1, "hello" }, 
            { 2, "world" },
            { 3, "foo" },
            { 4, "bar" },
        };
        
        var res = d.GetValuesInRange(1, 5, offset: 1, size: 2);
        Assert.IsTrue(res.SequenceEqual(["world", "foo"]));
    }

    [TestMethod]
    public void ShouldGetIndexOfKeyOrNext()
    {
        var d = new SortedList<int, string> 
        {
            { 1, "hello" }, 
            { 2, "world" },
            { 3, "foo" },
            { 4, "bar" },
            { 5, "baz" },
        };
        
        Assert.AreEqual(d.IndexOfKey(1), d.IndexOfKeyOrNext(1));
        Assert.AreEqual(d.IndexOfKey(2), d.IndexOfKeyOrNext(2));
        Assert.AreEqual(d.IndexOfKey(3), d.IndexOfKeyOrNext(3));
        Assert.AreEqual(d.IndexOfKey(4), d.IndexOfKeyOrNext(4));
        Assert.AreEqual(d.IndexOfKey(5), d.IndexOfKeyOrNext(5));
        
        Assert.AreNotEqual(d.IndexOfKey(6), d.IndexOfKeyOrNext(6));
        Assert.AreNotEqual(d.IndexOfKey(7), d.IndexOfKeyOrNext(7));        
        
        Assert.AreEqual(-1, d.IndexOfKeyOrNext(-2));
        Assert.AreEqual(-1, d.IndexOfKeyOrNext(-1));
        Assert.AreEqual(-1, d.IndexOfKeyOrNext(0));
        
        Assert.AreEqual(0, d.IndexOfKeyOrNext(1));
        Assert.AreEqual(1, d.IndexOfKeyOrNext(2));
        Assert.AreEqual(2, d.IndexOfKeyOrNext(3));
        Assert.AreEqual(3, d.IndexOfKeyOrNext(4));
        Assert.AreEqual(4, d.IndexOfKeyOrNext(5));
        
        Assert.AreEqual(5, d.IndexOfKeyOrNext(6));
        Assert.AreEqual(5, d.IndexOfKeyOrNext(7));
    }
    
    [TestMethod]
    public void ShouldGetRangeOverlap()
    {
        var d = new SortedList<int, string> 
        {
            { 1, "hello" }, 
            { 2, "world" },
            { 3, "foo" },
            { 4, "bar" },
            { 5, "baz" },
        };
        
        Assert.AreEqual(RangeOverlapType.Superset, d.GetRangeOverlap(2, 3));        
        Assert.AreEqual(RangeOverlapType.Subset, d.GetRangeOverlap(0, 6));
        Assert.AreEqual(RangeOverlapType.PartialLessThan, d.GetRangeOverlap(0, 1));
        Assert.AreEqual(RangeOverlapType.LessThan, d.GetRangeOverlap(-1, 0));
        Assert.AreEqual(RangeOverlapType.GreaterThan, d.GetRangeOverlap(6, 7));
        Assert.AreEqual(RangeOverlapType.GreaterThan, d.GetRangeOverlap(7, 8));
        Assert.AreEqual(RangeOverlapType.LessThan, d.GetRangeOverlap(-2, 0));
        Assert.AreEqual(RangeOverlapType.LessThan, d.GetRangeOverlap(-3, -1));
        Assert.ThrowsException<ArgumentOutOfRangeException>(() => d.GetRangeOverlap(5, 1));
    }
}
