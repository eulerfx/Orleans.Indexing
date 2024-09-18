#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Orleans.Indexing;

[TestClass]
public class IndexingQueueTests
{
    [TestMethod]
    public void ShouldEnqueueAndDequeue()
    {
        var grain = new TestIndexableGrain();
        
        var ups = new Dictionary<string, IndexedPropertyUpdate>
        {
            { "ProcessId", IndexedPropertyUpdate.Create(null, afterValue: "test", IndexUpdateVisibilityMode.NonTentative) }
        };

        var queue = new IndexingQueue();
        Assert.AreEqual(0, queue.Count);
        
        queue.AddAction(new IndexingAction(grain, grain.GetType(), Guid.NewGuid(), ups));
        queue.Punctuate();
        
        queue.AddAction(new IndexingAction(grain, grain.GetType(), Guid.NewGuid(), ups));
        
        var items = queue.EnumerateActionsUntilPunctuation().ToList();
        Assert.AreEqual(1, items.Count);
        
        var removed = queue.RemoveActions(items.Select(x => x.ActionId).ToHashSet());        
        Assert.AreEqual(1, removed);
                
        var items2 = queue.EnumerateActionsUntilPunctuation().ToList(); 
        Assert.AreEqual(1, items2.Count);
        
        var removed2 = queue.RemoveActions(items2.Select(x => x.ActionId).ToHashSet());
        Assert.AreEqual(1, removed2);
        
        Assert.AreEqual(0, queue.Count);
    }
}
