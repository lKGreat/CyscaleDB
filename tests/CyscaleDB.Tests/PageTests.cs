using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

public class PageTests
{
    [Fact]
    public void NewPage_ShouldHaveCorrectInitialState()
    {
        var page = new Page(0);

        Assert.Equal(0, page.PageId);
        Assert.Equal(PageType.Data, page.PageType);
        Assert.Equal(0, page.SlotCount);
        Assert.Equal(Constants.PageHeaderSize, page.FreeSpaceOffset);
        Assert.Equal(Constants.PageSize, page.FreeSpaceEnd);
        Assert.True(page.FreeSpace > 0);
    }

    [Fact]
    public void InsertRecord_ShouldReturnValidSlotNumber()
    {
        var page = new Page(0);
        var record = new byte[] { 1, 2, 3, 4, 5 };

        var slotNumber = page.InsertRecord(record);

        Assert.Equal(0, slotNumber);
        Assert.Equal(1, page.SlotCount);
        Assert.True(page.IsDirty);
    }

    [Fact]
    public void GetRecord_ShouldReturnInsertedData()
    {
        var page = new Page(0);
        var record = new byte[] { 1, 2, 3, 4, 5 };

        var slotNumber = page.InsertRecord(record);
        var retrieved = page.GetRecord(slotNumber);

        Assert.NotNull(retrieved);
        Assert.Equal(record, retrieved);
    }

    [Fact]
    public void InsertMultipleRecords_ShouldWorkCorrectly()
    {
        var page = new Page(0);
        var records = new[]
        {
            new byte[] { 1, 2, 3 },
            new byte[] { 4, 5, 6, 7 },
            new byte[] { 8, 9 }
        };

        for (int i = 0; i < records.Length; i++)
        {
            var slot = page.InsertRecord(records[i]);
            Assert.Equal(i, slot);
        }

        Assert.Equal(3, page.SlotCount);

        for (int i = 0; i < records.Length; i++)
        {
            var retrieved = page.GetRecord(i);
            Assert.Equal(records[i], retrieved);
        }
    }

    [Fact]
    public void DeleteRecord_ShouldMarkSlotAsDeleted()
    {
        var page = new Page(0);
        var record = new byte[] { 1, 2, 3, 4, 5 };

        var slotNumber = page.InsertRecord(record);
        var deleted = page.DeleteRecord(slotNumber);

        Assert.True(deleted);
        Assert.Null(page.GetRecord(slotNumber));
        Assert.Equal(1, page.SlotCount); // Slot count doesn't decrease
    }

    [Fact]
    public void UpdateRecord_SmallerRecord_ShouldUpdateInPlace()
    {
        var page = new Page(0);
        var original = new byte[] { 1, 2, 3, 4, 5 };
        var updated = new byte[] { 9, 8 };

        var slotNumber = page.InsertRecord(original);
        var result = page.UpdateRecord(slotNumber, updated);

        Assert.True(result);
        Assert.Equal(updated, page.GetRecord(slotNumber));
    }

    [Fact]
    public void UpdateRecord_LargerRecord_ShouldRelocate()
    {
        var page = new Page(0);
        var original = new byte[] { 1, 2 };
        var updated = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        var slotNumber = page.InsertRecord(original);
        var result = page.UpdateRecord(slotNumber, updated);

        Assert.True(result);
        Assert.Equal(updated, page.GetRecord(slotNumber));
    }

    [Fact]
    public void CanFit_ShouldReturnCorrectResult()
    {
        var page = new Page(0);

        Assert.True(page.CanFit(100));
        Assert.True(page.CanFit(1000));
        Assert.False(page.CanFit(Constants.PageSize)); // Too large
    }

    [Fact]
    public void InsertRecord_WhenPageFull_ShouldReturnNegative()
    {
        var page = new Page(0);
        
        // Fill the page
        var largeRecord = new byte[1000];
        while (page.InsertRecord(largeRecord) >= 0)
        {
            // Keep inserting until full
        }

        var result = page.InsertRecord(new byte[100]);
        Assert.Equal(-1, result);
    }

    [Fact]
    public void Checksum_ShouldVerifyCorrectly()
    {
        var page = new Page(0);
        var record = new byte[] { 1, 2, 3, 4, 5 };
        page.InsertRecord(record);

        page.UpdateChecksum();
        Assert.True(page.VerifyChecksum());
    }

    [Fact]
    public void EnumerateRecords_ShouldSkipDeletedRecords()
    {
        var page = new Page(0);
        
        page.InsertRecord(new byte[] { 1 });
        page.InsertRecord(new byte[] { 2 });
        page.InsertRecord(new byte[] { 3 });
        
        page.DeleteRecord(1); // Delete middle record

        var records = page.EnumerateRecords().ToList();
        
        Assert.Equal(2, records.Count);
        Assert.Equal(new byte[] { 1 }, records[0].Data);
        Assert.Equal(new byte[] { 3 }, records[1].Data);
    }

    [Fact]
    public void Compact_ShouldRemoveGaps()
    {
        var page = new Page(0);
        
        page.InsertRecord(new byte[] { 1, 2, 3 });
        page.InsertRecord(new byte[] { 4, 5, 6 });
        page.InsertRecord(new byte[] { 7, 8, 9 });
        
        var freeSpaceBefore = page.FreeSpace;
        page.DeleteRecord(1);
        
        page.Compact();

        // After compaction, free space should be recovered
        Assert.True(page.FreeSpace >= freeSpaceBefore);
        
        // Valid records should still be accessible
        Assert.Equal(new byte[] { 1, 2, 3 }, page.GetRecord(0));
        Assert.Null(page.GetRecord(1)); // Deleted
        Assert.Equal(new byte[] { 7, 8, 9 }, page.GetRecord(2));
    }

    [Fact]
    public void Page_FromExistingData_ShouldPreserveState()
    {
        var original = new Page(5, PageType.Data);
        original.InsertRecord(new byte[] { 1, 2, 3 });
        original.InsertRecord(new byte[] { 4, 5, 6 });
        original.UpdateChecksum();

        var data = original.GetData();
        var restored = new Page(5, data);

        Assert.Equal(original.PageId, restored.PageId);
        Assert.Equal(original.PageType, restored.PageType);
        Assert.Equal(original.SlotCount, restored.SlotCount);
        Assert.Equal(original.FreeSpaceOffset, restored.FreeSpaceOffset);
        Assert.True(restored.VerifyChecksum());
        Assert.Equal(new byte[] { 1, 2, 3 }, restored.GetRecord(0));
        Assert.Equal(new byte[] { 4, 5, 6 }, restored.GetRecord(1));
    }
}
