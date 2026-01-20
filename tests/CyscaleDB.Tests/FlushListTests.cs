using CyscaleDB.Core.Storage;
using Xunit;

namespace CyscaleDB.Tests;

public class FlushListTests : IDisposable
{
    private FlushList _flushList;

    public FlushListTests()
    {
        _flushList = new FlushList();
    }

    public void Dispose()
    {
        _flushList?.Dispose();
    }

    #region Basic Operations

    [Fact]
    public void FlushList_InitialState_IsEmpty()
    {
        Assert.Equal(0, _flushList.Count);
        Assert.Equal(-1, _flushList.OldestModificationLsn);
        Assert.Equal(-1, _flushList.NewestModificationLsn);
    }

    [Fact]
    public void AddDirtyPage_SinglePage_IncrementsCount()
    {
        var page = new Page(1, PageType.Data);
        
        _flushList.AddDirtyPage("test.cdb", 1, page, 100);
        
        Assert.Equal(1, _flushList.Count);
    }

    [Fact]
    public void AddDirtyPage_SinglePage_UpdatesOldestLsn()
    {
        var page = new Page(1, PageType.Data);
        
        _flushList.AddDirtyPage("test.cdb", 1, page, 100);
        
        Assert.Equal(100, _flushList.OldestModificationLsn);
    }

    [Fact]
    public void AddDirtyPage_MultiplePages_TracksOldestLsn()
    {
        var page1 = new Page(1, PageType.Data);
        var page2 = new Page(2, PageType.Data);
        var page3 = new Page(3, PageType.Data);
        
        _flushList.AddDirtyPage("test.cdb", 1, page1, 300);
        _flushList.AddDirtyPage("test.cdb", 2, page2, 100);
        _flushList.AddDirtyPage("test.cdb", 3, page3, 200);
        
        Assert.Equal(3, _flushList.Count);
        Assert.Equal(100, _flushList.OldestModificationLsn);
        Assert.Equal(300, _flushList.NewestModificationLsn);
    }

    [Fact]
    public void AddDirtyPage_SamePage_UpdatesExisting()
    {
        var page = new Page(1, PageType.Data);
        
        _flushList.AddDirtyPage("test.cdb", 1, page, 100);
        _flushList.AddDirtyPage("test.cdb", 1, page, 200);
        
        // Should still be one entry
        Assert.Equal(1, _flushList.Count);
    }

    #endregion

    #region Remove Tests

    [Fact]
    public void RemovePage_ExistingPage_ReturnsTrue()
    {
        var page = new Page(1, PageType.Data);
        _flushList.AddDirtyPage("test.cdb", 1, page, 100);
        
        var result = _flushList.RemovePage("test.cdb", 1);
        
        Assert.True(result);
        Assert.Equal(0, _flushList.Count);
    }

    [Fact]
    public void RemovePage_NonExistingPage_ReturnsFalse()
    {
        var result = _flushList.RemovePage("test.cdb", 999);
        
        Assert.False(result);
    }

    [Fact]
    public void RemovePage_UpdatesOldestLsn()
    {
        var page1 = new Page(1, PageType.Data);
        var page2 = new Page(2, PageType.Data);
        
        _flushList.AddDirtyPage("test.cdb", 1, page1, 100);
        _flushList.AddDirtyPage("test.cdb", 2, page2, 200);
        
        _flushList.RemovePage("test.cdb", 1);
        
        Assert.Equal(200, _flushList.OldestModificationLsn);
    }

    #endregion

    #region GetOldestDirtyPages Tests

    [Fact]
    public void GetOldestDirtyPages_ReturnsInLsnOrder()
    {
        var page1 = new Page(1, PageType.Data);
        var page2 = new Page(2, PageType.Data);
        var page3 = new Page(3, PageType.Data);
        
        _flushList.AddDirtyPage("test.cdb", 1, page1, 300);
        _flushList.AddDirtyPage("test.cdb", 2, page2, 100);
        _flushList.AddDirtyPage("test.cdb", 3, page3, 200);
        
        var oldest = _flushList.GetOldestDirtyPages(3);
        
        Assert.Equal(3, oldest.Count);
        Assert.Equal(2, oldest[0].PageId); // LSN 100
        Assert.Equal(3, oldest[1].PageId); // LSN 200
        Assert.Equal(1, oldest[2].PageId); // LSN 300
    }

    [Fact]
    public void GetOldestDirtyPages_LimitsCount()
    {
        for (int i = 1; i <= 10; i++)
        {
            var page = new Page(i, PageType.Data);
            _flushList.AddDirtyPage("test.cdb", i, page, i * 10);
        }
        
        var oldest = _flushList.GetOldestDirtyPages(3);
        
        Assert.Equal(3, oldest.Count);
    }

    [Fact]
    public void GetOldestDirtyPages_EmptyList_ReturnsEmpty()
    {
        var oldest = _flushList.GetOldestDirtyPages(10);
        
        Assert.Empty(oldest);
    }

    #endregion

    #region GetPagesOlderThan Tests

    [Fact]
    public void GetPagesOlderThan_ReturnsCorrectPages()
    {
        var page1 = new Page(1, PageType.Data);
        var page2 = new Page(2, PageType.Data);
        var page3 = new Page(3, PageType.Data);
        
        _flushList.AddDirtyPage("test.cdb", 1, page1, 100);
        _flushList.AddDirtyPage("test.cdb", 2, page2, 200);
        _flushList.AddDirtyPage("test.cdb", 3, page3, 300);
        
        var older = _flushList.GetPagesOlderThan(250);
        
        Assert.Equal(2, older.Count);
    }

    [Fact]
    public void GetPagesOlderThan_NoMatch_ReturnsEmpty()
    {
        var page = new Page(1, PageType.Data);
        _flushList.AddDirtyPage("test.cdb", 1, page, 100);
        
        var older = _flushList.GetPagesOlderThan(50);
        
        Assert.Empty(older);
    }

    #endregion

    #region FlushPages Tests

    [Fact]
    public void FlushPages_CallsFlushFunc()
    {
        var page1 = new Page(1, PageType.Data);
        var page2 = new Page(2, PageType.Data);
        
        _flushList.AddDirtyPage("test.cdb", 1, page1, 100);
        _flushList.AddDirtyPage("test.cdb", 2, page2, 200);
        
        var flushedPages = new List<int>();
        var flushed = _flushList.FlushPages(10, (path, pageId, page) =>
        {
            flushedPages.Add(pageId);
            return true;
        });
        
        Assert.Equal(2, flushed);
        Assert.Equal(0, _flushList.Count);
        Assert.Contains(1, flushedPages);
        Assert.Contains(2, flushedPages);
    }

    [Fact]
    public void FlushPages_HandlesFailedFlush()
    {
        var page1 = new Page(1, PageType.Data);
        var page2 = new Page(2, PageType.Data);
        
        _flushList.AddDirtyPage("test.cdb", 1, page1, 100);
        _flushList.AddDirtyPage("test.cdb", 2, page2, 200);
        
        var flushed = _flushList.FlushPages(10, (path, pageId, page) =>
        {
            return pageId == 2; // Only page 2 succeeds
        });
        
        Assert.Equal(1, flushed);
        Assert.Equal(1, _flushList.Count); // Page 1 still in list
    }

    [Fact]
    public void FlushPages_LimitsCount()
    {
        for (int i = 1; i <= 10; i++)
        {
            var page = new Page(i, PageType.Data);
            _flushList.AddDirtyPage("test.cdb", i, page, i * 10);
        }
        
        var flushed = _flushList.FlushPages(3, (_, _, _) => true);
        
        Assert.Equal(3, flushed);
        Assert.Equal(7, _flushList.Count);
    }

    #endregion

    #region ContainsPage Tests

    [Fact]
    public void ContainsPage_ExistingPage_ReturnsTrue()
    {
        var page = new Page(1, PageType.Data);
        _flushList.AddDirtyPage("test.cdb", 1, page, 100);
        
        Assert.True(_flushList.ContainsPage("test.cdb", 1));
    }

    [Fact]
    public void ContainsPage_NonExistingPage_ReturnsFalse()
    {
        Assert.False(_flushList.ContainsPage("test.cdb", 999));
    }

    #endregion

    #region Clear Tests

    [Fact]
    public void Clear_RemovesAllEntries()
    {
        var page1 = new Page(1, PageType.Data);
        var page2 = new Page(2, PageType.Data);
        
        _flushList.AddDirtyPage("test.cdb", 1, page1, 100);
        _flushList.AddDirtyPage("test.cdb", 2, page2, 200);
        
        _flushList.Clear();
        
        Assert.Equal(0, _flushList.Count);
        Assert.Equal(-1, _flushList.OldestModificationLsn);
    }

    #endregion

    #region Statistics Tests

    [Fact]
    public void Statistics_TrackCorrectly()
    {
        var page = new Page(1, PageType.Data);
        
        _flushList.AddDirtyPage("test.cdb", 1, page, 100);
        Assert.Equal(1, _flushList.TotalAdded);
        
        _flushList.RemovePage("test.cdb", 1);
        Assert.Equal(1, _flushList.TotalRemoved);
    }

    #endregion

    #region Concurrent Access Tests

    [Fact]
    public void ConcurrentAccess_IsThreadSafe()
    {
        var tasks = new List<Task>();
        
        // Add pages from multiple threads
        for (int t = 0; t < 5; t++)
        {
            int threadId = t;
            tasks.Add(Task.Run(() =>
            {
                for (int i = 0; i < 100; i++)
                {
                    var pageId = threadId * 1000 + i;
                    var page = new Page(pageId, PageType.Data);
                    _flushList.AddDirtyPage("test.cdb", pageId, page, pageId);
                }
            }));
        }
        
        Task.WaitAll(tasks.ToArray());
        
        Assert.Equal(500, _flushList.Count);
    }

    #endregion
}
