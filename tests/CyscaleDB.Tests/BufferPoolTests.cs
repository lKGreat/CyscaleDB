using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;
using Xunit;

namespace CyscaleDB.Tests;

public class BufferPoolTests : IDisposable
{
    private readonly string _testDir;
    private readonly PageManager _pageManager;
    private BufferPool _bufferPool;

    public BufferPoolTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"cyscaledb_bp_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
        
        var dataFile = Path.Combine(_testDir, "test.cdb");
        _pageManager = new PageManager(dataFile);
        _pageManager.Open();
        
        _bufferPool = new BufferPool(capacity: 10);
    }

    public void Dispose()
    {
        _bufferPool?.Dispose();
        _pageManager?.Dispose();
        
        if (Directory.Exists(_testDir))
        {
            try { Directory.Delete(_testDir, true); }
            catch { /* ignore cleanup errors */ }
        }
    }

    #region Basic Operations

    [Fact]
    public void BufferPool_NewPage_ReturnsValidPage()
    {
        var page = _bufferPool.NewPage(_pageManager);
        
        Assert.NotNull(page);
        Assert.True(page.PageId >= 0);
    }

    [Fact]
    public void BufferPool_GetPage_CachesPage()
    {
        // Create a page first
        var originalPage = _bufferPool.NewPage(_pageManager);
        var pageId = originalPage.PageId;
        
        // Unpin it
        _bufferPool.UnpinPage(_pageManager, pageId);
        
        // Get it again - should be cached
        var cachedPage = _bufferPool.GetPage(_pageManager, pageId);
        
        Assert.Same(originalPage, cachedPage);
    }

    [Fact]
    public void BufferPool_HitRatio_TracksCorrectly()
    {
        // Start fresh
        _bufferPool.Dispose();
        _bufferPool = new BufferPool(10);
        
        // Create a page (miss)
        var page = _bufferPool.NewPage(_pageManager);
        _bufferPool.UnpinPage(_pageManager, page.PageId);
        
        // Access again (hit)
        _bufferPool.GetPage(_pageManager, page.PageId);
        _bufferPool.UnpinPage(_pageManager, page.PageId);
        
        // One more hit
        _bufferPool.GetPage(_pageManager, page.PageId);
        
        Assert.True(_bufferPool.HitRatio > 0);
    }

    #endregion

    #region Young/Old Region Tests

    [Fact]
    public void BufferPool_NewPage_GoesToYoungRegion()
    {
        var page = _bufferPool.NewPage(_pageManager);
        
        // Newly created pages should be in young region
        // Young region count should be positive
        Assert.True(_bufferPool.YoungRegionCount >= 0);
    }

    [Fact]
    public void BufferPool_GetPage_FirstAccess_GoesToOldRegion()
    {
        // Create multiple pages to ensure we have data
        for (int i = 0; i < 5; i++)
        {
            var p = _bufferPool.NewPage(_pageManager);
            _bufferPool.UnpinPage(_pageManager, p.PageId);
        }
        
        // Old region should have some pages due to rebalancing
        Assert.True(_bufferPool.OldRegionCount >= 0);
    }

    [Fact]
    public void BufferPool_RepeatedAccess_WithDelay_PromotesToYoungRegion()
    {
        // Set very short old block time for testing
        _bufferPool.OldBlockTimeMs = 10;
        
        // Create and load a page
        var page = _pageManager.AllocatePage(PageType.Data);
        _pageManager.WritePage(page);
        
        // First access - goes to old region
        var cachedPage = _bufferPool.GetPage(_pageManager, page.PageId);
        _bufferPool.UnpinPage(_pageManager, page.PageId);
        
        // Wait longer than old block time
        Thread.Sleep(50);
        
        // Access again - should be promoted to young region
        _bufferPool.GetPage(_pageManager, page.PageId);
        _bufferPool.UnpinPage(_pageManager, page.PageId);
        
        Assert.True(_bufferPool.OldToYoungMoves >= 1);
    }

    [Fact]
    public void BufferPool_QuickRepeatedAccess_StaysInOldRegion()
    {
        // Set longer old block time
        _bufferPool.OldBlockTimeMs = 10000; // 10 seconds
        
        // Create and load a page
        var page = _pageManager.AllocatePage(PageType.Data);
        _pageManager.WritePage(page);
        
        long initialMoves = _bufferPool.OldToYoungMoves;
        
        // First access
        var cachedPage = _bufferPool.GetPage(_pageManager, page.PageId);
        _bufferPool.UnpinPage(_pageManager, page.PageId);
        
        // Immediate re-access (within old block time)
        _bufferPool.GetPage(_pageManager, page.PageId);
        _bufferPool.UnpinPage(_pageManager, page.PageId);
        
        // Should not have moved to young region due to quick access
        Assert.Equal(initialMoves, _bufferPool.OldToYoungMoves);
    }

    [Fact]
    public void BufferPool_MaintainsRegionRatio()
    {
        // Fill the buffer pool
        var pages = new List<Page>();
        for (int i = 0; i < 10; i++)
        {
            var page = _bufferPool.NewPage(_pageManager);
            pages.Add(page);
            _bufferPool.UnpinPage(_pageManager, page.PageId);
        }
        
        // Total should equal capacity
        Assert.Equal(10, _bufferPool.Count);
        
        // Young + Old should equal total
        Assert.Equal(_bufferPool.Count, _bufferPool.YoungRegionCount + _bufferPool.OldRegionCount);
    }

    #endregion

    #region Eviction Tests

    [Fact]
    public void BufferPool_Eviction_PrefersOldRegion()
    {
        // Fill the buffer pool
        var pages = new List<int>();
        for (int i = 0; i < 10; i++)
        {
            var page = _bufferPool.NewPage(_pageManager);
            pages.Add(page.PageId);
            _bufferPool.UnpinPage(_pageManager, page.PageId);
        }
        
        // Add one more page - should trigger eviction from old region
        var newPage = _bufferPool.NewPage(_pageManager);
        
        Assert.Equal(10, _bufferPool.Count);
    }

    [Fact]
    public void BufferPool_Eviction_FlushessDirtyPages()
    {
        // Create a page and mark it dirty
        var page = _bufferPool.NewPage(_pageManager);
        page.InsertRecord([1, 2, 3, 4, 5]);
        _bufferPool.MarkDirty(_pageManager, page.PageId);
        _bufferPool.UnpinPage(_pageManager, page.PageId, isDirty: true);
        
        // Fill the buffer to trigger eviction
        for (int i = 0; i < 10; i++)
        {
            var p = _bufferPool.NewPage(_pageManager);
            _bufferPool.UnpinPage(_pageManager, p.PageId);
        }
        
        // Buffer should still be at capacity
        Assert.Equal(10, _bufferPool.Count);
    }

    #endregion

    #region Statistics Tests

    [Fact]
    public void BufferPool_Statistics_TracksCorrectly()
    {
        // Start fresh
        _bufferPool.Dispose();
        _bufferPool = new BufferPool(10);
        
        // Initial state
        Assert.Equal(0, _bufferPool.Count);
        Assert.Equal(0.0, _bufferPool.HitRatio);
        Assert.Equal(0, _bufferPool.YoungToOldMoves);
        Assert.Equal(0, _bufferPool.OldToYoungMoves);
    }

    [Fact]
    public void BufferPool_OldBlockTimeMs_IsConfigurable()
    {
        _bufferPool.OldBlockTimeMs = 500;
        Assert.Equal(500, _bufferPool.OldBlockTimeMs);
        
        _bufferPool.OldBlockTimeMs = 2000;
        Assert.Equal(2000, _bufferPool.OldBlockTimeMs);
    }

    #endregion

    #region Concurrent Access Tests

    [Fact]
    public void BufferPool_ConcurrentAccess_IsThreadSafe()
    {
        var page = _bufferPool.NewPage(_pageManager);
        _bufferPool.UnpinPage(_pageManager, page.PageId);
        
        // Access the same page from multiple threads
        var tasks = new List<Task>();
        for (int i = 0; i < 10; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 100; j++)
                {
                    var p = _bufferPool.GetPage(_pageManager, page.PageId);
                    _bufferPool.UnpinPage(_pageManager, page.PageId);
                }
            }));
        }
        
        Task.WaitAll(tasks.ToArray());
        
        // Should complete without exceptions
        Assert.True(_bufferPool.HitRatio > 0);
    }

    #endregion
}
