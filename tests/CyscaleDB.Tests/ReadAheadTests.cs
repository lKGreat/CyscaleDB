using CyscaleDB.Core.Storage;
using Xunit;

namespace CyscaleDB.Tests;

public class ReadAheadTests : IDisposable
{
    private readonly string _testDir;
    private readonly PageManager _pageManager;
    private readonly BufferPool _bufferPool;
    private ReadAhead _readAhead;

    public ReadAheadTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"cyscaledb_ra_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
        
        var dataFile = Path.Combine(_testDir, "test.cdb");
        _pageManager = new PageManager(dataFile);
        _pageManager.Open();
        
        // Create some pages for testing
        for (int i = 0; i < 20; i++)
        {
            var page = _pageManager.AllocatePage(PageType.Data);
            page.InsertRecord(new byte[] { (byte)i, (byte)(i + 1), (byte)(i + 2) });
            _pageManager.WritePage(page);
        }
        
        _bufferPool = new BufferPool(capacity: 50);
        _readAhead = new ReadAhead(_bufferPool);
    }

    public void Dispose()
    {
        _readAhead?.Dispose();
        _bufferPool?.Dispose();
        _pageManager?.Dispose();
        
        if (Directory.Exists(_testDir))
        {
            try { Directory.Delete(_testDir, true); }
            catch { /* ignore cleanup errors */ }
        }
    }

    #region Basic Configuration

    [Fact]
    public void ReadAhead_DefaultConfiguration_IsValid()
    {
        Assert.True(_readAhead.Enabled);
        Assert.Equal(4, _readAhead.SequentialThreshold);
        Assert.Equal(8, _readAhead.PrefetchWindow);
        Assert.Equal(16, _readAhead.MaxPrefetchPages);
    }

    [Fact]
    public void ReadAhead_CanBeDisabled()
    {
        _readAhead.Enabled = false;
        
        var triggered = _readAhead.RecordAccess("test.cdb", 0);
        Assert.False(triggered);
        Assert.False(_readAhead.Enabled);
    }

    [Fact]
    public void ReadAhead_ConfigurationCanBeChanged()
    {
        _readAhead.SequentialThreshold = 2;
        _readAhead.PrefetchWindow = 4;
        _readAhead.MaxPrefetchPages = 10;
        
        Assert.Equal(2, _readAhead.SequentialThreshold);
        Assert.Equal(4, _readAhead.PrefetchWindow);
        Assert.Equal(10, _readAhead.MaxPrefetchPages);
    }

    #endregion

    #region Sequential Detection

    [Fact]
    public void RecordAccess_NonSequential_DoesNotTriggerPrefetch()
    {
        // Random access pattern
        _readAhead.RecordAccess("test.cdb", 5);
        _readAhead.RecordAccess("test.cdb", 10);
        _readAhead.RecordAccess("test.cdb", 2);
        _readAhead.RecordAccess("test.cdb", 15);
        
        Assert.False(_readAhead.IsSequentialAccess("test.cdb"));
    }

    [Fact]
    public void RecordAccess_Sequential_DetectsPattern()
    {
        // Sequential access pattern
        _readAhead.RecordAccess("test.cdb", 0);
        _readAhead.RecordAccess("test.cdb", 1);
        _readAhead.RecordAccess("test.cdb", 2);
        _readAhead.RecordAccess("test.cdb", 3);
        
        Assert.True(_readAhead.IsSequentialAccess("test.cdb"));
    }

    [Fact]
    public void RecordAccess_SequentialWithPrefetch_TriggersPrefetch()
    {
        _readAhead.SequentialThreshold = 3;
        
        // Access sequentially
        _readAhead.RecordAccess(_pageManager.FilePath, 0, _pageManager);
        _readAhead.RecordAccess(_pageManager.FilePath, 1, _pageManager);
        var triggered = _readAhead.RecordAccess(_pageManager.FilePath, 2, _pageManager);
        
        Assert.True(triggered);
        
        // Wait a bit for async prefetch
        Thread.Sleep(100);
        
        Assert.True(_readAhead.SequentialDetections >= 1);
    }

    [Fact]
    public void RecordAccess_BrokenSequence_ResetsCount()
    {
        // Start sequential
        _readAhead.RecordAccess("test.cdb", 0);
        _readAhead.RecordAccess("test.cdb", 1);
        _readAhead.RecordAccess("test.cdb", 2);
        
        // Break sequence
        _readAhead.RecordAccess("test.cdb", 10);
        
        Assert.False(_readAhead.IsSequentialAccess("test.cdb"));
        
        // Need to rebuild sequential pattern
        _readAhead.RecordAccess("test.cdb", 11);
        _readAhead.RecordAccess("test.cdb", 12);
        _readAhead.RecordAccess("test.cdb", 13);
        
        Assert.True(_readAhead.IsSequentialAccess("test.cdb"));
    }

    #endregion

    #region Prediction

    [Fact]
    public void GetPredictedPages_SequentialAccess_ReturnsPredictions()
    {
        // Establish sequential pattern
        _readAhead.RecordAccess("test.cdb", 0);
        _readAhead.RecordAccess("test.cdb", 1);
        _readAhead.RecordAccess("test.cdb", 2);
        _readAhead.RecordAccess("test.cdb", 3);
        
        var predictions = _readAhead.GetPredictedPages("test.cdb", 4);
        
        Assert.Equal(4, predictions.Count);
        Assert.Equal(4, predictions[0]);
        Assert.Equal(5, predictions[1]);
        Assert.Equal(6, predictions[2]);
        Assert.Equal(7, predictions[3]);
    }

    [Fact]
    public void GetPredictedPages_NonSequentialAccess_ReturnsEmpty()
    {
        _readAhead.RecordAccess("test.cdb", 5);
        _readAhead.RecordAccess("test.cdb", 10);
        
        var predictions = _readAhead.GetPredictedPages("test.cdb", 4);
        
        Assert.Empty(predictions);
    }

    [Fact]
    public void GetPredictedPages_UnknownFile_ReturnsEmpty()
    {
        var predictions = _readAhead.GetPredictedPages("unknown.cdb", 4);
        
        Assert.Empty(predictions);
    }

    #endregion

    #region Manual Prefetch

    [Fact]
    public void TriggerPrefetch_LoadsPages()
    {
        var initialStats = _readAhead.GetStats();
        
        _readAhead.TriggerPrefetch(_pageManager.FilePath, 5, 3, _pageManager);
        
        // Wait for async prefetch
        Thread.Sleep(200);
        
        var newStats = _readAhead.GetStats();
        Assert.True(newStats.PrefetchRequests > initialStats.PrefetchRequests);
    }

    [Fact]
    public void TriggerPrefetch_WhenDisabled_DoesNothing()
    {
        _readAhead.Enabled = false;
        var initialStats = _readAhead.GetStats();
        
        _readAhead.TriggerPrefetch(_pageManager.FilePath, 5, 3, _pageManager);
        
        // Wait briefly
        Thread.Sleep(50);
        
        var newStats = _readAhead.GetStats();
        Assert.Equal(initialStats.PrefetchRequests, newStats.PrefetchRequests);
    }

    #endregion

    #region History Management

    [Fact]
    public void ResetHistory_ClearsFileHistory()
    {
        _readAhead.RecordAccess("test.cdb", 0);
        _readAhead.RecordAccess("test.cdb", 1);
        _readAhead.RecordAccess("test.cdb", 2);
        _readAhead.RecordAccess("test.cdb", 3);
        
        Assert.True(_readAhead.IsSequentialAccess("test.cdb"));
        
        _readAhead.ResetHistory("test.cdb");
        
        Assert.False(_readAhead.IsSequentialAccess("test.cdb"));
    }

    [Fact]
    public void ClearHistory_ClearsAllHistory()
    {
        _readAhead.RecordAccess("file1.cdb", 0);
        _readAhead.RecordAccess("file2.cdb", 0);
        
        _readAhead.ClearHistory();
        
        var stats = _readAhead.GetStats();
        Assert.Equal(0, stats.TrackedFiles);
    }

    #endregion

    #region Statistics

    [Fact]
    public void GetStats_ReturnsValidStats()
    {
        var stats = _readAhead.GetStats();
        
        Assert.NotNull(stats);
        Assert.True(stats.PrefetchRequests >= 0);
        Assert.True(stats.PrefetchedPages >= 0);
    }

    [Fact]
    public void Stats_ToString_ReturnsFormattedString()
    {
        var stats = new ReadAheadStats
        {
            PrefetchRequests = 10,
            PrefetchedPages = 50,
            SequentialDetections = 5,
            TrackedFiles = 2
        };
        
        var str = stats.ToString();
        
        Assert.Contains("10", str);
        Assert.Contains("50", str);
        Assert.Contains("5", str);
        Assert.Contains("2", str);
    }

    #endregion

    #region Multiple Files

    [Fact]
    public void ReadAhead_TracksMultipleFilesIndependently()
    {
        // Sequential on file1
        _readAhead.RecordAccess("file1.cdb", 0);
        _readAhead.RecordAccess("file1.cdb", 1);
        _readAhead.RecordAccess("file1.cdb", 2);
        _readAhead.RecordAccess("file1.cdb", 3);
        
        // Random on file2
        _readAhead.RecordAccess("file2.cdb", 5);
        _readAhead.RecordAccess("file2.cdb", 2);
        
        Assert.True(_readAhead.IsSequentialAccess("file1.cdb"));
        Assert.False(_readAhead.IsSequentialAccess("file2.cdb"));
    }

    #endregion

    #region Edge Cases

    [Fact]
    public void RecordAccess_WithNullPageManager_DoesNotTriggerPrefetch()
    {
        _readAhead.SequentialThreshold = 2;
        
        _readAhead.RecordAccess("test.cdb", 0, null);
        var triggered = _readAhead.RecordAccess("test.cdb", 1, null);
        
        // Sequential detected but no prefetch without page manager
        Assert.False(triggered);
    }

    [Fact]
    public void ReadAhead_WithoutBufferPool_DoesNotPrefetch()
    {
        var raWithoutPool = new ReadAhead(bufferPool: null);
        raWithoutPool.SequentialThreshold = 2;
        
        raWithoutPool.RecordAccess("test.cdb", 0, _pageManager);
        var triggered = raWithoutPool.RecordAccess("test.cdb", 1, _pageManager);
        
        Assert.False(triggered);
        
        raWithoutPool.Dispose();
    }

    #endregion
}
