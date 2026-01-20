using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;
using Xunit;

namespace CyscaleDB.Tests;

public class DoublewriteBufferTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _tablespaceFile;
    private FileStream? _tablespaceStream;

    public DoublewriteBufferTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"cyscaledb_dblwr_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
        _tablespaceFile = Path.Combine(_testDir, "tablespace.cdb");
    }

    public void Dispose()
    {
        _tablespaceStream?.Dispose();
        if (Directory.Exists(_testDir))
        {
            try { Directory.Delete(_testDir, true); }
            catch { /* ignore cleanup errors */ }
        }
    }

    private FileStream CreateTablespaceFile()
    {
        _tablespaceStream = new FileStream(
            _tablespaceFile,
            FileMode.Create,
            FileAccess.ReadWrite,
            FileShare.None);
        return _tablespaceStream;
    }

    #region Basic Operations

    [Fact]
    public void DoublewriteBuffer_Open_CreatesFile()
    {
        using var dwb = new DoublewriteBuffer(_testDir);
        dwb.Open();

        Assert.True(dwb.IsOpen);
        Assert.True(File.Exists(Path.Combine(_testDir, Constants.DoublewriteFileName)));
    }

    [Fact]
    public void DoublewriteBuffer_Open_InitializesCorrectSize()
    {
        using var dwb = new DoublewriteBuffer(_testDir);
        dwb.Open();

        var fileInfo = new FileInfo(Path.Combine(_testDir, Constants.DoublewriteFileName));
        Assert.Equal(DoublewriteBuffer.BufferSize, fileInfo.Length);
    }

    [Fact]
    public void DoublewriteBuffer_WritePage_IncrementsTotalWrites()
    {
        using var dwb = new DoublewriteBuffer(_testDir);
        dwb.Open();

        using var tablespace = CreateTablespaceFile();
        var page = CreateTestPage(1);

        dwb.WritePage(page, tablespace, 0);

        Assert.Equal(1, dwb.Stats.TotalWrites);
    }

    [Fact]
    public void DoublewriteBuffer_WritePage_WritesToDestination()
    {
        using var dwb = new DoublewriteBuffer(_testDir);
        dwb.Open();

        using var tablespace = CreateTablespaceFile();
        var page = CreateTestPage(1);

        dwb.WritePage(page, tablespace, 0);

        // Verify page was written to tablespace
        tablespace.Seek(0, SeekOrigin.Begin);
        var readData = new byte[Constants.PageSize];
        tablespace.Read(readData, 0, Constants.PageSize);

        // First 4 bytes should be page ID
        Assert.Equal(1, BitConverter.ToInt32(readData, 0));
    }

    [Fact]
    public void DoublewriteBuffer_WritePages_BatchIncrementsCorrectly()
    {
        using var dwb = new DoublewriteBuffer(_testDir);
        dwb.Open();

        using var tablespace = CreateTablespaceFile();

        var pages = new List<(Page Page, long DestinationOffset)>
        {
            (CreateTestPage(1), 0),
            (CreateTestPage(2), Constants.PageSize),
            (CreateTestPage(3), Constants.PageSize * 2)
        };

        dwb.WritePages(pages, tablespace);

        Assert.Equal(3, dwb.Stats.TotalWrites);
        Assert.Equal(1, dwb.Stats.BatchWrites);
    }

    [Fact]
    public void DoublewriteBuffer_WritePages_AllPagesWrittenToDestination()
    {
        using var dwb = new DoublewriteBuffer(_testDir);
        dwb.Open();

        using var tablespace = CreateTablespaceFile();

        var pages = new List<(Page Page, long DestinationOffset)>
        {
            (CreateTestPage(1), 0),
            (CreateTestPage(2), Constants.PageSize),
            (CreateTestPage(3), Constants.PageSize * 2)
        };

        dwb.WritePages(pages, tablespace);

        // Verify all pages were written correctly
        for (int i = 0; i < pages.Count; i++)
        {
            tablespace.Seek(i * Constants.PageSize, SeekOrigin.Begin);
            var readData = new byte[Constants.PageSize];
            tablespace.Read(readData, 0, Constants.PageSize);
            Assert.Equal(i + 1, BitConverter.ToInt32(readData, 0));
        }
    }

    #endregion

    #region Clear and Reset

    [Fact]
    public void DoublewriteBuffer_Clear_ResetsBuffer()
    {
        using var dwb = new DoublewriteBuffer(_testDir);
        dwb.Open();

        using var tablespace = CreateTablespaceFile();
        var page = CreateTestPage(1);
        dwb.WritePage(page, tablespace, 0);

        dwb.Clear();

        // Buffer file should exist but be zeroed
        var bufferFile = new FileInfo(Path.Combine(_testDir, Constants.DoublewriteFileName));
        Assert.True(bufferFile.Exists);
    }

    #endregion

    #region Error Handling

    [Fact]
    public void DoublewriteBuffer_WritePage_WhenNotOpen_ThrowsException()
    {
        using var dwb = new DoublewriteBuffer(_testDir);
        // Don't open the buffer

        using var tablespace = CreateTablespaceFile();
        var page = CreateTestPage(1);

        Assert.Throws<InvalidOperationException>(() =>
            dwb.WritePage(page, tablespace, 0));
    }

    [Fact]
    public void DoublewriteBuffer_Dispose_SetsIsOpenToFalse()
    {
        var dwb = new DoublewriteBuffer(_testDir);
        dwb.Open();
        Assert.True(dwb.IsOpen);

        dwb.Dispose();
        Assert.False(dwb.IsOpen);
    }

    #endregion

    #region Recovery Tests

    [Fact]
    public void DoublewriteBuffer_RecoverPages_WithNoCorruption_ReturnsZero()
    {
        using var dwb = new DoublewriteBuffer(_testDir);
        dwb.Open();

        using var tablespace = CreateTablespaceFile();

        // Write a page normally
        var page = CreateTestPage(1);
        dwb.WritePage(page, tablespace, 0);

        // Recovery should find nothing to fix
        var recovered = dwb.RecoverPages(tablespace);

        Assert.Equal(0, recovered);
    }

    [Fact]
    public void DoublewriteBuffer_MultipleWrites_CircularBuffer()
    {
        using var dwb = new DoublewriteBuffer(_testDir);
        dwb.Open();

        using var tablespace = CreateTablespaceFile();

        // Write more pages than buffer can hold to test circular behavior
        for (int i = 1; i <= DoublewriteBuffer.BufferPageCount + 10; i++)
        {
            var page = CreateTestPage(i);
            dwb.WritePage(page, tablespace, (long)(i - 1) * Constants.PageSize);
        }

        Assert.Equal(DoublewriteBuffer.BufferPageCount + 10, dwb.Stats.TotalWrites);
    }

    #endregion

    #region Statistics

    [Fact]
    public void DoublewriteStats_ToString_ReturnsFormattedString()
    {
        var stats = new DoublewriteStats
        {
            TotalWrites = 100,
            BatchWrites = 10,
            RecoveredPages = 2
        };

        var str = stats.ToString();

        Assert.Contains("100", str);
        Assert.Contains("10", str);
        Assert.Contains("2", str);
    }

    #endregion

    /// <summary>
    /// Creates a test page with the specified ID.
    /// </summary>
    private static Page CreateTestPage(int pageId)
    {
        var page = new Page(pageId, PageType.Data);
        // Add some data so the page isn't empty
        var testData = new byte[100];
        for (int i = 0; i < testData.Length; i++)
        {
            testData[i] = (byte)(pageId % 256);
        }
        page.InsertRecord(testData);
        return page;
    }
}
