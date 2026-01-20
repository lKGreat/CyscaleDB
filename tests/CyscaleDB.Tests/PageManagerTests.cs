using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

public class PageManagerTests : IDisposable
{
    private readonly string _testDir;

    public PageManagerTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "CyscaleDB_Tests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, recursive: true);
        }
    }

    [Fact]
    public void Open_NewFile_ShouldCreateWithZeroPages()
    {
        var filePath = Path.Combine(_testDir, "test.cdb");
        using var pm = new PageManager(filePath);
        
        pm.Open();

        Assert.Equal(0, pm.PageCount);
        Assert.True(File.Exists(filePath));
    }

    [Fact]
    public void AllocatePage_ShouldIncrementPageCount()
    {
        var filePath = Path.Combine(_testDir, "test.cdb");
        using var pm = new PageManager(filePath);
        pm.Open();

        var page1 = pm.AllocatePage();
        var page2 = pm.AllocatePage();

        Assert.Equal(0, page1.PageId);
        Assert.Equal(1, page2.PageId);
        Assert.Equal(2, pm.PageCount);
    }

    [Fact]
    public void WriteThenRead_ShouldPreserveData()
    {
        var filePath = Path.Combine(_testDir, "test.cdb");
        
        // Write
        using (var pm = new PageManager(filePath))
        {
            pm.Open();
            var page = pm.AllocatePage();
            page.InsertRecord(new byte[] { 1, 2, 3, 4, 5 });
            pm.WritePage(page);
        }

        // Read
        using (var pm = new PageManager(filePath))
        {
            pm.Open(createIfNotExists: false);
            Assert.Equal(1, pm.PageCount);
            
            var page = pm.ReadPage(0);
            var record = page.GetRecord(0);
            Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, record);
        }
    }

    [Fact]
    public void ReadPage_InvalidId_ShouldThrow()
    {
        var filePath = Path.Combine(_testDir, "test.cdb");
        using var pm = new PageManager(filePath);
        pm.Open();
        pm.AllocatePage();

        Assert.Throws<ArgumentOutOfRangeException>(() => pm.ReadPage(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => pm.ReadPage(100));
    }

    [Fact]
    public void CorruptedPage_ShouldThrowPageCorruptedException()
    {
        var filePath = Path.Combine(_testDir, "test.cdb");
        
        // Create valid file
        using (var pm = new PageManager(filePath))
        {
            pm.Open();
            var page = pm.AllocatePage();
            page.InsertRecord(new byte[] { 1, 2, 3 });
            pm.WritePage(page);
        }

        // Corrupt the file
        using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Write))
        {
            // Corrupt the checksum area of the first data page (after header)
            fs.Seek(Constants.PageSize + 12, SeekOrigin.Begin);
            fs.Write(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF });
        }

        // Try to read
        using var pm2 = new PageManager(filePath);
        pm2.Open(createIfNotExists: false);
        
        Assert.Throws<PageCorruptedException>(() => pm2.ReadPage(0));
    }

    [Fact]
    public void MultiplePages_ShouldBeIndependent()
    {
        var filePath = Path.Combine(_testDir, "test.cdb");
        using var pm = new PageManager(filePath);
        pm.Open();

        var page1 = pm.AllocatePage();
        page1.InsertRecord(new byte[] { 1, 1, 1 });
        pm.WritePage(page1);

        var page2 = pm.AllocatePage();
        page2.InsertRecord(new byte[] { 2, 2, 2 });
        pm.WritePage(page2);

        var page3 = pm.AllocatePage();
        page3.InsertRecord(new byte[] { 3, 3, 3 });
        pm.WritePage(page3);

        // Re-read and verify
        var read1 = pm.ReadPage(0);
        var read2 = pm.ReadPage(1);
        var read3 = pm.ReadPage(2);

        Assert.Equal(new byte[] { 1, 1, 1 }, read1.GetRecord(0));
        Assert.Equal(new byte[] { 2, 2, 2 }, read2.GetRecord(0));
        Assert.Equal(new byte[] { 3, 3, 3 }, read3.GetRecord(0));
    }

    [Fact]
    public void Flush_ShouldPersistChanges()
    {
        var filePath = Path.Combine(_testDir, "test.cdb");
        using var pm = new PageManager(filePath);
        pm.Open();

        var page = pm.AllocatePage();
        page.InsertRecord(new byte[] { 1, 2, 3 });
        pm.WritePage(page);
        pm.Flush();

        // Verify file size
        var fileInfo = new FileInfo(filePath);
        Assert.True(fileInfo.Length >= Constants.PageSize * 2); // Header + 1 page
    }
}
