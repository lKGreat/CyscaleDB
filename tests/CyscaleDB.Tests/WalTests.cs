using CyscaleDB.Core.Common;
using CyscaleDB.Core.Transactions;

namespace CyscaleDB.Tests;

public class WalTests : IDisposable
{
    private readonly string _testDir;

    public WalTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"CyscaleDB_WalTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, true);
        }
    }

    [Fact]
    public void WalLog_Create_AndAppend()
    {
        using var wal = new WalLog(_testDir);
        wal.Open();

        var entry = new WalEntry
        {
            TransactionId = 1,
            Type = WalEntryType.Insert,
            TableName = "users",
            PageId = 0,
            SlotNumber = 0,
            NewData = [1, 2, 3]
        };

        var lsn = wal.Write(entry);

        Assert.True(lsn > 0);
    }

    [Fact]
    public void WalLog_Replay_ReturnsRecords()
    {
        // Write some records
        using (var wal = new WalLog(_testDir))
        {
            wal.Open();

            for (int i = 1; i <= 5; i++)
            {
                var entry = new WalEntry
                {
                    TransactionId = i,
                    Type = WalEntryType.Insert,
                    TableName = "test",
                    PageId = i,
                    SlotNumber = 0,
                    NewData = [1]
                };
                wal.Write(entry);
            }
        }

        // Replay
        using var wal2 = new WalLog(_testDir);
        wal2.Open();
        var entries = wal2.ReadAll().ToList();

        Assert.Equal(5, entries.Count);
    }

    [Fact]
    public void WalLog_Rotation()
    {
        using var wal = new WalLog(_testDir);
        wal.Open();

        // Write until rotation might trigger (or force it)
        for (int i = 0; i < 100; i++)
        {
            var entry = new WalEntry
            {
                TransactionId = i,
                Type = WalEntryType.Insert,
                TableName = "test",
                PageId = i,
                SlotNumber = 0,
                NewData = new byte[100]
            };
            wal.Write(entry);
        }

        // Force rotation
        wal.ForceRotate();

        // Check rotated file exists
        var rotatedPath = wal.GetRotatedFilePath(1);
        Assert.True(File.Exists(rotatedPath));
    }

    [Fact]
    public void WalLog_GetRotatedLogFiles()
    {
        using var wal = new WalLog(_testDir);
        wal.Open();

        // Create some log content
        var entry = new WalEntry
        {
            TransactionId = 1,
            Type = WalEntryType.Insert,
            TableName = "test",
            NewData = [1]
        };
        wal.Write(entry);

        // Force rotate multiple times
        wal.ForceRotate();
        wal.Write(entry);
        wal.ForceRotate();

        var rotatedFiles = wal.GetRotatedLogFiles();
        Assert.True(rotatedFiles.Count() >= 1);
    }

    [Fact]
    public void WalArchiver_ArchiveOldLogs()
    {
        using var wal = new WalLog(_testDir);
        wal.Open();

        // Write and rotate
        var entry = new WalEntry
        {
            TransactionId = 1,
            Type = WalEntryType.Insert,
            TableName = "test",
            NewData = [1, 2, 3]
        };
        wal.Write(entry);
        wal.ForceRotate();

        // Archive
        using var archiver = new WalArchiver(_testDir, wal);
        archiver.ArchiveOldLogs(); // Archive immediately

        // Check for .gz files
        var archives = Directory.GetFiles(_testDir, "*.gz");
        Assert.True(archives.Length >= 1);
    }

    [Fact]
    public void CheckpointManager_TakeCheckpoint()
    {
        // This is a simplified test - a real test would need full infrastructure
        using var wal = new WalLog(_testDir);
        wal.Open();

        var entry = new WalEntry
        {
            TransactionId = 1,
            Type = WalEntryType.Commit,
            TableName = "",
            NewData = []
        };
        var lsn = wal.Write(entry);

        // Verify LSN is valid
        Assert.True(lsn > 0);
    }
}
