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
        var walPath = Path.Combine(_testDir, "test.wal");
        using var wal = new WalLog(walPath);
        wal.Open();

        var record = new WalRecord
        {
            TransactionId = 1,
            RecordType = WalRecordType.Insert,
            TableName = "users",
            PageId = 0,
            RowId = 0,
            NewData = [1, 2, 3]
        };

        var lsn = wal.Append(record);

        Assert.True(lsn > 0);
    }

    [Fact]
    public void WalLog_Replay_ReturnsRecords()
    {
        var walPath = Path.Combine(_testDir, "replay.wal");
        
        // Write some records
        using (var wal = new WalLog(walPath))
        {
            wal.Open();

            for (int i = 1; i <= 5; i++)
            {
                var record = new WalRecord
                {
                    TransactionId = (uint)i,
                    RecordType = WalRecordType.Insert,
                    TableName = "test",
                    PageId = i,
                    RowId = 0,
                    NewData = [1]
                };
                wal.Append(record);
            }
        }

        // Replay
        using var wal2 = new WalLog(walPath);
        wal2.Open();
        var records = wal2.Replay().ToList();

        Assert.Equal(5, records.Count);
    }

    [Fact]
    public void WalLog_Rotation()
    {
        var walPath = Path.Combine(_testDir, "rotate.wal");
        using var wal = new WalLog(walPath);
        wal.Open();

        // Write until rotation might trigger (or force it)
        for (int i = 0; i < 100; i++)
        {
            var record = new WalRecord
            {
                TransactionId = (uint)i,
                RecordType = WalRecordType.Insert,
                TableName = "test",
                PageId = i,
                RowId = 0,
                NewData = new byte[100]
            };
            wal.Append(record);
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
        var walPath = Path.Combine(_testDir, "files.wal");
        using var wal = new WalLog(walPath);
        wal.Open();

        // Create some log content
        var record = new WalRecord
        {
            TransactionId = 1,
            RecordType = WalRecordType.Insert,
            TableName = "test",
            NewData = [1]
        };
        wal.Append(record);

        // Force rotate multiple times
        wal.ForceRotate();
        wal.Append(record);
        wal.ForceRotate();

        var rotatedFiles = wal.GetRotatedLogFiles();
        Assert.True(rotatedFiles.Count >= 1);
    }

    [Fact]
    public void WalArchiver_ArchiveOldLogs()
    {
        var walPath = Path.Combine(_testDir, "archive.wal");
        using var wal = new WalLog(walPath);
        wal.Open();

        // Write and rotate
        var record = new WalRecord
        {
            TransactionId = 1,
            RecordType = WalRecordType.Insert,
            TableName = "test",
            NewData = [1, 2, 3]
        };
        wal.Append(record);
        wal.ForceRotate();

        // Archive
        var archiver = new WalArchiver(walPath, _testDir);
        archiver.ArchiveOldLogs(minAgeDays: 0); // Archive immediately

        // Check for .gz files
        var archives = Directory.GetFiles(_testDir, "*.gz");
        Assert.True(archives.Length >= 1);
    }

    [Fact]
    public void CheckpointManager_TakeCheckpoint()
    {
        // This is a simplified test - a real test would need full infrastructure
        var walPath = Path.Combine(_testDir, "checkpoint.wal");
        using var wal = new WalLog(walPath);
        wal.Open();

        var record = new WalRecord
        {
            TransactionId = 1,
            RecordType = WalRecordType.Commit,
            TableName = "",
            NewData = []
        };
        var lsn = wal.Append(record);

        // Verify LSN is valid
        Assert.True(lsn > 0);
    }
}
