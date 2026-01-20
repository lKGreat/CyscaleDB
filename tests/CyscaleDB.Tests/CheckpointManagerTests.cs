using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Transactions;

namespace CyscaleDB.Tests;

public class CheckpointManagerTests : IDisposable
{
    private readonly string _testDir;

    public CheckpointManagerTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"CyscaleDB_CheckpointTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, true);
        }
    }

    private BufferPool CreateBufferPool() => new BufferPool();

    #region CheckpointInfo Tests

    [Fact]
    public void CheckpointInfo_Create_WithValidParameters()
    {
        var startTime = DateTime.UtcNow;
        var endTime = startTime.AddMilliseconds(100);
        var activeTransactions = new List<long> { 1, 2, 3 };

        var info = new CheckpointInfo(100L, startTime, endTime, activeTransactions);

        Assert.Equal(100L, info.CheckpointLsn);
        Assert.Equal(startTime, info.StartTime);
        Assert.Equal(endTime, info.EndTime);
        Assert.Equal(3, info.ActiveTransactions.Count);
        Assert.Contains(1L, info.ActiveTransactions);
        Assert.Contains(2L, info.ActiveTransactions);
        Assert.Contains(3L, info.ActiveTransactions);
    }

    [Fact]
    public void CheckpointInfo_Serialize_Deserialize()
    {
        var startTime = new DateTime(2026, 1, 15, 10, 30, 0, DateTimeKind.Utc);
        var endTime = new DateTime(2026, 1, 15, 10, 30, 1, DateTimeKind.Utc);
        var activeTransactions = new List<long> { 5, 10, 15 };

        var original = new CheckpointInfo(500L, startTime, endTime, activeTransactions);

        var bytes = original.Serialize();
        var deserialized = CheckpointInfo.Deserialize(bytes);

        Assert.Equal(original.CheckpointLsn, deserialized.CheckpointLsn);
        Assert.Equal(original.StartTime, deserialized.StartTime);
        Assert.Equal(original.EndTime, deserialized.EndTime);
        Assert.Equal(original.ActiveTransactions.Count, deserialized.ActiveTransactions.Count);
        for (int i = 0; i < original.ActiveTransactions.Count; i++)
        {
            Assert.Equal(original.ActiveTransactions[i], deserialized.ActiveTransactions[i]);
        }
    }

    [Fact]
    public void CheckpointInfo_Serialize_EmptyActiveTransactions()
    {
        var startTime = DateTime.UtcNow;
        var endTime = startTime.AddMilliseconds(50);

        var original = new CheckpointInfo(0L, startTime, endTime, []);

        var bytes = original.Serialize();
        var deserialized = CheckpointInfo.Deserialize(bytes);

        Assert.Empty(deserialized.ActiveTransactions);
    }

    #endregion

    #region CheckpointManager Basic Tests

    [Fact]
    public void CheckpointManager_Create_WithValidParameters()
    {
        using var walLog = new WalLog(_testDir);
        walLog.Open();

        var bufferPool = CreateBufferPool();

        using var cpManager = new CheckpointManager(_testDir, walLog, bufferPool);

        Assert.Null(cpManager.LastCheckpoint);
    }

    [Fact]
    public void CheckpointManager_TakeCheckpoint_CreatesFile()
    {
        using var walLog = new WalLog(_testDir);
        walLog.Open();

        // Write some WAL entries
        var entry = new WalEntry
        {
            TransactionId = 1,
            Type = WalEntryType.Commit,
            TableName = "",
            NewData = []
        };
        walLog.Write(entry);

        var bufferPool = CreateBufferPool();

        using var cpManager = new CheckpointManager(_testDir, walLog, bufferPool);

        var checkpoint = cpManager.TakeCheckpoint();

        Assert.NotNull(checkpoint);
        Assert.True(checkpoint.CheckpointLsn > 0);
        Assert.True(File.Exists(Path.Combine(_testDir, Constants.CheckpointFileName)));
    }

    [Fact]
    public void CheckpointManager_TakeCheckpoint_UpdatesLastCheckpoint()
    {
        using var walLog = new WalLog(_testDir);
        walLog.Open();

        var entry = new WalEntry
        {
            TransactionId = 1,
            Type = WalEntryType.Insert,
            TableName = "test",
            NewData = [1, 2, 3]
        };
        walLog.Write(entry);

        var bufferPool = CreateBufferPool();

        using var cpManager = new CheckpointManager(_testDir, walLog, bufferPool);

        Assert.Null(cpManager.LastCheckpoint);

        var checkpoint = cpManager.TakeCheckpoint();

        Assert.NotNull(cpManager.LastCheckpoint);
        Assert.Equal(checkpoint.CheckpointLsn, cpManager.LastCheckpoint!.CheckpointLsn);
    }

    [Fact]
    public void CheckpointManager_GetRecoveryStartLsn_ReturnsZero_WhenNoCheckpoint()
    {
        using var walLog = new WalLog(_testDir);
        walLog.Open();

        var bufferPool = CreateBufferPool();

        using var cpManager = new CheckpointManager(_testDir, walLog, bufferPool);

        var lsn = cpManager.GetRecoveryStartLsn();

        Assert.Equal(0L, lsn);
    }

    [Fact]
    public void CheckpointManager_GetRecoveryStartLsn_ReturnsCheckpointLsn()
    {
        using var walLog = new WalLog(_testDir);
        walLog.Open();

        var entry = new WalEntry
        {
            TransactionId = 1,
            Type = WalEntryType.Insert,
            TableName = "test",
            NewData = [1]
        };
        walLog.Write(entry);

        var bufferPool = CreateBufferPool();

        using var cpManager = new CheckpointManager(_testDir, walLog, bufferPool);
        var checkpoint = cpManager.TakeCheckpoint();

        var lsn = cpManager.GetRecoveryStartLsn();

        Assert.Equal(checkpoint.CheckpointLsn, lsn);
    }

    [Fact]
    public void CheckpointManager_LoadsExistingCheckpoint()
    {
        long checkpointLsn;

        // Create and save a checkpoint
        using (var walLog = new WalLog(_testDir))
        {
            walLog.Open();
            var entry = new WalEntry
            {
                TransactionId = 1,
                Type = WalEntryType.Insert,
                TableName = "test",
                NewData = [1]
            };
            walLog.Write(entry);

            var bufferPool = CreateBufferPool();
            using var cpManager = new CheckpointManager(_testDir, walLog, bufferPool);
            var checkpoint = cpManager.TakeCheckpoint();
            checkpointLsn = checkpoint.CheckpointLsn;
        }

        // Reopen and verify checkpoint is loaded
        using (var walLog = new WalLog(_testDir))
        {
            walLog.Open();
            var bufferPool = CreateBufferPool();
            using var cpManager = new CheckpointManager(_testDir, walLog, bufferPool);

            Assert.NotNull(cpManager.LastCheckpoint);
            Assert.Equal(checkpointLsn, cpManager.LastCheckpoint!.CheckpointLsn);
        }
    }

    [Fact]
    public void CheckpointManager_RaisesCompletedEvent()
    {
        using var walLog = new WalLog(_testDir);
        walLog.Open();

        var bufferPool = CreateBufferPool();
        using var cpManager = new CheckpointManager(_testDir, walLog, bufferPool);

        CheckpointInfo? eventCheckpoint = null;
        cpManager.CheckpointCompleted += (sender, args) => eventCheckpoint = args.Checkpoint;

        var checkpoint = cpManager.TakeCheckpoint();

        Assert.NotNull(eventCheckpoint);
        Assert.Equal(checkpoint.CheckpointLsn, eventCheckpoint!.CheckpointLsn);
    }

    #endregion

    #region Periodic Checkpoint Tests

    [Fact]
    public async Task CheckpointManager_StartPeriodicCheckpoint_CreatesCheckpoints()
    {
        using var walLog = new WalLog(_testDir);
        walLog.Open();

        // Write some data
        var entry = new WalEntry
        {
            TransactionId = 1,
            Type = WalEntryType.Insert,
            TableName = "test",
            NewData = [1]
        };
        walLog.Write(entry);

        var bufferPool = CreateBufferPool();
        using var cpManager = new CheckpointManager(_testDir, walLog, bufferPool);

        var checkpointCount = 0;
        cpManager.CheckpointCompleted += (_, _) => Interlocked.Increment(ref checkpointCount);

        // Start with very short interval for testing
        cpManager.StartPeriodicCheckpoint(TimeSpan.FromMilliseconds(100));

        // Wait for at least one checkpoint
        await Task.Delay(250);

        cpManager.StopPeriodicCheckpoint();

        Assert.True(checkpointCount >= 1);
    }

    [Fact]
    public void CheckpointManager_StopPeriodicCheckpoint_StopsTimer()
    {
        using var walLog = new WalLog(_testDir);
        walLog.Open();

        var bufferPool = CreateBufferPool();
        using var cpManager = new CheckpointManager(_testDir, walLog, bufferPool);

        cpManager.StartPeriodicCheckpoint(TimeSpan.FromHours(1));
        cpManager.StopPeriodicCheckpoint();

        // Should not throw
        cpManager.Dispose();
    }

    #endregion

    #region Recovery Tests

    [Fact]
    public void CheckpointManager_Recover_CallsRedoForCommitted()
    {
        using var walLog = new WalLog(_testDir);
        walLog.Open();

        // Begin transaction 1
        walLog.WriteBegin(1);

        // Insert
        var insertEntry = new WalEntry
        {
            TransactionId = 1,
            Type = WalEntryType.Insert,
            TableName = "test",
            PageId = 0,
            SlotNumber = 0,
            NewData = [1, 2, 3]
        };
        walLog.Write(insertEntry);

        // Commit transaction 1
        walLog.WriteCommit(1);
        walLog.Flush();

        var bufferPool = CreateBufferPool();
        using var cpManager = new CheckpointManager(_testDir, walLog, bufferPool);

        var redoCalled = 0;
        var undoCalled = 0;

        cpManager.Recover(
            entry => Interlocked.Increment(ref redoCalled),
            entry => Interlocked.Increment(ref undoCalled));

        Assert.True(redoCalled > 0);
        Assert.Equal(0, undoCalled);
    }

    [Fact]
    public void CheckpointManager_Recover_CallsUndoForUncommitted()
    {
        using var walLog = new WalLog(_testDir);
        walLog.Open();

        // Begin transaction 1 (will be committed)
        walLog.WriteBegin(1);
        var insertEntry1 = new WalEntry
        {
            TransactionId = 1,
            Type = WalEntryType.Insert,
            TableName = "test",
            PageId = 0,
            SlotNumber = 0,
            NewData = [1]
        };
        walLog.Write(insertEntry1);
        walLog.WriteCommit(1);

        // Begin transaction 2 (will NOT be committed - simulating crash)
        walLog.WriteBegin(2);
        var insertEntry2 = new WalEntry
        {
            TransactionId = 2,
            Type = WalEntryType.Insert,
            TableName = "test",
            PageId = 0,
            SlotNumber = 1,
            NewData = [2]
        };
        walLog.Write(insertEntry2);
        // No commit for transaction 2

        walLog.Flush();

        var bufferPool = CreateBufferPool();
        using var cpManager = new CheckpointManager(_testDir, walLog, bufferPool);

        var redoCalled = 0;
        var undoCalled = 0;

        cpManager.Recover(
            entry => Interlocked.Increment(ref redoCalled),
            entry => Interlocked.Increment(ref undoCalled));

        // Transaction 1's insert should be redone, transaction 2's should be undone
        Assert.True(redoCalled >= 1);
        Assert.True(undoCalled >= 1);
    }

    #endregion
}
