using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage.Index;
using Xunit;

namespace CyscaleDB.Tests;

/// <summary>
/// Tests for record locks, gap locks, and next-key locks.
/// </summary>
public class LockTests
{
    // Helper method to create a simple CompositeKey from a single int value
    private static CompositeKey MakeKey(int value) => new(new[] { DataValue.FromInt(value) });

    #region RecordLock Tests

    [Fact]
    public void RecordLock_Creation_SetsPropertiesCorrectly()
    {
        var key = MakeKey(1);
        var recordLock = new RecordLock(
            "testdb", "users", "PRIMARY", key, 
            transactionId: 100, RecordLockType.Exclusive);

        Assert.Equal("testdb", recordLock.DatabaseName);
        Assert.Equal("users", recordLock.TableName);
        Assert.Equal("PRIMARY", recordLock.IndexName);
        Assert.Equal(key, recordLock.Key);
        Assert.Equal(100, recordLock.TransactionId);
        Assert.Equal(RecordLockType.Exclusive, recordLock.LockType);
    }

    [Fact]
    public void RecordLock_SameTransaction_DoesNotConflict()
    {
        var key = MakeKey(1);
        var lock1 = new RecordLock("testdb", "users", "PRIMARY", key, 100, RecordLockType.Exclusive);
        var lock2 = new RecordLock("testdb", "users", "PRIMARY", key, 100, RecordLockType.Exclusive);

        Assert.False(lock1.ConflictsWith(lock2));
    }

    [Fact]
    public void RecordLock_SharedLocks_DoNotConflict()
    {
        var key = MakeKey(1);
        var lock1 = new RecordLock("testdb", "users", "PRIMARY", key, 100, RecordLockType.Shared);
        var lock2 = new RecordLock("testdb", "users", "PRIMARY", key, 101, RecordLockType.Shared);

        Assert.False(lock1.ConflictsWith(lock2));
    }

    [Fact]
    public void RecordLock_ExclusiveAndShared_Conflict()
    {
        var key = MakeKey(1);
        var exclusiveLock = new RecordLock("testdb", "users", "PRIMARY", key, 100, RecordLockType.Exclusive);
        var sharedLock = new RecordLock("testdb", "users", "PRIMARY", key, 101, RecordLockType.Shared);

        Assert.True(exclusiveLock.ConflictsWith(sharedLock));
        Assert.True(sharedLock.ConflictsWith(exclusiveLock));
    }

    [Fact]
    public void RecordLock_DifferentKeys_DoNotConflict()
    {
        var key1 = MakeKey(1);
        var key2 = MakeKey(2);
        var lock1 = new RecordLock("testdb", "users", "PRIMARY", key1, 100, RecordLockType.Exclusive);
        var lock2 = new RecordLock("testdb", "users", "PRIMARY", key2, 101, RecordLockType.Exclusive);

        Assert.False(lock1.ConflictsWith(lock2));
    }

    #endregion

    #region GapLock Tests

    [Fact]
    public void GapLock_Creation_SetsPropertiesCorrectly()
    {
        var lowerBound = MakeKey(1);
        var upperBound = MakeKey(10);
        var gapLock = new GapLock(
            "testdb", "users", "PRIMARY", 
            lowerBound, upperBound, 
            transactionId: 100);

        Assert.Equal("testdb", gapLock.DatabaseName);
        Assert.Equal("users", gapLock.TableName);
        Assert.Equal("PRIMARY", gapLock.IndexName);
        Assert.Equal(lowerBound, gapLock.LowerBound);
        Assert.Equal(upperBound, gapLock.UpperBound);
        Assert.Equal(100, gapLock.TransactionId);
        Assert.False(gapLock.IsNextKeyLock);
    }

    [Fact]
    public void GapLock_ContainsKey_KeyInRange_ReturnsTrue()
    {
        var gapLock = new GapLock(
            "testdb", "users", "PRIMARY",
            MakeKey(1),
            MakeKey(10),
            100);

        Assert.True(gapLock.ContainsKey(MakeKey(5)));
    }

    [Fact]
    public void GapLock_ContainsKey_KeyBelowRange_ReturnsFalse()
    {
        var gapLock = new GapLock(
            "testdb", "users", "PRIMARY",
            MakeKey(1),
            MakeKey(10),
            100);

        Assert.False(gapLock.ContainsKey(MakeKey(0)));
        Assert.False(gapLock.ContainsKey(MakeKey(1))); // Lower is exclusive
    }

    [Fact]
    public void GapLock_ContainsKey_KeyAtUpperBound_DependsOnNextKey()
    {
        var lower = MakeKey(1);
        var upper = MakeKey(10);

        // Pure gap lock: upper bound is exclusive
        var pureGapLock = new GapLock("testdb", "users", "PRIMARY", lower, upper, 100, isNextKeyLock: false);
        Assert.False(pureGapLock.ContainsKey(upper));

        // Next-key lock: upper bound is inclusive
        var nextKeyGapLock = new GapLock("testdb", "users", "PRIMARY", lower, upper, 100, isNextKeyLock: true);
        Assert.True(nextKeyGapLock.ContainsKey(upper));
    }

    [Fact]
    public void GapLock_BlocksInsert_SameTransaction_DoesNotBlock()
    {
        var gapLock = new GapLock(
            "testdb", "users", "PRIMARY",
            MakeKey(1),
            MakeKey(10),
            100);

        Assert.False(gapLock.BlocksInsert(MakeKey(5), 100));
    }

    [Fact]
    public void GapLock_BlocksInsert_DifferentTransaction_Blocks()
    {
        var gapLock = new GapLock(
            "testdb", "users", "PRIMARY",
            MakeKey(1),
            MakeKey(10),
            100);

        Assert.True(gapLock.BlocksInsert(MakeKey(5), 101));
    }

    #endregion

    #region NextKeyLock Tests

    [Fact]
    public void NextKeyLock_Creation_CreatesRecordAndGapLocks()
    {
        var previousKey = MakeKey(5);
        var key = MakeKey(10);
        
        var nextKeyLock = new NextKeyLock(
            "testdb", "users", "PRIMARY",
            previousKey, key,
            transactionId: 100,
            RecordLockMode.Exclusive);

        Assert.NotNull(nextKeyLock.RecordLock);
        Assert.NotNull(nextKeyLock.GapLock);
        Assert.Equal("testdb", nextKeyLock.DatabaseName);
        Assert.Equal("users", nextKeyLock.TableName);
        Assert.Equal("PRIMARY", nextKeyLock.IndexName);
        Assert.Equal(100, nextKeyLock.TransactionId);
        Assert.Equal(RecordLockMode.Exclusive, nextKeyLock.Mode);
    }

    [Fact]
    public void NextKeyLock_RecordLock_LocksTheKey()
    {
        var previousKey = MakeKey(5);
        var key = MakeKey(10);
        
        var nextKeyLock = new NextKeyLock(
            "testdb", "users", "PRIMARY",
            previousKey, key,
            transactionId: 100);

        Assert.Equal(key, nextKeyLock.RecordLock.Key);
    }

    [Fact]
    public void NextKeyLock_GapLock_LocksGapBeforeRecord()
    {
        var previousKey = MakeKey(5);
        var key = MakeKey(10);
        
        var nextKeyLock = new NextKeyLock(
            "testdb", "users", "PRIMARY",
            previousKey, key,
            transactionId: 100);

        Assert.Equal(previousKey, nextKeyLock.GapLock.LowerBound);
        Assert.Equal(key, nextKeyLock.GapLock.UpperBound);
        Assert.True(nextKeyLock.GapLock.IsNextKeyLock);
    }

    [Fact]
    public void NextKeyLock_BlocksInsert_InGap()
    {
        var previousKey = MakeKey(5);
        var key = MakeKey(10);
        
        var nextKeyLock = new NextKeyLock(
            "testdb", "users", "PRIMARY",
            previousKey, key,
            transactionId: 100);

        // Key 7 is in the gap (5, 10)
        Assert.True(nextKeyLock.BlocksInsert(MakeKey(7), 101));
        
        // Key 3 is outside the gap
        Assert.False(nextKeyLock.BlocksInsert(MakeKey(3), 101));
        
        // Key 15 is outside the gap
        Assert.False(nextKeyLock.BlocksInsert(MakeKey(15), 101));
    }

    [Fact]
    public void NextKeyLock_SameTransaction_DoesNotConflict()
    {
        var key = MakeKey(10);
        
        var lock1 = new NextKeyLock("testdb", "users", "PRIMARY", null, key, 100);
        var lock2 = new NextKeyLock("testdb", "users", "PRIMARY", null, key, 100);

        Assert.False(lock1.ConflictsWith(lock2));
    }

    [Fact]
    public void NextKeyLock_DifferentTransaction_ExclusiveLocks_Conflict()
    {
        var key = MakeKey(10);
        
        var lock1 = new NextKeyLock("testdb", "users", "PRIMARY", null, key, 100, RecordLockMode.Exclusive);
        var lock2 = new NextKeyLock("testdb", "users", "PRIMARY", null, key, 101, RecordLockMode.Exclusive);

        Assert.True(lock1.ConflictsWith(lock2));
    }

    [Fact]
    public void NextKeyLock_WithNullPreviousKey_LocksFromNegativeInfinity()
    {
        var key = MakeKey(10);
        
        var nextKeyLock = new NextKeyLock(
            "testdb", "users", "PRIMARY",
            null, // No previous key - locks from negative infinity
            key,
            transactionId: 100);

        Assert.Null(nextKeyLock.GapLock.LowerBound);
        Assert.Equal(key, nextKeyLock.GapLock.UpperBound);

        // Should block any insert less than or equal to key 10
        Assert.True(nextKeyLock.BlocksInsert(MakeKey(5), 101));
        Assert.True(nextKeyLock.BlocksInsert(MakeKey(-100), 101));
    }

    #endregion

    #region NextKeyLockManager Tests

    [Fact]
    public void NextKeyLockManager_AcquireNextKeyLock_Success()
    {
        var manager = new NextKeyLockManager();
        var previousKey = MakeKey(5);
        var key = MakeKey(10);

        var nextKeyLock = manager.AcquireNextKeyLock(
            "testdb", "users", "PRIMARY",
            previousKey, key, 100);

        Assert.NotNull(nextKeyLock);
        Assert.Equal(100, nextKeyLock.TransactionId);
    }

    [Fact]
    public void NextKeyLockManager_AcquireRangeLocks_LocksAllKeys()
    {
        var manager = new NextKeyLockManager();
        var keys = new List<CompositeKey>
        {
            MakeKey(5),
            MakeKey(10),
            MakeKey(15)
        };

        var locks = manager.AcquireRangeLocks("testdb", "users", "PRIMARY", keys, 100);

        Assert.Equal(3, locks.Count);
    }

    [Fact]
    public void NextKeyLockManager_IsInsertBlocked_WhenLockHeld()
    {
        var manager = new NextKeyLockManager();
        var previousKey = MakeKey(5);
        var key = MakeKey(10);

        manager.AcquireNextKeyLock("testdb", "users", "PRIMARY", previousKey, key, 100);

        // Insert at key 7 should be blocked for transaction 101
        Assert.True(manager.IsInsertBlocked("testdb", "users", "PRIMARY", MakeKey(7), 101));

        // Insert at key 7 should NOT be blocked for transaction 100 (same transaction)
        Assert.False(manager.IsInsertBlocked("testdb", "users", "PRIMARY", MakeKey(7), 100));
    }

    [Fact]
    public void NextKeyLockManager_ReleaseTransactionLocks_ReleasesAllLocks()
    {
        var manager = new NextKeyLockManager();
        var key = MakeKey(10);

        manager.AcquireNextKeyLock("testdb", "users", "PRIMARY", null, key, 100);
        
        // Should be blocked before release
        Assert.True(manager.IsInsertBlocked("testdb", "users", "PRIMARY", MakeKey(5), 101));

        manager.ReleaseTransactionLocks(100);

        // Should not be blocked after release
        Assert.False(manager.IsInsertBlocked("testdb", "users", "PRIMARY", MakeKey(5), 101));
    }

    [Fact]
    public void NextKeyLockManager_GetTransactionLocks_ReturnsCorrectLocks()
    {
        var manager = new NextKeyLockManager();
        var key1 = MakeKey(10);
        var key2 = MakeKey(20);

        manager.AcquireNextKeyLock("testdb", "users", "PRIMARY", null, key1, 100);
        manager.AcquireNextKeyLock("testdb", "users", "PRIMARY", key1, key2, 100);

        var locks = manager.GetTransactionLocks(100);
        Assert.Equal(2, locks.Count);

        var locksForOtherTx = manager.GetTransactionLocks(101);
        Assert.Empty(locksForOtherTx);
    }

    #endregion

    #region RecordLockManager Tests

    [Fact]
    public void RecordLockManager_AcquireLock_Success()
    {
        var manager = new RecordLockManager();
        var key = MakeKey(1);

        var recordLock = manager.AcquireLock(
            "testdb", "users", "PRIMARY", key, 100, RecordLockType.Exclusive);

        Assert.NotNull(recordLock);
        Assert.False(recordLock.IsWaiting);
    }

    [Fact]
    public void RecordLockManager_GetTransactionLocks_ReturnsCorrectLocks()
    {
        var manager = new RecordLockManager();
        var key1 = MakeKey(1);
        var key2 = MakeKey(2);

        manager.AcquireLock("testdb", "users", "PRIMARY", key1, 100, RecordLockType.Shared);
        manager.AcquireLock("testdb", "users", "PRIMARY", key2, 100, RecordLockType.Shared);

        var locks = manager.GetTransactionLocks(100);
        Assert.Equal(2, locks.Count);
    }

    [Fact]
    public void RecordLockManager_ReleaseTransactionLocks_ReleasesAllLocks()
    {
        var manager = new RecordLockManager();
        var key = MakeKey(1);

        manager.AcquireLock("testdb", "users", "PRIMARY", key, 100, RecordLockType.Exclusive);
        Assert.Single(manager.GetTransactionLocks(100));

        manager.ReleaseTransactionLocks(100);
        Assert.Empty(manager.GetTransactionLocks(100));
    }

    #endregion

    #region GapLockManager Tests

    [Fact]
    public void GapLockManager_AcquireGapLock_Success()
    {
        var manager = new GapLockManager();
        var lower = MakeKey(1);
        var upper = MakeKey(10);

        var gapLock = manager.AcquireGapLock(
            "testdb", "users", "PRIMARY", lower, upper, 100);

        Assert.NotNull(gapLock);
    }

    [Fact]
    public void GapLockManager_IsInsertBlocked_WhenGapLockHeld()
    {
        var manager = new GapLockManager();
        manager.AcquireGapLock(
            "testdb", "users", "PRIMARY",
            MakeKey(1),
            MakeKey(10),
            100);

        Assert.True(manager.IsInsertBlocked("testdb", "users", "PRIMARY", MakeKey(5), 101));

        Assert.False(manager.IsInsertBlocked("testdb", "users", "PRIMARY", MakeKey(15), 101));
    }

    #endregion
}
