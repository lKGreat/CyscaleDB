using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Transactions;

/// <summary>
/// Manages locks for concurrent transaction control.
/// Implements table-level and row-level locking with deadlock detection.
/// </summary>
public sealed class LockManager : IDisposable
{
    private readonly Dictionary<LockKey, LockState> _locks;
    private readonly Dictionary<long, HashSet<LockKey>> _transactionLocks;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Logger _logger;
    private readonly TimeSpan _lockTimeout;
    private bool _disposed;

    // Gap lock management using IntervalTree for O(log n) conflict detection
    private readonly Dictionary<string, IntervalTree<long, GapLockInfo>> _gapLocks;
    private readonly Dictionary<long, List<GapLockKey>> _transactionGapLocks;

    /// <summary>
    /// Creates a new lock manager.
    /// </summary>
    public LockManager(TimeSpan? lockTimeout = null)
    {
        _locks = new Dictionary<LockKey, LockState>();
        _transactionLocks = new Dictionary<long, HashSet<LockKey>>();
        _gapLocks = new Dictionary<string, IntervalTree<long, GapLockInfo>>();
        _transactionGapLocks = new Dictionary<long, List<GapLockKey>>();
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<LockManager>();
        _lockTimeout = lockTimeout ?? TimeSpan.FromSeconds(30);
    }

    /// <summary>
    /// Creates a LockManager using the current configuration settings.
    /// </summary>
    public static LockManager CreateFromConfiguration()
    {
        var config = Common.CyscaleDbConfiguration.Current;
        var timeout = TimeSpan.FromMilliseconds(config.LockWaitTimeoutMs);
        return new LockManager(timeout);
    }

    /// <summary>
    /// Acquires a lock on a table.
    /// </summary>
    public bool AcquireTableLock(Transaction transaction, string databaseName, string tableName, LockMode mode)
    {
        var key = new LockKey(databaseName, tableName);
        return AcquireLock(transaction, key, mode);
    }

    /// <summary>
    /// Acquires a lock on a row.
    /// </summary>
    public bool AcquireRowLock(Transaction transaction, string databaseName, string tableName, int pageId, short slotNumber, LockMode mode)
    {
        var key = new LockKey(databaseName, tableName, pageId, slotNumber);
        return AcquireLock(transaction, key, mode);
    }

    /// <summary>
    /// Acquires a gap lock on a key range. Uses IntervalTree for O(log n) conflict detection.
    /// </summary>
    /// <param name="transaction">The transaction acquiring the lock</param>
    /// <param name="databaseName">Database name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="indexName">Index name</param>
    /// <param name="lowKey">Lower bound of the key range</param>
    /// <param name="highKey">Upper bound of the key range</param>
    /// <param name="mode">Lock mode (typically Shared or Exclusive)</param>
    /// <returns>True if the gap lock was acquired</returns>
    public bool AcquireGapLock(Transaction transaction, string databaseName, string tableName, 
        string indexName, long lowKey, long highKey, LockMode mode)
    {
        var treeKey = $"{databaseName}.{tableName}.{indexName}";
        
        _lock.EnterWriteLock();
        try
        {
            // Get or create the interval tree for this index
            if (!_gapLocks.TryGetValue(treeKey, out var tree))
            {
                tree = new IntervalTree<long, GapLockInfo>();
                _gapLocks[treeKey] = tree;
            }

            // Check for conflicts using the interval tree (O(log n))
            var overlapping = tree.QueryRange(lowKey, highKey);
            foreach (var existing in overlapping)
            {
                if (existing.TransactionId != transaction.TransactionId)
                {
                    // Conflict: another transaction holds a gap lock on overlapping range
                    if (mode == LockMode.Exclusive || existing.Mode == LockMode.Exclusive)
                    {
                        _logger.Debug("Gap lock conflict: Transaction {0} blocked by {1} on range [{2}, {3}]",
                            transaction.TransactionId, existing.TransactionId, lowKey, highKey);
                        return false;
                    }
                }
            }

            // Insert the gap lock
            var lockInfo = new GapLockInfo(transaction.TransactionId, mode);
            tree.Insert(lowKey, highKey, lockInfo);

            // Track for transaction cleanup
            if (!_transactionGapLocks.TryGetValue(transaction.TransactionId, out var gapList))
            {
                gapList = new List<GapLockKey>();
                _transactionGapLocks[transaction.TransactionId] = gapList;
            }
            gapList.Add(new GapLockKey(treeKey, lowKey, highKey));

            _logger.Trace("Acquired gap lock: Transaction {0} on [{1}, {2}] mode {3}",
                transaction.TransactionId, lowKey, highKey, mode);

            return true;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Releases all gap locks held by a transaction.
    /// </summary>
    public void ReleaseGapLocks(Transaction transaction)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_transactionGapLocks.TryGetValue(transaction.TransactionId, out var gapList))
            {
                return;
            }

            foreach (var gapKey in gapList)
            {
                if (_gapLocks.TryGetValue(gapKey.TreeKey, out var tree))
                {
                    tree.Remove(gapKey.LowKey, gapKey.HighKey);
                }
            }

            _transactionGapLocks.Remove(transaction.TransactionId);
            _logger.Trace("Released {0} gap locks for transaction {1}", 
                gapList.Count, transaction.TransactionId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Checks if inserting a key would conflict with any gap lock.
    /// </summary>
    public bool HasGapLockConflict(Transaction transaction, string databaseName, string tableName, 
        string indexName, long key)
    {
        var treeKey = $"{databaseName}.{tableName}.{indexName}";

        _lock.EnterReadLock();
        try
        {
            if (!_gapLocks.TryGetValue(treeKey, out var tree))
            {
                return false; // No gap locks on this index
            }

            // Query for gap locks containing this key (O(log n))
            var overlapping = tree.Query(key);
            foreach (var lockInfo in overlapping)
            {
                if (lockInfo.TransactionId != transaction.TransactionId)
                {
                    return true; // Conflict with another transaction's gap lock
                }
            }

            return false;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Releases a specific lock held by a transaction.
    /// </summary>
    public void ReleaseLock(Transaction transaction, LockKey key)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_locks.TryGetValue(key, out var lockState))
                return;

            lockState.Holders.RemoveAll(h => h.TransactionId == transaction.TransactionId);

            if (lockState.Holders.Count == 0)
            {
                _locks.Remove(key);
            }

            if (_transactionLocks.TryGetValue(transaction.TransactionId, out var txLocks))
            {
                txLocks.Remove(key);
            }

            // Wake up waiting transactions
            lockState.WaitSignal.Set();

            _logger.Trace("Released lock on {0} by transaction {1}", key, transaction.TransactionId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Releases all locks held by a transaction.
    /// </summary>
    public void ReleaseAllLocks(Transaction transaction)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_transactionLocks.TryGetValue(transaction.TransactionId, out var txLocks))
                return;

            foreach (var key in txLocks.ToList())
            {
                if (_locks.TryGetValue(key, out var lockState))
                {
                    lockState.Holders.RemoveAll(h => h.TransactionId == transaction.TransactionId);
                    
                    if (lockState.Holders.Count == 0)
                    {
                        _locks.Remove(key);
                    }
                    else
                    {
                        lockState.WaitSignal.Set();
                    }
                }
            }

            _transactionLocks.Remove(transaction.TransactionId);
            transaction.HeldLocks.Clear();

            _logger.Debug("Released all locks for transaction {0}", transaction.TransactionId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Upgrades a shared lock to an exclusive lock.
    /// </summary>
    public bool UpgradeLock(Transaction transaction, LockKey key)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_locks.TryGetValue(key, out var lockState))
                return false;

            var holder = lockState.Holders.Find(h => h.TransactionId == transaction.TransactionId);
            if (holder == null || holder.Mode != LockMode.Shared)
                return false;

            // Check if we can upgrade (only holder)
            if (lockState.Holders.Count == 1)
            {
                holder.Mode = LockMode.Exclusive;
                return true;
            }

            return false;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private bool AcquireLock(Transaction transaction, LockKey key, LockMode mode)
    {
        var deadline = DateTime.UtcNow + _lockTimeout;

        while (DateTime.UtcNow < deadline)
        {
            _lock.EnterWriteLock();
            try
            {
                // Check if transaction already holds this lock
                if (_locks.TryGetValue(key, out var existingLock))
                {
                    var existingHolder = existingLock.Holders.Find(h => h.TransactionId == transaction.TransactionId);
                    if (existingHolder != null)
                    {
                        // Already holds lock - upgrade if needed
                        if (existingHolder.Mode == LockMode.Exclusive || mode == LockMode.Shared)
                        {
                            return true; // Already have sufficient lock
                        }

                        // Try to upgrade shared to exclusive
                        if (existingLock.Holders.Count == 1)
                        {
                            existingHolder.Mode = LockMode.Exclusive;
                            return true;
                        }

                        // Cannot upgrade - wait
                    }
                    else
                    {
                        // Check compatibility
                        if (IsCompatible(existingLock, mode))
                        {
                            // Can acquire
                            existingLock.Holders.Add(new LockHolder(transaction.TransactionId, mode));
                            TrackLock(transaction, key, mode);
                            _logger.Trace("Acquired {0} lock on {1} by transaction {2}", mode, key, transaction.TransactionId);
                            return true;
                        }

                        // Check for deadlock
                        if (DetectDeadlock(transaction, existingLock))
                        {
                            throw new DeadlockException(transaction.TransactionId);
                        }
                    }

                    // Need to wait
                    var waitSignal = existingLock.WaitSignal;
                    _lock.ExitWriteLock();

                    // Wait for lock to be released
                    var waitTime = deadline - DateTime.UtcNow;
                    if (waitTime > TimeSpan.Zero)
                    {
                        waitSignal.Wait(waitTime);
                    }

                    continue;
                }

                // No existing lock - create new one
                var newLock = new LockState();
                newLock.Holders.Add(new LockHolder(transaction.TransactionId, mode));
                _locks[key] = newLock;
                TrackLock(transaction, key, mode);

                _logger.Trace("Acquired {0} lock on {1} by transaction {2}", mode, key, transaction.TransactionId);
                return true;
            }
            finally
            {
                if (_lock.IsWriteLockHeld)
                    _lock.ExitWriteLock();
            }
        }

        // Timeout
        throw new LockTimeoutException(transaction.TransactionId, key.ToString());
    }

    private void TrackLock(Transaction transaction, LockKey key, LockMode mode)
    {
        if (!_transactionLocks.TryGetValue(transaction.TransactionId, out var txLocks))
        {
            txLocks = new HashSet<LockKey>();
            _transactionLocks[transaction.TransactionId] = txLocks;
        }

        txLocks.Add(key);
        transaction.HeldLocks.Add(new LockEntry(key, mode));
    }

    private static bool IsCompatible(LockState existingLock, LockMode requestedMode)
    {
        // Shared locks are compatible with other shared locks
        // Exclusive locks are not compatible with any other lock

        if (requestedMode == LockMode.Shared)
        {
            return existingLock.Holders.All(h => h.Mode == LockMode.Shared);
        }

        // Exclusive lock requested - must have no holders
        return existingLock.Holders.Count == 0;
    }

    private bool DetectDeadlock(Transaction requestingTx, LockState blockingLock)
    {
        // Simple deadlock detection: check if any holder is waiting for a lock we hold
        var visited = new HashSet<long>();
        var queue = new Queue<long>();

        foreach (var holder in blockingLock.Holders)
        {
            queue.Enqueue(holder.TransactionId);
        }

        while (queue.Count > 0)
        {
            var txId = queue.Dequeue();
            if (txId == requestingTx.TransactionId)
            {
                return true; // Cycle detected
            }

            if (!visited.Add(txId))
                continue;

            // Find locks this transaction is waiting for
            // For simplicity, we check if any lock held by requestingTx is wanted by txId
            // This is a simplified check - full deadlock detection would track wait-for graph
            if (_transactionLocks.TryGetValue(requestingTx.TransactionId, out var ourLocks))
            {
                foreach (var ourLock in ourLocks)
                {
                    if (_locks.TryGetValue(ourLock, out var lockState))
                    {
                        if (lockState.Holders.Any(h => h.TransactionId == txId))
                        {
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        
        foreach (var lockState in _locks.Values)
        {
            lockState.WaitSignal.Dispose();
        }

        _lock.Dispose();
    }
}

/// <summary>
/// Identifies a lockable resource.
/// </summary>
public readonly struct LockKey : IEquatable<LockKey>
{
    public string DatabaseName { get; }
    public string TableName { get; }
    public int? PageId { get; }
    public short? SlotNumber { get; }
    public LockGranularity Granularity { get; }

    /// <summary>
    /// Creates a table-level lock key.
    /// </summary>
    public LockKey(string databaseName, string tableName)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        PageId = null;
        SlotNumber = null;
        Granularity = LockGranularity.Table;
    }

    /// <summary>
    /// Creates a row-level lock key.
    /// </summary>
    public LockKey(string databaseName, string tableName, int pageId, short slotNumber)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        PageId = pageId;
        SlotNumber = slotNumber;
        Granularity = LockGranularity.Row;
    }

    public bool Equals(LockKey other)
    {
        return DatabaseName == other.DatabaseName &&
               TableName == other.TableName &&
               PageId == other.PageId &&
               SlotNumber == other.SlotNumber;
    }

    public override bool Equals(object? obj) => obj is LockKey other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(DatabaseName, TableName, PageId, SlotNumber);

    public override string ToString()
    {
        if (Granularity == LockGranularity.Table)
            return $"{DatabaseName}.{TableName}";
        return $"{DatabaseName}.{TableName}[{PageId}:{SlotNumber}]";
    }
}

/// <summary>
/// Lock granularity.
/// </summary>
public enum LockGranularity
{
    Database,
    Table,
    Page,
    Row
}

/// <summary>
/// Lock modes.
/// </summary>
public enum LockMode
{
    /// <summary>
    /// Shared lock (for reading).
    /// </summary>
    Shared,

    /// <summary>
    /// Exclusive lock (for writing).
    /// </summary>
    Exclusive,

    /// <summary>
    /// Intent shared (for table-level intent).
    /// Indicates that the transaction intends to acquire shared locks on rows within the table.
    /// </summary>
    IntentShared,

    /// <summary>
    /// Intent exclusive (for table-level intent).
    /// Indicates that the transaction intends to acquire exclusive locks on rows within the table.
    /// </summary>
    IntentExclusive,

    /// <summary>
    /// Shared intent exclusive (SIX) - combination of S and IX locks.
    /// Indicates the transaction is reading the whole table and modifying some rows.
    /// </summary>
    SharedIntentExclusive
}

/// <summary>
/// Internal state of a lock.
/// </summary>
internal class LockState
{
    public List<LockHolder> Holders { get; } = [];
    public ManualResetEventSlim WaitSignal { get; } = new(false);
}

/// <summary>
/// Represents a transaction holding a lock.
/// </summary>
internal class LockHolder
{
    public long TransactionId { get; }
    public LockMode Mode { get; set; }

    public LockHolder(long transactionId, LockMode mode)
    {
        TransactionId = transactionId;
        Mode = mode;
    }
}

/// <summary>
/// Entry tracking a lock held by a transaction.
/// </summary>
public class LockEntry
{
    public LockKey Key { get; }
    public LockMode Mode { get; }

    public LockEntry(LockKey key, LockMode mode)
    {
        Key = key;
        Mode = mode;
    }
}

/// <summary>
/// Represents an intent lock on a table.
/// Intent locks indicate that a transaction plans to acquire row-level locks.
/// 
/// Lock Compatibility Matrix:
///         IS      IX      S       X       SIX
/// IS      ✓       ✓       ✓       ✗       ✓
/// IX      ✓       ✓       ✗       ✗       ✗
/// S       ✓       ✗       ✓       ✗       ✗
/// X       ✗       ✗       ✗       ✗       ✗
/// SIX     ✓       ✗       ✗       ✗       ✗
/// </summary>
public sealed class IntentLock
{
    /// <summary>
    /// The database name.
    /// </summary>
    public string DatabaseName { get; }

    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// The transaction holding this lock.
    /// </summary>
    public long TransactionId { get; }

    /// <summary>
    /// The lock mode (IS, IX, S, X, or SIX).
    /// </summary>
    public LockMode Mode { get; }

    /// <summary>
    /// When this lock was acquired.
    /// </summary>
    public DateTime AcquiredAt { get; }

    /// <summary>
    /// Whether this lock is waiting to be granted.
    /// </summary>
    public bool IsWaiting { get; private set; }

    /// <summary>
    /// Creates a new intent lock.
    /// </summary>
    public IntentLock(string databaseName, string tableName, long transactionId, LockMode mode, bool isWaiting = false)
    {
        DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        TransactionId = transactionId;
        Mode = mode;
        IsWaiting = isWaiting;
        AcquiredAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Grants the lock.
    /// </summary>
    public void Grant()
    {
        IsWaiting = false;
    }

    /// <summary>
    /// Gets the table key for this lock.
    /// </summary>
    public string GetTableKey() => $"{DatabaseName}.{TableName}";

    /// <summary>
    /// Checks if this lock is compatible with another lock mode.
    /// </summary>
    public static bool AreCompatible(LockMode mode1, LockMode mode2)
    {
        // Same transaction is always compatible with itself
        // Lock compatibility matrix:
        //         IS      IX      S       X       SIX
        // IS      ✓       ✓       ✓       ✗       ✓
        // IX      ✓       ✓       ✗       ✗       ✗
        // S       ✓       ✗       ✓       ✗       ✗
        // X       ✗       ✗       ✗       ✗       ✗
        // SIX     ✓       ✗       ✗       ✗       ✗

        return (mode1, mode2) switch
        {
            // Exclusive is not compatible with anything
            (LockMode.Exclusive, _) => false,
            (_, LockMode.Exclusive) => false,

            // IS is compatible with IS, IX, S, SIX
            (LockMode.IntentShared, LockMode.IntentShared) => true,
            (LockMode.IntentShared, LockMode.IntentExclusive) => true,
            (LockMode.IntentShared, LockMode.Shared) => true,
            (LockMode.IntentShared, LockMode.SharedIntentExclusive) => true,

            // IX is compatible with IS, IX
            (LockMode.IntentExclusive, LockMode.IntentShared) => true,
            (LockMode.IntentExclusive, LockMode.IntentExclusive) => true,
            (LockMode.IntentExclusive, _) => false,

            // S is compatible with IS, S
            (LockMode.Shared, LockMode.IntentShared) => true,
            (LockMode.Shared, LockMode.Shared) => true,
            (LockMode.Shared, _) => false,

            // SIX is compatible with IS only
            (LockMode.SharedIntentExclusive, LockMode.IntentShared) => true,
            (LockMode.SharedIntentExclusive, _) => false,

            // Default: not compatible
            _ => false
        };
    }

    /// <summary>
    /// Checks if this lock conflicts with another lock.
    /// </summary>
    public bool ConflictsWith(IntentLock other)
    {
        // Same transaction doesn't conflict with itself
        if (TransactionId == other.TransactionId)
            return false;

        // Check if same table
        if (GetTableKey() != other.GetTableKey())
            return false;

        return !AreCompatible(Mode, other.Mode);
    }

    public override string ToString()
    {
        var waiting = IsWaiting ? " (waiting)" : "";
        return $"IntentLock({Mode}) on {GetTableKey()} by Tx{TransactionId}{waiting}";
    }
}

/// <summary>
/// Manages intent locks for table-level lock intentions.
/// </summary>
public sealed class IntentLockManager
{
    private readonly Dictionary<string, List<IntentLock>> _locksByTable = [];
    private readonly Dictionary<long, List<IntentLock>> _locksByTransaction = [];
    private readonly object _lock = new();
    private readonly Logger _logger;
    private readonly TimeSpan _lockTimeout;

    public IntentLockManager(TimeSpan? lockTimeout = null)
    {
        _logger = LogManager.Default.GetLogger<IntentLockManager>();
        _lockTimeout = lockTimeout ?? TimeSpan.FromSeconds(50);
    }

    /// <summary>
    /// Acquires an intent lock on a table.
    /// </summary>
    /// <param name="databaseName">Database name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="transactionId">Transaction ID</param>
    /// <param name="mode">Lock mode (typically IS or IX)</param>
    /// <returns>The acquired lock, or null if timeout</returns>
    public IntentLock? AcquireLock(string databaseName, string tableName, long transactionId, LockMode mode)
    {
        var newLock = new IntentLock(databaseName, tableName, transactionId, mode, isWaiting: true);
        var deadline = DateTime.UtcNow + _lockTimeout;

        lock (_lock)
        {
            var tableKey = newLock.GetTableKey();

            // Check if transaction already holds a compatible or stronger lock
            if (_locksByTransaction.TryGetValue(transactionId, out var txLocks))
            {
                var existingLock = txLocks.Find(l => l.GetTableKey() == tableKey);
                if (existingLock != null)
                {
                    // Check if existing lock is sufficient
                    if (IsLockSufficient(existingLock.Mode, mode))
                    {
                        return existingLock; // Already have sufficient lock
                    }

                    // Upgrade the lock if possible
                    var upgradedMode = GetUpgradedMode(existingLock.Mode, mode);
                    if (upgradedMode != existingLock.Mode)
                    {
                        // Need to upgrade - remove old lock and acquire new one
                        RemoveLockInternal(existingLock);
                        newLock = new IntentLock(databaseName, tableName, transactionId, upgradedMode, isWaiting: true);
                    }
                }
            }

            // Check for conflicts
            if (_locksByTable.TryGetValue(tableKey, out var tableLocks))
            {
                foreach (var existingLock in tableLocks)
                {
                    if (existingLock.ConflictsWith(newLock))
                    {
                        _logger.Debug("Intent lock conflict: {0} conflicts with {1}", newLock, existingLock);
                        // In a real implementation, we would wait
                        // For now, add as waiting
                        AddLockInternal(newLock);
                        return newLock;
                    }
                }
            }

            // No conflicts, grant the lock
            newLock.Grant();
            AddLockInternal(newLock);
            _logger.Debug("Acquired intent lock: {0}", newLock);
            return newLock;
        }
    }

    /// <summary>
    /// Acquires an Intent Shared (IS) lock.
    /// </summary>
    public IntentLock? AcquireIntentShared(string databaseName, string tableName, long transactionId)
    {
        return AcquireLock(databaseName, tableName, transactionId, LockMode.IntentShared);
    }

    /// <summary>
    /// Acquires an Intent Exclusive (IX) lock.
    /// </summary>
    public IntentLock? AcquireIntentExclusive(string databaseName, string tableName, long transactionId)
    {
        return AcquireLock(databaseName, tableName, transactionId, LockMode.IntentExclusive);
    }

    /// <summary>
    /// Acquires a table-level Shared (S) lock.
    /// </summary>
    public IntentLock? AcquireShared(string databaseName, string tableName, long transactionId)
    {
        return AcquireLock(databaseName, tableName, transactionId, LockMode.Shared);
    }

    /// <summary>
    /// Acquires a table-level Exclusive (X) lock.
    /// </summary>
    public IntentLock? AcquireExclusive(string databaseName, string tableName, long transactionId)
    {
        return AcquireLock(databaseName, tableName, transactionId, LockMode.Exclusive);
    }

    /// <summary>
    /// Releases all locks held by a transaction.
    /// </summary>
    public void ReleaseTransactionLocks(long transactionId)
    {
        lock (_lock)
        {
            if (!_locksByTransaction.TryGetValue(transactionId, out var txLocks))
                return;

            foreach (var intentLock in txLocks.ToList())
            {
                RemoveLockFromTable(intentLock);
            }

            _locksByTransaction.Remove(transactionId);
            _logger.Debug("Released all intent locks for Tx{0}", transactionId);
        }
    }

    /// <summary>
    /// Releases a specific lock.
    /// </summary>
    public void ReleaseLock(IntentLock intentLock)
    {
        lock (_lock)
        {
            RemoveLockInternal(intentLock);
            _logger.Debug("Released intent lock: {0}", intentLock);
        }
    }

    /// <summary>
    /// Gets all intent locks held by a transaction.
    /// </summary>
    public IReadOnlyList<IntentLock> GetTransactionLocks(long transactionId)
    {
        lock (_lock)
        {
            if (_locksByTransaction.TryGetValue(transactionId, out var locks))
            {
                return locks.AsReadOnly();
            }
            return [];
        }
    }

    /// <summary>
    /// Gets all intent locks on a table.
    /// </summary>
    public IReadOnlyList<IntentLock> GetTableLocks(string databaseName, string tableName)
    {
        lock (_lock)
        {
            var tableKey = $"{databaseName}.{tableName}";
            if (_locksByTable.TryGetValue(tableKey, out var locks))
            {
                return locks.AsReadOnly();
            }
            return [];
        }
    }

    /// <summary>
    /// Checks if a lock request would conflict with existing locks.
    /// </summary>
    public bool WouldConflict(string databaseName, string tableName, long transactionId, LockMode mode)
    {
        var testLock = new IntentLock(databaseName, tableName, transactionId, mode);

        lock (_lock)
        {
            var tableKey = testLock.GetTableKey();
            if (_locksByTable.TryGetValue(tableKey, out var tableLocks))
            {
                return tableLocks.Any(existing => existing.ConflictsWith(testLock));
            }
            return false;
        }
    }

    private void AddLockInternal(IntentLock intentLock)
    {
        var tableKey = intentLock.GetTableKey();

        if (!_locksByTable.TryGetValue(tableKey, out var tableLocks))
        {
            tableLocks = [];
            _locksByTable[tableKey] = tableLocks;
        }
        tableLocks.Add(intentLock);

        if (!_locksByTransaction.TryGetValue(intentLock.TransactionId, out var txLocks))
        {
            txLocks = [];
            _locksByTransaction[intentLock.TransactionId] = txLocks;
        }
        txLocks.Add(intentLock);
    }

    private void RemoveLockInternal(IntentLock intentLock)
    {
        RemoveLockFromTable(intentLock);
        RemoveLockFromTransaction(intentLock);
    }

    private void RemoveLockFromTable(IntentLock intentLock)
    {
        var tableKey = intentLock.GetTableKey();
        if (_locksByTable.TryGetValue(tableKey, out var tableLocks))
        {
            tableLocks.Remove(intentLock);
            if (tableLocks.Count == 0)
            {
                _locksByTable.Remove(tableKey);
            }
        }
    }

    private void RemoveLockFromTransaction(IntentLock intentLock)
    {
        if (_locksByTransaction.TryGetValue(intentLock.TransactionId, out var txLocks))
        {
            txLocks.Remove(intentLock);
            if (txLocks.Count == 0)
            {
                _locksByTransaction.Remove(intentLock.TransactionId);
            }
        }
    }

    private static bool IsLockSufficient(LockMode existing, LockMode requested)
    {
        // Check if existing lock mode covers the requested mode
        return (existing, requested) switch
        {
            // Exclusive covers everything
            (LockMode.Exclusive, _) => true,
            
            // SIX covers IX and IS
            (LockMode.SharedIntentExclusive, LockMode.IntentExclusive) => true,
            (LockMode.SharedIntentExclusive, LockMode.IntentShared) => true,
            (LockMode.SharedIntentExclusive, LockMode.Shared) => true,
            
            // IX covers IS
            (LockMode.IntentExclusive, LockMode.IntentShared) => true,
            
            // Shared covers IS
            (LockMode.Shared, LockMode.IntentShared) => true,
            
            // Same mode is sufficient
            _ when existing == requested => true,
            
            _ => false
        };
    }

    private static LockMode GetUpgradedMode(LockMode existing, LockMode requested)
    {
        // Determine the combined lock mode needed
        return (existing, requested) switch
        {
            // S + IX = SIX
            (LockMode.Shared, LockMode.IntentExclusive) => LockMode.SharedIntentExclusive,
            (LockMode.IntentExclusive, LockMode.Shared) => LockMode.SharedIntentExclusive,
            
            // IS + IX = IX
            (LockMode.IntentShared, LockMode.IntentExclusive) => LockMode.IntentExclusive,
            
            // IS + S = S
            (LockMode.IntentShared, LockMode.Shared) => LockMode.Shared,
            
            // Anything + X = X
            (_, LockMode.Exclusive) => LockMode.Exclusive,
            
            // Keep existing if it's stronger
            _ when IsLockSufficient(existing, requested) => existing,
            
            // Otherwise use requested
            _ => requested
        };
    }
}

/// <summary>
/// Information about a gap lock.
/// </summary>
public sealed class GapLockInfo
{
    public long TransactionId { get; }
    public LockMode Mode { get; }

    public GapLockInfo(long transactionId, LockMode mode)
    {
        TransactionId = transactionId;
        Mode = mode;
    }
}

/// <summary>
/// Key for tracking gap locks held by a transaction.
/// </summary>
internal readonly struct GapLockKey
{
    public string TreeKey { get; }
    public long LowKey { get; }
    public long HighKey { get; }

    public GapLockKey(string treeKey, long lowKey, long highKey)
    {
        TreeKey = treeKey;
        LowKey = lowKey;
        HighKey = highKey;
    }
}
