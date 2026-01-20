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

    /// <summary>
    /// Creates a new lock manager.
    /// </summary>
    public LockManager(TimeSpan? lockTimeout = null)
    {
        _locks = new Dictionary<LockKey, LockState>();
        _transactionLocks = new Dictionary<long, HashSet<LockKey>>();
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<LockManager>();
        _lockTimeout = lockTimeout ?? TimeSpan.FromSeconds(30);
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
    /// </summary>
    IntentShared,

    /// <summary>
    /// Intent exclusive (for table-level intent).
    /// </summary>
    IntentExclusive
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
