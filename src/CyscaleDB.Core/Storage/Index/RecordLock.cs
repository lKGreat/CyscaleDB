using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage.Index;

/// <summary>
/// Types of record locks in InnoDB-style locking.
/// </summary>
public enum RecordLockType : byte
{
    /// <summary>
    /// Shared lock (S) - allows concurrent reads.
    /// Multiple transactions can hold S locks on the same record.
    /// </summary>
    Shared = 0,

    /// <summary>
    /// Exclusive lock (X) - prevents concurrent access.
    /// Only one transaction can hold an X lock.
    /// </summary>
    Exclusive = 1,

    /// <summary>
    /// Record lock - locks only the index record.
    /// </summary>
    Record = 2,

    /// <summary>
    /// Gap lock - locks the gap before the index record.
    /// Prevents other transactions from inserting in the gap.
    /// </summary>
    Gap = 3,

    /// <summary>
    /// Next-key lock - combines record lock and gap lock.
    /// This is the default for REPEATABLE READ in InnoDB.
    /// </summary>
    NextKey = 4
}

/// <summary>
/// Represents a lock on an index record.
/// This is used for row-level locking in InnoDB-style MVCC.
/// </summary>
public sealed class RecordLock
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
    /// The index name (or primary key).
    /// </summary>
    public string IndexName { get; }

    /// <summary>
    /// The key value being locked.
    /// </summary>
    public CompositeKey Key { get; }

    /// <summary>
    /// The transaction holding this lock.
    /// </summary>
    public long TransactionId { get; }

    /// <summary>
    /// The type of lock.
    /// </summary>
    public RecordLockType LockType { get; }

    /// <summary>
    /// When this lock was acquired.
    /// </summary>
    public DateTime AcquiredAt { get; }

    /// <summary>
    /// Whether this lock is waiting to be granted.
    /// </summary>
    public bool IsWaiting { get; private set; }

    /// <summary>
    /// Creates a new record lock.
    /// </summary>
    public RecordLock(
        string databaseName,
        string tableName,
        string indexName,
        CompositeKey key,
        long transactionId,
        RecordLockType lockType,
        bool isWaiting = false)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        IndexName = indexName;
        Key = key;
        TransactionId = transactionId;
        LockType = lockType;
        IsWaiting = isWaiting;
        AcquiredAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Grants the lock (marks it as no longer waiting).
    /// </summary>
    public void Grant()
    {
        IsWaiting = false;
    }

    /// <summary>
    /// Gets a unique identifier for the locked resource.
    /// </summary>
    public string GetResourceId()
    {
        return $"{DatabaseName}.{TableName}.{IndexName}:{Key}";
    }

    /// <summary>
    /// Checks if this lock conflicts with another lock.
    /// </summary>
    public bool ConflictsWith(RecordLock other)
    {
        // Same transaction doesn't conflict with itself
        if (TransactionId == other.TransactionId)
            return false;

        // Check if same resource
        if (GetResourceId() != other.GetResourceId())
            return false;

        // S-S is compatible
        if (LockType == RecordLockType.Shared && other.LockType == RecordLockType.Shared)
            return false;

        // Gap locks don't conflict with each other (they only prevent inserts)
        if (LockType == RecordLockType.Gap && other.LockType == RecordLockType.Gap)
            return false;

        // All other combinations conflict
        return true;
    }

    public override string ToString()
    {
        var waiting = IsWaiting ? " (waiting)" : "";
        return $"RecordLock({LockType}) on {GetResourceId()} by Tx{TransactionId}{waiting}";
    }
}

/// <summary>
/// Manages record-level locks for InnoDB-style locking.
/// </summary>
public sealed class RecordLockManager
{
    private readonly Dictionary<string, List<RecordLock>> _locksByResource = [];
    private readonly Dictionary<long, List<RecordLock>> _locksByTransaction = [];
    private readonly object _lock = new();
    private readonly Logger _logger;
    private readonly TimeSpan _lockTimeout;

    public RecordLockManager(TimeSpan? lockTimeout = null)
    {
        _logger = LogManager.Default.GetLogger<RecordLockManager>();
        _lockTimeout = lockTimeout ?? TimeSpan.FromSeconds(50);
    }

    /// <summary>
    /// Acquires a record lock.
    /// </summary>
    /// <param name="databaseName">Database name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="indexName">Index name</param>
    /// <param name="key">The key to lock</param>
    /// <param name="transactionId">Transaction ID</param>
    /// <param name="lockType">Type of lock</param>
    /// <returns>The acquired lock, or null if couldn't acquire</returns>
    public RecordLock? AcquireLock(
        string databaseName,
        string tableName,
        string indexName,
        CompositeKey key,
        long transactionId,
        RecordLockType lockType)
    {
        var newLock = new RecordLock(
            databaseName, tableName, indexName, key, transactionId, lockType, isWaiting: true);

        lock (_lock)
        {
            var resourceId = newLock.GetResourceId();

            // Check for existing locks on this resource
            if (_locksByResource.TryGetValue(resourceId, out var existingLocks))
            {
                // Check for conflicts
                foreach (var existing in existingLocks)
                {
                    if (existing.ConflictsWith(newLock))
                    {
                        _logger.Debug("Lock conflict: {0} conflicts with {1}", newLock, existing);

                        // In a real implementation, we would wait or return error
                        // For now, we'll add the lock as waiting
                        AddLock(newLock);
                        return newLock;
                    }
                }
            }

            // No conflicts, grant the lock immediately
            newLock.Grant();
            AddLock(newLock);
            _logger.Debug("Acquired lock: {0}", newLock);
            return newLock;
        }
    }

    /// <summary>
    /// Releases all locks held by a transaction.
    /// </summary>
    public void ReleaseTransactionLocks(long transactionId)
    {
        lock (_lock)
        {
            if (!_locksByTransaction.TryGetValue(transactionId, out var locks))
                return;

            foreach (var recordLock in locks.ToList())
            {
                var resourceId = recordLock.GetResourceId();
                if (_locksByResource.TryGetValue(resourceId, out var resourceLocks))
                {
                    resourceLocks.Remove(recordLock);
                    if (resourceLocks.Count == 0)
                    {
                        _locksByResource.Remove(resourceId);
                    }
                    else
                    {
                        // Grant waiting locks
                        GrantWaitingLocks(resourceId, resourceLocks);
                    }
                }
            }

            _locksByTransaction.Remove(transactionId);
            _logger.Debug("Released all locks for Tx{0}", transactionId);
        }
    }

    /// <summary>
    /// Releases a specific lock.
    /// </summary>
    public void ReleaseLock(RecordLock recordLock)
    {
        lock (_lock)
        {
            var resourceId = recordLock.GetResourceId();

            if (_locksByResource.TryGetValue(resourceId, out var resourceLocks))
            {
                resourceLocks.Remove(recordLock);
                if (resourceLocks.Count == 0)
                {
                    _locksByResource.Remove(resourceId);
                }
                else
                {
                    GrantWaitingLocks(resourceId, resourceLocks);
                }
            }

            if (_locksByTransaction.TryGetValue(recordLock.TransactionId, out var txLocks))
            {
                txLocks.Remove(recordLock);
                if (txLocks.Count == 0)
                {
                    _locksByTransaction.Remove(recordLock.TransactionId);
                }
            }

            _logger.Debug("Released lock: {0}", recordLock);
        }
    }

    /// <summary>
    /// Gets all locks held by a transaction.
    /// </summary>
    public IReadOnlyList<RecordLock> GetTransactionLocks(long transactionId)
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
    /// Checks if a lock would conflict with existing locks.
    /// </summary>
    public bool WouldConflict(
        string databaseName,
        string tableName,
        string indexName,
        CompositeKey key,
        long transactionId,
        RecordLockType lockType)
    {
        var testLock = new RecordLock(
            databaseName, tableName, indexName, key, transactionId, lockType);

        lock (_lock)
        {
            var resourceId = testLock.GetResourceId();
            if (_locksByResource.TryGetValue(resourceId, out var existingLocks))
            {
                return existingLocks.Any(existing => existing.ConflictsWith(testLock));
            }
            return false;
        }
    }

    private void AddLock(RecordLock recordLock)
    {
        var resourceId = recordLock.GetResourceId();

        if (!_locksByResource.TryGetValue(resourceId, out var resourceLocks))
        {
            resourceLocks = [];
            _locksByResource[resourceId] = resourceLocks;
        }
        resourceLocks.Add(recordLock);

        if (!_locksByTransaction.TryGetValue(recordLock.TransactionId, out var txLocks))
        {
            txLocks = [];
            _locksByTransaction[recordLock.TransactionId] = txLocks;
        }
        txLocks.Add(recordLock);
    }

    private void GrantWaitingLocks(string resourceId, List<RecordLock> locks)
    {
        foreach (var waitingLock in locks.Where(l => l.IsWaiting).ToList())
        {
            bool canGrant = true;
            foreach (var other in locks.Where(l => !l.IsWaiting))
            {
                if (waitingLock.ConflictsWith(other))
                {
                    canGrant = false;
                    break;
                }
            }

            if (canGrant)
            {
                waitingLock.Grant();
                _logger.Debug("Granted waiting lock: {0}", waitingLock);
            }
        }
    }
}
