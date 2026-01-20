using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage.Index;

/// <summary>
/// Represents a gap lock on an index range.
/// Gap locks prevent other transactions from inserting into a gap.
/// They are used to prevent phantom reads in REPEATABLE READ isolation.
/// </summary>
public sealed class GapLock
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
    /// The index name.
    /// </summary>
    public string IndexName { get; }

    /// <summary>
    /// The lower bound of the gap (exclusive). Null means negative infinity.
    /// </summary>
    public CompositeKey? LowerBound { get; }

    /// <summary>
    /// The upper bound of the gap (inclusive for next-key, exclusive for pure gap).
    /// Null means positive infinity.
    /// </summary>
    public CompositeKey? UpperBound { get; }

    /// <summary>
    /// Whether this is a next-key lock (includes the upper bound record).
    /// </summary>
    public bool IsNextKeyLock { get; }

    /// <summary>
    /// The transaction holding this lock.
    /// </summary>
    public long TransactionId { get; }

    /// <summary>
    /// When this lock was acquired.
    /// </summary>
    public DateTime AcquiredAt { get; }

    /// <summary>
    /// Creates a new gap lock.
    /// </summary>
    /// <param name="databaseName">Database name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="indexName">Index name</param>
    /// <param name="lowerBound">Lower bound (exclusive)</param>
    /// <param name="upperBound">Upper bound</param>
    /// <param name="transactionId">Transaction ID</param>
    /// <param name="isNextKeyLock">Whether this is a next-key lock</param>
    public GapLock(
        string databaseName,
        string tableName,
        string indexName,
        CompositeKey? lowerBound,
        CompositeKey? upperBound,
        long transactionId,
        bool isNextKeyLock = false)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        IndexName = indexName;
        LowerBound = lowerBound;
        UpperBound = upperBound;
        TransactionId = transactionId;
        IsNextKeyLock = isNextKeyLock;
        AcquiredAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Checks if a key falls within this gap.
    /// </summary>
    public bool ContainsKey(CompositeKey key)
    {
        // Check lower bound (exclusive)
        if (LowerBound.HasValue && key <= LowerBound.Value)
            return false;

        // Check upper bound
        if (UpperBound.HasValue)
        {
            if (IsNextKeyLock)
            {
                // Next-key lock includes the upper bound
                if (key > UpperBound.Value)
                    return false;
            }
            else
            {
                // Pure gap lock excludes the upper bound
                if (key >= UpperBound.Value)
                    return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Checks if this gap lock would block an insert of the given key.
    /// </summary>
    public bool BlocksInsert(CompositeKey key, long insertTransactionId)
    {
        // Same transaction doesn't block itself
        if (TransactionId == insertTransactionId)
            return false;

        // Check if key is in the gap
        return ContainsKey(key);
    }

    /// <summary>
    /// Gets a unique identifier for this gap lock.
    /// </summary>
    public string GetLockId()
    {
        var lower = LowerBound.HasValue ? LowerBound.Value.ToString() : "-inf";
        var upper = UpperBound.HasValue ? UpperBound.Value.ToString() : "+inf";
        var lockType = IsNextKeyLock ? "NK" : "G";
        return $"{DatabaseName}.{TableName}.{IndexName}:({lower},{upper}){lockType}";
    }

    public override string ToString()
    {
        var lockType = IsNextKeyLock ? "NextKeyLock" : "GapLock";
        return $"{lockType} on {GetLockId()} by Tx{TransactionId}";
    }
}

/// <summary>
/// Manages gap locks for preventing phantom reads.
/// </summary>
public sealed class GapLockManager
{
    private readonly Dictionary<string, List<GapLock>> _gapLocksByIndex = [];
    private readonly Dictionary<long, List<GapLock>> _gapLocksByTransaction = [];
    private readonly object _lock = new();
    private readonly Logger _logger;

    public GapLockManager()
    {
        _logger = LogManager.Default.GetLogger<GapLockManager>();
    }

    /// <summary>
    /// Acquires a gap lock.
    /// </summary>
    public GapLock AcquireGapLock(
        string databaseName,
        string tableName,
        string indexName,
        CompositeKey? lowerBound,
        CompositeKey? upperBound,
        long transactionId,
        bool isNextKeyLock = false)
    {
        var gapLock = new GapLock(
            databaseName, tableName, indexName, lowerBound, upperBound, transactionId, isNextKeyLock);

        lock (_lock)
        {
            var indexKey = GetIndexKey(databaseName, tableName, indexName);

            if (!_gapLocksByIndex.TryGetValue(indexKey, out var indexLocks))
            {
                indexLocks = [];
                _gapLocksByIndex[indexKey] = indexLocks;
            }
            indexLocks.Add(gapLock);

            if (!_gapLocksByTransaction.TryGetValue(transactionId, out var txLocks))
            {
                txLocks = [];
                _gapLocksByTransaction[transactionId] = txLocks;
            }
            txLocks.Add(gapLock);

            _logger.Debug("Acquired gap lock: {0}", gapLock);
        }

        return gapLock;
    }

    /// <summary>
    /// Acquires a next-key lock for a specific record.
    /// This locks both the record and the gap before it.
    /// </summary>
    public GapLock AcquireNextKeyLock(
        string databaseName,
        string tableName,
        string indexName,
        CompositeKey? previousKey,
        CompositeKey currentKey,
        long transactionId)
    {
        return AcquireGapLock(
            databaseName, tableName, indexName,
            previousKey, currentKey, transactionId,
            isNextKeyLock: true);
    }

    /// <summary>
    /// Checks if an insert of the given key would be blocked by any gap lock.
    /// </summary>
    public bool IsInsertBlocked(
        string databaseName,
        string tableName,
        string indexName,
        CompositeKey key,
        long transactionId)
    {
        lock (_lock)
        {
            var indexKey = GetIndexKey(databaseName, tableName, indexName);

            if (!_gapLocksByIndex.TryGetValue(indexKey, out var indexLocks))
                return false;

            return indexLocks.Any(gl => gl.BlocksInsert(key, transactionId));
        }
    }

    /// <summary>
    /// Releases all gap locks held by a transaction.
    /// </summary>
    public void ReleaseTransactionLocks(long transactionId)
    {
        lock (_lock)
        {
            if (!_gapLocksByTransaction.TryGetValue(transactionId, out var txLocks))
                return;

            foreach (var gapLock in txLocks.ToList())
            {
                var indexKey = GetIndexKey(gapLock.DatabaseName, gapLock.TableName, gapLock.IndexName);
                if (_gapLocksByIndex.TryGetValue(indexKey, out var indexLocks))
                {
                    indexLocks.Remove(gapLock);
                    if (indexLocks.Count == 0)
                    {
                        _gapLocksByIndex.Remove(indexKey);
                    }
                }
            }

            _gapLocksByTransaction.Remove(transactionId);
            _logger.Debug("Released all gap locks for Tx{0}", transactionId);
        }
    }

    /// <summary>
    /// Releases a specific gap lock.
    /// </summary>
    public void ReleaseLock(GapLock gapLock)
    {
        lock (_lock)
        {
            var indexKey = GetIndexKey(gapLock.DatabaseName, gapLock.TableName, gapLock.IndexName);

            if (_gapLocksByIndex.TryGetValue(indexKey, out var indexLocks))
            {
                indexLocks.Remove(gapLock);
                if (indexLocks.Count == 0)
                {
                    _gapLocksByIndex.Remove(indexKey);
                }
            }

            if (_gapLocksByTransaction.TryGetValue(gapLock.TransactionId, out var txLocks))
            {
                txLocks.Remove(gapLock);
                if (txLocks.Count == 0)
                {
                    _gapLocksByTransaction.Remove(gapLock.TransactionId);
                }
            }

            _logger.Debug("Released gap lock: {0}", gapLock);
        }
    }

    /// <summary>
    /// Gets all gap locks on an index within a key range.
    /// </summary>
    public IReadOnlyList<GapLock> GetGapLocksInRange(
        string databaseName,
        string tableName,
        string indexName,
        CompositeKey? startKey,
        CompositeKey? endKey)
    {
        lock (_lock)
        {
            var indexKey = GetIndexKey(databaseName, tableName, indexName);

            if (!_gapLocksByIndex.TryGetValue(indexKey, out var indexLocks))
                return [];

            return indexLocks
                .Where(gl =>
                {
                    // Check if gap lock overlaps with the range
                    if (startKey.HasValue && gl.UpperBound.HasValue && gl.UpperBound.Value < startKey.Value)
                        return false;
                    if (endKey.HasValue && gl.LowerBound.HasValue && gl.LowerBound.Value > endKey.Value)
                        return false;
                    return true;
                })
                .ToList()
                .AsReadOnly();
        }
    }

    /// <summary>
    /// Gets all gap locks held by a transaction.
    /// </summary>
    public IReadOnlyList<GapLock> GetTransactionGapLocks(long transactionId)
    {
        lock (_lock)
        {
            if (_gapLocksByTransaction.TryGetValue(transactionId, out var locks))
            {
                return locks.AsReadOnly();
            }
            return [];
        }
    }

    private static string GetIndexKey(string databaseName, string tableName, string indexName)
    {
        return $"{databaseName}.{tableName}.{indexName}";
    }
}
