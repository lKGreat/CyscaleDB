using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Transactions;

/// <summary>
/// Manages transactions, including begin, commit, rollback, and recovery.
/// </summary>
public sealed class TransactionManager : IDisposable
{
    private readonly LockManager _lockManager;
    private readonly WalLog _walLog;
    private readonly Dictionary<long, Transaction> _activeTransactions;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Logger _logger;
    private bool _disposed;

    /// <summary>
    /// Gets the lock manager.
    /// </summary>
    public LockManager LockManager => _lockManager;

    /// <summary>
    /// Gets the WAL log.
    /// </summary>
    public WalLog WalLog => _walLog;

    /// <summary>
    /// Gets the number of active transactions.
    /// </summary>
    public int ActiveTransactionCount
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _activeTransactions.Count;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Creates a new transaction manager.
    /// </summary>
    public TransactionManager(string dataDirectory, TimeSpan? lockTimeout = null)
    {
        _lockManager = new LockManager(lockTimeout);
        _walLog = new WalLog(dataDirectory);
        _activeTransactions = new Dictionary<long, Transaction>();
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<TransactionManager>();
    }

    /// <summary>
    /// Initializes the transaction manager and performs recovery if needed.
    /// </summary>
    public void Initialize()
    {
        _walLog.Open();
        
        // Perform recovery
        Recover();
        
        _logger.Info("Transaction manager initialized");
    }

    /// <summary>
    /// Begins a new transaction.
    /// </summary>
    public Transaction Begin(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        var transaction = new Transaction(isolationLevel);

        _lock.EnterWriteLock();
        try
        {
            _activeTransactions[transaction.TransactionId] = transaction;
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        _walLog.WriteBegin(transaction.TransactionId);

        _logger.Debug("Started transaction {0}", transaction.TransactionId);
        return transaction;
    }

    /// <summary>
    /// Commits a transaction.
    /// </summary>
    public void Commit(Transaction transaction)
    {
        if (transaction == null)
            throw new ArgumentNullException(nameof(transaction));

        if (!transaction.IsActive)
            throw new TransactionException("Transaction is not active", transaction.TransactionId);

        _lock.EnterWriteLock();
        try
        {
            transaction.State = TransactionState.Committing;

            // Write commit record and flush
            _walLog.WriteCommit(transaction.TransactionId);

            // Release all locks
            _lockManager.ReleaseAllLocks(transaction);

            // Update state
            transaction.State = TransactionState.Committed;
            _activeTransactions.Remove(transaction.TransactionId);

            _logger.Debug("Committed transaction {0}", transaction.TransactionId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Rolls back a transaction.
    /// </summary>
    public void Rollback(Transaction transaction)
    {
        if (transaction == null)
            throw new ArgumentNullException(nameof(transaction));

        if (!transaction.IsActive)
            throw new TransactionException("Transaction is not active", transaction.TransactionId);

        _lock.EnterWriteLock();
        try
        {
            transaction.State = TransactionState.Aborting;

            // Write abort record
            _walLog.WriteAbort(transaction.TransactionId);

            // TODO: Undo all changes made by this transaction
            // This would involve reading the WAL entries for this transaction
            // and applying the undo operations

            // Release all locks
            _lockManager.ReleaseAllLocks(transaction);

            // Update state
            transaction.State = TransactionState.Aborted;
            _activeTransactions.Remove(transaction.TransactionId);

            _logger.Debug("Rolled back transaction {0}", transaction.TransactionId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Gets an active transaction by ID.
    /// </summary>
    public Transaction? GetTransaction(long transactionId)
    {
        _lock.EnterReadLock();
        try
        {
            return _activeTransactions.TryGetValue(transactionId, out var tx) ? tx : null;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Gets the IDs of all active transactions.
    /// </summary>
    public List<long> GetActiveTransactionIds()
    {
        _lock.EnterReadLock();
        try
        {
            return _activeTransactions.Keys.ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Creates a checkpoint.
    /// </summary>
    public void Checkpoint()
    {
        _lock.EnterReadLock();
        try
        {
            var activeIds = _activeTransactions.Keys.ToList();
            _walLog.WriteCheckpoint(activeIds);
            _walLog.Flush();

            _logger.Info("Created checkpoint with {0} active transactions", activeIds.Count);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Performs crash recovery using the WAL log.
    /// </summary>
    private void Recover()
    {
        _logger.Info("Starting recovery...");

        var entries = _walLog.ReadAll();
        if (entries.Count == 0)
        {
            _logger.Info("No WAL entries to recover");
            return;
        }

        // Analysis phase: identify committed and aborted transactions
        var transactionStatus = new Dictionary<long, TransactionState>();
        long? lastCheckpointLsn = null;

        foreach (var entry in entries)
        {
            switch (entry.Type)
            {
                case WalEntryType.Begin:
                    transactionStatus[entry.TransactionId] = TransactionState.Active;
                    break;

                case WalEntryType.Commit:
                    transactionStatus[entry.TransactionId] = TransactionState.Committed;
                    break;

                case WalEntryType.Abort:
                    transactionStatus[entry.TransactionId] = TransactionState.Aborted;
                    break;

                case WalEntryType.Checkpoint:
                    lastCheckpointLsn = entry.Lsn;
                    break;
            }
        }

        // Find uncommitted transactions (losers)
        var losers = transactionStatus
            .Where(kv => kv.Value == TransactionState.Active)
            .Select(kv => kv.Key)
            .ToHashSet();

        if (losers.Count > 0)
        {
            _logger.Info("Found {0} uncommitted transactions to roll back", losers.Count);

            // Undo phase: roll back uncommitted transactions
            // Read entries in reverse order
            for (int i = entries.Count - 1; i >= 0; i--)
            {
                var entry = entries[i];
                if (!losers.Contains(entry.TransactionId))
                    continue;

                // Apply undo for each operation
                switch (entry.Type)
                {
                    case WalEntryType.Insert:
                        // Undo insert = delete
                        _logger.Debug("Undo INSERT for transaction {0}", entry.TransactionId);
                        // TODO: Actually delete the row
                        break;

                    case WalEntryType.Update:
                        // Undo update = restore old value
                        _logger.Debug("Undo UPDATE for transaction {0}", entry.TransactionId);
                        // TODO: Actually restore the old value
                        break;

                    case WalEntryType.Delete:
                        // Undo delete = restore row
                        _logger.Debug("Undo DELETE for transaction {0}", entry.TransactionId);
                        // TODO: Actually restore the row
                        break;
                }
            }

            // Write abort records for losers
            foreach (var loserId in losers)
            {
                _walLog.WriteAbort(loserId);
            }
        }

        // Truncate WAL up to checkpoint if present
        if (lastCheckpointLsn.HasValue)
        {
            _walLog.Truncate(lastCheckpointLsn.Value);
        }

        _logger.Info("Recovery complete");
    }

    /// <summary>
    /// Forces a flush of all pending WAL entries.
    /// </summary>
    public void Flush()
    {
        _walLog.Flush();
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Rollback all active transactions
        _lock.EnterWriteLock();
        try
        {
            foreach (var tx in _activeTransactions.Values.ToList())
            {
                if (tx.IsActive)
                {
                    tx.State = TransactionState.Aborting;
                    _walLog.WriteAbort(tx.TransactionId);
                    _lockManager.ReleaseAllLocks(tx);
                    tx.State = TransactionState.Aborted;
                }
            }
            _activeTransactions.Clear();
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        _walLog.Dispose();
        _lockManager.Dispose();
        _lock.Dispose();

        _logger.Info("Transaction manager disposed");
    }
}
