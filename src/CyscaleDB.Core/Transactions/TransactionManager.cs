using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage.Mvcc;

namespace CyscaleDB.Core.Transactions;

/// <summary>
/// Manages transactions, including begin, commit, rollback, and recovery.
/// Implements IReadViewFactory for MVCC support.
/// </summary>
public sealed class TransactionManager : IDisposable, IReadViewFactory
{
    private readonly LockManager _lockManager;
    private readonly WalLog _walLog;
    private readonly UndoLog? _undoLog;
    private readonly Dictionary<long, Transaction> _activeTransactions;
    private readonly Dictionary<long, long> _transactionLastUndoPointer;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Logger _logger;
    private readonly string _dataDirectory;
    private long _nextTransactionId = 1;
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
    /// Gets the Undo log (null if undo logging is disabled).
    /// </summary>
    public UndoLog? UndoLog => _undoLog;

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
    /// Gets the next transaction ID that will be assigned.
    /// </summary>
    public long NextTransactionId
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _nextTransactionId;
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
    /// <param name="dataDirectory">The data directory for log files</param>
    /// <param name="lockTimeout">Lock timeout duration</param>
    /// <param name="enableUndoLog">Whether to enable undo logging</param>
    public TransactionManager(string dataDirectory, TimeSpan? lockTimeout = null, bool enableUndoLog = true)
    {
        _dataDirectory = dataDirectory;
        _lockManager = new LockManager(lockTimeout);
        _walLog = new WalLog(dataDirectory);
        _undoLog = enableUndoLog ? new UndoLog(dataDirectory) : null;
        _activeTransactions = new Dictionary<long, Transaction>();
        _transactionLastUndoPointer = new Dictionary<long, long>();
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<TransactionManager>();
    }

    /// <summary>
    /// Initializes the transaction manager and performs recovery if needed.
    /// </summary>
    public void Initialize()
    {
        _walLog.Open();
        _undoLog?.Open();
        
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
            _transactionLastUndoPointer[transaction.TransactionId] = 0;
            
            // Update next transaction ID
            if (transaction.TransactionId >= _nextTransactionId)
            {
                _nextTransactionId = transaction.TransactionId + 1;
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        _walLog.WriteBegin(transaction.TransactionId);

        _logger.Debug("Started transaction {0} with isolation level {1}", transaction.TransactionId, isolationLevel);
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
    /// Rolls back a transaction using undo records.
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

            // Apply undo records if undo log is enabled
            if (_undoLog != null && _transactionLastUndoPointer.TryGetValue(transaction.TransactionId, out var lastUndoPtr) && lastUndoPtr > 0)
            {
                ApplyUndoRecords(transaction.TransactionId, lastUndoPtr);
            }

            // Write abort record
            _walLog.WriteAbort(transaction.TransactionId);

            // Release all locks
            _lockManager.ReleaseAllLocks(transaction);

            // Update state
            transaction.State = TransactionState.Aborted;
            _activeTransactions.Remove(transaction.TransactionId);
            _transactionLastUndoPointer.Remove(transaction.TransactionId);

            _logger.Debug("Rolled back transaction {0}", transaction.TransactionId);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Applies undo records to rollback a transaction.
    /// </summary>
    private void ApplyUndoRecords(long transactionId, long startPointer)
    {
        if (_undoLog == null)
            return;

        var undoRecords = _undoLog.ReadTransactionUndos(transactionId, startPointer);
        
        foreach (var record in undoRecords)
        {
            _logger.Debug("Applying undo record: {0} for transaction {1}", record.Type, transactionId);
            
            // The actual undo application would be done by the StorageEngine
            // Here we just track that it needs to be done
            OnUndoApplied?.Invoke(this, new UndoAppliedEventArgs(record));
        }
    }

    /// <summary>
    /// Event raised when an undo record is applied during rollback.
    /// The StorageEngine subscribes to this to perform the actual data modifications.
    /// </summary>
    public event EventHandler<UndoAppliedEventArgs>? OnUndoApplied;

    /// <summary>
    /// Records an undo entry for a transaction.
    /// </summary>
    /// <param name="transactionId">The transaction ID</param>
    /// <param name="undoPointer">The undo pointer returned from UndoLog.Write</param>
    public void RecordUndoEntry(long transactionId, long undoPointer)
    {
        _lock.EnterWriteLock();
        try
        {
            _transactionLastUndoPointer[transactionId] = undoPointer;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Gets the last undo pointer for a transaction.
    /// </summary>
    public long GetLastUndoPointer(long transactionId)
    {
        _lock.EnterReadLock();
        try
        {
            return _transactionLastUndoPointer.TryGetValue(transactionId, out var ptr) ? ptr : 0;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Creates a ReadView for MVCC snapshot isolation.
    /// </summary>
    /// <param name="transactionId">The transaction ID requesting the ReadView</param>
    /// <returns>A new ReadView capturing the current transaction state</returns>
    public ReadView CreateReadView(long transactionId)
    {
        _lock.EnterReadLock();
        try
        {
            var activeIds = _activeTransactions.Keys.ToList();
            return ReadView.Create(activeIds, _nextTransactionId, transactionId);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Gets or creates a ReadView for a transaction based on its isolation level.
    /// </summary>
    /// <param name="transaction">The transaction</param>
    /// <returns>A ReadView appropriate for the transaction's isolation level</returns>
    public ReadView GetOrCreateReadView(Transaction transaction)
    {
        switch (transaction.IsolationLevel)
        {
            case IsolationLevel.ReadUncommitted:
                // No ReadView needed - always read current data
                return CreateReadView(transaction.TransactionId);
                
            case IsolationLevel.ReadCommitted:
                // Create a new ReadView for each statement
                return CreateReadView(transaction.TransactionId);
                
            case IsolationLevel.RepeatableRead:
            case IsolationLevel.Serializable:
                // Use the same ReadView for the entire transaction
                if (transaction.ReadView == null)
                {
                    transaction.ReadView = CreateReadView(transaction.TransactionId);
                }
                return transaction.ReadView;
                
            default:
                return CreateReadView(transaction.TransactionId);
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
        _undoLog?.Dispose();
        _lockManager.Dispose();
        _lock.Dispose();

        _logger.Info("Transaction manager disposed");
    }
}

/// <summary>
/// Event args for undo record application.
/// </summary>
public class UndoAppliedEventArgs : EventArgs
{
    /// <summary>
    /// The undo record being applied.
    /// </summary>
    public UndoRecord UndoRecord { get; }

    public UndoAppliedEventArgs(UndoRecord undoRecord)
    {
        UndoRecord = undoRecord ?? throw new ArgumentNullException(nameof(undoRecord));
    }
}
