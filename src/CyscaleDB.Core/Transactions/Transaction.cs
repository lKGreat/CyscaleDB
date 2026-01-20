namespace CyscaleDB.Core.Transactions;

/// <summary>
/// Represents a database transaction.
/// </summary>
public sealed class Transaction
{
    private static long _nextTransactionId = 1;

    /// <summary>
    /// The unique identifier of this transaction.
    /// </summary>
    public long TransactionId { get; }

    /// <summary>
    /// The current state of this transaction.
    /// </summary>
    public TransactionState State { get; internal set; }

    /// <summary>
    /// When this transaction started.
    /// </summary>
    public DateTime StartTime { get; }

    /// <summary>
    /// The isolation level of this transaction.
    /// </summary>
    public IsolationLevel IsolationLevel { get; }

    /// <summary>
    /// Whether this transaction is read-only.
    /// </summary>
    public bool IsReadOnly { get; set; }

    /// <summary>
    /// The locks held by this transaction.
    /// </summary>
    internal List<LockEntry> HeldLocks { get; } = [];

    /// <summary>
    /// The WAL log entries for this transaction.
    /// </summary>
    internal List<WalEntry> LogEntries { get; } = [];

    /// <summary>
    /// Creates a new transaction.
    /// </summary>
    public Transaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        TransactionId = Interlocked.Increment(ref _nextTransactionId);
        State = TransactionState.Active;
        StartTime = DateTime.UtcNow;
        IsolationLevel = isolationLevel;
    }

    /// <summary>
    /// Checks if this transaction is still active.
    /// </summary>
    public bool IsActive => State == TransactionState.Active;

    /// <summary>
    /// Checks if this transaction has been committed.
    /// </summary>
    public bool IsCommitted => State == TransactionState.Committed;

    /// <summary>
    /// Checks if this transaction has been aborted.
    /// </summary>
    public bool IsAborted => State == TransactionState.Aborted;

    public override string ToString()
    {
        return $"Transaction({TransactionId}, {State})";
    }
}

/// <summary>
/// Transaction states.
/// </summary>
public enum TransactionState
{
    /// <summary>
    /// Transaction is active and can perform operations.
    /// </summary>
    Active,

    /// <summary>
    /// Transaction is in the process of committing.
    /// </summary>
    Committing,

    /// <summary>
    /// Transaction has been successfully committed.
    /// </summary>
    Committed,

    /// <summary>
    /// Transaction is in the process of aborting.
    /// </summary>
    Aborting,

    /// <summary>
    /// Transaction has been aborted/rolled back.
    /// </summary>
    Aborted
}

/// <summary>
/// Transaction isolation levels.
/// </summary>
public enum IsolationLevel
{
    /// <summary>
    /// Read uncommitted (allows dirty reads).
    /// </summary>
    ReadUncommitted = 0,

    /// <summary>
    /// Read committed (prevents dirty reads).
    /// </summary>
    ReadCommitted = 1,

    /// <summary>
    /// Repeatable read (prevents non-repeatable reads).
    /// </summary>
    RepeatableRead = 2,

    /// <summary>
    /// Serializable (full isolation).
    /// </summary>
    Serializable = 3
}
