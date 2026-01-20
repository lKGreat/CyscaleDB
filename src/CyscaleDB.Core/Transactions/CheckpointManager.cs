using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Transactions;

/// <summary>
/// Manages checkpoints for crash recovery and WAL truncation.
/// </summary>
public sealed class CheckpointManager : IDisposable
{
    private readonly string _dataDirectory;
    private readonly string _checkpointFilePath;
    private readonly WalLog _walLog;
    private readonly BufferPool _bufferPool;
    private readonly TransactionManager? _transactionManager;
    private readonly Logger _logger;
    private Timer? _checkpointTimer;
    private CheckpointInfo? _lastCheckpoint;
    private readonly object _lock = new();
    private bool _disposed;

    /// <summary>
    /// Gets the last checkpoint information.
    /// </summary>
    public CheckpointInfo? LastCheckpoint => _lastCheckpoint;

    /// <summary>
    /// Event raised when a checkpoint is completed.
    /// </summary>
    public event EventHandler<CheckpointCompletedEventArgs>? CheckpointCompleted;

    /// <summary>
    /// Creates a new CheckpointManager.
    /// </summary>
    public CheckpointManager(
        string dataDirectory,
        WalLog walLog,
        BufferPool bufferPool,
        TransactionManager? transactionManager = null)
    {
        _dataDirectory = dataDirectory ?? throw new ArgumentNullException(nameof(dataDirectory));
        _walLog = walLog ?? throw new ArgumentNullException(nameof(walLog));
        _bufferPool = bufferPool ?? throw new ArgumentNullException(nameof(bufferPool));
        _transactionManager = transactionManager;
        _checkpointFilePath = Path.Combine(dataDirectory, Constants.CheckpointFileName);
        _logger = LogManager.Default.GetLogger<CheckpointManager>();

        // Load last checkpoint if exists
        LoadLastCheckpoint();
    }

    /// <summary>
    /// Starts periodic checkpointing.
    /// </summary>
    public void StartPeriodicCheckpoint(TimeSpan? interval = null)
    {
        var checkpointInterval = interval ?? TimeSpan.FromSeconds(Constants.CheckpointIntervalSeconds);

        _checkpointTimer = new Timer(
            _ => TakeCheckpoint(),
            null,
            checkpointInterval,
            checkpointInterval);

        _logger.Info("Started periodic checkpointing every {0}", checkpointInterval);
    }

    /// <summary>
    /// Stops periodic checkpointing.
    /// </summary>
    public void StopPeriodicCheckpoint()
    {
        _checkpointTimer?.Dispose();
        _checkpointTimer = null;
    }

    /// <summary>
    /// Takes a checkpoint - flushes all dirty pages and records checkpoint LSN.
    /// </summary>
    public CheckpointInfo TakeCheckpoint()
    {
        lock (_lock)
        {
            var startTime = DateTime.UtcNow;
            var startLsn = _walLog.CurrentLsn;

            _logger.Info("Starting checkpoint at LSN {0}", startLsn);

            try
            {
                // 1. Get list of active transactions
                var activeTransactions = GetActiveTransactions();

                // 2. Write checkpoint begin entry to WAL
                _walLog.WriteCheckpoint(activeTransactions);

                // 3. Flush all dirty pages from buffer pool to disk
                _bufferPool.FlushAll();

                // 4. Record checkpoint LSN
                var checkpointLsn = _walLog.CurrentLsn;

                // 5. Flush WAL to ensure checkpoint entry is durable
                _walLog.Flush();

                // 6. Create and save checkpoint info
                var checkpoint = new CheckpointInfo(
                    checkpointLsn,
                    startTime,
                    DateTime.UtcNow,
                    activeTransactions);

                SaveCheckpoint(checkpoint);
                _lastCheckpoint = checkpoint;

                // 7. Truncate old WAL entries (optional)
                var minActiveLsn = GetMinActiveLsn(activeTransactions);
                if (minActiveLsn > 0)
                {
                    _walLog.Truncate(minActiveLsn);
                }

                _logger.Info("Checkpoint completed at LSN {0}, duration: {1}ms",
                    checkpointLsn, (DateTime.UtcNow - startTime).TotalMilliseconds);

                // Raise event
                CheckpointCompleted?.Invoke(this, new CheckpointCompletedEventArgs(checkpoint));

                return checkpoint;
            }
            catch (Exception ex)
            {
                _logger.Error("Checkpoint failed: {0}", ex.Message);
                throw;
            }
        }
    }

    /// <summary>
    /// Gets the last checkpoint LSN for recovery.
    /// </summary>
    public long GetRecoveryStartLsn()
    {
        return _lastCheckpoint?.CheckpointLsn ?? 0;
    }

    /// <summary>
    /// Performs crash recovery using the checkpoint and WAL.
    /// </summary>
    public void Recover(Action<WalEntry> redoAction, Action<WalEntry> undoAction)
    {
        lock (_lock)
        {
            var startLsn = GetRecoveryStartLsn();
            _logger.Info("Starting recovery from LSN {0}", startLsn);

            // Read WAL entries from checkpoint LSN
            var entries = _walLog.ReadAll();
            var relevantEntries = entries.Where(e => e.Lsn >= startLsn).OrderBy(e => e.Lsn).ToList();

            // Track transaction states
            var activeTransactions = new Dictionary<long, List<WalEntry>>();
            var committedTransactions = new HashSet<long>();
            var abortedTransactions = new HashSet<long>();

            // Analysis pass - determine transaction states
            foreach (var entry in relevantEntries)
            {
                switch (entry.Type)
                {
                    case WalEntryType.Begin:
                        activeTransactions[entry.TransactionId] = [];
                        break;

                    case WalEntryType.Commit:
                        committedTransactions.Add(entry.TransactionId);
                        activeTransactions.Remove(entry.TransactionId);
                        break;

                    case WalEntryType.Abort:
                        abortedTransactions.Add(entry.TransactionId);
                        activeTransactions.Remove(entry.TransactionId);
                        break;

                    case WalEntryType.Insert:
                    case WalEntryType.Update:
                    case WalEntryType.Delete:
                        if (activeTransactions.TryGetValue(entry.TransactionId, out var txnEntries))
                        {
                            txnEntries.Add(entry);
                        }
                        break;
                }
            }

            // Redo pass - replay committed transactions
            foreach (var entry in relevantEntries)
            {
                if (entry.Type is WalEntryType.Insert or WalEntryType.Update or WalEntryType.Delete)
                {
                    if (committedTransactions.Contains(entry.TransactionId))
                    {
                        redoAction(entry);
                    }
                }
            }

            // Undo pass - rollback uncommitted transactions
            foreach (var (txnId, txnEntries) in activeTransactions)
            {
                // Undo in reverse order
                for (int i = txnEntries.Count - 1; i >= 0; i--)
                {
                    undoAction(txnEntries[i]);
                }

                // Write abort entry
                _walLog.WriteAbort(txnId);
            }

            _walLog.Flush();
            _logger.Info("Recovery completed. Redid {0} transactions, undid {1} transactions",
                committedTransactions.Count, activeTransactions.Count);
        }
    }

    private List<long> GetActiveTransactions()
    {
        if (_transactionManager == null)
            return [];

        return _transactionManager.GetActiveTransactionIds();
    }

    private long GetMinActiveLsn(List<long> activeTransactions)
    {
        if (activeTransactions.Count == 0)
            return _walLog.CurrentLsn;

        // In a full implementation, we'd track the start LSN of each active transaction
        // For now, return 0 to keep all WAL entries
        return 0;
    }

    private void LoadLastCheckpoint()
    {
        if (!File.Exists(_checkpointFilePath))
            return;

        try
        {
            var data = File.ReadAllBytes(_checkpointFilePath);
            _lastCheckpoint = CheckpointInfo.Deserialize(data);
            _logger.Info("Loaded last checkpoint from LSN {0}", _lastCheckpoint.CheckpointLsn);
        }
        catch (Exception ex)
        {
            _logger.Warning("Failed to load last checkpoint: {0}", ex.Message);
        }
    }

    private void SaveCheckpoint(CheckpointInfo checkpoint)
    {
        var data = checkpoint.Serialize();
        var tempPath = _checkpointFilePath + ".tmp";

        File.WriteAllBytes(tempPath, data);
        File.Move(tempPath, _checkpointFilePath, overwrite: true);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        StopPeriodicCheckpoint();
    }
}

/// <summary>
/// Contains information about a checkpoint.
/// </summary>
public sealed class CheckpointInfo
{
    /// <summary>
    /// The LSN at which the checkpoint was taken.
    /// </summary>
    public long CheckpointLsn { get; }

    /// <summary>
    /// When the checkpoint started.
    /// </summary>
    public DateTime StartTime { get; }

    /// <summary>
    /// When the checkpoint completed.
    /// </summary>
    public DateTime EndTime { get; }

    /// <summary>
    /// Transaction IDs that were active at checkpoint time.
    /// </summary>
    public IReadOnlyList<long> ActiveTransactions { get; }

    public CheckpointInfo(
        long checkpointLsn,
        DateTime startTime,
        DateTime endTime,
        List<long> activeTransactions)
    {
        CheckpointLsn = checkpointLsn;
        StartTime = startTime;
        EndTime = endTime;
        ActiveTransactions = activeTransactions.AsReadOnly();
    }

    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(CheckpointLsn);
        writer.Write(StartTime.Ticks);
        writer.Write(EndTime.Ticks);
        writer.Write(ActiveTransactions.Count);
        foreach (var txnId in ActiveTransactions)
        {
            writer.Write(txnId);
        }

        return stream.ToArray();
    }

    public static CheckpointInfo Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var checkpointLsn = reader.ReadInt64();
        var startTime = new DateTime(reader.ReadInt64());
        var endTime = new DateTime(reader.ReadInt64());
        var txnCount = reader.ReadInt32();
        var activeTransactions = new List<long>(txnCount);
        for (int i = 0; i < txnCount; i++)
        {
            activeTransactions.Add(reader.ReadInt64());
        }

        return new CheckpointInfo(checkpointLsn, startTime, endTime, activeTransactions);
    }
}

/// <summary>
/// Event args for checkpoint completion.
/// </summary>
public class CheckpointCompletedEventArgs : EventArgs
{
    public CheckpointInfo Checkpoint { get; }

    public CheckpointCompletedEventArgs(CheckpointInfo checkpoint)
    {
        Checkpoint = checkpoint;
    }
}
