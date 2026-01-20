using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.Mvcc;

namespace CyscaleDB.Core.Transactions;

/// <summary>
/// Manages the Undo Log for MVCC and transaction rollback.
/// 
/// The Undo Log stores previous versions of rows to:
/// 1. Support MVCC - provide consistent reads of old versions
/// 2. Support Rollback - restore rows to their previous state
/// 
/// Structure:
/// - UndoLog is organized into segments
/// - Each segment contains multiple undo records
/// - Records are linked via roll pointers for version chain traversal
/// </summary>
public sealed class UndoLog : IDisposable, IUndoLogReader
{
    private readonly string _dataDirectory;
    private readonly string _undoFilePath;
    private FileStream? _undoStream;
    private BinaryWriter? _writer;
    private readonly object _writeLock = new();
    private readonly Logger _logger;
    private long _nextUndoPointer;
    private bool _disposed;

    // Cache for recently accessed undo records
    private readonly Dictionary<long, UndoRecord> _recordCache;
    private readonly int _cacheSize;

    /// <summary>
    /// Gets the current undo pointer (next write position).
    /// </summary>
    public long CurrentUndoPointer => _nextUndoPointer;

    /// <summary>
    /// Creates a new Undo Log manager.
    /// </summary>
    /// <param name="dataDirectory">The directory to store undo log files</param>
    /// <param name="cacheSize">Size of the record cache</param>
    public UndoLog(string dataDirectory, int cacheSize = 1000)
    {
        _dataDirectory = dataDirectory;
        _undoFilePath = Path.Combine(dataDirectory, "cyscaledb" + Constants.UndoFileExtension);
        _logger = LogManager.Default.GetLogger<UndoLog>();
        _recordCache = new Dictionary<long, UndoRecord>(cacheSize);
        _cacheSize = cacheSize;
    }

    /// <summary>
    /// Opens the undo log file.
    /// </summary>
    public void Open()
    {
        var directory = Path.GetDirectoryName(_undoFilePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        _undoStream = new FileStream(
            _undoFilePath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None,
            bufferSize: 4096,
            FileOptions.RandomAccess);

        _writer = new BinaryWriter(_undoStream);

        // Position at end of file for writing
        _nextUndoPointer = _undoStream.Length > 0 ? _undoStream.Length : HeaderSize;

        // Write header if new file
        if (_undoStream.Length == 0)
        {
            WriteHeader();
        }

        _logger.Info("Opened Undo Log at {0}, next pointer: {1}", _undoFilePath, _nextUndoPointer);
    }

    private const int HeaderSize = 64; // Reserved space for file header

    private void WriteHeader()
    {
        _undoStream!.Seek(0, SeekOrigin.Begin);
        _writer!.Write("CYSCALEDB_UNDO"u8.ToArray()); // Magic bytes
        _writer.Write((int)1); // Version
        _writer.Write(DateTime.UtcNow.Ticks); // Creation time
        _writer.Write(new byte[HeaderSize - 14 - 4 - 8]); // Padding
        _undoStream.Flush();
        _nextUndoPointer = HeaderSize;
    }

    /// <summary>
    /// Writes an undo record and returns its pointer.
    /// </summary>
    public long Write(UndoRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);

        lock (_writeLock)
        {
            EnsureOpen();

            var data = record.Serialize();
            var pointer = _nextUndoPointer;

            // Seek to write position
            _undoStream!.Seek(pointer, SeekOrigin.Begin);

            // Write record: [length(4)][data][checksum(4)]
            _writer!.Write(data.Length);
            _writer.Write(data);
            _writer.Write(ComputeChecksum(data));

            _nextUndoPointer = _undoStream.Position;

            // Add to cache
            AddToCache(pointer, record);

            _logger.Trace("Wrote undo record at {0}, type={1}, size={2}", pointer, record.Type, data.Length);

            return pointer;
        }
    }

    /// <summary>
    /// Writes an INSERT undo record.
    /// </summary>
    public long WriteInsertUndo(
        long transactionId,
        int tableId,
        string databaseName,
        string tableName,
        RowId rowId,
        DataValue[] primaryKeyValues,
        long previousUndoPointer = 0)
    {
        var record = UndoRecord.CreateInsertUndo(
            transactionId, tableId, databaseName, tableName, rowId, primaryKeyValues, previousUndoPointer);
        return Write(record);
    }

    /// <summary>
    /// Writes an UPDATE undo record.
    /// </summary>
    public long WriteUpdateUndo(
        long transactionId,
        int tableId,
        string databaseName,
        string tableName,
        RowId rowId,
        Row oldRow,
        long previousUndoPointer = 0)
    {
        var record = UndoRecord.CreateUpdateUndo(
            transactionId, tableId, databaseName, tableName, rowId, oldRow, previousUndoPointer);
        return Write(record);
    }

    /// <summary>
    /// Writes a DELETE undo record.
    /// </summary>
    public long WriteDeleteUndo(
        long transactionId,
        int tableId,
        string databaseName,
        string tableName,
        RowId rowId,
        Row deletedRow,
        long previousUndoPointer = 0)
    {
        var record = UndoRecord.CreateDeleteUndo(
            transactionId, tableId, databaseName, tableName, rowId, deletedRow, previousUndoPointer);
        return Write(record);
    }

    /// <summary>
    /// Reads an undo record at the given pointer.
    /// </summary>
    public UndoRecord? Read(long undoPointer)
    {
        if (undoPointer <= 0 || undoPointer < HeaderSize)
            return null;

        // Check cache first
        if (_recordCache.TryGetValue(undoPointer, out var cached))
            return cached;

        lock (_writeLock)
        {
            EnsureOpen();

            if (undoPointer >= _undoStream!.Length)
                return null;

            try
            {
                _undoStream.Seek(undoPointer, SeekOrigin.Begin);
                using var reader = new BinaryReader(_undoStream, System.Text.Encoding.UTF8, leaveOpen: true);

                var length = reader.ReadInt32();
                var data = reader.ReadBytes(length);
                var checksum = reader.ReadUInt32();

                if (ComputeChecksum(data) != checksum)
                {
                    _logger.Warning("Corrupted undo record at pointer {0}", undoPointer);
                    return null;
                }

                var record = UndoRecord.Deserialize(data);
                AddToCache(undoPointer, record);

                return record;
            }
            catch (Exception ex)
            {
                _logger.Error("Error reading undo record at {0}: {1}", undoPointer, ex.Message);
                return null;
            }
        }
    }

    /// <summary>
    /// Reads all undo records for a transaction.
    /// </summary>
    public List<UndoRecord> ReadTransactionUndos(long transactionId, long startPointer)
    {
        var records = new List<UndoRecord>();
        var pointer = startPointer;

        while (pointer > 0)
        {
            var record = Read(pointer);
            if (record == null || record.TransactionId != transactionId)
                break;

            records.Add(record);
            pointer = record.PreviousUndoPointer;
        }

        return records;
    }

    /// <summary>
    /// Implementation of IUndoLogReader.ReadVersion for MVCC support.
    /// </summary>
    public RowVersion? ReadVersion(long rollPointer)
    {
        var record = Read(rollPointer);
        if (record == null)
            return null;

        // For MVCC, we return the row version from the undo record
        return new RowVersion(
            record.TransactionId,
            record.PreviousUndoPointer,
            record.Type == UndoRecordType.Delete,
            rowData: null // Row data would need schema to deserialize
        );
    }

    /// <summary>
    /// Reads a row version with the full row data.
    /// </summary>
    public RowVersion? ReadVersionWithData(long rollPointer, TableSchema schema)
    {
        var record = Read(rollPointer);
        if (record == null)
            return null;

        Row? rowData = null;
        if (record.Type == UndoRecordType.Update || record.Type == UndoRecordType.Delete)
        {
            try
            {
                rowData = record.GetOldRow(schema);
            }
            catch (Exception ex)
            {
                _logger.Warning("Failed to deserialize old row from undo record: {0}", ex.Message);
            }
        }

        return new RowVersion(
            record.TransactionId,
            record.PreviousUndoPointer,
            record.Type == UndoRecordType.Delete,
            rowData
        );
    }

    /// <summary>
    /// Flushes the undo log to disk.
    /// </summary>
    public void Flush()
    {
        lock (_writeLock)
        {
            _writer?.Flush();
            _undoStream?.Flush(true);
        }
    }

    /// <summary>
    /// Purges old undo records that are no longer needed.
    /// Records are safe to purge when no active transaction needs them.
    /// </summary>
    /// <param name="minActiveTransactionId">Minimum active transaction ID</param>
    public void Purge(long minActiveTransactionId)
    {
        // TODO: Implement purge logic
        // This involves:
        // 1. Finding undo records with TransactionId < minActiveTransactionId
        // 2. Checking that no active transaction's ReadView needs these records
        // 3. Marking space as reusable or compacting the file

        _logger.Info("Undo log purge requested for transactions < {0}", minActiveTransactionId);
    }

    private void AddToCache(long pointer, UndoRecord record)
    {
        if (_recordCache.Count >= _cacheSize)
        {
            // Simple eviction: remove a random entry
            var keyToRemove = _recordCache.Keys.First();
            _recordCache.Remove(keyToRemove);
        }
        _recordCache[pointer] = record;
    }

    private void EnsureOpen()
    {
        if (_undoStream == null || _writer == null)
            throw new InvalidOperationException("Undo log is not open");
    }

    private static uint ComputeChecksum(byte[] data)
    {
        // Simple checksum: sum of all bytes
        uint sum = 0;
        foreach (var b in data)
        {
            sum += b;
        }
        return sum;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        Flush();
        _writer?.Dispose();
        _undoStream?.Dispose();
        _recordCache.Clear();

        _logger.Info("Undo log disposed");
    }
}
