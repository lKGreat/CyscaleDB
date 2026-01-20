using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Transactions;

/// <summary>
/// Write-Ahead Log (WAL) for durability and crash recovery.
/// </summary>
public sealed class WalLog : IDisposable
{
    private readonly string _logFilePath;
    private FileStream? _logStream;
    private BinaryWriter? _writer;
    private readonly object _writeLock = new();
    private readonly Logger _logger;
    private long _logSequenceNumber;
    private bool _disposed;

    /// <summary>
    /// Gets the current log sequence number.
    /// </summary>
    public long CurrentLsn => _logSequenceNumber;

    /// <summary>
    /// Creates a new WAL log.
    /// </summary>
    public WalLog(string dataDirectory)
    {
        _logFilePath = Path.Combine(dataDirectory, "cyscaledb" + Constants.WalFileExtension);
        _logger = LogManager.Default.GetLogger<WalLog>();
    }

    /// <summary>
    /// Opens the WAL log.
    /// </summary>
    public void Open()
    {
        var directory = Path.GetDirectoryName(_logFilePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        _logStream = new FileStream(
            _logFilePath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None,
            bufferSize: 4096,
            FileOptions.SequentialScan);

        _writer = new BinaryWriter(_logStream);

        // Read to end to get current LSN
        if (_logStream.Length > 0)
        {
            _logSequenceNumber = RecoverLsn();
        }
        else
        {
            _logSequenceNumber = 0;
        }

        _logger.Info("Opened WAL log at {0}, LSN: {1}", _logFilePath, _logSequenceNumber);
    }

    /// <summary>
    /// Writes a log entry.
    /// </summary>
    public long Write(WalEntry entry)
    {
        lock (_writeLock)
        {
            EnsureOpen();

            entry.Lsn = Interlocked.Increment(ref _logSequenceNumber);

            // Serialize entry
            var data = entry.Serialize();

            // Write entry: [length(4)][data][checksum(4)]
            _writer!.Write(data.Length);
            _writer.Write(data);
            _writer.Write(ComputeChecksum(data));

            return entry.Lsn;
        }
    }

    /// <summary>
    /// Writes a BEGIN entry.
    /// </summary>
    public long WriteBegin(long transactionId)
    {
        return Write(new WalEntry
        {
            TransactionId = transactionId,
            Type = WalEntryType.Begin,
            Timestamp = DateTime.UtcNow
        });
    }

    /// <summary>
    /// Writes a COMMIT entry.
    /// </summary>
    public long WriteCommit(long transactionId)
    {
        var lsn = Write(new WalEntry
        {
            TransactionId = transactionId,
            Type = WalEntryType.Commit,
            Timestamp = DateTime.UtcNow
        });

        // Ensure commit is flushed to disk
        Flush();

        return lsn;
    }

    /// <summary>
    /// Writes an ABORT entry.
    /// </summary>
    public long WriteAbort(long transactionId)
    {
        return Write(new WalEntry
        {
            TransactionId = transactionId,
            Type = WalEntryType.Abort,
            Timestamp = DateTime.UtcNow
        });
    }

    /// <summary>
    /// Writes an INSERT entry.
    /// </summary>
    public long WriteInsert(long transactionId, string databaseName, string tableName, byte[] rowData)
    {
        return Write(new WalEntry
        {
            TransactionId = transactionId,
            Type = WalEntryType.Insert,
            DatabaseName = databaseName,
            TableName = tableName,
            NewData = rowData,
            Timestamp = DateTime.UtcNow
        });
    }

    /// <summary>
    /// Writes an UPDATE entry.
    /// </summary>
    public long WriteUpdate(long transactionId, string databaseName, string tableName, 
        int pageId, short slotNumber, byte[] oldData, byte[] newData)
    {
        return Write(new WalEntry
        {
            TransactionId = transactionId,
            Type = WalEntryType.Update,
            DatabaseName = databaseName,
            TableName = tableName,
            PageId = pageId,
            SlotNumber = slotNumber,
            OldData = oldData,
            NewData = newData,
            Timestamp = DateTime.UtcNow
        });
    }

    /// <summary>
    /// Writes a DELETE entry.
    /// </summary>
    public long WriteDelete(long transactionId, string databaseName, string tableName,
        int pageId, short slotNumber, byte[] oldData)
    {
        return Write(new WalEntry
        {
            TransactionId = transactionId,
            Type = WalEntryType.Delete,
            DatabaseName = databaseName,
            TableName = tableName,
            PageId = pageId,
            SlotNumber = slotNumber,
            OldData = oldData,
            Timestamp = DateTime.UtcNow
        });
    }

    /// <summary>
    /// Writes a CHECKPOINT entry.
    /// </summary>
    public long WriteCheckpoint(List<long> activeTransactions)
    {
        return Write(new WalEntry
        {
            TransactionId = 0,
            Type = WalEntryType.Checkpoint,
            Timestamp = DateTime.UtcNow,
            CheckpointData = activeTransactions.ToArray()
        });
    }

    /// <summary>
    /// Flushes the log to disk.
    /// </summary>
    public void Flush()
    {
        lock (_writeLock)
        {
            _writer?.Flush();
            _logStream?.Flush(true);
        }
    }

    /// <summary>
    /// Reads all entries from the log for recovery.
    /// </summary>
    public List<WalEntry> ReadAll()
    {
        var entries = new List<WalEntry>();

        lock (_writeLock)
        {
            EnsureOpen();
            _logStream!.Seek(0, SeekOrigin.Begin);

            using var reader = new BinaryReader(_logStream, System.Text.Encoding.UTF8, leaveOpen: true);

            while (_logStream.Position < _logStream.Length)
            {
                try
                {
                    var length = reader.ReadInt32();
                    var data = reader.ReadBytes(length);
                    var checksum = reader.ReadUInt32();

                    if (ComputeChecksum(data) != checksum)
                    {
                        _logger.Warning("Corrupted WAL entry at position {0}", _logStream.Position - length - 8);
                        break; // Stop at first corrupted entry
                    }

                    var entry = WalEntry.Deserialize(data);
                    entries.Add(entry);
                }
                catch (EndOfStreamException)
                {
                    break;
                }
            }
        }

        return entries;
    }

    /// <summary>
    /// Truncates the log up to (but not including) the given LSN.
    /// </summary>
    public void Truncate(long upToLsn)
    {
        lock (_writeLock)
        {
            EnsureOpen();

            // Read entries to keep
            var entriesToKeep = new List<WalEntry>();
            _logStream!.Seek(0, SeekOrigin.Begin);

            using (var reader = new BinaryReader(_logStream, System.Text.Encoding.UTF8, leaveOpen: true))
            {
                while (_logStream.Position < _logStream.Length)
                {
                    try
                    {
                        var length = reader.ReadInt32();
                        var data = reader.ReadBytes(length);
                        var checksum = reader.ReadUInt32();

                        if (ComputeChecksum(data) != checksum)
                            break;

                        var entry = WalEntry.Deserialize(data);
                        if (entry.Lsn >= upToLsn)
                        {
                            entriesToKeep.Add(entry);
                        }
                    }
                    catch (EndOfStreamException)
                    {
                        break;
                    }
                }
            }

            // Rewrite file with kept entries
            _logStream.SetLength(0);
            _logStream.Seek(0, SeekOrigin.Begin);

            foreach (var entry in entriesToKeep)
            {
                var data = entry.Serialize();
                _writer!.Write(data.Length);
                _writer.Write(data);
                _writer.Write(ComputeChecksum(data));
            }

            Flush();
            _logger.Info("Truncated WAL log up to LSN {0}", upToLsn);
        }
    }

    private long RecoverLsn()
    {
        long maxLsn = 0;
        _logStream!.Seek(0, SeekOrigin.Begin);

        using var reader = new BinaryReader(_logStream, System.Text.Encoding.UTF8, leaveOpen: true);

        while (_logStream.Position < _logStream.Length)
        {
            try
            {
                var length = reader.ReadInt32();
                var data = reader.ReadBytes(length);
                var checksum = reader.ReadUInt32();

                if (ComputeChecksum(data) != checksum)
                    break;

                var entry = WalEntry.Deserialize(data);
                if (entry.Lsn > maxLsn)
                    maxLsn = entry.Lsn;
            }
            catch
            {
                break;
            }
        }

        return maxLsn;
    }

    private void EnsureOpen()
    {
        if (_logStream == null || _writer == null)
            throw new InvalidOperationException("WAL log is not open");
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
        _logStream?.Dispose();
    }
}

/// <summary>
/// Types of WAL entries.
/// </summary>
public enum WalEntryType : byte
{
    Begin = 1,
    Commit = 2,
    Abort = 3,
    Insert = 4,
    Update = 5,
    Delete = 6,
    Checkpoint = 7
}

/// <summary>
/// A single WAL log entry.
/// </summary>
public class WalEntry
{
    /// <summary>
    /// Log sequence number.
    /// </summary>
    public long Lsn { get; set; }

    /// <summary>
    /// Transaction ID.
    /// </summary>
    public long TransactionId { get; set; }

    /// <summary>
    /// Entry type.
    /// </summary>
    public WalEntryType Type { get; set; }

    /// <summary>
    /// Timestamp.
    /// </summary>
    public DateTime Timestamp { get; set; }

    /// <summary>
    /// Database name (for data operations).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Table name (for data operations).
    /// </summary>
    public string? TableName { get; set; }

    /// <summary>
    /// Page ID (for update/delete).
    /// </summary>
    public int PageId { get; set; }

    /// <summary>
    /// Slot number (for update/delete).
    /// </summary>
    public short SlotNumber { get; set; }

    /// <summary>
    /// Old row data (for update/delete - used for undo).
    /// </summary>
    public byte[]? OldData { get; set; }

    /// <summary>
    /// New row data (for insert/update - used for redo).
    /// </summary>
    public byte[]? NewData { get; set; }

    /// <summary>
    /// Active transactions (for checkpoint).
    /// </summary>
    public long[]? CheckpointData { get; set; }

    /// <summary>
    /// Serializes this entry to bytes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(Lsn);
        writer.Write(TransactionId);
        writer.Write((byte)Type);
        writer.Write(Timestamp.Ticks);
        writer.Write(DatabaseName ?? string.Empty);
        writer.Write(TableName ?? string.Empty);
        writer.Write(PageId);
        writer.Write(SlotNumber);

        WriteByteArray(writer, OldData);
        WriteByteArray(writer, NewData);
        WriteInt64Array(writer, CheckpointData);

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes an entry from bytes.
    /// </summary>
    public static WalEntry Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var entry = new WalEntry
        {
            Lsn = reader.ReadInt64(),
            TransactionId = reader.ReadInt64(),
            Type = (WalEntryType)reader.ReadByte(),
            Timestamp = new DateTime(reader.ReadInt64()),
            DatabaseName = reader.ReadString(),
            TableName = reader.ReadString(),
            PageId = reader.ReadInt32(),
            SlotNumber = reader.ReadInt16(),
            OldData = ReadByteArray(reader),
            NewData = ReadByteArray(reader),
            CheckpointData = ReadInt64Array(reader)
        };

        if (string.IsNullOrEmpty(entry.DatabaseName))
            entry.DatabaseName = null;
        if (string.IsNullOrEmpty(entry.TableName))
            entry.TableName = null;

        return entry;
    }

    private static void WriteByteArray(BinaryWriter writer, byte[]? data)
    {
        if (data == null)
        {
            writer.Write(-1);
        }
        else
        {
            writer.Write(data.Length);
            writer.Write(data);
        }
    }

    private static byte[]? ReadByteArray(BinaryReader reader)
    {
        var length = reader.ReadInt32();
        if (length < 0)
            return null;
        return reader.ReadBytes(length);
    }

    private static void WriteInt64Array(BinaryWriter writer, long[]? data)
    {
        if (data == null)
        {
            writer.Write(-1);
        }
        else
        {
            writer.Write(data.Length);
            foreach (var val in data)
            {
                writer.Write(val);
            }
        }
    }

    private static long[]? ReadInt64Array(BinaryReader reader)
    {
        var length = reader.ReadInt32();
        if (length < 0)
            return null;

        var data = new long[length];
        for (int i = 0; i < length; i++)
        {
            data[i] = reader.ReadInt64();
        }
        return data;
    }
}
