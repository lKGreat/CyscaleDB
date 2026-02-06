using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Internal temporary table engine that stores rows in memory up to a configurable limit,
/// then automatically spills to disk. Used by operators like ORDER BY, GROUP BY, DISTINCT,
/// and UNION to handle datasets larger than available memory.
///
/// Memory phase: rows stored in List&lt;Row&gt; (fast random access).
/// Disk phase: rows serialized to a temporary file via SpillFile-like mechanism.
///
/// Configurable via system variables:
///   tmp_table_size (default 64 MB)
///   max_heap_table_size (default 64 MB)
/// The effective limit is min(tmp_table_size, max_heap_table_size).
/// </summary>
public sealed class TempTableEngine : IDisposable
{
    private readonly TableSchema _schema;
    private readonly long _maxMemoryBytes;
    private readonly string? _tempDirectory;

    private List<Row>? _memoryRows;
    private FileStream? _diskStream;
    private BinaryWriter? _diskWriter;
    private string? _diskFilePath;
    private long _estimatedMemoryUsed;
    private long _totalRowCount;
    private bool _isSpilledToDisk;
    private bool _finalized;
    private bool _disposed;

    /// <summary>
    /// Default max memory for a temp table (64 MB).
    /// </summary>
    public const long DefaultMaxMemory = 64 * 1024 * 1024;

    /// <summary>
    /// Gets the total number of rows stored.
    /// </summary>
    public long RowCount => _totalRowCount;

    /// <summary>
    /// Gets whether the temp table has spilled to disk.
    /// </summary>
    public bool IsSpilledToDisk => _isSpilledToDisk;

    /// <summary>
    /// Gets the estimated memory usage in bytes.
    /// </summary>
    public long EstimatedMemoryBytes => _estimatedMemoryUsed;

    /// <summary>
    /// Creates a new temporary table engine.
    /// </summary>
    /// <param name="schema">Row schema.</param>
    /// <param name="maxMemoryBytes">Maximum memory before spilling to disk.</param>
    /// <param name="tempDirectory">Directory for temporary files.</param>
    public TempTableEngine(TableSchema schema, long maxMemoryBytes = DefaultMaxMemory, string? tempDirectory = null)
    {
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _maxMemoryBytes = maxMemoryBytes;
        _tempDirectory = tempDirectory;
        _memoryRows = new List<Row>();
        _totalRowCount = 0;
        _estimatedMemoryUsed = 0;
        _isSpilledToDisk = false;
        _finalized = false;
    }

    /// <summary>
    /// Inserts a row into the temporary table.
    /// If memory limit is exceeded, the table automatically spills to disk.
    /// </summary>
    public void InsertRow(Row row)
    {
        if (_finalized)
            throw new InvalidOperationException("TempTableEngine has been finalized for reading.");

        if (!_isSpilledToDisk)
        {
            _memoryRows!.Add(row);
            _estimatedMemoryUsed += EstimateRowSize(row);
            _totalRowCount++;

            // Check if we need to spill to disk
            if (_estimatedMemoryUsed >= _maxMemoryBytes)
            {
                SpillToDisk();
            }
        }
        else
        {
            // Already on disk - write directly
            WriteRowToDisk(row);
            _totalRowCount++;
        }
    }

    /// <summary>
    /// Completes the temp table for reading. No more inserts allowed after this.
    /// </summary>
    public void Complete()
    {
        if (_finalized) return;
        _finalized = true;

        if (_isSpilledToDisk)
        {
            _diskWriter?.Flush();
            _diskWriter?.Dispose();
            _diskWriter = null;
            _diskStream?.Dispose();
            _diskStream = null;
        }
    }

    /// <summary>
    /// Creates a reader to iterate over all rows in the temp table.
    /// Must call Complete() before creating a reader.
    /// </summary>
    public TempTableReader CreateReader()
    {
        if (!_finalized)
            Complete();

        if (!_isSpilledToDisk)
        {
            return new TempTableReader(_memoryRows!);
        }
        else
        {
            return new TempTableReader(_diskFilePath!, _schema);
        }
    }

    /// <summary>
    /// Gets all rows in memory (only valid if not spilled to disk).
    /// </summary>
    public List<Row>? GetMemoryRows()
    {
        return _isSpilledToDisk ? null : _memoryRows;
    }

    #region Spill Logic

    private void SpillToDisk()
    {
        var dir = _tempDirectory ?? Path.GetTempPath();
        Directory.CreateDirectory(dir);
        _diskFilePath = Path.Combine(dir, $"cyscale_tmp_{Guid.NewGuid():N}.tmp");

        _diskStream = new FileStream(_diskFilePath, FileMode.Create, FileAccess.Write,
            FileShare.None, bufferSize: 64 * 1024, FileOptions.SequentialScan);
        _diskWriter = new BinaryWriter(_diskStream);

        // Write all memory rows to disk
        foreach (var row in _memoryRows!)
        {
            WriteRowToDisk(row);
        }

        // Free memory
        _memoryRows.Clear();
        _memoryRows = null;
        _estimatedMemoryUsed = 0;
        _isSpilledToDisk = true;
    }

    private void WriteRowToDisk(Row row)
    {
        var values = row.Values;
        var columnCount = values.Length;

        _diskWriter!.Write(columnCount);

        for (int i = 0; i < columnCount; i++)
            _diskWriter.Write((byte)values[i].Type);

        var nullBitmapSize = (columnCount + 7) / 8;
        Span<byte> nullBitmap = stackalloc byte[nullBitmapSize];
        nullBitmap.Clear();
        for (int i = 0; i < columnCount; i++)
        {
            if (values[i].IsNull)
                nullBitmap[i / 8] |= (byte)(1 << (i % 8));
        }
        _diskWriter.Write(nullBitmap);

        for (int i = 0; i < columnCount; i++)
        {
            if (!values[i].IsNull)
            {
                var bytes = values[i].SerializeValue();
                _diskWriter.Write(bytes.Length);
                _diskWriter.Write(bytes);
            }
        }
    }

    #endregion

    private static long EstimateRowSize(Row row)
    {
        long size = 200;
        foreach (var val in row.Values)
        {
            if (val.IsNull) continue;
            size += val.Type switch
            {
                DataType.TinyInt or DataType.Boolean => 1,
                DataType.SmallInt => 2,
                DataType.Int or DataType.Float or DataType.Date => 4,
                DataType.BigInt or DataType.Double or DataType.DateTime or DataType.Timestamp or DataType.Time => 8,
                DataType.Decimal => 16,
                DataType.VarChar or DataType.Char or DataType.Text or DataType.Json =>
                    24 + (val.AsString()?.Length ?? 0) * 2,
                DataType.Blob => 24 + (val.AsBlob()?.Length ?? 0),
                _ => 8
            };
        }
        return size;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _diskWriter?.Dispose();
        _diskStream?.Dispose();
        _memoryRows = null;

        if (_diskFilePath != null)
        {
            try { File.Delete(_diskFilePath); } catch { }
        }
    }
}

/// <summary>
/// Reads rows from a TempTableEngine (either from memory or disk).
/// </summary>
public sealed class TempTableReader : IDisposable
{
    private readonly List<Row>? _memoryRows;
    private int _memoryIndex;

    private readonly FileStream? _diskStream;
    private readonly BinaryReader? _diskReader;
    private readonly TableSchema? _schema;
    private bool _disposed;

    /// <summary>
    /// Creates a reader for in-memory rows.
    /// </summary>
    public TempTableReader(List<Row> rows)
    {
        _memoryRows = rows;
        _memoryIndex = 0;
    }

    /// <summary>
    /// Creates a reader for disk-based rows.
    /// </summary>
    public TempTableReader(string filePath, TableSchema schema)
    {
        _schema = schema;
        _diskStream = new FileStream(filePath, FileMode.Open, FileAccess.Read,
            FileShare.Read, bufferSize: 64 * 1024, FileOptions.SequentialScan);
        _diskReader = new BinaryReader(_diskStream);
    }

    /// <summary>
    /// Reads the next row. Returns null at end.
    /// </summary>
    public Row? ReadRow()
    {
        if (_memoryRows != null)
        {
            if (_memoryIndex >= _memoryRows.Count)
                return null;
            return _memoryRows[_memoryIndex++];
        }

        if (_diskReader == null || _diskStream == null)
            return null;

        if (_diskStream.Position >= _diskStream.Length)
            return null;

        try
        {
            var columnCount = _diskReader.ReadInt32();
            var types = new DataType[columnCount];
            for (int i = 0; i < columnCount; i++)
                types[i] = (DataType)_diskReader.ReadByte();

            var nullBitmapSize = (columnCount + 7) / 8;
            var nullBitmap = _diskReader.ReadBytes(nullBitmapSize);

            var values = new DataValue[columnCount];
            for (int i = 0; i < columnCount; i++)
            {
                var isNull = (nullBitmap[i / 8] & (1 << (i % 8))) != 0;
                if (isNull)
                {
                    values[i] = DataValue.Null;
                }
                else
                {
                    var valueLen = _diskReader.ReadInt32();
                    var valueBytes = _diskReader.ReadBytes(valueLen);
                    values[i] = DataValue.DeserializeValue(valueBytes, types[i]);
                }
            }

            return new Row(_schema!, values);
        }
        catch (EndOfStreamException)
        {
            return null;
        }
    }

    /// <summary>
    /// Resets the reader to the beginning.
    /// </summary>
    public void Reset()
    {
        if (_memoryRows != null)
        {
            _memoryIndex = 0;
        }
        else if (_diskStream != null)
        {
            _diskStream.Position = 0;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _diskReader?.Dispose();
        _diskStream?.Dispose();
    }
}
