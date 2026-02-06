using System.Buffers;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution;

/// <summary>
/// Provides disk-based spill storage for operators that exceed memory limits.
/// Rows are serialized to temporary files in a compact binary format and can be
/// read back in order. Supports partitioned spill files for hash-based operators.
/// 
/// Binary format per row:
/// [ColumnCount: int32][DataTypes: byte[]][NullBitmap: byte[]][Values...]
/// </summary>
public sealed class SpillFile : IDisposable
{
    private readonly string _filePath;
    private FileStream? _writeStream;
    private BinaryWriter? _writer;
    private long _rowCount;
    private bool _disposed;
    private readonly TableSchema _schema;

    /// <summary>
    /// Gets the file path of this spill file.
    /// </summary>
    public string FilePath => _filePath;

    /// <summary>
    /// Gets the number of rows written to this spill file.
    /// </summary>
    public long RowCount => _rowCount;

    /// <summary>
    /// Gets the size of the spill file in bytes.
    /// </summary>
    public long SizeBytes => _writeStream?.Length ?? (File.Exists(_filePath) ? new FileInfo(_filePath).Length : 0);

    /// <summary>
    /// Creates a new spill file in the specified temporary directory.
    /// </summary>
    /// <param name="schema">The schema for rows stored in this file.</param>
    /// <param name="tempDirectory">Directory for temporary files. Defaults to system temp.</param>
    /// <param name="prefix">Prefix for the temporary file name.</param>
    public SpillFile(TableSchema schema, string? tempDirectory = null, string prefix = "cyscale_spill")
    {
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));

        var dir = tempDirectory ?? Path.GetTempPath();
        Directory.CreateDirectory(dir);
        _filePath = Path.Combine(dir, $"{prefix}_{Guid.NewGuid():N}.tmp");
    }

    /// <summary>
    /// Opens the spill file for writing.
    /// </summary>
    public void OpenForWrite()
    {
        _writeStream = new FileStream(_filePath, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 64 * 1024, FileOptions.SequentialScan);
        _writer = new BinaryWriter(_writeStream);
        _rowCount = 0;
    }

    /// <summary>
    /// Writes a row to the spill file.
    /// </summary>
    public void WriteRow(Row row)
    {
        if (_writer == null)
            throw new InvalidOperationException("SpillFile is not open for writing.");

        var values = row.Values;
        var columnCount = values.Length;

        // Write column count
        _writer.Write(columnCount);

        // Write data types
        for (int i = 0; i < columnCount; i++)
        {
            _writer.Write((byte)values[i].Type);
        }

        // Write null bitmap
        var nullBitmapSize = (columnCount + 7) / 8;
        Span<byte> nullBitmap = stackalloc byte[nullBitmapSize];
        nullBitmap.Clear();
        for (int i = 0; i < columnCount; i++)
        {
            if (values[i].IsNull)
            {
                nullBitmap[i / 8] |= (byte)(1 << (i % 8));
            }
        }
        _writer.Write(nullBitmap);

        // Write non-null values
        for (int i = 0; i < columnCount; i++)
        {
            if (!values[i].IsNull)
            {
                var bytes = values[i].SerializeValue();
                _writer.Write(bytes.Length);
                _writer.Write(bytes);
            }
        }

        _rowCount++;
    }

    /// <summary>
    /// Flushes and closes the write stream. Must be called before reading.
    /// </summary>
    public void FinishWriting()
    {
        _writer?.Flush();
        _writer?.Dispose();
        _writer = null;
        _writeStream?.Dispose();
        _writeStream = null;
    }

    /// <summary>
    /// Creates a reader that iterates over all rows in this spill file.
    /// </summary>
    public SpillFileReader OpenForRead()
    {
        return new SpillFileReader(_filePath, _schema);
    }

    /// <summary>
    /// Deletes the temporary file.
    /// </summary>
    public void Delete()
    {
        try
        {
            if (File.Exists(_filePath))
                File.Delete(_filePath);
        }
        catch
        {
            // Best-effort cleanup
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _writer?.Dispose();
        _writeStream?.Dispose();
        Delete();
    }
}

/// <summary>
/// Reads rows sequentially from a spill file.
/// </summary>
public sealed class SpillFileReader : IDisposable
{
    private readonly FileStream _readStream;
    private readonly BinaryReader _reader;
    private readonly TableSchema _schema;
    private bool _disposed;

    public SpillFileReader(string filePath, TableSchema schema)
    {
        _schema = schema;
        _readStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 64 * 1024, FileOptions.SequentialScan);
        _reader = new BinaryReader(_readStream);
    }

    /// <summary>
    /// Reads the next row from the spill file. Returns null at end of file.
    /// </summary>
    public Row? ReadRow()
    {
        if (_readStream.Position >= _readStream.Length)
            return null;

        try
        {
            var columnCount = _reader.ReadInt32();

            // Read data types
            var types = new DataType[columnCount];
            for (int i = 0; i < columnCount; i++)
            {
                types[i] = (DataType)_reader.ReadByte();
            }

            // Read null bitmap
            var nullBitmapSize = (columnCount + 7) / 8;
            var nullBitmap = _reader.ReadBytes(nullBitmapSize);

            // Read values
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
                    var valueLen = _reader.ReadInt32();
                    var valueBytes = _reader.ReadBytes(valueLen);
                    values[i] = DataValue.DeserializeValue(valueBytes, types[i]);
                }
            }

            return new Row(_schema, values);
        }
        catch (EndOfStreamException)
        {
            return null;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _reader.Dispose();
        _readStream.Dispose();
    }
}

/// <summary>
/// Manages a collection of partitioned spill files for hash-based operators.
/// Rows are distributed across partitions using hash(key) % numPartitions.
/// </summary>
public sealed class PartitionedSpillFiles : IDisposable
{
    private readonly SpillFile[] _partitions;
    private readonly int _numPartitions;
    private bool _disposed;

    /// <summary>
    /// Gets the number of partitions.
    /// </summary>
    public int NumPartitions => _numPartitions;

    /// <summary>
    /// Creates a new set of partitioned spill files.
    /// </summary>
    /// <param name="schema">Row schema.</param>
    /// <param name="numPartitions">Number of partitions.</param>
    /// <param name="tempDirectory">Temporary directory.</param>
    public PartitionedSpillFiles(TableSchema schema, int numPartitions = 32, string? tempDirectory = null)
    {
        _numPartitions = numPartitions;
        _partitions = new SpillFile[numPartitions];
        for (int i = 0; i < numPartitions; i++)
        {
            _partitions[i] = new SpillFile(schema, tempDirectory, $"cyscale_part{i}");
            _partitions[i].OpenForWrite();
        }
    }

    /// <summary>
    /// Writes a row to the appropriate partition based on hash key.
    /// </summary>
    public void WriteRow(int hashCode, Row row)
    {
        var partition = Math.Abs(hashCode) % _numPartitions;
        _partitions[partition].WriteRow(row);
    }

    /// <summary>
    /// Gets the spill file for a specific partition.
    /// </summary>
    public SpillFile GetPartition(int partitionIndex)
    {
        return _partitions[partitionIndex];
    }

    /// <summary>
    /// Finishes writing to all partitions.
    /// </summary>
    public void FinishWriting()
    {
        for (int i = 0; i < _numPartitions; i++)
        {
            _partitions[i].FinishWriting();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var p in _partitions)
            p.Dispose();
    }
}
