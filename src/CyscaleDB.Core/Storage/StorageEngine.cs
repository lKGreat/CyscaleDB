using System.Collections.Concurrent;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// The main storage engine that coordinates Catalog, BufferPool, and Tables.
/// Provides a unified interface for database operations with configurable options.
/// </summary>
public sealed class StorageEngine : IDisposable
{
    private readonly string _dataDirectory;
    private readonly Catalog _catalog;
    private readonly BufferPool _bufferPool;
    private readonly ConcurrentDictionary<string, Table> _openTables;
    private readonly Logger _logger;
    private readonly StorageOptions _options;
    private bool _disposed;

    // Statistics
    private long _totalInserts;
    private long _totalUpdates;
    private long _totalDeletes;
    private long _totalScans;

    /// <summary>
    /// Gets the data directory.
    /// </summary>
    public string DataDirectory => _dataDirectory;

    /// <summary>
    /// Gets the catalog.
    /// </summary>
    public Catalog Catalog => _catalog;

    /// <summary>
    /// Gets the buffer pool.
    /// </summary>
    public BufferPool BufferPool => _bufferPool;

    /// <summary>
    /// Gets the storage options.
    /// </summary>
    public StorageOptions Options => _options;

    /// <summary>
    /// Creates a new storage engine with default options.
    /// </summary>
    /// <param name="dataDirectory">The root directory for data files.</param>
    /// <param name="bufferPoolSize">The number of pages to cache in memory.</param>
    public StorageEngine(string dataDirectory, int bufferPoolSize = Constants.DefaultBufferPoolSize)
        : this(new StorageOptions
        {
            DataDirectory = dataDirectory,
            BufferPoolSizePages = bufferPoolSize
        })
    {
    }

    /// <summary>
    /// Creates a new storage engine with custom options.
    /// </summary>
    /// <param name="options">Storage configuration options.</param>
    public StorageEngine(StorageOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));

        if (string.IsNullOrWhiteSpace(options.DataDirectory))
            throw new ArgumentException("Data directory cannot be empty", nameof(options));

        _dataDirectory = Path.GetFullPath(options.DataDirectory);
        _bufferPool = new BufferPool(options.BufferPoolSizePages);
        _bufferPool.OldBlockTimeMs = options.OldBlockTimeMs;
        _catalog = new Catalog(_dataDirectory);
        _openTables = new ConcurrentDictionary<string, Table>(StringComparer.OrdinalIgnoreCase);
        _logger = LogManager.Default.GetLogger<StorageEngine>();

        // Ensure data directory exists
        Directory.CreateDirectory(_dataDirectory);

        _logger.Info("Storage engine initialized at: {0} with {1} buffer pages",
            _dataDirectory, options.BufferPoolSizePages);
    }

    #region Database Operations

    /// <summary>
    /// Creates a new database.
    /// </summary>
    public DatabaseInfo CreateDatabase(string name)
    {
        EnsureNotDisposed();
        return _catalog.CreateDatabase(name);
    }

    /// <summary>
    /// Gets a database by name.
    /// </summary>
    public DatabaseInfo? GetDatabase(string name)
    {
        EnsureNotDisposed();
        return _catalog.GetDatabase(name);
    }

    /// <summary>
    /// Drops a database.
    /// </summary>
    public bool DropDatabase(string name)
    {
        EnsureNotDisposed();

        var db = _catalog.GetDatabase(name);
        if (db == null)
            return false;

        // Close all tables in this database
        foreach (var table in db.Tables)
        {
            CloseTable(name, table.TableName);
        }

        _catalog.DropDatabase(name);
        return true;
    }

    /// <summary>
    /// Gets all databases.
    /// </summary>
    public IReadOnlyCollection<DatabaseInfo> GetAllDatabases()
    {
        EnsureNotDisposed();
        return _catalog.Databases;
    }

    #endregion

    #region Table Operations

    /// <summary>
    /// Creates a new table.
    /// </summary>
    public TableSchema CreateTable(string databaseName, string tableName, IEnumerable<ColumnDefinition> columns)
    {
        EnsureNotDisposed();
        return _catalog.CreateTable(databaseName, tableName, columns);
    }

    /// <summary>
    /// Gets a table schema.
    /// </summary>
    public TableSchema? GetTableSchema(string databaseName, string tableName)
    {
        EnsureNotDisposed();
        return _catalog.GetTableSchema(databaseName, tableName);
    }

    /// <summary>
    /// Opens a table for reading and writing.
    /// </summary>
    public Table OpenTable(string databaseName, string tableName)
    {
        EnsureNotDisposed();

        var key = GetTableKey(databaseName, tableName);

        return _openTables.GetOrAdd(key, _ =>
        {
            var schema = _catalog.GetTableSchema(databaseName, tableName);
            if (schema == null)
                throw new TableNotFoundException($"{databaseName}.{tableName}");

            var db = _catalog.GetDatabase(databaseName)!;
            var filePath = Path.Combine(db.DataDirectory, $"{tableName}{Constants.DataFileExtension}");
            var pageManager = new PageManager(filePath);

            var table = new Table(schema, pageManager, _bufferPool);
            table.Open();

            _logger.Debug("Opened table: {0}.{1}", databaseName, tableName);
            return table;
        });
    }

    /// <summary>
    /// Closes a table.
    /// </summary>
    public void CloseTable(string databaseName, string tableName)
    {
        var key = GetTableKey(databaseName, tableName);

        if (_openTables.TryRemove(key, out var table))
        {
            _bufferPool.EvictAll(table.PageManager);
            table.Dispose();
            _logger.Debug("Closed table: {0}.{1}", databaseName, tableName);
        }
    }

    /// <summary>
    /// Drops a table.
    /// </summary>
    public bool DropTable(string databaseName, string tableName)
    {
        EnsureNotDisposed();

        // Check if table exists first
        var schema = _catalog.GetTableSchema(databaseName, tableName);
        if (schema == null)
            return false;

        // Close the table first
        CloseTable(databaseName, tableName);

        // Remove from catalog (this also deletes the data file)
        _catalog.DropTable(databaseName, tableName);

        return true;
    }

    #endregion

    #region Row Operations

    /// <summary>
    /// Inserts a row into a table.
    /// </summary>
    public RowId InsertRow(string databaseName, string tableName, Row row)
    {
        var table = OpenTable(databaseName, tableName);
        var result = table.InsertRow(row);
        Interlocked.Increment(ref _totalInserts);
        return result;
    }

    /// <summary>
    /// Inserts a row into a table using values.
    /// </summary>
    public RowId InsertRow(string databaseName, string tableName, DataValue[] values)
    {
        var table = OpenTable(databaseName, tableName);
        var row = new Row(table.Schema, values);
        var result = table.InsertRow(row);
        Interlocked.Increment(ref _totalInserts);
        return result;
    }

    /// <summary>
    /// Gets a row by its RowId.
    /// </summary>
    public Row? GetRow(string databaseName, string tableName, RowId rowId)
    {
        var table = OpenTable(databaseName, tableName);
        return table.GetRowBySlot(rowId);
    }

    /// <summary>
    /// Scans all rows in a table.
    /// </summary>
    public IEnumerable<Row> ScanTable(string databaseName, string tableName)
    {
        var table = OpenTable(databaseName, tableName);
        Interlocked.Increment(ref _totalScans);

        // Trigger read-ahead if configured
        if (_options.ReadAheadPages > 0)
        {
            _bufferPool.PrefetchPages(table.PageManager, 0, _options.ReadAheadPages);
        }

        return table.ScanTable();
    }

    /// <summary>
    /// Updates a row.
    /// </summary>
    public bool UpdateRow(string databaseName, string tableName, RowId rowId, Row newRow)
    {
        var table = OpenTable(databaseName, tableName);
        var result = table.UpdateRow(rowId, newRow);
        if (result)
            Interlocked.Increment(ref _totalUpdates);
        return result;
    }

    /// <summary>
    /// Deletes a row.
    /// </summary>
    public bool DeleteRow(string databaseName, string tableName, RowId rowId)
    {
        var table = OpenTable(databaseName, tableName);
        var result = table.DeleteRow(rowId);
        if (result)
            Interlocked.Increment(ref _totalDeletes);
        return result;
    }

    #endregion

    #region Utility Methods

    /// <summary>
    /// Flushes all dirty pages to disk.
    /// </summary>
    public void Flush()
    {
        EnsureNotDisposed();

        _bufferPool.FlushAll();
        
        foreach (var table in _openTables.Values)
        {
            table.PageManager.Flush();
        }

        _catalog.Flush();
        _logger.Debug("Flushed all data to disk");
    }

    /// <summary>
    /// Gets buffer pool statistics (legacy method for compatibility).
    /// </summary>
    public (int cachedPages, int capacity, double hitRatio) GetBufferPoolStats()
    {
        return (_bufferPool.Count, _bufferPool.Capacity, _bufferPool.HitRatio);
    }

    /// <summary>
    /// Gets comprehensive storage engine statistics.
    /// </summary>
    public StorageEngineStats GetStats()
    {
        EnsureNotDisposed();

        var bufferStats = _bufferPool.GetStats();

        return new StorageEngineStats
        {
            DataDirectory = _dataDirectory,
            OpenTables = _openTables.Count,
            TotalInserts = _totalInserts,
            TotalUpdates = _totalUpdates,
            TotalDeletes = _totalDeletes,
            TotalScans = _totalScans,
            BufferPoolStats = bufferStats,
            BufferPoolSizePages = _options.BufferPoolSizePages,
            ReadAheadPages = _options.ReadAheadPages,
            FlushMode = _options.FlushMode
        };
    }

    private static string GetTableKey(string databaseName, string tableName)
    {
        return $"{databaseName}.{tableName}";
    }

    private void EnsureNotDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(StorageEngine));
    }

    #endregion

    #region Optimization Operations

    /// <summary>
    /// Optimizes a single table by compacting data and reclaiming space.
    /// </summary>
    public OptimizeResult OptimizeTable(string databaseName, string tableName)
    {
        EnsureNotDisposed();

        // Close the table from buffer pool first
        CloseTable(databaseName, tableName);

        // Reopen without buffer pool for optimization
        var schema = _catalog.GetTableSchema(databaseName, tableName);
        if (schema == null)
            throw new TableNotFoundException(tableName);

        var db = _catalog.GetDatabase(databaseName)!;
        var filePath = Path.Combine(db.DataDirectory, $"{tableName}{Constants.DataFileExtension}");
        var pageManager = new PageManager(filePath);
        
        using var table = new Table(schema, pageManager, null); // No buffer pool
        table.Open();

        var result = table.Optimize();

        _logger.Info("Optimized table {0}.{1}: {2}", databaseName, tableName, result);

        return result;
    }

    /// <summary>
    /// Shrinks a database by optimizing all tables and compacting free pages.
    /// </summary>
    public DatabaseShrinkResult ShrinkDatabase(string databaseName)
    {
        EnsureNotDisposed();

        var db = _catalog.GetDatabase(databaseName);
        if (db == null)
            throw new DatabaseNotFoundException(databaseName);

        var startTime = DateTime.UtcNow;
        var tableResults = new List<(string TableName, OptimizeResult Result)>();
        long totalSpaceReclaimed = 0;

        _logger.Info("Starting database shrink for {0}", databaseName);

        foreach (var tableSchema in db.Tables)
        {
            try
            {
                var result = OptimizeTable(databaseName, tableSchema.TableName);
                tableResults.Add((tableSchema.TableName, result));
                totalSpaceReclaimed += result.SpaceReclaimed;
            }
            catch (Exception ex)
            {
                _logger.Error("Error optimizing table {0}.{1}: {2}", 
                    databaseName, tableSchema.TableName, ex.Message);
            }
        }

        var duration = DateTime.UtcNow - startTime;

        var shrinkResult = new DatabaseShrinkResult(
            databaseName,
            tableResults,
            totalSpaceReclaimed,
            duration);

        _logger.Info("Database shrink completed for {0}: {1} bytes reclaimed in {2}ms",
            databaseName, totalSpaceReclaimed, duration.TotalMilliseconds);

        return shrinkResult;
    }

    /// <summary>
    /// Gets statistics for a table.
    /// </summary>
    public TableStatistics GetTableStatistics(string databaseName, string tableName)
    {
        EnsureNotDisposed();

        var table = OpenTable(databaseName, tableName);
        return table.GetStatistics();
    }

    /// <summary>
    /// Gets statistics for all tables in a database.
    /// </summary>
    public Dictionary<string, TableStatistics> GetDatabaseStatistics(string databaseName)
    {
        EnsureNotDisposed();

        var db = _catalog.GetDatabase(databaseName);
        if (db == null)
            throw new DatabaseNotFoundException(databaseName);

        var stats = new Dictionary<string, TableStatistics>();

        foreach (var tableSchema in db.Tables)
        {
            try
            {
                var tableStats = GetTableStatistics(databaseName, tableSchema.TableName);
                stats[tableSchema.TableName] = tableStats;
            }
            catch (Exception ex)
            {
                _logger.Error("Error getting stats for table {0}.{1}: {2}",
                    databaseName, tableSchema.TableName, ex.Message);
            }
        }

        return stats;
    }

    #endregion

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Close all open tables
        foreach (var table in _openTables.Values)
        {
            table.Dispose();
        }
        _openTables.Clear();

        // Dispose buffer pool (will flush dirty pages)
        _bufferPool.Dispose();

        // Save catalog
        _catalog.Dispose();

        _logger.Info("Storage engine shut down");
    }
}

/// <summary>
/// Result of a database shrink operation.
/// </summary>
public sealed class DatabaseShrinkResult
{
    /// <summary>
    /// Name of the database that was shrunk.
    /// </summary>
    public string DatabaseName { get; }

    /// <summary>
    /// Results for each table optimization.
    /// </summary>
    public IReadOnlyList<(string TableName, OptimizeResult Result)> TableResults { get; }

    /// <summary>
    /// Total space reclaimed in bytes.
    /// </summary>
    public long TotalSpaceReclaimed { get; }

    /// <summary>
    /// Total duration of the shrink operation.
    /// </summary>
    public TimeSpan Duration { get; }

    public DatabaseShrinkResult(
        string databaseName,
        List<(string TableName, OptimizeResult Result)> tableResults,
        long totalSpaceReclaimed,
        TimeSpan duration)
    {
        DatabaseName = databaseName;
        TableResults = tableResults.AsReadOnly();
        TotalSpaceReclaimed = totalSpaceReclaimed;
        Duration = duration;
    }

    public override string ToString() =>
        $"Shrink {DatabaseName}: {TableResults.Count} tables, {TotalSpaceReclaimed} bytes reclaimed in {Duration.TotalMilliseconds:F2}ms";
}

/// <summary>
/// Comprehensive statistics for the storage engine.
/// </summary>
public sealed class StorageEngineStats
{
    /// <summary>
    /// Data directory path.
    /// </summary>
    public string DataDirectory { get; init; } = "";

    /// <summary>
    /// Number of currently open tables.
    /// </summary>
    public int OpenTables { get; init; }

    /// <summary>
    /// Total number of insert operations.
    /// </summary>
    public long TotalInserts { get; init; }

    /// <summary>
    /// Total number of update operations.
    /// </summary>
    public long TotalUpdates { get; init; }

    /// <summary>
    /// Total number of delete operations.
    /// </summary>
    public long TotalDeletes { get; init; }

    /// <summary>
    /// Total number of table scan operations.
    /// </summary>
    public long TotalScans { get; init; }

    /// <summary>
    /// Buffer pool statistics.
    /// </summary>
    public BufferPoolStats? BufferPoolStats { get; init; }

    /// <summary>
    /// Configured buffer pool size in pages.
    /// </summary>
    public int BufferPoolSizePages { get; init; }

    /// <summary>
    /// Configured read-ahead pages.
    /// </summary>
    public int ReadAheadPages { get; init; }

    /// <summary>
    /// Configured flush mode.
    /// </summary>
    public FlushMode FlushMode { get; init; }

    public override string ToString()
    {
        return $"StorageEngine: {OpenTables} tables, inserts={TotalInserts}, updates={TotalUpdates}, deletes={TotalDeletes}, scans={TotalScans}";
    }
}
