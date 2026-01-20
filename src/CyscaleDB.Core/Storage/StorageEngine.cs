using System.Collections.Concurrent;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// The main storage engine that coordinates Catalog, BufferPool, and Tables.
/// Provides a unified interface for database operations.
/// </summary>
public sealed class StorageEngine : IDisposable
{
    private readonly string _dataDirectory;
    private readonly Catalog _catalog;
    private readonly BufferPool _bufferPool;
    private readonly ConcurrentDictionary<string, Table> _openTables;
    private readonly Logger _logger;
    private bool _disposed;

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
    /// Creates a new storage engine.
    /// </summary>
    /// <param name="dataDirectory">The root directory for data files.</param>
    /// <param name="bufferPoolSize">The number of pages to cache in memory.</param>
    public StorageEngine(string dataDirectory, int bufferPoolSize = Constants.DefaultBufferPoolSize)
    {
        if (string.IsNullOrWhiteSpace(dataDirectory))
            throw new ArgumentException("Data directory cannot be empty", nameof(dataDirectory));

        _dataDirectory = Path.GetFullPath(dataDirectory);
        _bufferPool = new BufferPool(bufferPoolSize);
        _catalog = new Catalog(_dataDirectory);
        _openTables = new ConcurrentDictionary<string, Table>(StringComparer.OrdinalIgnoreCase);
        _logger = LogManager.Default.GetLogger<StorageEngine>();

        // Ensure data directory exists
        Directory.CreateDirectory(_dataDirectory);

        _logger.Info("Storage engine initialized at: {0}", _dataDirectory);
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
        return table.InsertRow(row);
    }

    /// <summary>
    /// Inserts a row into a table using values.
    /// </summary>
    public RowId InsertRow(string databaseName, string tableName, DataValue[] values)
    {
        var table = OpenTable(databaseName, tableName);
        var row = new Row(table.Schema, values);
        return table.InsertRow(row);
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
        return table.ScanTable();
    }

    /// <summary>
    /// Updates a row.
    /// </summary>
    public bool UpdateRow(string databaseName, string tableName, RowId rowId, Row newRow)
    {
        var table = OpenTable(databaseName, tableName);
        return table.UpdateRow(rowId, newRow);
    }

    /// <summary>
    /// Deletes a row.
    /// </summary>
    public bool DeleteRow(string databaseName, string tableName, RowId rowId)
    {
        var table = OpenTable(databaseName, tableName);
        return table.DeleteRow(rowId);
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
    /// Gets buffer pool statistics.
    /// </summary>
    public (int cachedPages, int capacity, double hitRatio) GetBufferPoolStats()
    {
        return (_bufferPool.Count, _bufferPool.Capacity, _bufferPool.HitRatio);
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
