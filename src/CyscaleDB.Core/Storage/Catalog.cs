using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Manages database metadata (catalog) including databases and their schemas.
/// Provides persistence of catalog information to disk.
/// </summary>
public sealed class Catalog : IDisposable
{
    private readonly string _dataDirectory;
    private readonly Dictionary<string, DatabaseInfo> _databases;
    private readonly Dictionary<string, Table> _openTables;
    private readonly BufferPool _bufferPool;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Logger _logger;
    private int _nextDatabaseId;
    private bool _disposed;

    /// <summary>
    /// Gets all databases in the catalog.
    /// </summary>
    public IReadOnlyCollection<DatabaseInfo> Databases
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _databases.Values.ToList();
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Gets the shared buffer pool.
    /// </summary>
    public BufferPool BufferPool => _bufferPool;

    /// <summary>
    /// Creates a new Catalog instance.
    /// </summary>
    public Catalog(string dataDirectory, int bufferPoolSize = Constants.DefaultBufferPoolSize)
    {
        _dataDirectory = dataDirectory ?? throw new ArgumentNullException(nameof(dataDirectory));
        _databases = new Dictionary<string, DatabaseInfo>(StringComparer.OrdinalIgnoreCase);
        _openTables = new Dictionary<string, Table>(StringComparer.OrdinalIgnoreCase);
        _bufferPool = new BufferPool(bufferPoolSize);
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<Catalog>();
        _nextDatabaseId = 1;
    }

    /// <summary>
    /// Initializes the catalog by loading existing metadata from disk.
    /// </summary>
    public void Initialize()
    {
        _lock.EnterWriteLock();
        try
        {
            // Ensure data directory exists
            if (!Directory.Exists(_dataDirectory))
            {
                Directory.CreateDirectory(_dataDirectory);
                _logger.Info("Created data directory: {0}", _dataDirectory);
            }

            // Load catalog from disk
            var catalogPath = GetCatalogFilePath();
            if (File.Exists(catalogPath))
            {
                LoadCatalog(catalogPath);
                _logger.Info("Loaded catalog with {0} databases", _databases.Count);
            }
            else
            {
                // Create default database
                CreateDatabaseInternal(Constants.DefaultDatabaseName);
                SaveCatalog();
                _logger.Info("Initialized new catalog with default database");
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Creates a new database.
    /// </summary>
    public DatabaseInfo CreateDatabase(string databaseName)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("Database name cannot be empty", nameof(databaseName));

        _lock.EnterWriteLock();
        try
        {
            if (_databases.ContainsKey(databaseName))
                throw new DatabaseExistsException(databaseName);

            var db = CreateDatabaseInternal(databaseName);
            SaveCatalog();

            _logger.Info("Created database: {0}", databaseName);
            return db;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Drops a database and all its tables.
    /// </summary>
    public void DropDatabase(string databaseName)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("Database name cannot be empty", nameof(databaseName));

        if (databaseName.Equals(Constants.SystemDatabaseName, StringComparison.OrdinalIgnoreCase))
            throw new CyscaleException("Cannot drop system database", ErrorCode.ConstraintViolation);

        _lock.EnterWriteLock();
        try
        {
            if (!_databases.TryGetValue(databaseName, out var db))
                throw new DatabaseNotFoundException(databaseName);

            // Close and remove all tables in this database
            foreach (var table in db.Tables)
            {
                var tableKey = GetTableKey(databaseName, table.TableName);
                if (_openTables.TryGetValue(tableKey, out var openTable))
                {
                    openTable.Dispose();
                    _openTables.Remove(tableKey);
                }

                // Delete table data file
                var tablePath = Path.Combine(db.DataDirectory, $"{table.TableName}{Constants.DataFileExtension}");
                if (File.Exists(tablePath))
                {
                    File.Delete(tablePath);
                }
            }

            // Remove database directory if empty
            if (Directory.Exists(db.DataDirectory))
            {
                var files = Directory.GetFiles(db.DataDirectory);
                if (files.Length == 0)
                {
                    Directory.Delete(db.DataDirectory);
                }
            }

            _databases.Remove(databaseName);
            SaveCatalog();

            _logger.Info("Dropped database: {0}", databaseName);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Gets a database by name.
    /// </summary>
    public DatabaseInfo? GetDatabase(string databaseName)
    {
        _lock.EnterReadLock();
        try
        {
            return _databases.TryGetValue(databaseName, out var db) ? db : null;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Checks if a database exists.
    /// </summary>
    public bool DatabaseExists(string databaseName)
    {
        _lock.EnterReadLock();
        try
        {
            return _databases.ContainsKey(databaseName);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Creates a new table in the specified database.
    /// </summary>
    public TableSchema CreateTable(string databaseName, string tableName, IEnumerable<ColumnDefinition> columns)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_databases.TryGetValue(databaseName, out var db))
                throw new DatabaseNotFoundException(databaseName);

            if (db.HasTable(tableName))
                throw new TableExistsException(tableName);

            var tableId = db.GetNextTableId();
            var schema = new TableSchema(tableId, databaseName, tableName, columns);
            db.AddTable(schema);

            // Create and open the table file
            var tablePath = Path.Combine(db.DataDirectory, $"{tableName}{Constants.DataFileExtension}");
            var pageManager = new PageManager(tablePath);
            var table = new Table(schema, pageManager, _bufferPool);
            table.Open(createIfNotExists: true);

            var tableKey = GetTableKey(databaseName, tableName);
            _openTables[tableKey] = table;

            SaveCatalog();

            _logger.Info("Created table: {0}.{1}", databaseName, tableName);
            return schema;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Drops a table from the specified database.
    /// </summary>
    public void DropTable(string databaseName, string tableName)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_databases.TryGetValue(databaseName, out var db))
                throw new DatabaseNotFoundException(databaseName);

            if (!db.HasTable(tableName))
                throw new TableNotFoundException(tableName);

            var tableKey = GetTableKey(databaseName, tableName);

            // Close the table if open
            if (_openTables.TryGetValue(tableKey, out var table))
            {
                table.Dispose();
                _openTables.Remove(tableKey);
            }

            // Delete the data file
            var tablePath = Path.Combine(db.DataDirectory, $"{tableName}{Constants.DataFileExtension}");
            if (File.Exists(tablePath))
            {
                File.Delete(tablePath);
            }

            db.RemoveTable(tableName);
            SaveCatalog();

            _logger.Info("Dropped table: {0}.{1}", databaseName, tableName);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Gets a table schema by name.
    /// </summary>
    public TableSchema? GetTableSchema(string databaseName, string tableName)
    {
        _lock.EnterReadLock();
        try
        {
            if (!_databases.TryGetValue(databaseName, out var db))
                return null;

            return db.GetTable(tableName);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Gets a Table instance for performing data operations.
    /// </summary>
    public Table? GetTable(string databaseName, string tableName)
    {
        _lock.EnterUpgradeableReadLock();
        try
        {
            if (!_databases.TryGetValue(databaseName, out var db))
                return null;

            var schema = db.GetTable(tableName);
            if (schema == null)
                return null;

            var tableKey = GetTableKey(databaseName, tableName);

            // Check if already open
            if (_openTables.TryGetValue(tableKey, out var table))
                return table;

            _lock.EnterWriteLock();
            try
            {
                // Double-check after acquiring write lock
                if (_openTables.TryGetValue(tableKey, out table))
                    return table;

                // Open the table
                var tablePath = Path.Combine(db.DataDirectory, $"{tableName}{Constants.DataFileExtension}");
                var pageManager = new PageManager(tablePath);
                table = new Table(schema, pageManager, _bufferPool);
                table.Open(createIfNotExists: false);
                _openTables[tableKey] = table;

                return table;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
        finally
        {
            _lock.ExitUpgradeableReadLock();
        }
    }

    /// <summary>
    /// Lists all tables in a database.
    /// </summary>
    public IReadOnlyList<string> ListTables(string databaseName)
    {
        _lock.EnterReadLock();
        try
        {
            if (!_databases.TryGetValue(databaseName, out var db))
                throw new DatabaseNotFoundException(databaseName);

            return db.Tables.Select(t => t.TableName).ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Updates the catalog metadata for a table (e.g., after row operations).
    /// </summary>
    public void UpdateTableSchema(TableSchema schema)
    {
        _lock.EnterWriteLock();
        try
        {
            SaveCatalog();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    #region View Operations

    /// <summary>
    /// Creates a new view in the specified database.
    /// </summary>
    public ViewInfo CreateView(string databaseName, string viewName, string definition, IEnumerable<string>? columnNames = null, bool orReplace = false)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_databases.TryGetValue(databaseName, out var db))
                throw new DatabaseNotFoundException(databaseName);

            // Check if view exists
            if (db.HasView(viewName))
            {
                if (orReplace)
                {
                    db.RemoveView(viewName);
                }
                else
                {
                    throw new ViewExistsException(viewName);
                }
            }

            // Check if a table with the same name exists
            if (db.HasTable(viewName))
                throw new CyscaleException($"A table with name '{viewName}' already exists", ErrorCode.ViewExists);

            var viewId = db.GetNextViewId();
            var view = new ViewInfo(viewId, viewName, databaseName, definition, columnNames, orReplace);

            if (orReplace)
            {
                db.AddOrReplaceView(view);
            }
            else
            {
                db.AddView(view);
            }

            SaveCatalog();

            _logger.Info("Created view: {0}.{1}", databaseName, viewName);
            return view;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Drops a view from the specified database.
    /// </summary>
    public bool DropView(string databaseName, string viewName, bool ifExists = false)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_databases.TryGetValue(databaseName, out var db))
            {
                if (ifExists) return false;
                throw new DatabaseNotFoundException(databaseName);
            }

            if (!db.HasView(viewName))
            {
                if (ifExists) return false;
                throw new ViewNotFoundException(viewName);
            }

            db.RemoveView(viewName);
            SaveCatalog();

            _logger.Info("Dropped view: {0}.{1}", databaseName, viewName);
            return true;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Gets a view by name.
    /// </summary>
    public ViewInfo? GetView(string databaseName, string viewName)
    {
        _lock.EnterReadLock();
        try
        {
            if (!_databases.TryGetValue(databaseName, out var db))
                return null;

            return db.GetView(viewName);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Lists all views in a database.
    /// </summary>
    public IReadOnlyList<string> ListViews(string databaseName)
    {
        _lock.EnterReadLock();
        try
        {
            if (!_databases.TryGetValue(databaseName, out var db))
                throw new DatabaseNotFoundException(databaseName);

            return db.Views.Select(v => v.ViewName).ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Checks if a name is a table (not a view).
    /// </summary>
    public bool IsTable(string databaseName, string name)
    {
        _lock.EnterReadLock();
        try
        {
            if (!_databases.TryGetValue(databaseName, out var db))
                return false;

            return db.HasTable(name);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Checks if a name is a view.
    /// </summary>
    public bool IsView(string databaseName, string name)
    {
        _lock.EnterReadLock();
        try
        {
            if (!_databases.TryGetValue(databaseName, out var db))
                return false;

            return db.HasView(name);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    #endregion

    /// <summary>
    /// Flushes all data to disk.
    /// </summary>
    public void Flush()
    {
        _lock.EnterWriteLock();
        try
        {
            // Flush all open tables
            foreach (var table in _openTables.Values)
            {
                table.Flush();
            }

            // Save catalog
            SaveCatalog();

            _logger.Debug("Flushed catalog and all tables");
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private DatabaseInfo CreateDatabaseInternal(string databaseName)
    {
        var dbId = _nextDatabaseId++;
        var dbDirectory = Path.Combine(_dataDirectory, databaseName);

        if (!Directory.Exists(dbDirectory))
        {
            Directory.CreateDirectory(dbDirectory);
        }

        var db = new DatabaseInfo(dbId, databaseName, dbDirectory);
        _databases[databaseName] = db;
        return db;
    }

    private string GetCatalogFilePath()
    {
        return Path.Combine(_dataDirectory, Constants.CatalogFileName);
    }

    private string GetTableKey(string databaseName, string tableName)
    {
        return $"{databaseName}.{tableName}";
    }

    private void LoadCatalog(string path)
    {
        var data = File.ReadAllBytes(path);
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        // Magic number
        var magic = reader.ReadUInt32();
        if (magic != 0x43594341) // "CYCA"
            throw new StorageException($"Invalid catalog file format");

        // Version
        var version = reader.ReadInt32();
        if (version != 1)
            throw new StorageException($"Unsupported catalog version: {version}");

        // Next database ID
        _nextDatabaseId = reader.ReadInt32();

        // Databases
        var dbCount = reader.ReadInt32();
        for (int i = 0; i < dbCount; i++)
        {
            var dbLength = reader.ReadInt32();
            var dbBytes = reader.ReadBytes(dbLength);
            var db = DatabaseInfo.Deserialize(dbBytes);
            _databases[db.Name] = db;
        }
    }

    private void SaveCatalog()
    {
        var path = GetCatalogFilePath();

        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        // Magic number "CYCA"
        writer.Write(0x43594341u);

        // Version
        writer.Write(1);

        // Next database ID
        writer.Write(_nextDatabaseId);

        // Databases
        writer.Write(_databases.Count);
        foreach (var db in _databases.Values)
        {
            var dbBytes = db.Serialize();
            writer.Write(dbBytes.Length);
            writer.Write(dbBytes);
        }

        // Write to file atomically
        var tempPath = path + ".tmp";
        File.WriteAllBytes(tempPath, stream.ToArray());
        File.Move(tempPath, path, overwrite: true);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        _lock.EnterWriteLock();
        try
        {
            // Close all open tables
            foreach (var table in _openTables.Values)
            {
                table.Dispose();
            }
            _openTables.Clear();

            // Flush buffer pool
            _bufferPool.Dispose();

            // Save catalog
            SaveCatalog();
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        _lock.Dispose();
        _logger.Info("Catalog disposed");
    }
}
