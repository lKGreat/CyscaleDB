using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage.Index;

/// <summary>
/// Manages full-text indexes for all tables in the database.
/// Provides operations to create, drop, and search full-text indexes.
/// </summary>
public sealed class FullTextIndexManager : IDisposable
{
    /// <summary>
    /// Key for identifying a full-text index: (DatabaseName, TableName, IndexName)
    /// </summary>
    private readonly record struct FullTextIndexKey(string DatabaseName, string TableName, string IndexName);

    /// <summary>
    /// Information about a full-text index including its column configuration.
    /// </summary>
    private sealed class FullTextIndexInfo
    {
        public FullTextIndex Index { get; }
        public List<string> Columns { get; }
        public List<int> ColumnOrdinals { get; set; } = new();

        public FullTextIndexInfo(FullTextIndex index, List<string> columns)
        {
            Index = index;
            Columns = columns;
        }
    }

    private readonly Dictionary<FullTextIndexKey, FullTextIndexInfo> _indexes;
    private readonly Dictionary<(string DbName, string TableName), List<FullTextIndexKey>> _tableIndexes;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Logger _logger;
    private readonly string _dataDirectory;
    private bool _disposed;

    /// <summary>
    /// Gets the singleton instance of FullTextIndexManager.
    /// </summary>
    public static FullTextIndexManager Instance { get; } = new();

    private FullTextIndexManager()
    {
        _indexes = new Dictionary<FullTextIndexKey, FullTextIndexInfo>();
        _tableIndexes = new Dictionary<(string, string), List<FullTextIndexKey>>();
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<FullTextIndexManager>();
        _dataDirectory = Path.Combine(Environment.CurrentDirectory, "data", "fulltext");
    }

    /// <summary>
    /// Creates a new full-text index on the specified columns of a table.
    /// </summary>
    public bool CreateIndex(
        string databaseName,
        string tableName,
        string indexName,
        IEnumerable<string> columns,
        TableSchema schema)
    {
        var key = new FullTextIndexKey(databaseName, tableName, indexName);
        var columnList = columns.ToList();

        _lock.EnterWriteLock();
        try
        {
            if (_indexes.ContainsKey(key))
            {
                _logger.Warning("Full-text index {0}.{1}.{2} already exists", databaseName, tableName, indexName);
                return false;
            }

            // Validate columns exist and are text types
            var ordinals = new List<int>();
            foreach (var colName in columnList)
            {
                var ordinal = schema.GetColumnOrdinal(colName);
                if (ordinal < 0)
                {
                    throw new ColumnNotFoundException(colName, tableName);
                }

                var colDef = schema.Columns[ordinal];
                if (!IsTextColumn(colDef.DataType))
                {
                    throw new InvalidOperationException(
                        $"Column '{colName}' is of type {colDef.DataType}, which cannot be used in a FULLTEXT index. " +
                        "Only CHAR, VARCHAR, and TEXT columns are allowed.");
                }

                ordinals.Add(ordinal);
            }

            var index = new FullTextIndex();
            var info = new FullTextIndexInfo(index, columnList)
            {
                ColumnOrdinals = ordinals
            };

            _indexes[key] = info;

            // Track by table
            var tableKey = (databaseName, tableName);
            if (!_tableIndexes.TryGetValue(tableKey, out var indexList))
            {
                indexList = new List<FullTextIndexKey>();
                _tableIndexes[tableKey] = indexList;
            }
            indexList.Add(key);

            _logger.Info("Created full-text index {0}.{1}.{2} on columns ({3})",
                databaseName, tableName, indexName, string.Join(", ", columnList));

            return true;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Drops a full-text index.
    /// </summary>
    public bool DropIndex(string databaseName, string tableName, string indexName)
    {
        var key = new FullTextIndexKey(databaseName, tableName, indexName);

        _lock.EnterWriteLock();
        try
        {
            if (!_indexes.TryGetValue(key, out var info))
            {
                return false;
            }

            info.Index.Dispose();
            _indexes.Remove(key);

            // Remove from table index list
            var tableKey = (databaseName, tableName);
            if (_tableIndexes.TryGetValue(tableKey, out var indexList))
            {
                indexList.Remove(key);
                if (indexList.Count == 0)
                {
                    _tableIndexes.Remove(tableKey);
                }
            }

            // Delete persisted index file
            var filePath = GetIndexFilePath(databaseName, tableName, indexName);
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            _logger.Info("Dropped full-text index {0}.{1}.{2}", databaseName, tableName, indexName);
            return true;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Gets a full-text index by name.
    /// </summary>
    public FullTextIndex? GetIndex(string databaseName, string tableName, string indexName)
    {
        var key = new FullTextIndexKey(databaseName, tableName, indexName);

        _lock.EnterReadLock();
        try
        {
            return _indexes.TryGetValue(key, out var info) ? info.Index : null;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Gets a full-text index that covers the specified columns.
    /// Returns the first matching index or null if none found.
    /// </summary>
    public FullTextIndex? GetIndexForColumns(string databaseName, string tableName, IEnumerable<string> columns)
    {
        var columnSet = new HashSet<string>(columns, StringComparer.OrdinalIgnoreCase);
        var tableKey = (databaseName, tableName);

        _lock.EnterReadLock();
        try
        {
            if (!_tableIndexes.TryGetValue(tableKey, out var indexKeys))
            {
                return null;
            }

            foreach (var key in indexKeys)
            {
                if (_indexes.TryGetValue(key, out var info))
                {
                    // Check if this index covers all requested columns
                    var indexColumns = new HashSet<string>(info.Columns, StringComparer.OrdinalIgnoreCase);
                    if (columnSet.IsSubsetOf(indexColumns))
                    {
                        return info.Index;
                    }
                }
            }

            return null;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Indexes a row that was inserted into the table.
    /// </summary>
    public void IndexRow(string databaseName, string tableName, int rowId, Row row)
    {
        var tableKey = (databaseName, tableName);

        _lock.EnterReadLock();
        try
        {
            if (!_tableIndexes.TryGetValue(tableKey, out var indexKeys))
            {
                return; // No full-text indexes on this table
            }

            foreach (var key in indexKeys)
            {
                if (_indexes.TryGetValue(key, out var info))
                {
                    var text = ExtractTextFromRow(row, info.ColumnOrdinals);
                    if (!string.IsNullOrEmpty(text))
                    {
                        info.Index.AddDocument(rowId, text);
                    }
                }
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Updates the index for a row that was modified.
    /// </summary>
    public void UpdateRow(string databaseName, string tableName, int rowId, Row newRow)
    {
        // For updates, we simply re-index the row (AddDocument handles removal of old data)
        IndexRow(databaseName, tableName, rowId, newRow);
    }

    /// <summary>
    /// Removes a row from all full-text indexes on the table.
    /// </summary>
    public void RemoveRow(string databaseName, string tableName, int rowId)
    {
        var tableKey = (databaseName, tableName);

        _lock.EnterReadLock();
        try
        {
            if (!_tableIndexes.TryGetValue(tableKey, out var indexKeys))
            {
                return;
            }

            foreach (var key in indexKeys)
            {
                if (_indexes.TryGetValue(key, out var info))
                {
                    info.Index.RemoveDocument(rowId);
                }
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Searches a full-text index by index name.
    /// </summary>
    public List<SearchResult> Search(
        string databaseName,
        string tableName,
        string indexName,
        string query,
        int maxResults = 100)
    {
        var index = GetIndex(databaseName, tableName, indexName);
        return index?.Search(query, maxResults) ?? new List<SearchResult>();
    }

    /// <summary>
    /// Searches using the first full-text index that covers the specified columns.
    /// </summary>
    public List<SearchResult> SearchByColumns(
        string databaseName,
        string tableName,
        IEnumerable<string> columns,
        string query,
        int maxResults = 100)
    {
        var index = GetIndexForColumns(databaseName, tableName, columns);
        return index?.Search(query, maxResults) ?? new List<SearchResult>();
    }

    /// <summary>
    /// Gets all full-text indexes for a table.
    /// </summary>
    public IReadOnlyList<string> GetTableIndexNames(string databaseName, string tableName)
    {
        var tableKey = (databaseName, tableName);

        _lock.EnterReadLock();
        try
        {
            if (_tableIndexes.TryGetValue(tableKey, out var indexKeys))
            {
                return indexKeys.Select(k => k.IndexName).ToList();
            }
            return Array.Empty<string>();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Checks if a full-text index exists.
    /// </summary>
    public bool IndexExists(string databaseName, string tableName, string indexName)
    {
        var key = new FullTextIndexKey(databaseName, tableName, indexName);

        _lock.EnterReadLock();
        try
        {
            return _indexes.ContainsKey(key);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Drops all full-text indexes for a table (called when table is dropped).
    /// </summary>
    public void DropTableIndexes(string databaseName, string tableName)
    {
        var tableKey = (databaseName, tableName);

        _lock.EnterWriteLock();
        try
        {
            if (!_tableIndexes.TryGetValue(tableKey, out var indexKeys))
            {
                return;
            }

            foreach (var key in indexKeys.ToList())
            {
                if (_indexes.TryGetValue(key, out var info))
                {
                    info.Index.Dispose();
                    _indexes.Remove(key);
                }
            }

            _tableIndexes.Remove(tableKey);

            _logger.Info("Dropped all full-text indexes for table {0}.{1}", databaseName, tableName);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private static string ExtractTextFromRow(Row row, List<int> columnOrdinals)
    {
        var parts = new List<string>();
        foreach (var ordinal in columnOrdinals)
        {
            var value = row.GetValue(ordinal);
            if (!value.IsNull)
            {
                var text = value.GetRawValue()?.ToString();
                if (!string.IsNullOrWhiteSpace(text))
                {
                    parts.Add(text);
                }
            }
        }
        return string.Join(" ", parts);
    }

    private static bool IsTextColumn(DataType type)
    {
        return type == DataType.VarChar ||
               type == DataType.Char ||
               type == DataType.Text ||
               type == DataType.Json;
    }

    private string GetIndexFilePath(string databaseName, string tableName, string indexName)
    {
        return Path.Combine(_dataDirectory, databaseName, $"{tableName}_{indexName}.fti");
    }

    private string GetMetadataFilePath(string databaseName, string tableName, string indexName)
    {
        return Path.Combine(_dataDirectory, databaseName, $"{tableName}_{indexName}.ftm");
    }

    /// <summary>
    /// Saves a specific full-text index to disk.
    /// </summary>
    public void SaveIndex(string databaseName, string tableName, string indexName)
    {
        var key = new FullTextIndexKey(databaseName, tableName, indexName);

        _lock.EnterReadLock();
        try
        {
            if (!_indexes.TryGetValue(key, out var info))
            {
                _logger.Warning("Full-text index {0}.{1}.{2} not found for saving", 
                    databaseName, tableName, indexName);
                return;
            }

            var indexPath = GetIndexFilePath(databaseName, tableName, indexName);
            var metadataPath = GetMetadataFilePath(databaseName, tableName, indexName);

            // Ensure directory exists
            var directory = Path.GetDirectoryName(indexPath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            // Save the index data
            info.Index.Save(indexPath);

            // Save metadata (column names and ordinals)
            using var metaStream = new FileStream(metadataPath, FileMode.Create, FileAccess.Write);
            using var writer = new BinaryWriter(metaStream);
            
            writer.Write((byte)1); // Version
            writer.Write(info.Columns.Count);
            foreach (var col in info.Columns)
            {
                writer.Write(col);
            }
            writer.Write(info.ColumnOrdinals.Count);
            foreach (var ordinal in info.ColumnOrdinals)
            {
                writer.Write(ordinal);
            }

            _logger.Info("Saved full-text index {0}.{1}.{2} to disk", databaseName, tableName, indexName);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Saves all full-text indexes to disk.
    /// </summary>
    public void SaveAllIndexes()
    {
        _lock.EnterReadLock();
        try
        {
            foreach (var key in _indexes.Keys)
            {
                SaveIndex(key.DatabaseName, key.TableName, key.IndexName);
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Loads a full-text index from disk.
    /// </summary>
    public bool LoadIndex(string databaseName, string tableName, string indexName)
    {
        var key = new FullTextIndexKey(databaseName, tableName, indexName);
        var indexPath = GetIndexFilePath(databaseName, tableName, indexName);
        var metadataPath = GetMetadataFilePath(databaseName, tableName, indexName);

        if (!File.Exists(indexPath) || !File.Exists(metadataPath))
        {
            _logger.Warning("Full-text index files not found for {0}.{1}.{2}", 
                databaseName, tableName, indexName);
            return false;
        }

        _lock.EnterWriteLock();
        try
        {
            // Remove existing if present
            if (_indexes.TryGetValue(key, out var existingInfo))
            {
                existingInfo.Index.Dispose();
                _indexes.Remove(key);
            }

            // Load metadata
            using var metaStream = new FileStream(metadataPath, FileMode.Open, FileAccess.Read);
            using var reader = new BinaryReader(metaStream);

            var version = reader.ReadByte();
            if (version != 1)
            {
                throw new InvalidDataException($"Unsupported metadata version: {version}");
            }

            var columnCount = reader.ReadInt32();
            var columns = new List<string>(columnCount);
            for (int i = 0; i < columnCount; i++)
            {
                columns.Add(reader.ReadString());
            }

            var ordinalCount = reader.ReadInt32();
            var ordinals = new List<int>(ordinalCount);
            for (int i = 0; i < ordinalCount; i++)
            {
                ordinals.Add(reader.ReadInt32());
            }

            // Load index
            var index = FullTextIndex.LoadFromFile(indexPath);
            var info = new FullTextIndexInfo(index, columns)
            {
                ColumnOrdinals = ordinals
            };

            _indexes[key] = info;

            // Track by table
            var tableKey = (databaseName, tableName);
            if (!_tableIndexes.TryGetValue(tableKey, out var indexList))
            {
                indexList = new List<FullTextIndexKey>();
                _tableIndexes[tableKey] = indexList;
            }
            if (!indexList.Contains(key))
            {
                indexList.Add(key);
            }

            _logger.Info("Loaded full-text index {0}.{1}.{2} from disk", databaseName, tableName, indexName);
            return true;
        }
        catch (Exception ex)
        {
            _logger.Error("Failed to load full-text index {0}.{1}.{2}: {3}", 
                databaseName, tableName, indexName, ex.Message);
            return false;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Loads all full-text indexes for a database from disk.
    /// </summary>
    public void LoadDatabaseIndexes(string databaseName)
    {
        var dbDir = Path.Combine(_dataDirectory, databaseName);
        if (!Directory.Exists(dbDir))
        {
            return;
        }

        // Find all .ftm (metadata) files
        foreach (var metaFile in Directory.GetFiles(dbDir, "*.ftm"))
        {
            var fileName = Path.GetFileNameWithoutExtension(metaFile);
            var parts = fileName.Split('_', 2);
            if (parts.Length == 2)
            {
                var tableName = parts[0];
                var indexName = parts[1];
                LoadIndex(databaseName, tableName, indexName);
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _lock.EnterWriteLock();
        try
        {
            foreach (var info in _indexes.Values)
            {
                info.Index.Dispose();
            }
            _indexes.Clear();
            _tableIndexes.Clear();
            _disposed = true;
        }
        finally
        {
            _lock.ExitWriteLock();
            _lock.Dispose();
        }
    }
}
