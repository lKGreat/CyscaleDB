using System.Collections.Concurrent;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage.Index;

/// <summary>
/// Manages indexes for tables in the database.
/// </summary>
public sealed class IndexManager : IDisposable
{
    private readonly string _dataDirectory;
    private readonly ConcurrentDictionary<string, List<IndexInfo>> _tableIndexes;
    private readonly ConcurrentDictionary<string, object> _openIndexes; // Can be BTreeIndex or HashIndex
    private readonly Logger _logger;
    private int _nextIndexId;
    private bool _disposed;

    /// <summary>
    /// Creates a new IndexManager.
    /// </summary>
    public IndexManager(string dataDirectory)
    {
        _dataDirectory = dataDirectory ?? throw new ArgumentNullException(nameof(dataDirectory));
        _tableIndexes = new ConcurrentDictionary<string, List<IndexInfo>>(StringComparer.OrdinalIgnoreCase);
        _openIndexes = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        _logger = LogManager.Default.GetLogger<IndexManager>();
        _nextIndexId = 1;
    }

    /// <summary>
    /// Creates a new index.
    /// </summary>
    public IndexInfo CreateIndex(
        string indexName,
        string tableName,
        string databaseName,
        IndexType type,
        IEnumerable<string> columns,
        bool isUnique = false,
        bool isPrimaryKey = false,
        TableSchema? schema = null)
    {
        var tableKey = GetTableKey(databaseName, tableName);

        // Check if index already exists
        if (_tableIndexes.TryGetValue(tableKey, out var indexes))
        {
            if (indexes.Any(i => i.IndexName.Equals(indexName, StringComparison.OrdinalIgnoreCase)))
            {
                throw new IndexExistsException(indexName);
            }
        }

        var indexId = Interlocked.Increment(ref _nextIndexId);
        var info = new IndexInfo(indexId, indexName, tableName, databaseName, type, columns, isUnique, isPrimaryKey);

        // Resolve column ordinals if schema provided
        if (schema != null)
        {
            info.ResolveColumnOrdinals(schema);
        }

        // Set file path
        var dbDirectory = Path.Combine(_dataDirectory, databaseName);
        var indexFile = Path.Combine(dbDirectory, $"{tableName}_{indexName}{info.GetFileExtension()}");
        info.SetFilePath(indexFile);

        // Add to table indexes
        _tableIndexes.AddOrUpdate(
            tableKey,
            _ => [info],
            (_, list) => { list.Add(info); return list; });

        _logger.Info("Created index: {0}", info);

        return info;
    }

    /// <summary>
    /// Drops an index.
    /// </summary>
    public bool DropIndex(string databaseName, string tableName, string indexName)
    {
        var tableKey = GetTableKey(databaseName, tableName);
        var indexKey = GetIndexKey(databaseName, tableName, indexName);

        // Close and remove open index
        if (_openIndexes.TryRemove(indexKey, out var openIndex))
        {
            if (openIndex is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }

        // Remove from table indexes
        if (_tableIndexes.TryGetValue(tableKey, out var indexes))
        {
            var index = indexes.FirstOrDefault(i => i.IndexName.Equals(indexName, StringComparison.OrdinalIgnoreCase));
            if (index != null)
            {
                indexes.Remove(index);

                // Delete index file
                if (File.Exists(index.FilePath))
                {
                    File.Delete(index.FilePath);
                }

                _logger.Info("Dropped index: {0}", indexName);
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Gets an index by name.
    /// </summary>
    public IndexInfo? GetIndex(string databaseName, string tableName, string indexName)
    {
        var tableKey = GetTableKey(databaseName, tableName);

        if (_tableIndexes.TryGetValue(tableKey, out var indexes))
        {
            return indexes.FirstOrDefault(i => i.IndexName.Equals(indexName, StringComparison.OrdinalIgnoreCase));
        }

        return null;
    }

    /// <summary>
    /// Gets all indexes for a table.
    /// </summary>
    public IReadOnlyList<IndexInfo> GetTableIndexes(string databaseName, string tableName)
    {
        var tableKey = GetTableKey(databaseName, tableName);

        if (_tableIndexes.TryGetValue(tableKey, out var indexes))
        {
            return indexes.AsReadOnly();
        }

        return [];
    }

    /// <summary>
    /// Opens a B-Tree index for operations.
    /// </summary>
    public BTreeIndex OpenBTreeIndex(IndexInfo info)
    {
        if (info.Type != IndexType.BTree)
            throw new ArgumentException("Index is not a B-Tree index", nameof(info));

        var indexKey = GetIndexKey(info.DatabaseName, info.TableName, info.IndexName);

        if (_openIndexes.TryGetValue(indexKey, out var existingIndex))
        {
            return (BTreeIndex)existingIndex;
        }

        var index = new BTreeIndex(info, info.FilePath);
        index.Open(createIfNotExists: true);

        _openIndexes[indexKey] = index;
        return index;
    }

    /// <summary>
    /// Opens a Hash index for operations.
    /// </summary>
    public HashIndex OpenHashIndex(IndexInfo info)
    {
        if (info.Type != IndexType.Hash)
            throw new ArgumentException("Index is not a Hash index", nameof(info));

        var indexKey = GetIndexKey(info.DatabaseName, info.TableName, info.IndexName);

        if (_openIndexes.TryGetValue(indexKey, out var existingIndex))
        {
            return (HashIndex)existingIndex;
        }

        var index = new HashIndex(info, info.FilePath);
        index.Open(createIfNotExists: true);

        _openIndexes[indexKey] = index;
        return index;
    }

    /// <summary>
    /// Inserts a key into all indexes for a table.
    /// </summary>
    public void InsertKey(string databaseName, string tableName, Row row)
    {
        var tableKey = GetTableKey(databaseName, tableName);

        if (!_tableIndexes.TryGetValue(tableKey, out var indexes))
            return;

        foreach (var info in indexes)
        {
            var keyValues = info.ExtractKeyValues(row);

            if (info.Type == IndexType.BTree)
            {
                var index = OpenBTreeIndex(info);
                index.Insert(keyValues, row.RowId);
            }
            else if (info.Type == IndexType.Hash)
            {
                var index = OpenHashIndex(info);
                index.Insert(keyValues, row.RowId);
            }
        }
    }

    /// <summary>
    /// Deletes a key from all indexes for a table.
    /// </summary>
    public void DeleteKey(string databaseName, string tableName, Row row)
    {
        var tableKey = GetTableKey(databaseName, tableName);

        if (!_tableIndexes.TryGetValue(tableKey, out var indexes))
            return;

        foreach (var info in indexes)
        {
            var keyValues = info.ExtractKeyValues(row);

            if (info.Type == IndexType.BTree)
            {
                var index = OpenBTreeIndex(info);
                index.Delete(keyValues, row.RowId);
            }
            else if (info.Type == IndexType.Hash)
            {
                var index = OpenHashIndex(info);
                index.Delete(keyValues, row.RowId);
            }
        }
    }

    /// <summary>
    /// Updates a key in all indexes for a table.
    /// </summary>
    public void UpdateKey(string databaseName, string tableName, Row oldRow, Row newRow)
    {
        var tableKey = GetTableKey(databaseName, tableName);

        if (!_tableIndexes.TryGetValue(tableKey, out var indexes))
            return;

        foreach (var info in indexes)
        {
            var oldKeyValues = info.ExtractKeyValues(oldRow);
            var newKeyValues = info.ExtractKeyValues(newRow);

            if (info.Type == IndexType.BTree)
            {
                var index = OpenBTreeIndex(info);
                index.Update(oldKeyValues, newKeyValues, newRow.RowId);
            }
            else if (info.Type == IndexType.Hash)
            {
                var index = OpenHashIndex(info);
                index.Update(oldKeyValues, newKeyValues, newRow.RowId);
            }
        }
    }

    /// <summary>
    /// Looks up RowIds using an index.
    /// </summary>
    public IEnumerable<RowId> Lookup(IndexInfo info, DataValue[] keyValues)
    {
        if (info.Type == IndexType.BTree)
        {
            var index = OpenBTreeIndex(info);
            return index.Lookup(keyValues);
        }
        else if (info.Type == IndexType.Hash)
        {
            var index = OpenHashIndex(info);
            return index.Lookup(keyValues);
        }

        return [];
    }

    /// <summary>
    /// Performs a range scan using a B-Tree index.
    /// </summary>
    public IEnumerable<RowId> RangeScan(IndexInfo info, DataValue[]? startKeyValues, DataValue[]? endKeyValues)
    {
        if (info.Type != IndexType.BTree)
            throw new ArgumentException("Range scan requires a B-Tree index", nameof(info));

        var index = OpenBTreeIndex(info);
        return index.RangeScan(startKeyValues, endKeyValues);
    }

    /// <summary>
    /// Builds index from existing table data.
    /// </summary>
    public void BuildIndex(IndexInfo info, IEnumerable<Row> rows)
    {
        if (info.Type == IndexType.BTree)
        {
            var index = OpenBTreeIndex(info);
            foreach (var row in rows)
            {
                var keyValues = info.ExtractKeyValues(row);
                index.Insert(keyValues, row.RowId);
            }
            index.Flush();
        }
        else if (info.Type == IndexType.Hash)
        {
            var index = OpenHashIndex(info);
            foreach (var row in rows)
            {
                var keyValues = info.ExtractKeyValues(row);
                index.Insert(keyValues, row.RowId);
            }
            index.Flush();
        }

        _logger.Info("Built index {0} on {1}.{2}", info.IndexName, info.DatabaseName, info.TableName);
    }

    /// <summary>
    /// Flushes all open indexes.
    /// </summary>
    public void Flush()
    {
        foreach (var index in _openIndexes.Values)
        {
            if (index is BTreeIndex btree)
                btree.Flush();
            else if (index is HashIndex hash)
                hash.Flush();
        }
    }

    private static string GetTableKey(string databaseName, string tableName)
    {
        return $"{databaseName}.{tableName}";
    }

    private static string GetIndexKey(string databaseName, string tableName, string indexName)
    {
        return $"{databaseName}.{tableName}.{indexName}";
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        foreach (var index in _openIndexes.Values)
        {
            if (index is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }

        _openIndexes.Clear();
        _tableIndexes.Clear();
    }
}
