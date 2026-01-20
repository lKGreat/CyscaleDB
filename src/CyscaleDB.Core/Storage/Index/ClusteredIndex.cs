using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage.Mvcc;

namespace CyscaleDB.Core.Storage.Index;

/// <summary>
/// Clustered index implementation for InnoDB-style primary key storage.
/// 
/// In a clustered index:
/// - The leaf nodes store complete row data, not just RowIds
/// - The index IS the table data (no separate heap)
/// - Primary key lookups are very efficient (single index traversal)
/// - Secondary indexes must store primary key values for lookback
/// 
/// Structure:
/// - Internal nodes: [Key1][ChildPtr1][Key2][ChildPtr2]...
/// - Leaf nodes: [Key1][RowData1][Key2][RowData2]...
/// </summary>
public sealed class ClusteredIndex : IDisposable
{
    private readonly IndexInfo _info;
    private readonly TableSchema _schema;
    private readonly PageManager _pageManager;
    private readonly int _capacity;
    private readonly object _lock = new();
    private readonly Logger _logger;
    private int _rootPageId;
    private bool _disposed;

    /// <summary>
    /// Gets the index metadata.
    /// </summary>
    public IndexInfo Info => _info;

    /// <summary>
    /// Gets the table schema.
    /// </summary>
    public TableSchema Schema => _schema;

    /// <summary>
    /// Creates a new clustered index.
    /// </summary>
    /// <param name="info">Index metadata</param>
    /// <param name="schema">Table schema</param>
    /// <param name="filePath">Path to the index file</param>
    /// <param name="capacity">Maximum keys per node</param>
    public ClusteredIndex(IndexInfo info, TableSchema schema, string filePath, int capacity = 15)
    {
        _info = info ?? throw new ArgumentNullException(nameof(info));
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _capacity = capacity;
        _pageManager = new PageManager(filePath);
        _logger = LogManager.Default.GetLogger<ClusteredIndex>();
    }

    /// <summary>
    /// Opens the index file.
    /// </summary>
    public void Open(bool createIfNotExists = true)
    {
        _pageManager.Open(createIfNotExists);

        if (_pageManager.PageCount == 0)
        {
            // Create root page (leaf node)
            var rootPage = CreatePage(isLeaf: true);
            _rootPageId = rootPage.PageId;
            WritePage(rootPage);
            _logger.Debug("Created new clustered index with root page {0}", _rootPageId);
        }
        else
        {
            _rootPageId = 0;
            _logger.Debug("Opened clustered index with root page {0}", _rootPageId);
        }
    }

    /// <summary>
    /// Inserts a row into the clustered index.
    /// </summary>
    /// <param name="row">The row to insert</param>
    /// <returns>True if inserted successfully</returns>
    public bool Insert(Row row)
    {
        var key = GetPrimaryKeyValues(row);
        var compositeKey = IndexInfo.CreateCompositeKey(key);

        lock (_lock)
        {
            // Check for duplicates (primary key must be unique)
            var existing = Lookup(key);
            if (existing != null)
            {
                throw new ConstraintViolationException(
                    $"Duplicate entry for primary key '{FormatKey(key)}'",
                    _info.IndexName);
            }

            var result = InsertRecursive(_rootPageId, compositeKey, row);

            if (result.HasValue)
            {
                // Root was split, create new root
                var newRoot = CreatePage(isLeaf: false);
                newRoot.SetChildPageId(0, _rootPageId);
                newRoot.InsertKeyChild(result.Value.Key, result.Value.NewPageId);
                WritePage(newRoot);
                _rootPageId = newRoot.PageId;
                _logger.Debug("Clustered index root split, new root: {0}", _rootPageId);
            }

            return true;
        }
    }

    /// <summary>
    /// Updates a row in the clustered index.
    /// </summary>
    /// <param name="oldRow">The old row (to find by primary key)</param>
    /// <param name="newRow">The new row data</param>
    /// <returns>True if updated successfully</returns>
    public bool Update(Row oldRow, Row newRow)
    {
        var oldKey = GetPrimaryKeyValues(oldRow);
        var newKey = GetPrimaryKeyValues(newRow);

        // Check if primary key changed
        var pkChanged = !KeysEqual(oldKey, newKey);

        lock (_lock)
        {
            if (pkChanged)
            {
                // If PK changed, delete old entry and insert new
                if (!Delete(oldKey))
                    return false;
                return Insert(newRow);
            }
            else
            {
                // Just update the row data in place
                return UpdateInPlace(IndexInfo.CreateCompositeKey(oldKey), newRow);
            }
        }
    }

    /// <summary>
    /// Deletes a row from the clustered index by primary key.
    /// </summary>
    /// <param name="primaryKeyValues">Primary key values</param>
    /// <returns>True if deleted successfully</returns>
    public bool Delete(DataValue[] primaryKeyValues)
    {
        var key = IndexInfo.CreateCompositeKey(primaryKeyValues);

        lock (_lock)
        {
            return DeleteRecursive(_rootPageId, key);
        }
    }

    /// <summary>
    /// Looks up a row by primary key.
    /// </summary>
    /// <param name="primaryKeyValues">Primary key values</param>
    /// <returns>The row, or null if not found</returns>
    public Row? Lookup(DataValue[] primaryKeyValues)
    {
        var key = IndexInfo.CreateCompositeKey(primaryKeyValues);

        lock (_lock)
        {
            var leaf = FindLeafPage(_rootPageId, key);
            return leaf.SearchKeyGetRow(key, _schema);
        }
    }

    /// <summary>
    /// Looks up a row by primary key with MVCC visibility check.
    /// </summary>
    /// <param name="primaryKeyValues">Primary key values</param>
    /// <param name="readView">ReadView for visibility check</param>
    /// <param name="versionChainManager">Version chain manager for history traversal</param>
    /// <returns>The visible row, or null if not visible</returns>
    public Row? Lookup(DataValue[] primaryKeyValues, ReadView readView, VersionChainManager? versionChainManager = null)
    {
        var row = Lookup(primaryKeyValues);
        if (row == null)
            return null;

        versionChainManager ??= new VersionChainManager();
        return versionChainManager.FindVisibleVersion(row, readView);
    }

    /// <summary>
    /// Performs a range scan returning all rows with keys in the specified range.
    /// </summary>
    public IEnumerable<Row> RangeScan(DataValue[]? startKey, DataValue[]? endKey)
    {
        CompositeKey? startComposite = startKey != null ? IndexInfo.CreateCompositeKey(startKey) : null;
        CompositeKey? endComposite = endKey != null ? IndexInfo.CreateCompositeKey(endKey) : null;

        lock (_lock)
        {
            ClusteredIndexPage currentLeaf;

            if (startComposite.HasValue)
            {
                currentLeaf = FindLeafPage(_rootPageId, startComposite.Value);
            }
            else
            {
                currentLeaf = FindLeftmostLeaf(_rootPageId);
            }

            while (currentLeaf != null)
            {
                foreach (var row in currentLeaf.RangeScanRows(startComposite, endComposite, _schema))
                {
                    if (endComposite.HasValue)
                    {
                        var rowKey = IndexInfo.CreateCompositeKey(GetPrimaryKeyValues(row));
                        if (rowKey > endComposite.Value)
                            yield break;
                    }
                    yield return row;
                }

                // Move to next leaf
                if (currentLeaf.NextLeafPageId >= 0)
                {
                    currentLeaf = ReadPage(currentLeaf.NextLeafPageId);
                }
                else
                {
                    break;
                }
            }
        }
    }

    /// <summary>
    /// Scans all rows in the clustered index.
    /// </summary>
    public IEnumerable<Row> ScanAll()
    {
        return RangeScan(null, null);
    }

    /// <summary>
    /// Scans all rows with MVCC visibility filtering.
    /// </summary>
    public IEnumerable<Row> ScanAll(ReadView readView, VersionChainManager? versionChainManager = null)
    {
        versionChainManager ??= new VersionChainManager();

        foreach (var row in ScanAll())
        {
            var visibleRow = versionChainManager.FindVisibleVersion(row, readView);
            if (visibleRow != null)
            {
                yield return visibleRow;
            }
        }
    }

    /// <summary>
    /// Flushes all data to disk.
    /// </summary>
    public void Flush()
    {
        lock (_lock)
        {
            _pageManager.Flush();
        }
    }

    #region Private Methods

    private DataValue[] GetPrimaryKeyValues(Row row)
    {
        var pkColumns = _schema.PrimaryKeyColumns;
        var values = new DataValue[pkColumns.Count];
        for (int i = 0; i < pkColumns.Count; i++)
        {
            values[i] = row.GetValue(pkColumns[i].Name);
        }
        return values;
    }

    private static bool KeysEqual(DataValue[] key1, DataValue[] key2)
    {
        if (key1.Length != key2.Length)
            return false;
        for (int i = 0; i < key1.Length; i++)
        {
            if (!key1[i].Equals(key2[i]))
                return false;
        }
        return true;
    }

    private static string FormatKey(DataValue[] key)
    {
        return string.Join(", ", key.Select(k => k.ToString()));
    }

    private (CompositeKey Key, int NewPageId)? InsertRecursive(int pageId, CompositeKey key, Row row)
    {
        var page = ReadPage(pageId);

        if (page.IsLeaf)
        {
            if (page.InsertRow(key, row))
            {
                WritePage(page);
                return null;
            }
            else
            {
                // Page is full, need to split
                var newPageId = _pageManager.PageCount;
                var (medianKey, newPage) = page.SplitWithRows(newPageId, _schema);

                // Insert into appropriate page
                if (key < medianKey)
                {
                    page.InsertRow(key, row);
                }
                else
                {
                    newPage.InsertRow(key, row);
                }

                WritePage(page);
                WritePage(newPage);

                return (medianKey, newPageId);
            }
        }
        else
        {
            // Internal node - find child and recurse
            var childPageId = page.FindChildPageId(key);
            var result = InsertRecursive(childPageId, key, row);

            if (result.HasValue)
            {
                // Child was split, insert separator key
                if (page.InsertKeyChild(result.Value.Key, result.Value.NewPageId))
                {
                    WritePage(page);
                    return null;
                }
                else
                {
                    // Internal node is full, split it too
                    var newPageId = _pageManager.PageCount;
                    var (medianKey, newPage) = page.Split(newPageId);

                    if (result.Value.Key < medianKey)
                    {
                        page.InsertKeyChild(result.Value.Key, result.Value.NewPageId);
                    }
                    else
                    {
                        newPage.InsertKeyChild(result.Value.Key, result.Value.NewPageId);
                    }

                    WritePage(page);
                    WritePage(newPage);

                    return (medianKey, newPageId);
                }
            }

            return null;
        }
    }

    private bool UpdateInPlace(CompositeKey key, Row newRow)
    {
        var leaf = FindLeafPage(_rootPageId, key);
        if (leaf.UpdateRow(key, newRow))
        {
            WritePage(leaf);
            return true;
        }
        return false;
    }

    private bool DeleteRecursive(int pageId, CompositeKey key)
    {
        var page = ReadPage(pageId);

        if (page.IsLeaf)
        {
            if (page.DeleteRow(key))
            {
                WritePage(page);
                return true;
            }
            return false;
        }
        else
        {
            var childPageId = page.FindChildPageId(key);
            return DeleteRecursive(childPageId, key);
        }
    }

    private ClusteredIndexPage FindLeafPage(int pageId, CompositeKey key)
    {
        var page = ReadPage(pageId);

        while (!page.IsLeaf)
        {
            var childPageId = page.FindChildPageId(key);
            page = ReadPage(childPageId);
        }

        return page;
    }

    private ClusteredIndexPage FindLeftmostLeaf(int pageId)
    {
        var page = ReadPage(pageId);

        while (!page.IsLeaf)
        {
            var childPageId = page.GetChildPageId(0);
            page = ReadPage(childPageId);
        }

        return page;
    }

    private ClusteredIndexPage CreatePage(bool isLeaf)
    {
        var rawPage = _pageManager.AllocatePage();
        return new ClusteredIndexPage(rawPage.PageId, isLeaf, _capacity);
    }

    private ClusteredIndexPage ReadPage(int pageId)
    {
        var rawPage = _pageManager.ReadPage(pageId);
        return new ClusteredIndexPage(pageId, rawPage.GetData(), _capacity);
    }

    private void WritePage(ClusteredIndexPage page)
    {
        var rawPage = new Page(page.PageId, page.GetData());
        _pageManager.WritePage(rawPage);
    }

    #endregion

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        Flush();
        _pageManager.Dispose();
    }
}

/// <summary>
/// Page structure for clustered index.
/// Leaf nodes store complete row data, internal nodes store keys and child pointers.
/// </summary>
public sealed class ClusteredIndexPage
{
    private readonly int _pageId;
    private readonly bool _isLeaf;
    private readonly int _capacity;

    // Internal node: keys and child pointers
    private readonly List<CompositeKey> _keys = [];
    private readonly List<int> _childPageIds = [];

    // Leaf node: keys and row data
    private readonly List<(CompositeKey Key, byte[] RowData)> _entries = [];

    // Leaf node links
    private int _nextLeafPageId = -1;
    private int _parentPageId = -1;

    public int PageId => _pageId;
    public bool IsLeaf => _isLeaf;
    public int NextLeafPageId => _nextLeafPageId;
    public int ParentPageId { get => _parentPageId; set => _parentPageId = value; }
    public int KeyCount => _isLeaf ? _entries.Count : _keys.Count;

    public ClusteredIndexPage(int pageId, bool isLeaf, int capacity)
    {
        _pageId = pageId;
        _isLeaf = isLeaf;
        _capacity = capacity;
    }

    public ClusteredIndexPage(int pageId, byte[] data, int capacity)
    {
        _pageId = pageId;
        _capacity = capacity;

        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        _isLeaf = reader.ReadBoolean();
        _parentPageId = reader.ReadInt32();
        _nextLeafPageId = reader.ReadInt32();

        int count = reader.ReadInt32();

        if (_isLeaf)
        {
            for (int i = 0; i < count; i++)
            {
                var key = CompositeKey.Deserialize(reader);
                var rowDataLen = reader.ReadInt32();
                var rowData = reader.ReadBytes(rowDataLen);
                _entries.Add((key, rowData));
            }
        }
        else
        {
            int childCount = reader.ReadInt32();
            for (int i = 0; i < childCount; i++)
            {
                _childPageIds.Add(reader.ReadInt32());
            }
            for (int i = 0; i < count; i++)
            {
                _keys.Add(CompositeKey.Deserialize(reader));
            }
        }
    }

    public byte[] GetData()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(_isLeaf);
        writer.Write(_parentPageId);
        writer.Write(_nextLeafPageId);

        if (_isLeaf)
        {
            writer.Write(_entries.Count);
            foreach (var (key, rowData) in _entries)
            {
                key.Serialize(writer);
                writer.Write(rowData.Length);
                writer.Write(rowData);
            }
        }
        else
        {
            writer.Write(_keys.Count);
            writer.Write(_childPageIds.Count);
            foreach (var childId in _childPageIds)
            {
                writer.Write(childId);
            }
            foreach (var key in _keys)
            {
                key.Serialize(writer);
            }
        }

        return stream.ToArray();
    }

    #region Leaf Node Operations

    public bool InsertRow(CompositeKey key, Row row)
    {
        if (_entries.Count >= _capacity)
            return false;

        var rowData = row.Serialize();
        int insertPos = 0;

        for (int i = 0; i < _entries.Count; i++)
        {
            if (key < _entries[i].Key)
            {
                insertPos = i;
                break;
            }
            insertPos = i + 1;
        }

        _entries.Insert(insertPos, (key, rowData));
        return true;
    }

    public bool UpdateRow(CompositeKey key, Row newRow)
    {
        for (int i = 0; i < _entries.Count; i++)
        {
            if (_entries[i].Key == key)
            {
                _entries[i] = (key, newRow.Serialize());
                return true;
            }
        }
        return false;
    }

    public bool DeleteRow(CompositeKey key)
    {
        for (int i = 0; i < _entries.Count; i++)
        {
            if (_entries[i].Key == key)
            {
                _entries.RemoveAt(i);
                return true;
            }
        }
        return false;
    }

    public Row? SearchKeyGetRow(CompositeKey key, TableSchema schema)
    {
        foreach (var (k, rowData) in _entries)
        {
            if (k == key)
            {
                return Row.Deserialize(rowData, schema);
            }
        }
        return null;
    }

    public IEnumerable<Row> RangeScanRows(CompositeKey? startKey, CompositeKey? endKey, TableSchema schema)
    {
        foreach (var (key, rowData) in _entries)
        {
            if (startKey.HasValue && key < startKey.Value)
                continue;
            if (endKey.HasValue && key > endKey.Value)
                yield break;

            yield return Row.Deserialize(rowData, schema);
        }
    }

    public (CompositeKey MedianKey, ClusteredIndexPage NewPage) SplitWithRows(int newPageId, TableSchema schema)
    {
        var newPage = new ClusteredIndexPage(newPageId, isLeaf: true, _capacity);

        int midIndex = _entries.Count / 2;
        var medianKey = _entries[midIndex].Key;

        // Move half entries to new page
        for (int i = midIndex; i < _entries.Count; i++)
        {
            newPage._entries.Add(_entries[i]);
        }

        // Remove moved entries from this page
        _entries.RemoveRange(midIndex, _entries.Count - midIndex);

        // Update leaf chain
        newPage._nextLeafPageId = _nextLeafPageId;
        _nextLeafPageId = newPageId;

        return (medianKey, newPage);
    }

    #endregion

    #region Internal Node Operations

    public int FindChildPageId(CompositeKey key)
    {
        for (int i = 0; i < _keys.Count; i++)
        {
            if (key < _keys[i])
                return _childPageIds[i];
        }
        return _childPageIds[^1];
    }

    public int GetChildPageId(int index)
    {
        return _childPageIds[index];
    }

    public void SetChildPageId(int index, int pageId)
    {
        while (_childPageIds.Count <= index)
        {
            _childPageIds.Add(-1);
        }
        _childPageIds[index] = pageId;
    }

    public bool InsertKeyChild(CompositeKey key, int childPageId)
    {
        if (_keys.Count >= _capacity)
            return false;

        int insertPos = 0;
        for (int i = 0; i < _keys.Count; i++)
        {
            if (key < _keys[i])
            {
                insertPos = i;
                break;
            }
            insertPos = i + 1;
        }

        _keys.Insert(insertPos, key);
        _childPageIds.Insert(insertPos + 1, childPageId);
        return true;
    }

    public (CompositeKey MedianKey, ClusteredIndexPage NewPage) Split(int newPageId)
    {
        var newPage = new ClusteredIndexPage(newPageId, isLeaf: false, _capacity);

        int midIndex = _keys.Count / 2;
        var medianKey = _keys[midIndex];

        // Move keys after median to new page
        for (int i = midIndex + 1; i < _keys.Count; i++)
        {
            newPage._keys.Add(_keys[i]);
        }

        // Move corresponding child pointers
        for (int i = midIndex + 1; i < _childPageIds.Count; i++)
        {
            newPage._childPageIds.Add(_childPageIds[i]);
        }

        // Remove moved entries
        _keys.RemoveRange(midIndex, _keys.Count - midIndex);
        _childPageIds.RemoveRange(midIndex + 1, _childPageIds.Count - midIndex - 1);

        return (medianKey, newPage);
    }

    #endregion
}
