using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage.Mvcc;

namespace CyscaleDB.Core.Storage.Index;

/// <summary>
/// Secondary index implementation for InnoDB-style non-primary indexes.
/// 
/// In a secondary index:
/// - Leaf nodes store the indexed column values + primary key values
/// - To get full row data, a "lookback" to the clustered index is required
/// - This is the same as MySQL InnoDB's secondary index structure
/// 
/// Structure:
/// - Internal nodes: [IndexKey1][ChildPtr1][IndexKey2][ChildPtr2]...
/// - Leaf nodes: [IndexKey1][PKValues1][IndexKey2][PKValues2]...
/// </summary>
public sealed class SecondaryIndex : IDisposable
{
    private readonly IndexInfo _info;
    private readonly IReadOnlyList<int> _primaryKeyOrdinals;
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
    /// Creates a new secondary index.
    /// </summary>
    /// <param name="info">Index metadata</param>
    /// <param name="primaryKeyOrdinals">Ordinals of primary key columns in the table schema</param>
    /// <param name="filePath">Path to the index file</param>
    /// <param name="capacity">Maximum keys per node</param>
    public SecondaryIndex(
        IndexInfo info,
        IReadOnlyList<int> primaryKeyOrdinals,
        string filePath,
        int capacity = 15)
    {
        _info = info ?? throw new ArgumentNullException(nameof(info));
        _primaryKeyOrdinals = primaryKeyOrdinals ?? throw new ArgumentNullException(nameof(primaryKeyOrdinals));
        _capacity = capacity;
        _pageManager = new PageManager(filePath);
        _logger = LogManager.Default.GetLogger<SecondaryIndex>();
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
            _logger.Debug("Created new secondary index with root page {0}", _rootPageId);
        }
        else
        {
            _rootPageId = 0;
            _logger.Debug("Opened secondary index with root page {0}", _rootPageId);
        }
    }

    /// <summary>
    /// Inserts an entry into the secondary index.
    /// </summary>
    /// <param name="row">The row containing the index key values and primary key values</param>
    /// <returns>True if inserted successfully</returns>
    public bool Insert(Row row)
    {
        var indexKeyValues = ExtractIndexKeyValues(row);
        var pkValues = ExtractPrimaryKeyValues(row);
        var compositeKey = CreateSecondaryKey(indexKeyValues, pkValues);

        lock (_lock)
        {
            // Check for duplicates if unique index (except primary key which is handled by clustered index)
            if (_info.IsUnique && !_info.IsPrimaryKey)
            {
                var existing = LookupByIndexKey(indexKeyValues);
                if (existing.Any())
                {
                    throw new ConstraintViolationException(
                        $"Duplicate entry for unique key '{_info.IndexName}'",
                        _info.IndexName);
                }
            }

            var result = InsertRecursive(_rootPageId, compositeKey, pkValues);

            if (result.HasValue)
            {
                // Root was split, create new root
                var newRoot = CreatePage(isLeaf: false);
                newRoot.SetChildPageId(0, _rootPageId);
                newRoot.InsertKeyChild(result.Value.Key, result.Value.NewPageId);
                WritePage(newRoot);
                _rootPageId = newRoot.PageId;
                _logger.Debug("Secondary index root split, new root: {0}", _rootPageId);
            }

            return true;
        }
    }

    /// <summary>
    /// Deletes an entry from the secondary index.
    /// </summary>
    /// <param name="row">The row containing the index key values and primary key values</param>
    /// <returns>True if deleted successfully</returns>
    public bool Delete(Row row)
    {
        var indexKeyValues = ExtractIndexKeyValues(row);
        var pkValues = ExtractPrimaryKeyValues(row);
        var compositeKey = CreateSecondaryKey(indexKeyValues, pkValues);

        lock (_lock)
        {
            return DeleteRecursive(_rootPageId, compositeKey);
        }
    }

    /// <summary>
    /// Updates an entry in the secondary index.
    /// </summary>
    /// <param name="oldRow">The old row</param>
    /// <param name="newRow">The new row</param>
    /// <returns>True if updated successfully</returns>
    public bool Update(Row oldRow, Row newRow)
    {
        var oldIndexKey = ExtractIndexKeyValues(oldRow);
        var newIndexKey = ExtractIndexKeyValues(newRow);

        // Check if index key changed
        var keyChanged = !KeysEqual(oldIndexKey, newIndexKey);

        if (keyChanged)
        {
            // Delete old entry and insert new
            if (!Delete(oldRow))
                return false;
            return Insert(newRow);
        }

        // Key unchanged, no update needed (PK changes are handled elsewhere)
        return true;
    }

    /// <summary>
    /// Looks up entries by index key values, returning primary key values.
    /// </summary>
    /// <param name="indexKeyValues">The index key values to look up</param>
    /// <returns>Primary key values of matching entries</returns>
    public IEnumerable<DataValue[]> LookupByIndexKey(DataValue[] indexKeyValues)
    {
        var indexKey = IndexInfo.CreateCompositeKey(indexKeyValues);

        lock (_lock)
        {
            var leaf = FindLeafPage(_rootPageId, indexKey);
            return leaf.LookupByIndexKey(indexKey);
        }
    }

    /// <summary>
    /// Performs a range scan returning primary key values.
    /// </summary>
    public IEnumerable<DataValue[]> RangeScan(DataValue[]? startKey, DataValue[]? endKey)
    {
        CompositeKey? startComposite = startKey != null ? IndexInfo.CreateCompositeKey(startKey) : null;
        CompositeKey? endComposite = endKey != null ? IndexInfo.CreateCompositeKey(endKey) : null;

        lock (_lock)
        {
            SecondaryIndexPage currentLeaf;

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
                foreach (var pkValues in currentLeaf.RangeScanPrimaryKeys(startComposite, endComposite))
                {
                    yield return pkValues;
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
    /// Scans all entries in the index.
    /// </summary>
    public IEnumerable<DataValue[]> ScanAll()
    {
        return RangeScan(null, null);
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

    private DataValue[] ExtractIndexKeyValues(Row row)
    {
        var values = new DataValue[_info.ColumnOrdinals.Count];
        for (int i = 0; i < _info.ColumnOrdinals.Count; i++)
        {
            values[i] = row.Values[_info.ColumnOrdinals[i]];
        }
        return values;
    }

    private DataValue[] ExtractPrimaryKeyValues(Row row)
    {
        var values = new DataValue[_primaryKeyOrdinals.Count];
        for (int i = 0; i < _primaryKeyOrdinals.Count; i++)
        {
            values[i] = row.Values[_primaryKeyOrdinals[i]];
        }
        return values;
    }

    /// <summary>
    /// Creates a composite key for the secondary index by combining index key and PK.
    /// This ensures uniqueness even for non-unique indexes.
    /// </summary>
    private static CompositeKey CreateSecondaryKey(DataValue[] indexKey, DataValue[] pkValues)
    {
        var combined = new DataValue[indexKey.Length + pkValues.Length];
        Array.Copy(indexKey, combined, indexKey.Length);
        Array.Copy(pkValues, 0, combined, indexKey.Length, pkValues.Length);
        return new CompositeKey(combined);
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

    private (CompositeKey Key, int NewPageId)? InsertRecursive(int pageId, CompositeKey key, DataValue[] pkValues)
    {
        var page = ReadPage(pageId);

        if (page.IsLeaf)
        {
            if (page.InsertEntry(key, pkValues))
            {
                WritePage(page);
                return null;
            }
            else
            {
                // Page is full, need to split
                var newPageId = _pageManager.PageCount;
                var (medianKey, newPage) = page.SplitWithEntries(newPageId);

                // Insert into appropriate page
                if (key < medianKey)
                {
                    page.InsertEntry(key, pkValues);
                }
                else
                {
                    newPage.InsertEntry(key, pkValues);
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
            var result = InsertRecursive(childPageId, key, pkValues);

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

    private bool DeleteRecursive(int pageId, CompositeKey key)
    {
        var page = ReadPage(pageId);

        if (page.IsLeaf)
        {
            if (page.DeleteEntry(key))
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

    private SecondaryIndexPage FindLeafPage(int pageId, CompositeKey key)
    {
        var page = ReadPage(pageId);

        while (!page.IsLeaf)
        {
            var childPageId = page.FindChildPageId(key);
            page = ReadPage(childPageId);
        }

        return page;
    }

    private SecondaryIndexPage FindLeftmostLeaf(int pageId)
    {
        var page = ReadPage(pageId);

        while (!page.IsLeaf)
        {
            var childPageId = page.GetChildPageId(0);
            page = ReadPage(childPageId);
        }

        return page;
    }

    private SecondaryIndexPage CreatePage(bool isLeaf)
    {
        var rawPage = _pageManager.AllocatePage();
        return new SecondaryIndexPage(rawPage.PageId, isLeaf, _capacity, _info.ColumnOrdinals.Count);
    }

    private SecondaryIndexPage ReadPage(int pageId)
    {
        var rawPage = _pageManager.ReadPage(pageId);
        return new SecondaryIndexPage(pageId, rawPage.GetData(), _capacity, _info.ColumnOrdinals.Count);
    }

    private void WritePage(SecondaryIndexPage page)
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
/// Page structure for secondary index.
/// Leaf nodes store composite key (index key + PK) and just the PK values for lookback.
/// </summary>
public sealed class SecondaryIndexPage
{
    private readonly int _pageId;
    private readonly bool _isLeaf;
    private readonly int _capacity;
    private readonly int _indexKeyLength;

    // Internal node: keys and child pointers
    private readonly List<CompositeKey> _keys = [];
    private readonly List<int> _childPageIds = [];

    // Leaf node: composite keys and primary key values
    private readonly List<(CompositeKey Key, DataValue[] PrimaryKeyValues)> _entries = [];

    // Leaf node links
    private int _nextLeafPageId = -1;
    private int _parentPageId = -1;

    public int PageId => _pageId;
    public bool IsLeaf => _isLeaf;
    public int NextLeafPageId => _nextLeafPageId;
    public int ParentPageId { get => _parentPageId; set => _parentPageId = value; }
    public int KeyCount => _isLeaf ? _entries.Count : _keys.Count;

    public SecondaryIndexPage(int pageId, bool isLeaf, int capacity, int indexKeyLength)
    {
        _pageId = pageId;
        _isLeaf = isLeaf;
        _capacity = capacity;
        _indexKeyLength = indexKeyLength;
    }

    public SecondaryIndexPage(int pageId, byte[] data, int capacity, int indexKeyLength)
    {
        _pageId = pageId;
        _capacity = capacity;
        _indexKeyLength = indexKeyLength;

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
                var pkCount = reader.ReadInt32();
                var pkValues = new DataValue[pkCount];
                for (int j = 0; j < pkCount; j++)
                {
                    var len = reader.ReadInt32();
                    var bytes = reader.ReadBytes(len);
                    pkValues[j] = DataValue.Deserialize(bytes);
                }
                _entries.Add((key, pkValues));
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
            foreach (var (key, pkValues) in _entries)
            {
                key.Serialize(writer);
                writer.Write(pkValues.Length);
                foreach (var pk in pkValues)
                {
                    var bytes = pk.Serialize();
                    writer.Write(bytes.Length);
                    writer.Write(bytes);
                }
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

    public bool InsertEntry(CompositeKey key, DataValue[] pkValues)
    {
        if (_entries.Count >= _capacity)
            return false;

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

        _entries.Insert(insertPos, (key, pkValues));
        return true;
    }

    public bool DeleteEntry(CompositeKey key)
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

    /// <summary>
    /// Looks up entries by the index key portion only (not including PK).
    /// </summary>
    public IEnumerable<DataValue[]> LookupByIndexKey(CompositeKey indexKey)
    {
        foreach (var (key, pkValues) in _entries)
        {
            // Compare only the index key portion (first _indexKeyLength values)
            if (CompareIndexKeyPortion(key, indexKey) == 0)
            {
                yield return pkValues;
            }
        }
    }

    private int CompareIndexKeyPortion(CompositeKey fullKey, CompositeKey indexKey)
    {
        for (int i = 0; i < indexKey.Length && i < _indexKeyLength; i++)
        {
            var cmp = fullKey.Values[i].CompareTo(indexKey.Values[i]);
            if (cmp != 0)
                return cmp;
        }
        return 0;
    }

    public IEnumerable<DataValue[]> RangeScanPrimaryKeys(CompositeKey? startKey, CompositeKey? endKey)
    {
        foreach (var (key, pkValues) in _entries)
        {
            if (startKey.HasValue && key < startKey.Value)
                continue;
            if (endKey.HasValue && key > endKey.Value)
                yield break;

            yield return pkValues;
        }
    }

    public (CompositeKey MedianKey, SecondaryIndexPage NewPage) SplitWithEntries(int newPageId)
    {
        var newPage = new SecondaryIndexPage(newPageId, isLeaf: true, _capacity, _indexKeyLength);

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

    public (CompositeKey MedianKey, SecondaryIndexPage NewPage) Split(int newPageId)
    {
        var newPage = new SecondaryIndexPage(newPageId, isLeaf: false, _capacity, _indexKeyLength);

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
