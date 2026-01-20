using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage.Index;

/// <summary>
/// B+ Tree index implementation for range queries and ordered access.
/// </summary>
public sealed class BTreeIndex : IDisposable
{
    private readonly IndexInfo _info;
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
    /// Gets whether this index is unique.
    /// </summary>
    public bool IsUnique => _info.IsUnique;

    /// <summary>
    /// Creates a new B-Tree index.
    /// </summary>
    public BTreeIndex(IndexInfo info, string filePath, int capacity = 15)
    {
        _info = info ?? throw new ArgumentNullException(nameof(info));
        _capacity = capacity;
        _pageManager = new PageManager(filePath);
        _logger = LogManager.Default.GetLogger<BTreeIndex>();
    }

    /// <summary>
    /// Opens the index file.
    /// </summary>
    public void Open(bool createIfNotExists = true)
    {
        _pageManager.Open(createIfNotExists);

        if (_pageManager.PageCount == 0)
        {
            // Create root page
            var rootPage = CreatePage(isLeaf: true);
            _rootPageId = rootPage.PageId;
            WritePage(rootPage);
            _logger.Debug("Created new B-Tree index with root page {0}", _rootPageId);
        }
        else
        {
            // Read root page ID from first page header
            var firstPage = _pageManager.ReadPage(0);
            _rootPageId = 0;
            _logger.Debug("Opened B-Tree index with root page {0}", _rootPageId);
        }
    }

    /// <summary>
    /// Inserts a key-RowId pair into the index.
    /// </summary>
    public bool Insert(DataValue[] keyValues, RowId rowId)
    {
        var key = IndexInfo.CreateCompositeKey(keyValues);

        lock (_lock)
        {
            // Check for duplicates if unique index
            if (IsUnique && Lookup(keyValues).Any())
            {
                throw new ConstraintViolationException($"Duplicate key violation on index '{_info.IndexName}'", _info.IndexName);
            }

            var result = InsertRecursive(_rootPageId, key, rowId);

            if (result.HasValue)
            {
                // Root was split, create new root
                var newRoot = CreatePage(isLeaf: false);
                newRoot.SetChildPageId(0, _rootPageId);
                newRoot.InsertKeyChild(result.Value.Key, result.Value.NewPageId);

                // Update child pages' parent pointers
                var oldRoot = ReadPage(_rootPageId);
                oldRoot.ParentPageId = newRoot.PageId;
                WritePage(oldRoot);

                var newChild = ReadPage(result.Value.NewPageId);
                newChild.ParentPageId = newRoot.PageId;
                WritePage(newChild);

                WritePage(newRoot);
                _rootPageId = newRoot.PageId;

                _logger.Debug("B-Tree root split, new root: {0}", _rootPageId);
            }

            return true;
        }
    }

    /// <summary>
    /// Deletes a key-RowId pair from the index.
    /// </summary>
    public bool Delete(DataValue[] keyValues, RowId rowId)
    {
        var key = IndexInfo.CreateCompositeKey(keyValues);

        lock (_lock)
        {
            return DeleteRecursive(_rootPageId, key, rowId);
        }
    }

    /// <summary>
    /// Updates an index entry (delete old, insert new).
    /// </summary>
    public bool Update(DataValue[] oldKeyValues, DataValue[] newKeyValues, RowId rowId)
    {
        lock (_lock)
        {
            if (!Delete(oldKeyValues, rowId))
                return false;
            return Insert(newKeyValues, rowId);
        }
    }

    /// <summary>
    /// Looks up RowIds for the given key values.
    /// </summary>
    public IEnumerable<RowId> Lookup(DataValue[] keyValues)
    {
        var key = IndexInfo.CreateCompositeKey(keyValues);

        lock (_lock)
        {
            var leaf = FindLeafPage(_rootPageId, key);
            var rowId = leaf.SearchKey(key);
            if (rowId.HasValue)
            {
                yield return rowId.Value;
            }
        }
    }

    /// <summary>
    /// Performs a range scan from startKey to endKey.
    /// </summary>
    public IEnumerable<RowId> RangeScan(DataValue[]? startKeyValues, DataValue[]? endKeyValues)
    {
        CompositeKey? startKey = startKeyValues != null ? IndexInfo.CreateCompositeKey(startKeyValues) : null;
        CompositeKey? endKey = endKeyValues != null ? IndexInfo.CreateCompositeKey(endKeyValues) : null;

        lock (_lock)
        {
            BTreePage currentLeaf;

            if (startKey.HasValue)
            {
                currentLeaf = FindLeafPage(_rootPageId, startKey.Value);
            }
            else
            {
                // Start from leftmost leaf
                currentLeaf = FindLeftmostLeaf(_rootPageId);
            }

            while (currentLeaf != null)
            {
                foreach (var (key, rowId) in currentLeaf.RangeScan(startKey, endKey))
                {
                    if (endKey.HasValue && key > endKey.Value)
                        yield break;

                    yield return rowId;
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
    public IEnumerable<RowId> ScanAll()
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

    private (CompositeKey Key, int NewPageId)? InsertRecursive(int pageId, CompositeKey key, RowId rowId)
    {
        var page = ReadPage(pageId);

        if (page.IsLeaf)
        {
            if (page.InsertKey(key, rowId))
            {
                WritePage(page);
                return null;
            }
            else
            {
                // Page is full, need to split
                var newPageId = _pageManager.PageCount;
                var (medianKey, newPage) = page.Split(newPageId);
                
                // Insert into appropriate page
                if (key < medianKey)
                {
                    page.InsertKey(key, rowId);
                }
                else
                {
                    newPage.InsertKey(key, rowId);
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
            var result = InsertRecursive(childPageId, key, rowId);

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

    private bool DeleteRecursive(int pageId, CompositeKey key, RowId rowId)
    {
        var page = ReadPage(pageId);

        if (page.IsLeaf)
        {
            if (page.DeleteKey(key))
            {
                WritePage(page);
                return true;
            }
            return false;
        }
        else
        {
            var childPageId = page.FindChildPageId(key);
            return DeleteRecursive(childPageId, key, rowId);
        }
    }

    private BTreePage FindLeafPage(int pageId, CompositeKey key)
    {
        var page = ReadPage(pageId);

        while (!page.IsLeaf)
        {
            var childPageId = page.FindChildPageId(key);
            page = ReadPage(childPageId);
        }

        return page;
    }

    private BTreePage FindLeftmostLeaf(int pageId)
    {
        var page = ReadPage(pageId);

        while (!page.IsLeaf)
        {
            var childPageId = page.GetChildPageId(0);
            page = ReadPage(childPageId);
        }

        return page;
    }

    private BTreePage CreatePage(bool isLeaf)
    {
        var rawPage = _pageManager.AllocatePage();
        return new BTreePage(rawPage.PageId, isLeaf, _capacity);
    }

    private BTreePage ReadPage(int pageId)
    {
        var rawPage = _pageManager.ReadPage(pageId);
        return new BTreePage(pageId, rawPage.GetData(), _capacity);
    }

    private void WritePage(BTreePage page)
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
