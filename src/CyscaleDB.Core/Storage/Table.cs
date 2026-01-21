using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Represents a table and provides operations for inserting, reading, and scanning rows.
/// </summary>
public sealed class Table : IDisposable
{
    private readonly TableSchema _schema;
    private readonly PageManager _pageManager;
    private readonly BufferPool? _bufferPool;
    private readonly Logger _logger;
    private bool _disposed;
    
    // Lazy columns added during online DDL - maps column ordinal to default value
    private readonly Dictionary<int, DataValue?> _lazyColumns = new();

    /// <summary>
    /// Gets the table schema.
    /// </summary>
    public TableSchema Schema => _schema;

    /// <summary>
    /// Gets the PageManager for this table.
    /// </summary>
    public PageManager PageManager => _pageManager;

    /// <summary>
    /// Creates a new table instance.
    /// </summary>
    /// <param name="schema">The table schema.</param>
    /// <param name="pageManager">The page manager for this table's data file.</param>
    /// <param name="bufferPool">Optional buffer pool for caching pages.</param>
    public Table(TableSchema schema, PageManager pageManager, BufferPool? bufferPool = null)
    {
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _pageManager = pageManager ?? throw new ArgumentNullException(nameof(pageManager));
        _bufferPool = bufferPool;
        _logger = LogManager.Default.GetLogger<Table>();
    }

    /// <summary>
    /// Gets the lazy columns that were added during online DDL.
    /// </summary>
    public IReadOnlyDictionary<int, DataValue?> LazyColumns => _lazyColumns;

    /// <summary>
    /// Sets a column as lazy (for online ADD COLUMN).
    /// Rows that were written before this column was added will return the default value.
    /// </summary>
    public void SetLazyColumn(int columnOrdinal, DataValue? defaultValue)
    {
        _lazyColumns[columnOrdinal] = defaultValue;
        _logger.Trace("Set lazy column at ordinal {0} with default value", columnOrdinal);
    }

    /// <summary>
    /// Clears a lazy column (after backfill is complete).
    /// </summary>
    public void ClearLazyColumn(int columnOrdinal)
    {
        _lazyColumns.Remove(columnOrdinal);
    }

    /// <summary>
    /// Checks if a column is lazy.
    /// </summary>
    public bool IsLazyColumn(int columnOrdinal)
    {
        return _lazyColumns.ContainsKey(columnOrdinal);
    }

    /// <summary>
    /// Gets the default value for a lazy column.
    /// </summary>
    public DataValue? GetLazyColumnDefault(int columnOrdinal)
    {
        return _lazyColumns.GetValueOrDefault(columnOrdinal);
    }

    /// <summary>
    /// Opens the table's data file.
    /// </summary>
    public void Open(bool createIfNotExists = true)
    {
        _pageManager.Open(createIfNotExists);
    }

    /// <summary>
    /// Inserts a row into the table.
    /// Returns the RowId where the row was inserted.
    /// </summary>
    public RowId InsertRow(Row row)
    {
        if (row == null)
            throw new ArgumentNullException(nameof(row));

        if (row.Schema != _schema)
            throw new ArgumentException("Row schema does not match table schema", nameof(row));

        // Validate row data
        _schema.ValidateRow(row.Values);

        // Handle auto-increment columns
        for (int i = 0; i < _schema.Columns.Count; i++)
        {
            var column = _schema.Columns[i];
            if (column.IsAutoIncrement && row.Values[i].IsNull)
            {
                var nextValue = _schema.GetNextAutoIncrementValue();
                row.Values[i] = DataValue.FromBigInt(nextValue);
            }
        }

        // Serialize row
        var rowData = row.Serialize();

        if (rowData.Length > Constants.MaxRowSize)
        {
            throw new StorageException($"Row size ({rowData.Length}) exceeds maximum ({Constants.MaxRowSize})");
        }

        // Find a page with enough space or allocate new one
        Page page;
        int pageId;
        
        if (_bufferPool != null)
        {
            (page, pageId) = FindPageWithSpaceBuffered(rowData.Length);
        }
        else
        {
            (page, pageId) = FindPageWithSpace(rowData.Length);
        }

        // Insert record
        var slotNumber = page.InsertRecord(rowData);
        if (slotNumber < 0)
        {
            throw new StorageException($"Failed to insert record into page {pageId}");
        }

        // Write page back
        if (_bufferPool != null)
        {
            _bufferPool.UnpinPage(_pageManager, pageId, isDirty: true);
        }
        else
        {
            _pageManager.WritePage(page);
        }

        // Update row count
        _schema.UpdateRowCount(1);

        var rowId = new RowId(pageId, (short)slotNumber);
        row.RowId = rowId;

        _logger.Trace("Inserted row into table {0} at {1}", _schema.FullName, rowId);

        return rowId;
    }

    /// <summary>
    /// Gets a row by its RowId.
    /// </summary>
    public Row? GetRowBySlot(RowId rowId)
    {
        if (!rowId.IsValid)
            return null;

        Page page;
        if (_bufferPool != null)
        {
            page = _bufferPool.GetPage(_pageManager, rowId.PageId);
        }
        else
        {
            page = _pageManager.ReadPage(rowId.PageId);
        }

        var recordData = page.GetRecord(rowId.SlotNumber);

        if (_bufferPool != null)
        {
            _bufferPool.UnpinPage(_pageManager, rowId.PageId);
        }

        if (recordData == null)
            return null;

        var row = Row.Deserialize(recordData, _schema);
        row.RowId = rowId;
        return row;
    }

    /// <summary>
    /// Scans all rows in the table.
    /// </summary>
    public IEnumerable<Row> ScanTable()
    {
        var pageCount = _pageManager.PageCount;

        for (int pageId = 0; pageId < pageCount; pageId++)
        {
            Page page;
            if (_bufferPool != null)
            {
                page = _bufferPool.GetPage(_pageManager, pageId);
            }
            else
            {
                page = _pageManager.ReadPage(pageId);
            }

            foreach (var (slotNumber, recordData) in page.EnumerateRecords())
            {
                var row = Row.Deserialize(recordData, _schema);
                row.RowId = new RowId(pageId, (short)slotNumber);
                yield return row;
            }

            if (_bufferPool != null)
            {
                _bufferPool.UnpinPage(_pageManager, pageId);
            }
        }
    }

    /// <summary>
    /// Updates a row at the given RowId.
    /// </summary>
    public bool UpdateRow(RowId rowId, Row newRow)
    {
        if (newRow == null)
            throw new ArgumentNullException(nameof(newRow));

        if (newRow.Schema != _schema)
            throw new ArgumentException("Row schema does not match table schema", nameof(newRow));

        if (!rowId.IsValid)
            return false;

        // Validate row data
        _schema.ValidateRow(newRow.Values);

        Page page;
        if (_bufferPool != null)
        {
            page = _bufferPool.GetPage(_pageManager, rowId.PageId);
        }
        else
        {
            page = _pageManager.ReadPage(rowId.PageId);
        }

        var newRowData = newRow.Serialize();

        if (page.UpdateRecord(rowId.SlotNumber, newRowData))
        {
            if (_bufferPool != null)
            {
                _bufferPool.UnpinPage(_pageManager, rowId.PageId, isDirty: true);
            }
            else
            {
                _pageManager.WritePage(page);
            }
            
            _logger.Trace("Updated row at {0} in table {1}", rowId, _schema.FullName);
            return true;
        }

        if (_bufferPool != null)
        {
            _bufferPool.UnpinPage(_pageManager, rowId.PageId);
        }

        return false;
    }

    /// <summary>
    /// Deletes a row by its RowId.
    /// </summary>
    public bool DeleteRow(RowId rowId)
    {
        if (!rowId.IsValid)
            return false;

        Page page;
        if (_bufferPool != null)
        {
            page = _bufferPool.GetPage(_pageManager, rowId.PageId);
        }
        else
        {
            page = _pageManager.ReadPage(rowId.PageId);
        }

        if (page.DeleteRecord(rowId.SlotNumber))
        {
            if (_bufferPool != null)
            {
                _bufferPool.UnpinPage(_pageManager, rowId.PageId, isDirty: true);
            }
            else
            {
                _pageManager.WritePage(page);
            }
            
            _schema.UpdateRowCount(-1);
            _logger.Trace("Deleted row at {0} from table {1}", rowId, _schema.FullName);
            return true;
        }

        if (_bufferPool != null)
        {
            _bufferPool.UnpinPage(_pageManager, rowId.PageId);
        }

        return false;
    }

    /// <summary>
    /// Finds a page with enough space for the given record size.
    /// If no page has enough space, allocates a new page.
    /// </summary>
    private (Page page, int pageId) FindPageWithSpace(int recordSize)
    {
        var pageCount = _pageManager.PageCount;

        // Try existing pages first
        for (int pageId = 0; pageId < pageCount; pageId++)
        {
            var page = _pageManager.ReadPage(pageId);
            if (page.CanFit(recordSize))
            {
                return (page, pageId);
            }
        }

        // No page has enough space, allocate new page
        var newPage = _pageManager.AllocatePage();
        return (newPage, newPage.PageId);
    }

    /// <summary>
    /// Finds a page with enough space using buffer pool.
    /// </summary>
    private (Page page, int pageId) FindPageWithSpaceBuffered(int recordSize)
    {
        var pageCount = _pageManager.PageCount;

        // Try existing pages first
        for (int pageId = 0; pageId < pageCount; pageId++)
        {
            var page = _bufferPool!.GetPage(_pageManager, pageId);
            if (page.CanFit(recordSize))
            {
                return (page, pageId);
            }
            _bufferPool.UnpinPage(_pageManager, pageId);
        }

        // No page has enough space, allocate new page
        var newPage = _bufferPool!.NewPage(_pageManager);
        return (newPage, newPage.PageId);
    }

    /// <summary>
    /// Flushes all dirty pages for this table to disk.
    /// </summary>
    public void Flush()
    {
        if (_bufferPool != null)
        {
            _bufferPool.FlushAll(_pageManager);
        }
        _pageManager.Flush();
    }

    #region Table Optimization

    /// <summary>
    /// Optimizes the table by compacting data and reclaiming space from deleted rows.
    /// Returns statistics about the optimization.
    /// </summary>
    public OptimizeResult Optimize()
    {
        _logger.Info("Starting optimization of table {0}", _schema.FullName);

        var startTime = DateTime.UtcNow;
        var originalPageCount = _pageManager.PageCount;
        long rowsProcessed = 0;
        long spaceReclaimed = 0;

        // Collect all valid rows
        var validRows = new List<(Row Row, byte[] Data)>();
        long originalDataSize = 0;

        for (int pageId = 0; pageId < _pageManager.PageCount; pageId++)
        {
            var page = _pageManager.ReadPage(pageId);
            originalDataSize += Constants.PageSize;

            foreach (var (slotNumber, recordData) in page.EnumerateRecords())
            {
                var row = Row.Deserialize(recordData, _schema);
                validRows.Add((row, recordData));
                rowsProcessed++;
            }
        }

        if (validRows.Count == 0)
        {
            // Table is empty, just truncate
            _pageManager.Truncate(0);
            spaceReclaimed = originalDataSize;
        }
        else
        {
            // Create a temporary file for the compacted data
            var tempFilePath = _pageManager.FilePath + ".tmp";
            var tempPageManager = new PageManager(tempFilePath);
            tempPageManager.Open(createIfNotExists: true);

            try
            {
                // Write rows to new file in a compact manner
                Page? currentPage = null;
                int currentPageId = -1;

                foreach (var (row, recordData) in validRows)
                {
                    // Find or create a page with space
                    if (currentPage == null || !currentPage.CanFit(recordData.Length))
                    {
                        if (currentPage != null)
                        {
                            tempPageManager.WritePage(currentPage);
                        }
                        currentPage = tempPageManager.AllocatePage();
                        currentPageId = currentPage.PageId;
                    }

                    var slotNumber = currentPage.InsertRecord(recordData);
                    if (slotNumber < 0)
                    {
                        // Page full, allocate new one
                        tempPageManager.WritePage(currentPage);
                        currentPage = tempPageManager.AllocatePage();
                        currentPageId = currentPage.PageId;
                        slotNumber = currentPage.InsertRecord(recordData);
                    }
                }

                // Write last page
                if (currentPage != null)
                {
                    tempPageManager.WritePage(currentPage);
                }

                tempPageManager.Flush();
                tempPageManager.Dispose();

                // Close original file and replace with temp file
                _pageManager.Close();

                var originalPath = _pageManager.FilePath;
                var backupPath = originalPath + ".bak";

                // Backup original file
                if (File.Exists(originalPath))
                {
                    File.Move(originalPath, backupPath, overwrite: true);
                }

                // Move temp to original
                File.Move(tempFilePath, originalPath);

                // Delete backup
                if (File.Exists(backupPath))
                {
                    File.Delete(backupPath);
                }

                // Reopen the page manager
                _pageManager.Open(createIfNotExists: false);

                var newPageCount = _pageManager.PageCount;
                spaceReclaimed = (originalPageCount - newPageCount) * Constants.PageSize;
            }
            catch
            {
                // Cleanup temp file on error
                tempPageManager.Dispose();
                if (File.Exists(tempFilePath))
                {
                    File.Delete(tempFilePath);
                }

                // Reopen original file
                _pageManager.Open(createIfNotExists: false);
                throw;
            }
        }

        var result = new OptimizeResult(
            rowsProcessed,
            originalPageCount,
            _pageManager.PageCount,
            spaceReclaimed,
            DateTime.UtcNow - startTime);

        _logger.Info("Optimization of table {0} complete: {1} rows, {2} pages -> {3} pages, {4} bytes reclaimed",
            _schema.FullName, rowsProcessed, originalPageCount, _pageManager.PageCount, spaceReclaimed);

        return result;
    }

    /// <summary>
    /// Compacts individual pages to reclaim space from deleted records.
    /// Less aggressive than full Optimize().
    /// </summary>
    public int CompactPages()
    {
        int compactedCount = 0;

        for (int pageId = 0; pageId < _pageManager.PageCount; pageId++)
        {
            var page = _pageManager.ReadPage(pageId);
            var originalFreeSpace = page.FreeSpace;

            page.Compact();

            if (page.FreeSpace > originalFreeSpace)
            {
                _pageManager.WritePage(page);
                compactedCount++;
            }
        }

        _logger.Debug("Compacted {0} pages in table {1}", compactedCount, _schema.FullName);
        return compactedCount;
    }

    /// <summary>
    /// Gets table statistics including fragmentation info.
    /// </summary>
    public TableStatistics GetStatistics()
    {
        long totalRows = 0;
        long totalDataSize = 0;
        long totalFreeSpace = 0;
        int emptyPages = 0;
        int fragmentedPages = 0;

        for (int pageId = 0; pageId < _pageManager.PageCount; pageId++)
        {
            var page = _pageManager.ReadPage(pageId);
            var recordCount = 0;
            var dataSize = 0;

            foreach (var (_, recordData) in page.EnumerateRecords())
            {
                recordCount++;
                dataSize += recordData.Length;
            }

            totalRows += recordCount;
            totalDataSize += dataSize;
            totalFreeSpace += page.FreeSpace;

            if (recordCount == 0)
            {
                emptyPages++;
            }
            else if (page.FreeSpace > Constants.PageSize / 2)
            {
                fragmentedPages++;
            }
        }

        var totalSpace = _pageManager.PageCount * Constants.PageSize;
        var fragmentation = totalSpace > 0 ? (double)totalFreeSpace / totalSpace * 100 : 0;

        return new TableStatistics(
            totalRows,
            _pageManager.PageCount,
            totalDataSize,
            totalFreeSpace,
            emptyPages,
            fragmentedPages,
            fragmentation);
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
/// Result of a table optimization operation.
/// </summary>
public sealed class OptimizeResult
{
    /// <summary>
    /// Number of rows processed during optimization.
    /// </summary>
    public long RowsProcessed { get; }

    /// <summary>
    /// Original number of pages before optimization.
    /// </summary>
    public int OriginalPageCount { get; }

    /// <summary>
    /// Number of pages after optimization.
    /// </summary>
    public int NewPageCount { get; }

    /// <summary>
    /// Bytes of space reclaimed.
    /// </summary>
    public long SpaceReclaimed { get; }

    /// <summary>
    /// Duration of the optimization.
    /// </summary>
    public TimeSpan Duration { get; }

    public OptimizeResult(
        long rowsProcessed,
        int originalPageCount,
        int newPageCount,
        long spaceReclaimed,
        TimeSpan duration)
    {
        RowsProcessed = rowsProcessed;
        OriginalPageCount = originalPageCount;
        NewPageCount = newPageCount;
        SpaceReclaimed = spaceReclaimed;
        Duration = duration;
    }

    public override string ToString() =>
        $"Optimize: {RowsProcessed} rows, {OriginalPageCount} -> {NewPageCount} pages, {SpaceReclaimed} bytes reclaimed in {Duration.TotalMilliseconds:F2}ms";
}

/// <summary>
/// Table storage statistics.
/// </summary>
public sealed class TableStatistics
{
    /// <summary>
    /// Total number of rows in the table.
    /// </summary>
    public long RowCount { get; }

    /// <summary>
    /// Total number of pages in the table.
    /// </summary>
    public int PageCount { get; }

    /// <summary>
    /// Total data size in bytes.
    /// </summary>
    public long DataSize { get; }

    /// <summary>
    /// Total free space in bytes.
    /// </summary>
    public long FreeSpace { get; }

    /// <summary>
    /// Number of empty pages.
    /// </summary>
    public int EmptyPages { get; }

    /// <summary>
    /// Number of fragmented pages (more than 50% free).
    /// </summary>
    public int FragmentedPages { get; }

    /// <summary>
    /// Fragmentation percentage.
    /// </summary>
    public double FragmentationPercent { get; }

    public TableStatistics(
        long rowCount,
        int pageCount,
        long dataSize,
        long freeSpace,
        int emptyPages,
        int fragmentedPages,
        double fragmentationPercent)
    {
        RowCount = rowCount;
        PageCount = pageCount;
        DataSize = dataSize;
        FreeSpace = freeSpace;
        EmptyPages = emptyPages;
        FragmentedPages = fragmentedPages;
        FragmentationPercent = fragmentationPercent;
    }

    public override string ToString() =>
        $"Stats: {RowCount} rows, {PageCount} pages, {DataSize} bytes data, {FragmentationPercent:F1}% fragmented";
}
