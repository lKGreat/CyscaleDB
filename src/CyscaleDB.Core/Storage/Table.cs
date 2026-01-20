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

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        
        Flush();
        _pageManager.Dispose();
    }
}
