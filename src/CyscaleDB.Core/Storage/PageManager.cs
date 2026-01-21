using System.Buffers;
using CyscaleDB.Core.Common;
using Microsoft.Win32.SafeHandles;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Manages page allocation and file I/O for a single table file.
/// Each table has its own data file (.cdb).
/// Supports both synchronous and asynchronous I/O operations with memory pooling.
/// </summary>
public sealed class PageManager : IDisposable
{
    private readonly string _filePath;
    private FileStream? _fileStream;
    private SafeFileHandle? _fileHandle;
    private readonly object _fileLock = new();
    private readonly SemaphoreSlim _asyncLock = new(1, 1);
    private readonly Logger _logger;
    private int _pageCount;
    private bool _disposed;

    // I/O Statistics
    private long _readCount;
    private long _writeCount;
    private long _asyncReadCount;
    private long _asyncWriteCount;

    /// <summary>
    /// File header size (first page is reserved for file metadata).
    /// </summary>
    private const int FileHeaderSize = Constants.PageSize;

    /// <summary>
    /// Gets the number of pages in the file.
    /// </summary>
    public int PageCount => _pageCount;

    /// <summary>
    /// Gets the file path.
    /// </summary>
    public string FilePath => _filePath;

    /// <summary>
    /// Gets the total number of synchronous reads.
    /// </summary>
    public long ReadCount => _readCount;

    /// <summary>
    /// Gets the total number of synchronous writes.
    /// </summary>
    public long WriteCount => _writeCount;

    /// <summary>
    /// Gets the total number of asynchronous reads.
    /// </summary>
    public long AsyncReadCount => _asyncReadCount;

    /// <summary>
    /// Gets the total number of asynchronous writes.
    /// </summary>
    public long AsyncWriteCount => _asyncWriteCount;

    /// <summary>
    /// Creates a new PageManager for the given file path.
    /// </summary>
    public PageManager(string filePath)
    {
        _filePath = filePath;
        _logger = LogManager.Default.GetLogger<PageManager>();
    }

    /// <summary>
    /// Opens or creates the data file.
    /// </summary>
    public void Open(bool createIfNotExists = true)
    {
        lock (_fileLock)
        {
            if (_fileStream != null)
                return;

            var exists = File.Exists(_filePath);
            
            if (!exists && !createIfNotExists)
                throw new StorageException($"Data file not found: {_filePath}", ErrorCode.FileNotFound);

            // Ensure directory exists
            var directory = Path.GetDirectoryName(_filePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            _fileStream = new FileStream(
                _filePath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None,
                Constants.PageSize,
                FileOptions.RandomAccess | FileOptions.Asynchronous);

            // Get SafeFileHandle for RandomAccess operations
            _fileHandle = _fileStream.SafeFileHandle;

            if (!exists || _fileStream.Length == 0)
            {
                // Initialize new file with header page
                InitializeFile();
            }
            else
            {
                // Read existing file header
                ReadFileHeader();
            }

            _logger.Debug("Opened data file: {0}, pages: {1}", _filePath, _pageCount);
        }
    }

    /// <summary>
    /// Closes the data file.
    /// </summary>
    public void Close()
    {
        lock (_fileLock)
        {
            if (_fileStream != null)
            {
                _fileStream.Flush();
                _fileStream.Dispose();
                _fileStream = null;
                _fileHandle = null;
            }
        }
    }

    /// <summary>
    /// Allocates a new page.
    /// </summary>
    public Page AllocatePage(PageType pageType = PageType.Data)
    {
        EnsureOpen();

        lock (_fileLock)
        {
            var pageId = _pageCount;
            _pageCount++;

            var page = new Page(pageId, pageType);

            // Write the page immediately to allocate disk space
            WritePage(page);

            // Update file header
            WriteFileHeader();

            _logger.Trace("Allocated page {0}", pageId);
            return page;
        }
    }

    /// <summary>
    /// Reads a page from disk.
    /// </summary>
    public Page ReadPage(int pageId)
    {
        EnsureOpen();

        if (pageId < 0 || pageId >= _pageCount)
            throw new ArgumentOutOfRangeException(nameof(pageId), $"Invalid page ID: {pageId}");

        lock (_fileLock)
        {
            var offset = GetPageOffset(pageId);
            var data = new byte[Constants.PageSize];

            _fileStream!.Seek(offset, SeekOrigin.Begin);
            _fileStream.ReadExactly(data, 0, Constants.PageSize);

            Interlocked.Increment(ref _readCount);

            var page = new Page(pageId, data);

            // Verify checksum
            if (!page.VerifyChecksum())
            {
                throw new PageCorruptedException(pageId);
            }

            return page;
        }
    }

    /// <summary>
    /// Reads a page from disk asynchronously using memory pooling.
    /// </summary>
    public async ValueTask<Page> ReadPageAsync(int pageId, CancellationToken cancellationToken = default)
    {
        EnsureOpen();

        if (pageId < 0 || pageId >= _pageCount)
            throw new ArgumentOutOfRangeException(nameof(pageId), $"Invalid page ID: {pageId}");

        var buffer = IoBufferPool.RentPage();
        try
        {
            await _asyncLock.WaitAsync(cancellationToken);
            try
            {
                var offset = GetPageOffset(pageId);

                // Use RandomAccess for efficient async I/O without seeking
                if (_fileHandle != null)
                {
                    await RandomAccess.ReadAsync(_fileHandle, buffer.AsMemory(0, Constants.PageSize), offset, cancellationToken);
                }
                else
                {
                    // Fallback to FileStream
                    _fileStream!.Seek(offset, SeekOrigin.Begin);
                    await _fileStream.ReadExactlyAsync(buffer.AsMemory(0, Constants.PageSize), cancellationToken);
                }

                Interlocked.Increment(ref _asyncReadCount);
            }
            finally
            {
                _asyncLock.Release();
            }

            // Copy buffer to page data (page owns its data)
            var data = new byte[Constants.PageSize];
            Buffer.BlockCopy(buffer, 0, data, 0, Constants.PageSize);

            var page = new Page(pageId, data);

            // Verify checksum
            if (!page.VerifyChecksum())
            {
                throw new PageCorruptedException(pageId);
            }

            return page;
        }
        finally
        {
            IoBufferPool.ReturnPage(buffer);
        }
    }

    /// <summary>
    /// Reads multiple pages asynchronously (batch read for read-ahead).
    /// </summary>
    public async ValueTask<Page[]> ReadPagesAsync(int startPageId, int count, CancellationToken cancellationToken = default)
    {
        EnsureOpen();

        if (startPageId < 0)
            throw new ArgumentOutOfRangeException(nameof(startPageId));

        // Limit count to available pages
        count = Math.Min(count, _pageCount - startPageId);
        if (count <= 0)
            return Array.Empty<Page>();

        var pages = new Page[count];
        var tasks = new ValueTask<Page>[count];

        for (int i = 0; i < count; i++)
        {
            tasks[i] = ReadPageAsync(startPageId + i, cancellationToken);
        }

        for (int i = 0; i < count; i++)
        {
            pages[i] = await tasks[i];
        }

        return pages;
    }

    /// <summary>
    /// Writes a page to disk.
    /// </summary>
    public void WritePage(Page page)
    {
        EnsureOpen();

        if (page.PageId < 0 || page.PageId >= _pageCount)
            throw new ArgumentOutOfRangeException(nameof(page), $"Invalid page ID: {page.PageId}");

        lock (_fileLock)
        {
            // Update checksum before writing
            page.UpdateChecksum();

            var offset = GetPageOffset(page.PageId);
            _fileStream!.Seek(offset, SeekOrigin.Begin);
            _fileStream.Write(page.GetData(), 0, Constants.PageSize);

            Interlocked.Increment(ref _writeCount);

            page.IsDirty = false;
        }
    }

    /// <summary>
    /// Writes a page to disk asynchronously.
    /// </summary>
    public async ValueTask WritePageAsync(Page page, CancellationToken cancellationToken = default)
    {
        EnsureOpen();

        if (page.PageId < 0 || page.PageId >= _pageCount)
            throw new ArgumentOutOfRangeException(nameof(page), $"Invalid page ID: {page.PageId}");

        // Update checksum before writing
        page.UpdateChecksum();

        var data = page.GetData();

        await _asyncLock.WaitAsync(cancellationToken);
        try
        {
            var offset = GetPageOffset(page.PageId);

            // Use RandomAccess for efficient async I/O
            if (_fileHandle != null)
            {
                await RandomAccess.WriteAsync(_fileHandle, data.AsMemory(), offset, cancellationToken);
            }
            else
            {
                // Fallback to FileStream
                _fileStream!.Seek(offset, SeekOrigin.Begin);
                await _fileStream.WriteAsync(data.AsMemory(), cancellationToken);
            }

            Interlocked.Increment(ref _asyncWriteCount);
        }
        finally
        {
            _asyncLock.Release();
        }

        page.IsDirty = false;
    }

    /// <summary>
    /// Writes multiple pages asynchronously (batch write for flush).
    /// </summary>
    public async ValueTask WritePagesAsync(IEnumerable<Page> pages, CancellationToken cancellationToken = default)
    {
        EnsureOpen();

        var pageList = pages.ToList();
        if (pageList.Count == 0)
            return;

        var tasks = new List<ValueTask>(pageList.Count);

        foreach (var page in pageList)
        {
            tasks.Add(WritePageAsync(page, cancellationToken));
        }

        foreach (var task in tasks)
        {
            await task;
        }
    }

    /// <summary>
    /// Flushes all pending writes to disk.
    /// </summary>
    public void Flush()
    {
        lock (_fileLock)
        {
            _fileStream?.Flush(true);
        }
    }

    /// <summary>
    /// Flushes all pending writes to disk asynchronously.
    /// </summary>
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        await _asyncLock.WaitAsync(cancellationToken);
        try
        {
            if (_fileStream != null)
            {
                await _fileStream.FlushAsync(cancellationToken);
            }
        }
        finally
        {
            _asyncLock.Release();
        }
    }

    /// <summary>
    /// Gets I/O statistics for this page manager.
    /// </summary>
    public PageManagerStats GetStats()
    {
        return new PageManagerStats
        {
            FilePath = _filePath,
            PageCount = _pageCount,
            ReadCount = _readCount,
            WriteCount = _writeCount,
            AsyncReadCount = _asyncReadCount,
            AsyncWriteCount = _asyncWriteCount,
            TotalReads = _readCount + _asyncReadCount,
            TotalWrites = _writeCount + _asyncWriteCount
        };
    }

    /// <summary>
    /// Truncates the file to the specified number of pages.
    /// </summary>
    public void Truncate(int newPageCount)
    {
        EnsureOpen();

        lock (_fileLock)
        {
            if (newPageCount < 0)
                throw new ArgumentOutOfRangeException(nameof(newPageCount));

            _pageCount = newPageCount;
            var newLength = FileHeaderSize + ((long)newPageCount * Constants.PageSize);
            _fileStream!.SetLength(newLength);
            WriteFileHeader();
            Flush();

            _logger.Debug("Truncated file {0} to {1} pages", _filePath, newPageCount);
        }
    }

    /// <summary>
    /// Gets the file offset for a page.
    /// </summary>
    private long GetPageOffset(int pageId)
    {
        // First page (ID 0) starts after file header
        return FileHeaderSize + ((long)pageId * Constants.PageSize);
    }

    /// <summary>
    /// Initializes a new data file.
    /// </summary>
    private void InitializeFile()
    {
        _pageCount = 0;
        WriteFileHeader();
        _logger.Debug("Initialized new data file: {0}", _filePath);
    }

    /// <summary>
    /// Reads the file header.
    /// </summary>
    private void ReadFileHeader()
    {
        var header = new byte[FileHeaderSize];
        _fileStream!.Seek(0, SeekOrigin.Begin);
        _fileStream.ReadExactly(header, 0, FileHeaderSize);

        // Magic number check
        var magic = BitConverter.ToUInt32(header, 0);
        if (magic != 0x43594442) // "CYDB"
            throw new StorageException($"Invalid data file format: {_filePath}");

        // Version
        var version = BitConverter.ToInt32(header, 4);
        if (version != 1)
            throw new StorageException($"Unsupported data file version: {version}");

        // Page count
        _pageCount = BitConverter.ToInt32(header, 8);
    }

    /// <summary>
    /// Writes the file header.
    /// </summary>
    private void WriteFileHeader()
    {
        var header = new byte[FileHeaderSize];

        // Magic number "CYDB"
        BitConverter.TryWriteBytes(header.AsSpan(0, 4), 0x43594442u);

        // Version
        BitConverter.TryWriteBytes(header.AsSpan(4, 4), 1);

        // Page count
        BitConverter.TryWriteBytes(header.AsSpan(8, 4), _pageCount);

        // Reserved space for future use (page size, flags, etc.)
        BitConverter.TryWriteBytes(header.AsSpan(12, 4), Constants.PageSize);

        _fileStream!.Seek(0, SeekOrigin.Begin);
        _fileStream.Write(header, 0, FileHeaderSize);
    }

    private void EnsureOpen()
    {
        if (_fileStream == null)
            throw new InvalidOperationException("Data file is not open");
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        Close();
        _asyncLock.Dispose();
    }
}

/// <summary>
/// Statistics for page manager I/O operations.
/// </summary>
public sealed class PageManagerStats
{
    /// <summary>
    /// Path to the data file.
    /// </summary>
    public string FilePath { get; init; } = "";

    /// <summary>
    /// Total number of pages in the file.
    /// </summary>
    public int PageCount { get; init; }

    /// <summary>
    /// Number of synchronous read operations.
    /// </summary>
    public long ReadCount { get; init; }

    /// <summary>
    /// Number of synchronous write operations.
    /// </summary>
    public long WriteCount { get; init; }

    /// <summary>
    /// Number of asynchronous read operations.
    /// </summary>
    public long AsyncReadCount { get; init; }

    /// <summary>
    /// Number of asynchronous write operations.
    /// </summary>
    public long AsyncWriteCount { get; init; }

    /// <summary>
    /// Total read operations (sync + async).
    /// </summary>
    public long TotalReads { get; init; }

    /// <summary>
    /// Total write operations (sync + async).
    /// </summary>
    public long TotalWrites { get; init; }

    public override string ToString()
    {
        return $"PageManager [{FilePath}]: {PageCount} pages, reads={TotalReads}, writes={TotalWrites}";
    }
}
