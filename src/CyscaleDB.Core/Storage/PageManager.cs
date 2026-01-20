using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Manages page allocation and file I/O for a single table file.
/// Each table has its own data file (.cdb).
/// </summary>
public sealed class PageManager : IDisposable
{
    private readonly string _filePath;
    private FileStream? _fileStream;
    private readonly object _fileLock = new();
    private readonly Logger _logger;
    private int _pageCount;
    private bool _disposed;

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
                FileOptions.RandomAccess);

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

            page.IsDirty = false;
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
    }
}
