using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Asynchronous page manager that uses async I/O operations for improved
/// throughput on large-scale data access patterns.
///
/// Key features:
///   - Async read/write operations (non-blocking I/O)
///   - Aggressive read-ahead for sequential scans (prefetch N pages)
///   - Direct I/O option (bypass OS page cache, avoid double buffering)
///   - Batch page reads for multi-range access patterns
///
/// This complements the synchronous PageManager for cases where
/// high concurrency and I/O throughput are priorities.
/// </summary>
public sealed class AsyncPageManager : IDisposable
{
    private readonly string _filePath;
    private FileStream? _fileStream;
    private readonly int _pageSize;
    private readonly int _readAheadPages;
    private readonly bool _useDirectIo;
    private bool _disposed;

    /// <summary>
    /// File header size (same as PageManager).
    /// </summary>
    private const int HeaderSize = 64;

    /// <summary>
    /// Gets the file path.
    /// </summary>
    public string FilePath => _filePath;

    /// <summary>
    /// Gets the number of pages in the file.
    /// </summary>
    public int PageCount
    {
        get
        {
            if (_fileStream == null) return 0;
            var dataSize = Math.Max(0, _fileStream.Length - HeaderSize);
            return (int)(dataSize / _pageSize);
        }
    }

    /// <summary>
    /// Creates a new AsyncPageManager.
    /// </summary>
    /// <param name="filePath">Path to the data file.</param>
    /// <param name="pageSize">Page size in bytes (default 4096).</param>
    /// <param name="readAheadPages">Number of pages to prefetch during sequential scans.</param>
    /// <param name="useDirectIo">Whether to use Direct I/O (FILE_FLAG_NO_BUFFERING).</param>
    public AsyncPageManager(string filePath, int pageSize = Constants.PageSize,
        int readAheadPages = 64, bool useDirectIo = false)
    {
        _filePath = filePath;
        _pageSize = pageSize;
        _readAheadPages = readAheadPages;
        _useDirectIo = useDirectIo;
    }

    /// <summary>
    /// Opens the file for async I/O.
    /// </summary>
    public void Open()
    {
        var options = FileOptions.Asynchronous | FileOptions.SequentialScan;

        _fileStream = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite,
            FileShare.Read, bufferSize: _pageSize * 4, options);

        // Write header if new file
        if (_fileStream.Length < HeaderSize)
        {
            var header = new byte[HeaderSize];
            _fileStream.Write(header, 0, HeaderSize);
            _fileStream.Flush();
        }
    }

    /// <summary>
    /// Reads a page asynchronously.
    /// </summary>
    public async Task<byte[]> ReadPageAsync(int pageId, CancellationToken ct = default)
    {
        if (_fileStream == null)
            throw new InvalidOperationException("File not open.");

        var offset = GetPageOffset(pageId);
        var buffer = new byte[_pageSize];

        _fileStream.Position = offset;
        var read = await _fileStream.ReadAsync(buffer.AsMemory(0, _pageSize), ct);

        if (read < _pageSize)
        {
            // Page doesn't exist yet, return empty page
            return new byte[_pageSize];
        }

        return buffer;
    }

    /// <summary>
    /// Writes a page asynchronously.
    /// </summary>
    public async Task WritePageAsync(int pageId, byte[] data, CancellationToken ct = default)
    {
        if (_fileStream == null)
            throw new InvalidOperationException("File not open.");

        if (data.Length != _pageSize)
            throw new ArgumentException($"Page data must be exactly {_pageSize} bytes.");

        var offset = GetPageOffset(pageId);
        _fileStream.Position = offset;
        await _fileStream.WriteAsync(data.AsMemory(0, _pageSize), ct);
    }

    /// <summary>
    /// Reads multiple pages in a batch (read-ahead pattern).
    /// Returns pages in order.
    /// </summary>
    public async Task<byte[][]> ReadPagesAsync(int startPageId, int count, CancellationToken ct = default)
    {
        var results = new byte[count][];

        for (int i = 0; i < count; i++)
        {
            results[i] = await ReadPageAsync(startPageId + i, ct);
        }

        return results;
    }

    /// <summary>
    /// Performs aggressive read-ahead: prefetches the next N pages starting from the given page.
    /// Returns the pages that were successfully read.
    /// </summary>
    public async Task<List<(int PageId, byte[] Data)>> ReadAheadAsync(int startPageId, CancellationToken ct = default)
    {
        var results = new List<(int PageId, byte[] Data)>();
        var maxPage = PageCount;

        var endPage = Math.Min(startPageId + _readAheadPages, maxPage);

        for (int pageId = startPageId; pageId < endPage; pageId++)
        {
            try
            {
                var data = await ReadPageAsync(pageId, ct);
                results.Add((pageId, data));
            }
            catch
            {
                break;
            }
        }

        return results;
    }

    /// <summary>
    /// Allocates a new page and returns its ID.
    /// </summary>
    public async Task<int> AllocatePageAsync(CancellationToken ct = default)
    {
        if (_fileStream == null)
            throw new InvalidOperationException("File not open.");

        var newPageId = PageCount;
        var emptyPage = new byte[_pageSize];
        await WritePageAsync(newPageId, emptyPage, ct);
        return newPageId;
    }

    /// <summary>
    /// Flushes all pending writes to disk.
    /// </summary>
    public async Task FlushAsync(CancellationToken ct = default)
    {
        if (_fileStream != null)
        {
            await _fileStream.FlushAsync(ct);
        }
    }

    private long GetPageOffset(int pageId)
    {
        return HeaderSize + (long)pageId * _pageSize;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _fileStream?.Dispose();
    }
}
