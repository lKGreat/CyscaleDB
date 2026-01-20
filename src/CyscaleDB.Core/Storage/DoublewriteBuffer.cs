using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// The Doublewrite Buffer prevents partial page writes from corrupting the database.
/// 
/// In traditional disk-based storage, a 16KB page write is not atomic. If a crash
/// occurs during a page flush, the page on disk could be partially written, leaving
/// the database in an inconsistent state.
/// 
/// The doublewrite buffer solves this by:
/// 1. First writing dirty pages to a contiguous area (the doublewrite buffer)
/// 2. Then writing the pages to their final location in the tablespace
/// 
/// During crash recovery, if a page in the tablespace is corrupted (partial write),
/// it can be restored from the doublewrite buffer copy.
/// </summary>
public sealed class DoublewriteBuffer : IDisposable
{
    /// <summary>
    /// Size of each page in bytes (16KB like InnoDB).
    /// </summary>
    public const int PageSize = Constants.PageSize;

    /// <summary>
    /// Number of pages that can be batched in the buffer.
    /// </summary>
    public const int BufferPageCount = 64;

    /// <summary>
    /// Total size of the doublewrite buffer.
    /// </summary>
    public const int BufferSize = PageSize * BufferPageCount;

    private readonly string _bufferFilePath;
    private FileStream? _fileStream;
    private readonly byte[] _memoryBuffer;
    private readonly Logger _logger;
    private readonly object _writeLock = new();
    private int _currentPosition;
    private bool _disposed;

    /// <summary>
    /// Tracks statistics about doublewrite operations.
    /// </summary>
    public DoublewriteStats Stats { get; } = new();

    /// <summary>
    /// Gets whether the doublewrite buffer is open and ready.
    /// </summary>
    public bool IsOpen => _fileStream != null && !_disposed;

    /// <summary>
    /// Creates a new doublewrite buffer.
    /// </summary>
    /// <param name="dataDirectory">Directory for the doublewrite buffer file.</param>
    public DoublewriteBuffer(string dataDirectory)
    {
        _bufferFilePath = Path.Combine(dataDirectory, Constants.DoublewriteFileName);
        _memoryBuffer = new byte[BufferSize];
        _logger = LogManager.Default.GetLogger<DoublewriteBuffer>();
    }

    /// <summary>
    /// Opens the doublewrite buffer, creating or recovering from existing file.
    /// </summary>
    public void Open()
    {
        var directory = Path.GetDirectoryName(_bufferFilePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        _fileStream = new FileStream(
            _bufferFilePath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None,
            bufferSize: PageSize,
            FileOptions.WriteThrough); // Ensure data reaches disk

        // Initialize the buffer file if new
        if (_fileStream.Length < BufferSize)
        {
            InitializeBufferFile();
        }

        _currentPosition = 0;
        _logger.Info("Doublewrite buffer opened: {0}", _bufferFilePath);
    }

    /// <summary>
    /// Initializes a new buffer file with zeros.
    /// </summary>
    private void InitializeBufferFile()
    {
        var zeros = new byte[BufferSize];
        _fileStream!.Write(zeros, 0, zeros.Length);
        _fileStream.Flush();
        _logger.Debug("Initialized doublewrite buffer file ({0} KB)", BufferSize / 1024);
    }

    /// <summary>
    /// Writes a page to its final destination using the doublewrite buffer for safety.
    /// </summary>
    /// <param name="page">The page to write.</param>
    /// <param name="destinationStream">The file stream for the final destination.</param>
    /// <param name="destinationOffset">The byte offset in the destination file.</param>
    public void WritePage(Page page, FileStream destinationStream, long destinationOffset)
    {
        if (!IsOpen)
            throw new InvalidOperationException("Doublewrite buffer is not open");

        var pageData = page.GetData();
        if (pageData.Length != PageSize)
            throw new ArgumentException($"Page size must be {PageSize} bytes");

        lock (_writeLock)
        {
            // Step 1: Write to doublewrite buffer
            WriteToBuffer(page.PageId, pageData);

            // Step 2: Write to final destination
            WriteToDestination(destinationStream, destinationOffset, pageData);

            Stats.TotalWrites++;
        }

        _logger.Debug("Page {0} written via doublewrite buffer", page.PageId);
    }

    /// <summary>
    /// Writes multiple pages to their destinations using the doublewrite buffer.
    /// More efficient for batch flushes.
    /// </summary>
    /// <param name="pages">The pages to write, with their destination info.</param>
    /// <param name="destinationStream">The common destination file stream.</param>
    public void WritePages(IReadOnlyList<(Page Page, long DestinationOffset)> pages, FileStream destinationStream)
    {
        if (!IsOpen)
            throw new InvalidOperationException("Doublewrite buffer is not open");

        if (pages.Count == 0)
            return;

        lock (_writeLock)
        {
            // Step 1: Write all pages to doublewrite buffer first
            foreach (var (page, _) in pages)
            {
                var pageData = page.GetData();
                if (pageData.Length != PageSize)
                    throw new ArgumentException($"Page size must be {PageSize} bytes");

                WriteToBuffer(page.PageId, pageData);
            }

            // Ensure doublewrite buffer is synced to disk
            _fileStream!.Flush();

            // Step 2: Write all pages to their final destinations
            foreach (var (page, offset) in pages)
            {
                WriteToDestination(destinationStream, offset, page.GetData());
            }

            Stats.TotalWrites += pages.Count;
            Stats.BatchWrites++;
        }

        _logger.Debug("Batch of {0} pages written via doublewrite buffer", pages.Count);
    }

    /// <summary>
    /// Writes page data to the in-memory buffer and doublewrite file.
    /// </summary>
    private void WriteToBuffer(int pageId, byte[] pageData)
    {
        // Circular buffer position
        var bufferOffset = (_currentPosition * PageSize) % BufferSize;

        // Copy to memory buffer
        Array.Copy(pageData, 0, _memoryBuffer, bufferOffset, PageSize);

        // Write page ID at end of buffer slot for identification
        var pageIdBytes = BitConverter.GetBytes(pageId);
        Array.Copy(pageIdBytes, 0, _memoryBuffer, bufferOffset + PageSize - 4, 4);

        // Write to file
        _fileStream!.Seek(bufferOffset, SeekOrigin.Begin);
        _fileStream.Write(_memoryBuffer, bufferOffset, PageSize);

        _currentPosition = (_currentPosition + 1) % BufferPageCount;
    }

    /// <summary>
    /// Writes page data to the final destination.
    /// </summary>
    private static void WriteToDestination(FileStream stream, long offset, byte[] pageData)
    {
        stream.Seek(offset, SeekOrigin.Begin);
        stream.Write(pageData, 0, pageData.Length);
        stream.Flush();
    }

    /// <summary>
    /// Recovers any partially written pages from the doublewrite buffer.
    /// Should be called during database startup.
    /// </summary>
    /// <param name="tablespaceStream">The tablespace file to check/recover.</param>
    /// <returns>Number of pages recovered.</returns>
    public int RecoverPages(FileStream tablespaceStream)
    {
        if (!IsOpen)
            throw new InvalidOperationException("Doublewrite buffer is not open");

        _logger.Info("Starting doublewrite buffer recovery check...");
        int recoveredCount = 0;

        // Read entire doublewrite buffer into memory
        _fileStream!.Seek(0, SeekOrigin.Begin);
        _fileStream.Read(_memoryBuffer, 0, BufferSize);

        // Check each slot in the buffer
        for (int slot = 0; slot < BufferPageCount; slot++)
        {
            var bufferOffset = slot * PageSize;
            var pageIdBytes = new byte[4];
            Array.Copy(_memoryBuffer, bufferOffset + PageSize - 4, pageIdBytes, 0, 4);
            var pageId = BitConverter.ToInt32(pageIdBytes, 0);

            // Skip empty slots (page ID = 0 usually means empty)
            if (pageId <= 0)
                continue;

            // Calculate page offset in tablespace
            var tablespaceOffset = (long)pageId * PageSize;
            if (tablespaceOffset >= tablespaceStream.Length)
                continue;

            // Read page from tablespace
            var tablespacePageData = new byte[PageSize];
            tablespaceStream.Seek(tablespaceOffset, SeekOrigin.Begin);
            tablespaceStream.Read(tablespacePageData, 0, PageSize);

            // Check if page is corrupted (simple checksum check)
            if (IsPageCorrupted(tablespacePageData, pageId))
            {
                // Restore from doublewrite buffer
                var bufferPageData = new byte[PageSize];
                Array.Copy(_memoryBuffer, bufferOffset, bufferPageData, 0, PageSize);

                // Verify buffer copy is valid
                if (!IsPageCorrupted(bufferPageData, pageId))
                {
                    tablespaceStream.Seek(tablespaceOffset, SeekOrigin.Begin);
                    tablespaceStream.Write(bufferPageData, 0, PageSize);
                    tablespaceStream.Flush();
                    recoveredCount++;
                    _logger.Warning("Recovered corrupted page {0} from doublewrite buffer", pageId);
                }
            }
        }

        Stats.RecoveredPages += recoveredCount;
        _logger.Info("Doublewrite recovery complete. {0} pages recovered.", recoveredCount);
        return recoveredCount;
    }

    /// <summary>
    /// Simple corruption check - in production, this would use checksums.
    /// </summary>
    private static bool IsPageCorrupted(byte[] pageData, int expectedPageId)
    {
        if (pageData.Length != PageSize)
            return true;

        // Check if page ID in header matches expected
        // Page structure: first 4 bytes are page ID
        var actualPageId = BitConverter.ToInt32(pageData, 0);

        // Check for obvious corruption patterns
        // 1. Page ID mismatch
        if (actualPageId != expectedPageId)
            return true;

        // 2. All zeros (unwritten page) - not necessarily corrupted
        bool allZeros = true;
        for (int i = 0; i < Math.Min(64, pageData.Length); i++)
        {
            if (pageData[i] != 0)
            {
                allZeros = false;
                break;
            }
        }

        // For a page that should have data, all zeros indicates corruption
        if (allZeros && expectedPageId > 0)
            return true;

        return false;
    }

    /// <summary>
    /// Clears the doublewrite buffer (for testing or maintenance).
    /// </summary>
    public void Clear()
    {
        if (!IsOpen)
            return;

        lock (_writeLock)
        {
            Array.Clear(_memoryBuffer, 0, _memoryBuffer.Length);
            _fileStream!.Seek(0, SeekOrigin.Begin);
            _fileStream.Write(_memoryBuffer, 0, BufferSize);
            _fileStream.Flush();
            _currentPosition = 0;
        }

        _logger.Debug("Doublewrite buffer cleared");
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        if (_fileStream != null)
        {
            _fileStream.Flush();
            _fileStream.Dispose();
            _fileStream = null;
        }

        _logger.Debug("Doublewrite buffer disposed");
    }
}

/// <summary>
/// Statistics about doublewrite buffer operations.
/// </summary>
public class DoublewriteStats
{
    /// <summary>
    /// Total number of page writes through the buffer.
    /// </summary>
    public long TotalWrites { get; set; }

    /// <summary>
    /// Number of batch write operations.
    /// </summary>
    public long BatchWrites { get; set; }

    /// <summary>
    /// Number of pages recovered during startup.
    /// </summary>
    public int RecoveredPages { get; set; }

    public override string ToString()
    {
        return $"Writes: {TotalWrites}, Batches: {BatchWrites}, Recovered: {RecoveredPages}";
    }
}
