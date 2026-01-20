using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Linear read-ahead (prefetch) implementation to improve sequential scan performance.
/// Detects sequential access patterns and proactively loads pages that are likely to be needed.
/// 
/// How it works:
/// 1. Tracks recent page accesses to detect sequential patterns
/// 2. When sequential access is detected, triggers background prefetch
/// 3. Prefetched pages are loaded into the buffer pool asynchronously
/// 4. Configurable threshold and window size for tuning
/// </summary>
public sealed class ReadAhead : IDisposable
{
    private readonly BufferPool? _bufferPool;
    private readonly Dictionary<string, AccessHistory> _accessHistory;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Logger _logger;
    private bool _disposed;
    private bool _enabled = true;

    /// <summary>
    /// Number of sequential page accesses required to trigger read-ahead.
    /// </summary>
    public int SequentialThreshold { get; set; } = 4;

    /// <summary>
    /// Number of pages to prefetch ahead of the current position.
    /// </summary>
    public int PrefetchWindow { get; set; } = 8;

    /// <summary>
    /// Maximum number of pages to prefetch per request.
    /// </summary>
    public int MaxPrefetchPages { get; set; } = 16;

    /// <summary>
    /// Gets or sets whether read-ahead is enabled.
    /// </summary>
    public bool Enabled
    {
        get => _enabled;
        set => _enabled = value;
    }

    // Statistics
    private long _prefetchRequests;
    private long _prefetchedPages;
    private long _sequentialDetections;

    /// <summary>
    /// Gets the total number of prefetch requests.
    /// </summary>
    public long PrefetchRequests => _prefetchRequests;

    /// <summary>
    /// Gets the total number of pages prefetched.
    /// </summary>
    public long PrefetchedPages => _prefetchedPages;

    /// <summary>
    /// Gets the number of sequential access pattern detections.
    /// </summary>
    public long SequentialDetections => _sequentialDetections;

    /// <summary>
    /// Creates a new read-ahead manager.
    /// </summary>
    /// <param name="bufferPool">Optional buffer pool for caching prefetched pages.</param>
    public ReadAhead(BufferPool? bufferPool = null)
    {
        _bufferPool = bufferPool;
        _accessHistory = new Dictionary<string, AccessHistory>();
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<ReadAhead>();
    }

    /// <summary>
    /// Records a page access and triggers prefetch if sequential pattern is detected.
    /// </summary>
    /// <param name="filePath">The path to the data file.</param>
    /// <param name="pageId">The page ID that was accessed.</param>
    /// <param name="pageManager">The page manager for loading pages.</param>
    /// <returns>True if prefetch was triggered, false otherwise.</returns>
    public bool RecordAccess(string filePath, int pageId, PageManager? pageManager = null)
    {
        if (!_enabled)
            return false;

        _lock.EnterWriteLock();
        try
        {
            if (!_accessHistory.TryGetValue(filePath, out var history))
            {
                history = new AccessHistory();
                _accessHistory[filePath] = history;
            }

            // Check if this access is sequential
            bool isSequential = history.LastPageId >= 0 && pageId == history.LastPageId + 1;

            if (isSequential)
            {
                history.SequentialCount++;
            }
            else
            {
                // Reset sequential count on non-sequential access
                history.SequentialCount = 1;
                history.Direction = pageId > history.LastPageId ? 1 : -1;
            }

            history.LastPageId = pageId;
            history.LastAccessTime = DateTime.UtcNow;

            // Check if we should trigger prefetch
            if (history.SequentialCount >= SequentialThreshold)
            {
                Interlocked.Increment(ref _sequentialDetections);

                // Trigger prefetch in background if we have a page manager
                if (pageManager != null && _bufferPool != null)
                {
                    var prefetchStart = pageId + 1;
                    var prefetchCount = Math.Min(PrefetchWindow, MaxPrefetchPages);

                    // Fire and forget - don't wait for prefetch to complete
                    _ = Task.Run(() => PrefetchPagesAsync(filePath, prefetchStart, prefetchCount, pageManager));

                    return true;
                }
            }

            return false;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Asynchronously prefetches pages into the buffer pool.
    /// </summary>
    private async Task PrefetchPagesAsync(string filePath, int startPageId, int count, PageManager pageManager)
    {
        Interlocked.Increment(ref _prefetchRequests);
        int prefetched = 0;

        try
        {
            for (int i = 0; i < count; i++)
            {
                var pageId = startPageId + i;

                // Check if page exists (don't prefetch past end of file)
                if (pageId >= pageManager.PageCount)
                    break;

                try
                {
                    // Load page into buffer pool
                    var page = _bufferPool!.GetPage(pageManager, pageId);
                    _bufferPool.UnpinPage(pageManager, pageId);
                    prefetched++;
                }
                catch
                {
                    // Ignore errors during prefetch - it's just an optimization
                    break;
                }

                // Small delay to avoid overwhelming I/O
                await Task.Delay(1);
            }

            if (prefetched > 0)
            {
                Interlocked.Add(ref _prefetchedPages, prefetched);
                _logger.Trace("Prefetched {0} pages for {1} starting at page {2}",
                    prefetched, filePath, startPageId);
            }
        }
        catch (Exception ex)
        {
            _logger.Debug("Prefetch failed: {0}", ex.Message);
        }
    }

    /// <summary>
    /// Manually triggers prefetch for the specified page range.
    /// Useful for table scans that know they'll need a range of pages.
    /// </summary>
    /// <param name="filePath">The path to the data file.</param>
    /// <param name="startPageId">The starting page ID.</param>
    /// <param name="count">Number of pages to prefetch.</param>
    /// <param name="pageManager">The page manager for loading pages.</param>
    public void TriggerPrefetch(string filePath, int startPageId, int count, PageManager pageManager)
    {
        if (!_enabled || _bufferPool == null)
            return;

        var actualCount = Math.Min(count, MaxPrefetchPages);
        _ = Task.Run(() => PrefetchPagesAsync(filePath, startPageId, actualCount, pageManager));
    }

    /// <summary>
    /// Gets the predicted next page IDs based on current access pattern.
    /// </summary>
    /// <param name="filePath">The path to the data file.</param>
    /// <param name="count">Number of predictions to return.</param>
    /// <returns>List of predicted page IDs.</returns>
    public IReadOnlyList<int> GetPredictedPages(string filePath, int count = 8)
    {
        var result = new List<int>();

        _lock.EnterReadLock();
        try
        {
            if (_accessHistory.TryGetValue(filePath, out var history))
            {
                if (history.SequentialCount >= SequentialThreshold)
                {
                    var nextPage = history.LastPageId + history.Direction;
                    for (int i = 0; i < count; i++)
                    {
                        result.Add(nextPage + i * history.Direction);
                    }
                }
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }

        return result;
    }

    /// <summary>
    /// Checks if the access pattern for a file is currently sequential.
    /// </summary>
    /// <param name="filePath">The path to the data file.</param>
    /// <returns>True if sequential access pattern is detected.</returns>
    public bool IsSequentialAccess(string filePath)
    {
        _lock.EnterReadLock();
        try
        {
            if (_accessHistory.TryGetValue(filePath, out var history))
            {
                return history.SequentialCount >= SequentialThreshold;
            }
            return false;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Resets the access history for a file.
    /// </summary>
    /// <param name="filePath">The path to the data file.</param>
    public void ResetHistory(string filePath)
    {
        _lock.EnterWriteLock();
        try
        {
            _accessHistory.Remove(filePath);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Clears all access history.
    /// </summary>
    public void ClearHistory()
    {
        _lock.EnterWriteLock();
        try
        {
            _accessHistory.Clear();
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        _logger.Debug("Read-ahead history cleared");
    }

    /// <summary>
    /// Gets statistics about the read-ahead system.
    /// </summary>
    public ReadAheadStats GetStats()
    {
        return new ReadAheadStats
        {
            PrefetchRequests = _prefetchRequests,
            PrefetchedPages = _prefetchedPages,
            SequentialDetections = _sequentialDetections,
            TrackedFiles = _accessHistory.Count
        };
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _lock.Dispose();
    }

    /// <summary>
    /// Tracks access history for a single file.
    /// </summary>
    private sealed class AccessHistory
    {
        public int LastPageId { get; set; } = -1;
        public int SequentialCount { get; set; }
        public int Direction { get; set; } = 1; // 1 = forward, -1 = backward
        public DateTime LastAccessTime { get; set; }
    }
}

/// <summary>
/// Statistics about the read-ahead system.
/// </summary>
public class ReadAheadStats
{
    /// <summary>
    /// Total number of prefetch requests triggered.
    /// </summary>
    public long PrefetchRequests { get; set; }

    /// <summary>
    /// Total number of pages prefetched.
    /// </summary>
    public long PrefetchedPages { get; set; }

    /// <summary>
    /// Number of times sequential access was detected.
    /// </summary>
    public long SequentialDetections { get; set; }

    /// <summary>
    /// Number of files being tracked for access patterns.
    /// </summary>
    public int TrackedFiles { get; set; }

    public override string ToString()
    {
        return $"Requests: {PrefetchRequests}, Pages: {PrefetchedPages}, " +
               $"Sequential: {SequentialDetections}, Files: {TrackedFiles}";
    }
}
