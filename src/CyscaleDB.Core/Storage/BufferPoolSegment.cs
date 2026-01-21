using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Key for identifying pages in the buffer pool segment.
/// </summary>
internal readonly struct BufferKey : IEquatable<BufferKey>
{
    public string FilePath { get; }
    public int PageId { get; }

    public BufferKey(string filePath, int pageId)
    {
        FilePath = filePath;
        PageId = pageId;
    }

    public bool Equals(BufferKey other)
    {
        return PageId == other.PageId && FilePath == other.FilePath;
    }

    public override bool Equals(object? obj)
    {
        return obj is BufferKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(FilePath, PageId);
    }
}

/// <summary>
/// Frame containing a cached page and its metadata.
/// </summary>
internal sealed class BufferFrame
{
    public Page Page { get; }
    public bool IsDirty { get; set; }
    public int PinCount { get; set; }
    public DateTime LoadTime { get; set; }
    public LinkedListNode<BufferKey>? LruNode { get; set; }

    public BufferFrame(Page page)
    {
        Page = page;
    }
}

/// <summary>
/// Represents a segment of a buffer pool for fine-grained locking.
/// Each segment maintains its own set of pages and LRU list, reducing lock contention.
/// This is used by SegmentedBufferPool for high-concurrency scenarios.
/// </summary>
internal sealed class BufferPoolSegment : IDisposable
{
    private readonly Dictionary<BufferKey, BufferFrame> _frames;
    private readonly LinkedList<BufferKey> _lruList;
    private readonly ReaderWriterLockSlim _lock;
    private readonly int _capacity;
    private readonly Logger _logger;
    private bool _disposed;

    // Statistics
    private long _hitCount;
    private long _missCount;

    /// <summary>
    /// Gets the number of cached pages in this segment.
    /// </summary>
    public int Count
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _frames.Count;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Gets the capacity of this segment.
    /// </summary>
    public int Capacity => _capacity;

    /// <summary>
    /// Gets the hit count for this segment.
    /// </summary>
    public long HitCount => Interlocked.Read(ref _hitCount);

    /// <summary>
    /// Gets the miss count for this segment.
    /// </summary>
    public long MissCount => Interlocked.Read(ref _missCount);

    /// <summary>
    /// Gets the hit ratio for this segment.
    /// </summary>
    public double HitRatio
    {
        get
        {
            var total = _hitCount + _missCount;
            return total > 0 ? (double)_hitCount / total : 0;
        }
    }

    /// <summary>
    /// Creates a new buffer pool segment with the specified capacity.
    /// </summary>
    public BufferPoolSegment(int capacity)
    {
        _capacity = capacity;
        _frames = new Dictionary<BufferKey, BufferFrame>(capacity);
        _lruList = new LinkedList<BufferKey>();
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<BufferPoolSegment>();
    }

    /// <summary>
    /// Tries to get a page from this segment.
    /// </summary>
    /// <param name="key">The buffer key</param>
    /// <param name="page">The page if found</param>
    /// <returns>True if the page was found</returns>
    public bool TryGetPage(BufferKey key, out Page? page)
    {
        _lock.EnterUpgradeableReadLock();
        try
        {
            if (_frames.TryGetValue(key, out var frame))
            {
                Interlocked.Increment(ref _hitCount);
                
                _lock.EnterWriteLock();
                try
                {
                    // Move to front of LRU list
                    if (frame.LruNode != null)
                    {
                        _lruList.Remove(frame.LruNode);
                        _lruList.AddFirst(frame.LruNode);
                    }
                    frame.PinCount++;
                    page = frame.Page;
                    return true;
                }
                finally
                {
                    _lock.ExitWriteLock();
                }
            }

            Interlocked.Increment(ref _missCount);
            page = null;
            return false;
        }
        finally
        {
            _lock.ExitUpgradeableReadLock();
        }
    }

    /// <summary>
    /// Adds a page to this segment, evicting if necessary.
    /// </summary>
    /// <param name="key">The buffer key</param>
    /// <param name="page">The page to add</param>
    /// <returns>The evicted page if any</returns>
    public Page? AddPage(BufferKey key, Page page)
    {
        Page? evictedPage = null;

        _lock.EnterWriteLock();
        try
        {
            // Check if already present
            if (_frames.ContainsKey(key))
            {
                return null;
            }

            // Evict if at capacity
            if (_frames.Count >= _capacity)
            {
                evictedPage = EvictLru();
            }

            // Add new page
            var frame = new BufferFrame(page)
            {
                LoadTime = DateTime.UtcNow,
                PinCount = 1
            };

            var node = _lruList.AddFirst(key);
            frame.LruNode = node;
            _frames[key] = frame;

            return evictedPage;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Unpins a page in this segment.
    /// </summary>
    public void UnpinPage(BufferKey key, bool isDirty)
    {
        _lock.EnterWriteLock();
        try
        {
            if (_frames.TryGetValue(key, out var frame))
            {
                frame.PinCount--;
                if (isDirty)
                {
                    frame.IsDirty = true;
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Gets all dirty pages in this segment.
    /// </summary>
    public List<(BufferKey Key, Page Page)> GetDirtyPages()
    {
        var result = new List<(BufferKey, Page)>();

        _lock.EnterReadLock();
        try
        {
            foreach (var kvp in _frames)
            {
                if (kvp.Value.IsDirty)
                {
                    result.Add((kvp.Key, kvp.Value.Page));
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
    /// Marks a page as clean after flushing.
    /// </summary>
    public void MarkClean(BufferKey key)
    {
        _lock.EnterWriteLock();
        try
        {
            if (_frames.TryGetValue(key, out var frame))
            {
                frame.IsDirty = false;
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Removes a page from this segment.
    /// </summary>
    public bool RemovePage(BufferKey key)
    {
        _lock.EnterWriteLock();
        try
        {
            if (_frames.TryGetValue(key, out var frame))
            {
                if (frame.PinCount > 0)
                {
                    return false; // Can't remove pinned page
                }

                if (frame.LruNode != null)
                {
                    _lruList.Remove(frame.LruNode);
                }
                _frames.Remove(key);
                return true;
            }

            return false;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Clears all pages from this segment.
    /// </summary>
    public void Clear()
    {
        _lock.EnterWriteLock();
        try
        {
            _frames.Clear();
            _lruList.Clear();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private Page? EvictLru()
    {
        // Find an unpinned page from the tail (LRU end)
        var current = _lruList.Last;
        while (current != null)
        {
            var key = current.Value;
            if (_frames.TryGetValue(key, out var frame) && frame.PinCount == 0)
            {
                var evictedPage = frame.Page;
                
                _lruList.Remove(current);
                _frames.Remove(key);

                if (frame.IsDirty)
                {
                    return evictedPage; // Caller needs to flush this
                }

                return null;
            }

            current = current.Previous;
        }

        return null; // All pages pinned
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _lock.Dispose();
    }
}

/// <summary>
/// A segmented buffer pool that uses multiple segments to reduce lock contention.
/// Each page is assigned to a segment based on its page ID hash.
/// </summary>
public sealed class SegmentedBufferPool : IDisposable
{
    private const int DefaultSegmentCount = 16;
    private readonly BufferPoolSegment[] _segments;
    private readonly int _segmentCount;
    private readonly Logger _logger;
    private bool _disposed;

    /// <summary>
    /// Gets the total number of cached pages.
    /// </summary>
    public int Count => _segments.Sum(s => s.Count);

    /// <summary>
    /// Gets the total capacity.
    /// </summary>
    public int Capacity => _segments.Sum(s => s.Capacity);

    /// <summary>
    /// Gets the overall hit ratio.
    /// </summary>
    public double HitRatio
    {
        get
        {
            long totalHits = _segments.Sum(s => s.HitCount);
            long totalMisses = _segments.Sum(s => s.MissCount);
            var total = totalHits + totalMisses;
            return total > 0 ? (double)totalHits / total : 0;
        }
    }

    /// <summary>
    /// Creates a new segmented buffer pool.
    /// </summary>
    /// <param name="totalCapacity">Total capacity across all segments</param>
    /// <param name="segmentCount">Number of segments (default 16)</param>
    public SegmentedBufferPool(int totalCapacity, int segmentCount = DefaultSegmentCount)
    {
        _segmentCount = segmentCount;
        _segments = new BufferPoolSegment[segmentCount];
        
        int capacityPerSegment = (totalCapacity + segmentCount - 1) / segmentCount;
        
        for (int i = 0; i < segmentCount; i++)
        {
            _segments[i] = new BufferPoolSegment(capacityPerSegment);
        }

        _logger = LogManager.Default.GetLogger<SegmentedBufferPool>();
    }

    /// <summary>
    /// Creates a SegmentedBufferPool using the current configuration settings.
    /// Uses 16 segments by default for optimal concurrency.
    /// </summary>
    public static SegmentedBufferPool CreateFromConfiguration()
    {
        var config = Common.CyscaleDbConfiguration.Current;
        return new SegmentedBufferPool(config.BufferPoolSizePages, DefaultSegmentCount);
    }

    /// <summary>
    /// Gets a page from the segmented buffer pool.
    /// </summary>
    public Page? GetPage(PageManager pageManager, int pageId)
    {
        var key = new BufferKey(pageManager.FilePath, pageId);
        var segment = GetSegment(key);

        if (segment.TryGetPage(key, out var page))
        {
            return page;
        }

        // Page not in buffer - load from disk
        var loadedPage = pageManager.ReadPage(pageId);
        segment.AddPage(key, loadedPage);
        return loadedPage;
    }

    /// <summary>
    /// Unpins a page.
    /// </summary>
    public void UnpinPage(string filePath, int pageId, bool isDirty = false)
    {
        var key = new BufferKey(filePath, pageId);
        var segment = GetSegment(key);
        segment.UnpinPage(key, isDirty);
    }

    /// <summary>
    /// Flushes all dirty pages to disk.
    /// </summary>
    public void FlushAll(Func<string, PageManager> getPageManager)
    {
        foreach (var segment in _segments)
        {
            var dirtyPages = segment.GetDirtyPages();
            foreach (var (key, page) in dirtyPages)
            {
                var pageManager = getPageManager(key.FilePath);
                pageManager.WritePage(page);
                segment.MarkClean(key);
            }
        }
    }

    /// <summary>
    /// Clears all pages from all segments.
    /// </summary>
    public void Clear()
    {
        foreach (var segment in _segments)
        {
            segment.Clear();
        }
    }

    private BufferPoolSegment GetSegment(BufferKey key)
    {
        int hash = key.GetHashCode();
        int index = ((hash % _segmentCount) + _segmentCount) % _segmentCount;
        return _segments[index];
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        foreach (var segment in _segments)
        {
            segment.Dispose();
        }
    }
}
