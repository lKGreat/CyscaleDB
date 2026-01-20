using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Buffer pool that caches pages in memory using LRU eviction policy.
/// Thread-safe for concurrent access.
/// </summary>
public sealed class BufferPool : IDisposable
{
    private readonly int _capacity;
    private readonly Dictionary<BufferKey, BufferFrame> _frames;
    private readonly LinkedList<BufferKey> _lruList;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Logger _logger;
    private bool _disposed;

    // Statistics
    private long _hitCount;
    private long _missCount;

    /// <summary>
    /// Gets the number of cached pages.
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
    /// Gets the capacity of the buffer pool.
    /// </summary>
    public int Capacity => _capacity;

    /// <summary>
    /// Gets the cache hit ratio.
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
    /// Creates a new buffer pool with the specified capacity.
    /// </summary>
    public BufferPool(int capacity = Constants.DefaultBufferPoolSize)
    {
        _capacity = capacity;
        _frames = new Dictionary<BufferKey, BufferFrame>(capacity);
        _lruList = new LinkedList<BufferKey>();
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<BufferPool>();
    }

    /// <summary>
    /// Gets a page from the buffer pool or loads it from disk.
    /// </summary>
    public Page GetPage(PageManager pageManager, int pageId)
    {
        var key = new BufferKey(pageManager.FilePath, pageId);

        _lock.EnterUpgradeableReadLock();
        try
        {
            // Check if page is already in buffer
            if (_frames.TryGetValue(key, out var frame))
            {
                Interlocked.Increment(ref _hitCount);
                
                _lock.EnterWriteLock();
                try
                {
                    // Move to front of LRU list
                    _lruList.Remove(frame.LruNode);
                    _lruList.AddFirst(frame.LruNode);
                    frame.PinCount++;
                }
                finally
                {
                    _lock.ExitWriteLock();
                }

                return frame.Page;
            }

            // Page not in buffer - need to load it
            Interlocked.Increment(ref _missCount);

            _lock.EnterWriteLock();
            try
            {
                // Double-check after acquiring write lock
                if (_frames.TryGetValue(key, out frame))
                {
                    _lruList.Remove(frame.LruNode);
                    _lruList.AddFirst(frame.LruNode);
                    frame.PinCount++;
                    return frame.Page;
                }

                // Make room if necessary
                while (_frames.Count >= _capacity)
                {
                    EvictPage();
                }

                // Load page from disk
                var page = pageManager.ReadPage(pageId);

                // Add to buffer
                var lruNode = _lruList.AddFirst(key);
                frame = new BufferFrame(page, pageManager, lruNode)
                {
                    PinCount = 1
                };
                _frames[key] = frame;

                _logger.Trace("Loaded page {0} from {1}", pageId, pageManager.FilePath);
                return page;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
        finally
        {
            _lock.ExitUpgradeableReadLock();
        }
    }

    /// <summary>
    /// Unpins a page, allowing it to be evicted.
    /// </summary>
    public void UnpinPage(PageManager pageManager, int pageId, bool isDirty = false)
    {
        var key = new BufferKey(pageManager.FilePath, pageId);

        _lock.EnterWriteLock();
        try
        {
            if (_frames.TryGetValue(key, out var frame))
            {
                if (isDirty)
                    frame.Page.IsDirty = true;

                if (frame.PinCount > 0)
                    frame.PinCount--;
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Marks a page as dirty.
    /// </summary>
    public void MarkDirty(PageManager pageManager, int pageId)
    {
        var key = new BufferKey(pageManager.FilePath, pageId);

        _lock.EnterReadLock();
        try
        {
            if (_frames.TryGetValue(key, out var frame))
            {
                frame.Page.IsDirty = true;
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Flushes a specific page to disk if it's dirty.
    /// </summary>
    public void FlushPage(PageManager pageManager, int pageId)
    {
        var key = new BufferKey(pageManager.FilePath, pageId);

        _lock.EnterWriteLock();
        try
        {
            if (_frames.TryGetValue(key, out var frame) && frame.Page.IsDirty)
            {
                pageManager.WritePage(frame.Page);
                frame.Page.IsDirty = false;
                _logger.Trace("Flushed page {0} to {1}", pageId, pageManager.FilePath);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Flushes all dirty pages for a specific page manager.
    /// </summary>
    public void FlushAll(PageManager pageManager)
    {
        _lock.EnterWriteLock();
        try
        {
            foreach (var (key, frame) in _frames)
            {
                if (key.FilePath == pageManager.FilePath && frame.Page.IsDirty)
                {
                    pageManager.WritePage(frame.Page);
                    frame.Page.IsDirty = false;
                }
            }
            _logger.Debug("Flushed all dirty pages for {0}", pageManager.FilePath);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Flushes all dirty pages to all page managers.
    /// </summary>
    public void FlushAll()
    {
        _lock.EnterWriteLock();
        try
        {
            foreach (var (_, frame) in _frames)
            {
                if (frame.Page.IsDirty)
                {
                    frame.PageManager.WritePage(frame.Page);
                    frame.Page.IsDirty = false;
                }
            }
            _logger.Debug("Flushed all dirty pages");
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Evicts all pages for a specific page manager.
    /// </summary>
    public void EvictAll(PageManager pageManager)
    {
        _lock.EnterWriteLock();
        try
        {
            var keysToRemove = _frames
                .Where(kv => kv.Key.FilePath == pageManager.FilePath)
                .Select(kv => kv.Key)
                .ToList();

            foreach (var key in keysToRemove)
            {
                var frame = _frames[key];
                
                // Flush if dirty
                if (frame.Page.IsDirty)
                {
                    pageManager.WritePage(frame.Page);
                }

                _lruList.Remove(frame.LruNode);
                _frames.Remove(key);
            }

            _logger.Debug("Evicted {0} pages for {1}", keysToRemove.Count, pageManager.FilePath);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Creates a new page and adds it to the buffer pool.
    /// </summary>
    public Page NewPage(PageManager pageManager, PageType pageType = PageType.Data)
    {
        _lock.EnterWriteLock();
        try
        {
            // Make room if necessary
            while (_frames.Count >= _capacity)
            {
                EvictPage();
            }

            // Allocate new page
            var page = pageManager.AllocatePage(pageType);
            var key = new BufferKey(pageManager.FilePath, page.PageId);

            // Add to buffer
            var lruNode = _lruList.AddFirst(key);
            var frame = new BufferFrame(page, pageManager, lruNode)
            {
                PinCount = 1
            };
            _frames[key] = frame;

            return page;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Evicts the least recently used unpinned page.
    /// Must be called while holding write lock.
    /// </summary>
    private void EvictPage()
    {
        // Find LRU unpinned page
        var node = _lruList.Last;
        while (node != null)
        {
            var key = node.Value;
            if (_frames.TryGetValue(key, out var frame) && frame.PinCount == 0)
            {
                // Flush if dirty
                if (frame.Page.IsDirty)
                {
                    frame.PageManager.WritePage(frame.Page);
                }

                // Remove from buffer
                _lruList.Remove(node);
                _frames.Remove(key);

                _logger.Trace("Evicted page {0} from {1}", key.PageId, key.FilePath);
                return;
            }

            node = node.Previous;
        }

        // All pages are pinned - this shouldn't happen in normal operation
        throw new StorageException("Buffer pool is full and all pages are pinned");
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Flush all dirty pages
        FlushAll();

        _lock.Dispose();
    }

    /// <summary>
    /// Key for identifying pages in the buffer pool.
    /// </summary>
    private readonly struct BufferKey : IEquatable<BufferKey>
    {
        public string FilePath { get; }
        public int PageId { get; }

        public BufferKey(string filePath, int pageId)
        {
            FilePath = filePath;
            PageId = pageId;
        }

        public bool Equals(BufferKey other) =>
            FilePath == other.FilePath && PageId == other.PageId;

        public override bool Equals(object? obj) =>
            obj is BufferKey other && Equals(other);

        public override int GetHashCode() =>
            HashCode.Combine(FilePath, PageId);
    }

    /// <summary>
    /// Frame holding a page in the buffer pool.
    /// </summary>
    private sealed class BufferFrame
    {
        public Page Page { get; }
        public PageManager PageManager { get; }
        public LinkedListNode<BufferKey> LruNode { get; }
        public int PinCount { get; set; }

        public BufferFrame(Page page, PageManager pageManager, LinkedListNode<BufferKey> lruNode)
        {
            Page = page;
            PageManager = pageManager;
            LruNode = lruNode;
        }
    }
}
