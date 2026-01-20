using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Buffer pool that caches pages in memory using LRU eviction policy with Young/Old region separation.
/// Similar to InnoDB's midpoint insertion strategy, this prevents table scans from evicting hot pages.
/// 
/// Structure:
/// - Young region (head): Hot pages that have been accessed multiple times
/// - Old region (tail): Cold pages that were recently loaded or haven't been re-accessed
/// 
/// When a page is first loaded, it goes to the old region midpoint.
/// If accessed again within OldBlockTime, it moves to the young region head.
/// This prevents one-time sequential scans from evicting frequently used data.
/// 
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

    /// <summary>
    /// Percentage of the buffer pool reserved for the old region (0-100).
    /// Default is 37.5% like InnoDB (3/8 of the buffer).
    /// </summary>
    private const int OldBlockPercentage = 37;

    /// <summary>
    /// Time in milliseconds a page must wait in the old region before being moved to young region on re-access.
    /// Prevents table scans from polluting the young region.
    /// </summary>
    private int _oldBlockTimeMs = 1000;

    /// <summary>
    /// Boundary node between young and old regions.
    /// Pages before this node are in young region, after are in old region.
    /// </summary>
    private LinkedListNode<BufferKey>? _oldRegionBoundary;

    /// <summary>
    /// Number of pages in the old region.
    /// </summary>
    private int _oldRegionSize;

    // Statistics
    private long _hitCount;
    private long _missCount;
    private long _youngToOldMoves;
    private long _oldToYoungMoves;

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
    /// Gets or sets the old block time in milliseconds.
    /// Pages must be in the old region for at least this time before being moved to young on re-access.
    /// </summary>
    public int OldBlockTimeMs
    {
        get => _oldBlockTimeMs;
        set => _oldBlockTimeMs = value;
    }

    /// <summary>
    /// Gets the number of pages in the young region.
    /// </summary>
    public int YoungRegionCount
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _frames.Count - _oldRegionSize;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Gets the number of pages in the old region.
    /// </summary>
    public int OldRegionCount
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _oldRegionSize;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Gets the number of pages moved from young to old region.
    /// </summary>
    public long YoungToOldMoves => _youngToOldMoves;

    /// <summary>
    /// Gets the number of pages moved from old to young region.
    /// </summary>
    public long OldToYoungMoves => _oldToYoungMoves;

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
    /// Uses midpoint insertion strategy for new pages.
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
                    // Check if page should be promoted from old to young region
                    if (frame.IsInOldRegion)
                    {
                        var timeSinceLoad = (DateTime.UtcNow - frame.LoadTime).TotalMilliseconds;
                        if (timeSinceLoad >= _oldBlockTimeMs)
                        {
                            // Promote to young region (head of list)
                            MoveToYoungRegion(frame);
                            Interlocked.Increment(ref _oldToYoungMoves);
                        }
                        // If accessed too soon, leave in old region (scan resistance)
                    }
                    else
                    {
                        // Already in young region - move to head
                        _lruList.Remove(frame.LruNode);
                        _lruList.AddFirst(frame.LruNode);
                    }
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
                    // Same promotion logic as above
                    if (frame.IsInOldRegion)
                    {
                        var timeSinceLoad = (DateTime.UtcNow - frame.LoadTime).TotalMilliseconds;
                        if (timeSinceLoad >= _oldBlockTimeMs)
                        {
                            MoveToYoungRegion(frame);
                            Interlocked.Increment(ref _oldToYoungMoves);
                        }
                    }
                    else
                    {
                        _lruList.Remove(frame.LruNode);
                        _lruList.AddFirst(frame.LruNode);
                    }
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

                // Add to buffer at midpoint (old region) using midpoint insertion strategy
                var lruNode = InsertAtMidpoint(key);
                frame = new BufferFrame(page, pageManager, lruNode)
                {
                    PinCount = 1,
                    IsInOldRegion = true,
                    LoadTime = DateTime.UtcNow
                };
                _frames[key] = frame;
                _oldRegionSize++;

                _logger.Trace("Loaded page {0} from {1} into old region", pageId, pageManager.FilePath);
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
    /// Inserts a new page at the midpoint (between young and old regions).
    /// Must be called while holding write lock.
    /// </summary>
    private LinkedListNode<BufferKey> InsertAtMidpoint(BufferKey key)
    {
        if (_oldRegionBoundary == null || _lruList.Count == 0)
        {
            // First page or boundary not set - just add to list and set boundary
            var node = _lruList.AddFirst(key);
            _oldRegionBoundary = node;
            return node;
        }

        // Insert after the boundary (into old region)
        var newNode = _lruList.AddAfter(_oldRegionBoundary, key);
        return newNode;
    }

    /// <summary>
    /// Moves a page from old region to young region (head of list).
    /// Must be called while holding write lock.
    /// </summary>
    private void MoveToYoungRegion(BufferFrame frame)
    {
        if (!frame.IsInOldRegion)
            return;

        // If this is the boundary, move boundary to next node
        if (frame.LruNode == _oldRegionBoundary)
        {
            _oldRegionBoundary = frame.LruNode.Next;
        }

        // Move to head of list (young region)
        _lruList.Remove(frame.LruNode);
        _lruList.AddFirst(frame.LruNode);

        frame.IsInOldRegion = false;
        _oldRegionSize--;
    }

    /// <summary>
    /// Rebalances the young/old regions to maintain the correct ratio.
    /// Must be called while holding write lock.
    /// </summary>
    private void RebalanceRegions()
    {
        if (_frames.Count == 0)
            return;

        int targetOldSize = (_frames.Count * OldBlockPercentage) / 100;

        // Move pages from young to old if needed
        while (_oldRegionSize < targetOldSize && _oldRegionBoundary != null)
        {
            var youngTail = _oldRegionBoundary.Previous;
            if (youngTail == null)
                break;

            if (_frames.TryGetValue(youngTail.Value, out var frame))
            {
                frame.IsInOldRegion = true;
                _oldRegionBoundary = youngTail;
                _oldRegionSize++;
                Interlocked.Increment(ref _youngToOldMoves);
            }
            else
            {
                break;
            }
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
    /// New pages go directly to young region since they're typically actively used.
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

            // Add to buffer at head (young region) - newly created pages are hot
            var lruNode = _lruList.AddFirst(key);
            var frame = new BufferFrame(page, pageManager, lruNode)
            {
                PinCount = 1,
                IsInOldRegion = false,
                LoadTime = DateTime.UtcNow
            };
            _frames[key] = frame;

            // Update boundary if needed
            if (_oldRegionBoundary == null)
            {
                _oldRegionBoundary = lruNode;
            }

            // Rebalance to maintain young/old ratio
            RebalanceRegions();

            return page;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Evicts the least recently used unpinned page, preferring old region.
    /// Must be called while holding write lock.
    /// </summary>
    private void EvictPage()
    {
        // First try to evict from old region (tail of list)
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

                // Update boundary if this is the boundary node
                if (node == _oldRegionBoundary)
                {
                    _oldRegionBoundary = node.Previous;
                }

                // Update old region count
                if (frame.IsInOldRegion)
                {
                    _oldRegionSize--;
                }

                // Remove from buffer
                _lruList.Remove(node);
                _frames.Remove(key);

                _logger.Trace("Evicted page {0} from {1} (was in {2} region)", 
                    key.PageId, key.FilePath, frame.IsInOldRegion ? "old" : "young");
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

        /// <summary>
        /// Whether this page is in the old (cold) region.
        /// Pages start in the old region and are promoted to young on re-access.
        /// </summary>
        public bool IsInOldRegion { get; set; }

        /// <summary>
        /// When this page was loaded into the buffer pool.
        /// Used to determine if the page has been in old region long enough for promotion.
        /// </summary>
        public DateTime LoadTime { get; set; }

        public BufferFrame(Page page, PageManager pageManager, LinkedListNode<BufferKey> lruNode)
        {
            Page = page;
            PageManager = pageManager;
            LruNode = lruNode;
        }
    }
}
