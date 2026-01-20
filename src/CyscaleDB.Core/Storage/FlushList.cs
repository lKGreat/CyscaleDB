using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// The FlushList manages dirty pages that need to be flushed to disk.
/// Pages are ordered by their oldest modification LSN (Log Sequence Number),
/// enabling efficient background flushing and proper checkpoint management.
/// 
/// Key features:
/// - Pages are ordered by modification LSN for proper recovery order
/// - Supports adaptive flushing based on LSN age
/// - Tracks oldest dirty page for checkpoint determination
/// - Thread-safe for concurrent access
/// </summary>
public sealed class FlushList : IDisposable
{
    private readonly SortedDictionary<long, FlushListEntry> _entriesByLsn;
    private readonly Dictionary<FlushListKey, FlushListEntry> _entriesByPage;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Logger _logger;
    private long _oldestModificationLsn;
    private long _newestModificationLsn;
    private bool _disposed;

    // Statistics
    private long _totalFlushes;
    private long _totalAdded;
    private long _totalRemoved;

    /// <summary>
    /// Gets the number of dirty pages in the flush list.
    /// </summary>
    public int Count
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _entriesByPage.Count;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Gets the oldest modification LSN in the flush list.
    /// Returns -1 if the list is empty.
    /// </summary>
    public long OldestModificationLsn
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _entriesByPage.Count > 0 ? _oldestModificationLsn : -1;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Gets the newest modification LSN in the flush list.
    /// Returns -1 if the list is empty.
    /// </summary>
    public long NewestModificationLsn
    {
        get
        {
            _lock.EnterReadLock();
            try
            {
                return _entriesByPage.Count > 0 ? _newestModificationLsn : -1;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// Gets the total number of pages that have been flushed.
    /// </summary>
    public long TotalFlushes => _totalFlushes;

    /// <summary>
    /// Gets the total number of pages that have been added to the flush list.
    /// </summary>
    public long TotalAdded => _totalAdded;

    /// <summary>
    /// Gets the total number of pages that have been removed from the flush list.
    /// </summary>
    public long TotalRemoved => _totalRemoved;

    /// <summary>
    /// Creates a new flush list.
    /// </summary>
    public FlushList()
    {
        _entriesByLsn = new SortedDictionary<long, FlushListEntry>();
        _entriesByPage = new Dictionary<FlushListKey, FlushListEntry>();
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<FlushList>();
        _oldestModificationLsn = long.MaxValue;
        _newestModificationLsn = 0;
    }

    /// <summary>
    /// Adds or updates a dirty page in the flush list.
    /// The page is positioned based on its oldest modification LSN.
    /// </summary>
    /// <param name="filePath">The path to the data file.</param>
    /// <param name="pageId">The page ID.</param>
    /// <param name="page">The dirty page.</param>
    /// <param name="lsn">The LSN of the modification.</param>
    public void AddDirtyPage(string filePath, int pageId, Page page, long lsn)
    {
        var key = new FlushListKey(filePath, pageId);

        _lock.EnterWriteLock();
        try
        {
            if (_entriesByPage.TryGetValue(key, out var existingEntry))
            {
                // Page already in flush list - update newest LSN
                existingEntry.NewestLsn = Math.Max(existingEntry.NewestLsn, lsn);
                existingEntry.ModificationCount++;
            }
            else
            {
                // New entry - add to both collections
                var entry = new FlushListEntry(key, page, lsn);
                
                // Use unique LSN key (combine with page ID to handle concurrent modifications)
                var lsnKey = lsn * 1000000L + pageId;
                while (_entriesByLsn.ContainsKey(lsnKey))
                {
                    lsnKey++;
                }

                entry.LsnKey = lsnKey;
                _entriesByLsn[lsnKey] = entry;
                _entriesByPage[key] = entry;

                // Update oldest/newest tracking
                if (lsn < _oldestModificationLsn)
                {
                    _oldestModificationLsn = lsn;
                }
                if (lsn > _newestModificationLsn)
                {
                    _newestModificationLsn = lsn;
                }

                Interlocked.Increment(ref _totalAdded);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        _logger.Trace("Added page {0}:{1} to flush list with LSN {2}", filePath, pageId, lsn);
    }

    /// <summary>
    /// Removes a page from the flush list (after it has been flushed).
    /// </summary>
    /// <param name="filePath">The path to the data file.</param>
    /// <param name="pageId">The page ID.</param>
    /// <returns>True if the page was removed, false if it wasn't in the list.</returns>
    public bool RemovePage(string filePath, int pageId)
    {
        var key = new FlushListKey(filePath, pageId);

        _lock.EnterWriteLock();
        try
        {
            if (_entriesByPage.TryGetValue(key, out var entry))
            {
                _entriesByLsn.Remove(entry.LsnKey);
                _entriesByPage.Remove(key);

                Interlocked.Increment(ref _totalRemoved);

                // Recalculate oldest LSN if needed
                if (_entriesByLsn.Count > 0)
                {
                    var firstEntry = _entriesByLsn.First().Value;
                    _oldestModificationLsn = firstEntry.OldestLsn;
                }
                else
                {
                    _oldestModificationLsn = long.MaxValue;
                    _newestModificationLsn = 0;
                }

                _logger.Trace("Removed page {0}:{1} from flush list", filePath, pageId);
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
    /// Gets the oldest dirty pages for flushing.
    /// </summary>
    /// <param name="count">Maximum number of pages to return.</param>
    /// <returns>List of pages ordered by oldest modification LSN.</returns>
    public IReadOnlyList<(string FilePath, int PageId, Page Page)> GetOldestDirtyPages(int count)
    {
        var result = new List<(string FilePath, int PageId, Page Page)>();

        _lock.EnterReadLock();
        try
        {
            foreach (var kvp in _entriesByLsn)
            {
                if (result.Count >= count)
                    break;

                var entry = kvp.Value;
                result.Add((entry.Key.FilePath, entry.Key.PageId, entry.Page));
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }

        return result;
    }

    /// <summary>
    /// Gets dirty pages that are older than the specified LSN threshold.
    /// Useful for adaptive flushing to keep up with redo log.
    /// </summary>
    /// <param name="lsnThreshold">Flush pages with oldest LSN less than this value.</param>
    /// <returns>List of pages ordered by oldest modification LSN.</returns>
    public IReadOnlyList<(string FilePath, int PageId, Page Page)> GetPagesOlderThan(long lsnThreshold)
    {
        var result = new List<(string FilePath, int PageId, Page Page)>();

        _lock.EnterReadLock();
        try
        {
            foreach (var kvp in _entriesByLsn)
            {
                var entry = kvp.Value;
                if (entry.OldestLsn >= lsnThreshold)
                    break;

                result.Add((entry.Key.FilePath, entry.Key.PageId, entry.Page));
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }

        return result;
    }

    /// <summary>
    /// Flushes dirty pages to disk using the provided flush function.
    /// </summary>
    /// <param name="count">Maximum number of pages to flush.</param>
    /// <param name="flushFunc">Function to call for each page (returns true if flush succeeded).</param>
    /// <returns>Number of pages actually flushed.</returns>
    public int FlushPages(int count, Func<string, int, Page, bool> flushFunc)
    {
        var pagesToFlush = GetOldestDirtyPages(count);
        int flushedCount = 0;

        foreach (var (filePath, pageId, page) in pagesToFlush)
        {
            try
            {
                if (flushFunc(filePath, pageId, page))
                {
                    RemovePage(filePath, pageId);
                    flushedCount++;
                    Interlocked.Increment(ref _totalFlushes);
                }
            }
            catch (Exception ex)
            {
                _logger.Error("Failed to flush page {0}:{1}: {2}", filePath, pageId, ex.Message);
            }
        }

        if (flushedCount > 0)
        {
            _logger.Debug("Flushed {0} dirty pages", flushedCount);
        }

        return flushedCount;
    }

    /// <summary>
    /// Checks if a page is in the flush list.
    /// </summary>
    public bool ContainsPage(string filePath, int pageId)
    {
        var key = new FlushListKey(filePath, pageId);

        _lock.EnterReadLock();
        try
        {
            return _entriesByPage.ContainsKey(key);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Clears all entries from the flush list.
    /// </summary>
    public void Clear()
    {
        _lock.EnterWriteLock();
        try
        {
            _entriesByLsn.Clear();
            _entriesByPage.Clear();
            _oldestModificationLsn = long.MaxValue;
            _newestModificationLsn = 0;
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        _logger.Debug("Flush list cleared");
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _lock.Dispose();
    }

    /// <summary>
    /// Key for identifying pages in the flush list.
    /// </summary>
    private readonly struct FlushListKey : IEquatable<FlushListKey>
    {
        public string FilePath { get; }
        public int PageId { get; }

        public FlushListKey(string filePath, int pageId)
        {
            FilePath = filePath;
            PageId = pageId;
        }

        public bool Equals(FlushListKey other) =>
            FilePath == other.FilePath && PageId == other.PageId;

        public override bool Equals(object? obj) =>
            obj is FlushListKey other && Equals(other);

        public override int GetHashCode() =>
            HashCode.Combine(FilePath, PageId);
    }

    /// <summary>
    /// Entry in the flush list representing a dirty page.
    /// </summary>
    private sealed class FlushListEntry
    {
        public FlushListKey Key { get; }
        public Page Page { get; }
        public long OldestLsn { get; }
        public long NewestLsn { get; set; }
        public int ModificationCount { get; set; }
        public DateTime AddedTime { get; }
        public long LsnKey { get; set; }

        public FlushListEntry(FlushListKey key, Page page, long lsn)
        {
            Key = key;
            Page = page;
            OldestLsn = lsn;
            NewestLsn = lsn;
            ModificationCount = 1;
            AddedTime = DateTime.UtcNow;
        }
    }
}
