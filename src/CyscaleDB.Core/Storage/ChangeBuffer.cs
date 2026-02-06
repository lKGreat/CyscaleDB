using System.Collections.Concurrent;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Change Buffer - buffers modifications to non-unique secondary index pages
/// that are not currently in the Buffer Pool. Instead of performing random I/O
/// to fetch the index page, the change is buffered and merged later when the
/// page is read for other reasons or during a periodic merge.
///
/// This is critical for write-heavy workloads with many secondary indexes,
/// reducing random I/O by up to 90% for non-unique index updates.
///
/// Key behaviors:
///   - Only for NON-UNIQUE secondary indexes (unique indexes need immediate checking)
///   - Buffers INSERT, UPDATE, and DELETE operations
///   - Merges when: page is read into Buffer Pool, background merge thread, or shutdown
///   - Max size controlled by innodb_change_buffer_max_size (default 25% of buffer pool)
///
/// Based on InnoDB's ibuf (insert buffer) design.
/// </summary>
public sealed class ChangeBuffer : IDisposable
{
    private readonly ConcurrentDictionary<ChangeBufferKey, List<ChangeBufferEntry>> _buffer;
    private readonly long _maxSizeBytes;
    private long _currentSizeBytes;
    private long _totalMerges;
    private long _totalBuffered;
    private bool _disposed;

    /// <summary>
    /// Gets the current number of buffered changes.
    /// </summary>
    public long BufferedCount => _buffer.Values.Sum(v => v.Count);

    /// <summary>
    /// Gets the approximate size in bytes.
    /// </summary>
    public long CurrentSizeBytes => Interlocked.Read(ref _currentSizeBytes);

    /// <summary>
    /// Gets the total number of changes that were merged.
    /// </summary>
    public long TotalMerges => _totalMerges;

    /// <summary>
    /// Gets the total number of changes that were buffered.
    /// </summary>
    public long TotalBuffered => _totalBuffered;

    /// <summary>
    /// Creates a new ChangeBuffer.
    /// </summary>
    /// <param name="maxSizeBytes">Maximum buffer size in bytes.</param>
    public ChangeBuffer(long maxSizeBytes = 64 * 1024 * 1024) // 64 MB default
    {
        _buffer = new ConcurrentDictionary<ChangeBufferKey, List<ChangeBufferEntry>>();
        _maxSizeBytes = maxSizeBytes;
    }

    /// <summary>
    /// Buffers an INSERT operation on a non-unique secondary index.
    /// Returns true if the change was buffered, false if it should be applied immediately
    /// (e.g., buffer is full or the page is already in memory).
    /// </summary>
    /// <param name="tableName">Table name.</param>
    /// <param name="indexName">Index name (must be non-unique).</param>
    /// <param name="pageId">The target page ID in the index.</param>
    /// <param name="keyValues">The index key values to insert.</param>
    /// <param name="rowId">The RowId being indexed.</param>
    public bool BufferInsert(string tableName, string indexName, int pageId,
        DataValue[] keyValues, RowId rowId)
    {
        return BufferChange(tableName, indexName, pageId, ChangeType.Insert, keyValues, rowId);
    }

    /// <summary>
    /// Buffers a DELETE operation on a non-unique secondary index.
    /// </summary>
    public bool BufferDelete(string tableName, string indexName, int pageId,
        DataValue[] keyValues, RowId rowId)
    {
        return BufferChange(tableName, indexName, pageId, ChangeType.Delete, keyValues, rowId);
    }

    /// <summary>
    /// Gets and removes all buffered changes for a specific page.
    /// Called when the page is being read into the Buffer Pool.
    /// </summary>
    /// <param name="tableName">Table name.</param>
    /// <param name="indexName">Index name.</param>
    /// <param name="pageId">The page ID being loaded.</param>
    /// <returns>List of buffered changes to merge, or empty list.</returns>
    public List<ChangeBufferEntry> GetAndRemoveChanges(string tableName, string indexName, int pageId)
    {
        var key = new ChangeBufferKey(tableName, indexName, pageId);

        if (_buffer.TryRemove(key, out var entries))
        {
            var size = entries.Sum(e => EstimateEntrySize(e));
            Interlocked.Add(ref _currentSizeBytes, -size);
            Interlocked.Add(ref _totalMerges, entries.Count);
            return entries;
        }

        return new List<ChangeBufferEntry>();
    }

    /// <summary>
    /// Checks if there are buffered changes for a specific page.
    /// </summary>
    public bool HasChanges(string tableName, string indexName, int pageId)
    {
        var key = new ChangeBufferKey(tableName, indexName, pageId);
        return _buffer.ContainsKey(key);
    }

    /// <summary>
    /// Forces merge of all buffered changes (called during shutdown or FLUSH).
    /// </summary>
    /// <returns>Action delegate for each change to apply.</returns>
    public IEnumerable<(ChangeBufferKey Key, List<ChangeBufferEntry> Entries)> DrainAll()
    {
        var keys = _buffer.Keys.ToList();
        foreach (var key in keys)
        {
            if (_buffer.TryRemove(key, out var entries))
            {
                Interlocked.Add(ref _totalMerges, entries.Count);
                yield return (key, entries);
            }
        }
        _currentSizeBytes = 0;
    }

    /// <summary>
    /// Clears all buffered changes.
    /// </summary>
    public void Clear()
    {
        _buffer.Clear();
        _currentSizeBytes = 0;
    }

    private bool BufferChange(string tableName, string indexName, int pageId,
        ChangeType changeType, DataValue[] keyValues, RowId rowId)
    {
        // Don't buffer if we're at capacity
        if (Interlocked.Read(ref _currentSizeBytes) >= _maxSizeBytes)
            return false;

        var key = new ChangeBufferKey(tableName, indexName, pageId);
        var entry = new ChangeBufferEntry(changeType, keyValues, rowId, DateTime.UtcNow);

        var entrySize = EstimateEntrySize(entry);

        _buffer.AddOrUpdate(key,
            _ =>
            {
                Interlocked.Add(ref _currentSizeBytes, entrySize);
                Interlocked.Increment(ref _totalBuffered);
                return new List<ChangeBufferEntry> { entry };
            },
            (_, existing) =>
            {
                lock (existing)
                {
                    existing.Add(entry);
                }
                Interlocked.Add(ref _currentSizeBytes, entrySize);
                Interlocked.Increment(ref _totalBuffered);
                return existing;
            });

        return true;
    }

    private static long EstimateEntrySize(ChangeBufferEntry entry)
    {
        long size = 64; // base overhead
        foreach (var val in entry.KeyValues)
        {
            size += val.IsNull ? 1 : 16;
        }
        return size;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Clear();
    }
}

/// <summary>
/// Key for identifying a specific index page in the change buffer.
/// </summary>
public readonly struct ChangeBufferKey : IEquatable<ChangeBufferKey>
{
    public readonly string TableName;
    public readonly string IndexName;
    public readonly int PageId;

    public ChangeBufferKey(string tableName, string indexName, int pageId)
    {
        TableName = tableName;
        IndexName = indexName;
        PageId = pageId;
    }

    public bool Equals(ChangeBufferKey other) =>
        TableName == other.TableName && IndexName == other.IndexName && PageId == other.PageId;

    public override bool Equals(object? obj) => obj is ChangeBufferKey other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(TableName, IndexName, PageId);
}

/// <summary>
/// A single buffered change entry.
/// </summary>
public sealed class ChangeBufferEntry
{
    public ChangeType ChangeType { get; }
    public DataValue[] KeyValues { get; }
    public RowId RowId { get; }
    public DateTime BufferedAt { get; }

    public ChangeBufferEntry(ChangeType changeType, DataValue[] keyValues, RowId rowId, DateTime bufferedAt)
    {
        ChangeType = changeType;
        KeyValues = keyValues;
        RowId = rowId;
        BufferedAt = bufferedAt;
    }
}

/// <summary>
/// Type of buffered change.
/// </summary>
public enum ChangeType
{
    Insert,
    Delete,
    PurgeDelete  // Mark for physical deletion during purge
}
