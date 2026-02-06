using System.Collections.Concurrent;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Adaptive Hash Index (AHI) - automatically builds in-memory hash indexes
/// on hot B-Tree pages to accelerate point lookups from O(log n) to O(1).
///
/// InnoDB monitors access patterns on B-Tree leaf pages. When a particular
/// page/prefix is accessed frequently, AHI automatically constructs a hash
/// index entry mapping (search key → page + record pointer).
///
/// Key behaviors:
///   - Automatically enabled/disabled (innodb_adaptive_hash_index)
///   - Partitioned into N parts to reduce lock contention
///   - Evicted under memory pressure or when access patterns change
///   - Only useful for equality lookups (=), not range scans
/// </summary>
public sealed class AdaptiveHashIndex : IDisposable
{
    private readonly int _numPartitions;
    private readonly ConcurrentDictionary<AhiKey, AhiEntry>[] _partitions;
    private readonly long[] _hitCounts;
    private readonly long[] _missCounts;
    private readonly int _maxEntriesPerPartition;
    private readonly bool _enabled;
    private bool _disposed;

    // Access pattern tracking
    private readonly ConcurrentDictionary<string, AccessPattern> _accessPatterns = new();
    private const int HotThreshold = 100; // accesses before building AHI entry

    /// <summary>
    /// Gets whether AHI is enabled.
    /// </summary>
    public bool IsEnabled => _enabled;

    /// <summary>
    /// Gets total hit count across all partitions.
    /// </summary>
    public long TotalHits => _hitCounts.Sum();

    /// <summary>
    /// Gets total miss count across all partitions.
    /// </summary>
    public long TotalMisses => _missCounts.Sum();

    /// <summary>
    /// Gets total number of entries across all partitions.
    /// </summary>
    public long TotalEntries => _partitions.Sum(p => p.Count);

    /// <summary>
    /// Creates a new Adaptive Hash Index.
    /// </summary>
    /// <param name="numPartitions">Number of hash table partitions (reduces contention).</param>
    /// <param name="maxEntriesPerPartition">Max entries per partition before eviction.</param>
    /// <param name="enabled">Whether AHI is enabled.</param>
    public AdaptiveHashIndex(int numPartitions = 8, int maxEntriesPerPartition = 100000, bool enabled = true)
    {
        _numPartitions = numPartitions;
        _maxEntriesPerPartition = maxEntriesPerPartition;
        _enabled = enabled;

        _partitions = new ConcurrentDictionary<AhiKey, AhiEntry>[numPartitions];
        _hitCounts = new long[numPartitions];
        _missCounts = new long[numPartitions];

        for (int i = 0; i < numPartitions; i++)
        {
            _partitions[i] = new ConcurrentDictionary<AhiKey, AhiEntry>();
        }
    }

    /// <summary>
    /// Looks up a value in the AHI. Returns the RowId if found, null otherwise.
    /// </summary>
    /// <param name="tableName">Table name (used for partitioning).</param>
    /// <param name="indexName">Index name.</param>
    /// <param name="searchKey">The search key value.</param>
    /// <returns>The RowId if found in AHI cache, null otherwise.</returns>
    public RowId? Lookup(string tableName, string indexName, DataValue searchKey)
    {
        if (!_enabled) return null;

        var ahiKey = new AhiKey(tableName, indexName, searchKey);
        var partition = GetPartition(ahiKey);

        if (_partitions[partition].TryGetValue(ahiKey, out var entry))
        {
            Interlocked.Increment(ref _hitCounts[partition]);
            entry.LastAccess = Environment.TickCount64;
            return entry.RowId;
        }

        Interlocked.Increment(ref _missCounts[partition]);
        return null;
    }

    /// <summary>
    /// Records an access pattern. When a key is accessed frequently,
    /// automatically builds an AHI entry.
    /// </summary>
    /// <param name="tableName">Table name.</param>
    /// <param name="indexName">Index name.</param>
    /// <param name="searchKey">The key that was searched.</param>
    /// <param name="rowId">The RowId found via B-Tree traversal.</param>
    public void RecordAccess(string tableName, string indexName, DataValue searchKey, RowId rowId)
    {
        if (!_enabled) return;

        var patternKey = $"{tableName}.{indexName}";
        var pattern = _accessPatterns.GetOrAdd(patternKey, _ => new AccessPattern());
        var count = Interlocked.Increment(ref pattern.AccessCount);

        // Only build AHI entry for hot keys
        if (count >= HotThreshold)
        {
            Insert(tableName, indexName, searchKey, rowId);
        }
    }

    /// <summary>
    /// Directly inserts an entry into the AHI.
    /// </summary>
    public void Insert(string tableName, string indexName, DataValue searchKey, RowId rowId)
    {
        if (!_enabled) return;

        var ahiKey = new AhiKey(tableName, indexName, searchKey);
        var partition = GetPartition(ahiKey);
        var dict = _partitions[partition];

        // Evict if over capacity
        if (dict.Count >= _maxEntriesPerPartition)
        {
            EvictPartition(partition);
        }

        dict[ahiKey] = new AhiEntry(rowId);
    }

    /// <summary>
    /// Invalidates AHI entries for a specific table (e.g., after DML).
    /// </summary>
    public void InvalidateTable(string tableName)
    {
        if (!_enabled) return;

        for (int i = 0; i < _numPartitions; i++)
        {
            var toRemove = _partitions[i].Keys
                .Where(k => k.TableName == tableName)
                .ToList();
            foreach (var key in toRemove)
            {
                _partitions[i].TryRemove(key, out _);
            }
        }
    }

    /// <summary>
    /// Invalidates a specific AHI entry.
    /// </summary>
    public void Invalidate(string tableName, string indexName, DataValue searchKey)
    {
        if (!_enabled) return;

        var ahiKey = new AhiKey(tableName, indexName, searchKey);
        var partition = GetPartition(ahiKey);
        _partitions[partition].TryRemove(ahiKey, out _);
    }

    /// <summary>
    /// Clears all AHI entries.
    /// </summary>
    public void Clear()
    {
        for (int i = 0; i < _numPartitions; i++)
        {
            _partitions[i].Clear();
            _hitCounts[i] = 0;
            _missCounts[i] = 0;
        }
        _accessPatterns.Clear();
    }

    private int GetPartition(AhiKey key)
    {
        return Math.Abs(key.GetHashCode()) % _numPartitions;
    }

    private void EvictPartition(int partition)
    {
        // Evict ~25% of entries (LRU approximation)
        var dict = _partitions[partition];
        var entries = dict.ToList()
            .OrderBy(e => e.Value.LastAccess)
            .Take(dict.Count / 4)
            .Select(e => e.Key)
            .ToList();

        foreach (var key in entries)
        {
            dict.TryRemove(key, out _);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Clear();
    }
}

/// <summary>
/// Key for AHI lookup: (table, index, search value).
/// </summary>
internal readonly struct AhiKey : IEquatable<AhiKey>
{
    public readonly string TableName;
    public readonly string IndexName;
    public readonly DataValue SearchKey;

    public AhiKey(string tableName, string indexName, DataValue searchKey)
    {
        TableName = tableName;
        IndexName = indexName;
        SearchKey = searchKey;
    }

    public bool Equals(AhiKey other) =>
        TableName == other.TableName && IndexName == other.IndexName && SearchKey.Equals(other.SearchKey);

    public override bool Equals(object? obj) => obj is AhiKey other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(TableName, IndexName, SearchKey);
}

/// <summary>
/// AHI entry storing the cached RowId and access metadata.
/// </summary>
internal sealed class AhiEntry
{
    public RowId RowId { get; }
    public long LastAccess { get; set; }

    public AhiEntry(RowId rowId)
    {
        RowId = rowId;
        LastAccess = Environment.TickCount64;
    }
}

/// <summary>
/// Tracks access patterns for AHI auto-build decisions.
/// </summary>
internal sealed class AccessPattern
{
    public long AccessCount;
}
