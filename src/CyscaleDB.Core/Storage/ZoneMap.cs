using System.Collections.Concurrent;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Zone Map (MIN/MAX page index) for data skipping optimization.
/// 
/// For each data page and each column, tracks the minimum and maximum values.
/// During a table scan with a predicate like WHERE x > 100, pages where
/// max(x) &lt; 100 can be skipped entirely without reading any rows.
///
/// This technique (also called "Small Materialized Aggregates" or "Data Skipping")
/// can eliminate 90%+ of I/O for selective queries on large tables.
///
/// Similar to:
///   - Parquet/ORC row group statistics
///   - SQL Server columnstore segment elimination
///   - Apache Iceberg partition pruning
/// </summary>
public sealed class ZoneMap
{
    private readonly ConcurrentDictionary<ZoneMapKey, ZoneMapEntry> _entries = new();

    /// <summary>
    /// Gets the total number of zone map entries.
    /// </summary>
    public int EntryCount => _entries.Count;

    /// <summary>
    /// Records min/max values for a column on a specific page.
    /// Should be called when a page is written or loaded into the buffer pool.
    /// </summary>
    public void UpdatePageStats(string tableName, int pageId, string columnName,
        DataValue minValue, DataValue maxValue, int rowCount)
    {
        var key = new ZoneMapKey(tableName, pageId, columnName);
        _entries[key] = new ZoneMapEntry(minValue, maxValue, rowCount);
    }

    /// <summary>
    /// Checks if a page can be skipped for a given predicate.
    /// Returns true if the page definitely does NOT contain matching rows.
    /// Returns false if the page MIGHT contain matching rows (must be scanned).
    /// </summary>
    /// <param name="tableName">Table name.</param>
    /// <param name="pageId">Page ID.</param>
    /// <param name="columnName">Column being filtered.</param>
    /// <param name="op">Comparison operator (=, <, >, <=, >=, !=).</param>
    /// <param name="value">The comparison value.</param>
    /// <returns>True if the page can be safely skipped.</returns>
    public bool CanSkipPage(string tableName, int pageId, string columnName,
        string op, DataValue value)
    {
        var key = new ZoneMapKey(tableName, pageId, columnName);
        if (!_entries.TryGetValue(key, out var entry))
            return false; // No stats available, can't skip

        if (entry.MinValue.IsNull && entry.MaxValue.IsNull)
            return false; // All nulls, can't determine

        return op switch
        {
            "=" =>
                // Can skip if value < min or value > max
                (!entry.MinValue.IsNull && value.CompareTo(entry.MinValue) < 0) ||
                (!entry.MaxValue.IsNull && value.CompareTo(entry.MaxValue) > 0),

            "<" =>
                // Can skip if min >= value (all values on page >= value)
                !entry.MinValue.IsNull && entry.MinValue.CompareTo(value) >= 0,

            "<=" =>
                // Can skip if min > value
                !entry.MinValue.IsNull && entry.MinValue.CompareTo(value) > 0,

            ">" =>
                // Can skip if max <= value
                !entry.MaxValue.IsNull && entry.MaxValue.CompareTo(value) <= 0,

            ">=" =>
                // Can skip if max < value
                !entry.MaxValue.IsNull && entry.MaxValue.CompareTo(value) < 0,

            "!=" or "<>" =>
                // Can skip only if all values are the same and equal to the filter value
                !entry.MinValue.IsNull && !entry.MaxValue.IsNull &&
                entry.MinValue.Equals(entry.MaxValue) && entry.MinValue.Equals(value),

            _ => false
        };
    }

    /// <summary>
    /// Gets the zone map entry for a specific page and column.
    /// </summary>
    public ZoneMapEntry? GetEntry(string tableName, int pageId, string columnName)
    {
        var key = new ZoneMapKey(tableName, pageId, columnName);
        return _entries.TryGetValue(key, out var entry) ? entry : null;
    }

    /// <summary>
    /// Removes zone map entries for a table (e.g., after DROP TABLE).
    /// </summary>
    public void RemoveTable(string tableName)
    {
        var toRemove = _entries.Keys.Where(k => k.TableName == tableName).ToList();
        foreach (var key in toRemove)
            _entries.TryRemove(key, out _);
    }

    /// <summary>
    /// Removes zone map entries for a specific page (e.g., after page rewrite).
    /// </summary>
    public void RemovePage(string tableName, int pageId)
    {
        var toRemove = _entries.Keys
            .Where(k => k.TableName == tableName && k.PageId == pageId)
            .ToList();
        foreach (var key in toRemove)
            _entries.TryRemove(key, out _);
    }

    /// <summary>
    /// Clears all zone map entries.
    /// </summary>
    public void Clear()
    {
        _entries.Clear();
    }
}

/// <summary>
/// Key for zone map lookups.
/// </summary>
public readonly struct ZoneMapKey : IEquatable<ZoneMapKey>
{
    public readonly string TableName;
    public readonly int PageId;
    public readonly string ColumnName;

    public ZoneMapKey(string tableName, int pageId, string columnName)
    {
        TableName = tableName;
        PageId = pageId;
        ColumnName = columnName;
    }

    public bool Equals(ZoneMapKey other) =>
        TableName == other.TableName && PageId == other.PageId && ColumnName == other.ColumnName;

    public override bool Equals(object? obj) => obj is ZoneMapKey other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(TableName, PageId, ColumnName);
}

/// <summary>
/// Zone map entry storing min/max statistics for a page-column combination.
/// </summary>
public sealed class ZoneMapEntry
{
    public DataValue MinValue { get; }
    public DataValue MaxValue { get; }
    public int RowCount { get; }

    public ZoneMapEntry(DataValue minValue, DataValue maxValue, int rowCount)
    {
        MinValue = minValue;
        MaxValue = maxValue;
        RowCount = rowCount;
    }
}
