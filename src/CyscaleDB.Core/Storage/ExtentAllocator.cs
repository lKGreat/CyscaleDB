using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Extent-based page allocation for reducing fragmentation and improving
/// sequential I/O performance. An Extent is a group of 8 contiguous pages.
///
/// Two types of extents (similar to SQL Server):
///   - Uniform Extent: all 8 pages belong to the same table/index
///   - Mixed Extent: pages from different small tables share the extent
///
/// Small tables (< 8 pages) start in mixed extents. Once a table reaches
/// 8 pages, it gets its own uniform extents.
///
/// This reduces external fragmentation and ensures sequential scans
/// read 8 pages at a time (32KB chunks).
/// </summary>
public sealed class ExtentAllocator
{
    /// <summary>
    /// Number of pages per extent.
    /// </summary>
    public const int PagesPerExtent = 8;

    /// <summary>
    /// Size of an extent in bytes.
    /// </summary>
    public const int ExtentSizeBytes = PagesPerExtent * Constants.PageSize;

    private readonly Dictionary<int, ExtentInfo> _extents = new();
    private readonly Dictionary<string, List<int>> _tableExtents = new(); // table → extent IDs
    private int _nextExtentId;
    private readonly object _lock = new();

    /// <summary>
    /// Total number of extents allocated.
    /// </summary>
    public int ExtentCount => _extents.Count;

    /// <summary>
    /// Allocates a page for a specific table.
    /// Tries to use an existing extent with free space, or allocates a new extent.
    /// </summary>
    /// <param name="tableName">The table requesting the page.</param>
    /// <param name="pageAllocator">Function to actually allocate the physical page.</param>
    /// <returns>The allocated page ID.</returns>
    public int AllocatePage(string tableName, Func<int> pageAllocator)
    {
        lock (_lock)
        {
            // Try to find an existing extent with free space for this table
            if (_tableExtents.TryGetValue(tableName, out var extentIds))
            {
                foreach (var extentId in extentIds)
                {
                    var extent = _extents[extentId];
                    if (extent.FreePages > 0)
                    {
                        extent.FreePages--;
                        return pageAllocator();
                    }
                }
            }

            // Allocate a new extent
            var newExtentId = _nextExtentId++;
            var newExtent = new ExtentInfo
            {
                ExtentId = newExtentId,
                OwnerTable = tableName,
                IsUniform = true,
                FreePages = PagesPerExtent - 1, // One page used immediately
                StartPageId = -1 // Will be set when first page is allocated
            };

            _extents[newExtentId] = newExtent;

            if (!_tableExtents.TryGetValue(tableName, out var tableExtentList))
            {
                tableExtentList = new List<int>();
                _tableExtents[tableName] = tableExtentList;
            }
            tableExtentList.Add(newExtentId);

            // Allocate the first page in the new extent
            var pageId = pageAllocator();
            newExtent.StartPageId = pageId;

            // Pre-allocate remaining pages in the extent to ensure contiguity
            for (int i = 1; i < PagesPerExtent; i++)
            {
                pageAllocator(); // Reserve contiguous pages
            }

            return pageId;
        }
    }

    /// <summary>
    /// Gets extent information for a table.
    /// </summary>
    public List<ExtentInfo> GetTableExtents(string tableName)
    {
        lock (_lock)
        {
            if (!_tableExtents.TryGetValue(tableName, out var extentIds))
                return new List<ExtentInfo>();

            return extentIds.Select(id => _extents[id]).ToList();
        }
    }

    /// <summary>
    /// Deallocates all extents for a table (e.g., after DROP TABLE).
    /// </summary>
    public void DeallocateTable(string tableName)
    {
        lock (_lock)
        {
            if (_tableExtents.TryGetValue(tableName, out var extentIds))
            {
                foreach (var id in extentIds)
                    _extents.Remove(id);
                _tableExtents.Remove(tableName);
            }
        }
    }
}

/// <summary>
/// Information about an allocated extent.
/// </summary>
public class ExtentInfo
{
    public int ExtentId { get; set; }
    public string OwnerTable { get; set; } = "";
    public bool IsUniform { get; set; }
    public int FreePages { get; set; }
    public int StartPageId { get; set; }
}
