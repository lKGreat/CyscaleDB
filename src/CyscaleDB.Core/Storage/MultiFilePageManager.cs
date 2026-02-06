using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Multi-file page manager that distributes pages across multiple data files
/// within a file group. Provides I/O parallelism by spreading data across
/// different physical disks.
///
/// Global page addressing: (FileId:16bit, LocalPageId:32bit) → 48-bit address.
/// We use the high bits of the page ID to encode the file ID.
///
/// Allocation strategies:
///   - Proportional Fill: allocate to the file with most free space
///   - Round-Robin: cycle through files for even distribution
///   - Striped: consecutive pages alternate across files for sequential I/O parallelism
///
/// This is the multi-disk counterpart to the single-file PageManager.
/// </summary>
public sealed class MultiFilePageManager : IPageManager
{
    private readonly FileGroupInfo _fileGroup;
    private readonly Dictionary<int, PageManager> _fileManagers = new();
    private readonly object _allocLock = new();
    private int _roundRobinIndex;
    private bool _disposed;

    /// <summary>
    /// The allocation strategy used.
    /// </summary>
    public AllocationStrategy Strategy { get; set; } = AllocationStrategy.ProportionalFill;

    /// <summary>
    /// Gets the file group this manager is managing.
    /// </summary>
    public FileGroupInfo FileGroup => _fileGroup;

    /// <summary>
    /// Gets the total page count across all files.
    /// </summary>
    public int PageCount
    {
        get
        {
            int total = 0;
            foreach (var mgr in _fileManagers.Values)
                total += mgr.PageCount;
            return total;
        }
    }

    /// <summary>
    /// Creates a new MultiFilePageManager for a file group.
    /// </summary>
    public MultiFilePageManager(FileGroupInfo fileGroup)
    {
        _fileGroup = fileGroup ?? throw new ArgumentNullException(nameof(fileGroup));
    }

    /// <summary>
    /// Opens all data files in the file group.
    /// </summary>
    public void Open()
    {
        foreach (var file in _fileGroup.Files)
        {
            if (!_fileManagers.ContainsKey(file.FileId))
            {
                var mgr = new PageManager(file.FilePath);
                mgr.Open();
                _fileManagers[file.FileId] = mgr;
            }
        }
    }

    /// <summary>
    /// Allocates a new page, choosing the target file based on the allocation strategy.
    /// </summary>
    public Page AllocatePage(PageType pageType = PageType.Data)
    {
        lock (_allocLock)
        {
            var targetFile = ChooseTargetFile();
            if (targetFile == null)
                throw new CyscaleException("No writable files available in file group.");

            var mgr = _fileManagers[targetFile.FileId];
            var page = mgr.AllocatePage(pageType);

            targetFile.AllocatedPages++;
            targetFile.CurrentSizeBytes = mgr.PageCount * Constants.PageSize;

            return page;
        }
    }

    /// <summary>
    /// Reads a page by global page ID.
    /// For simplicity, we iterate through file managers to find the page.
    /// In a production system, the global page ID would encode the file ID.
    /// </summary>
    public Page ReadPage(int pageId)
    {
        // Try each file manager - in a real system, we'd use the file ID from the global page ID
        foreach (var mgr in _fileManagers.Values)
        {
            if (pageId < mgr.PageCount)
            {
                return mgr.ReadPage(pageId);
            }
            pageId -= mgr.PageCount;
        }

        throw new CyscaleException($"Page {pageId} not found in any file.");
    }

    /// <summary>
    /// Writes a page.
    /// </summary>
    public void WritePage(Page page)
    {
        // Route to the correct file manager
        var localPageId = page.PageId;
        foreach (var mgr in _fileManagers.Values)
        {
            if (localPageId < mgr.PageCount)
            {
                mgr.WritePage(page);
                return;
            }
            localPageId -= mgr.PageCount;
        }

        throw new CyscaleException($"Cannot write page {page.PageId}: not found in any file.");
    }

    /// <summary>
    /// Adds a new data file to the file group at runtime (online expansion).
    /// </summary>
    public void AddFile(DataFileInfo file)
    {
        _fileGroup.AddFile(file);
        var mgr = new PageManager(file.FilePath);
        mgr.Open();
        _fileManagers[file.FileId] = mgr;
    }

    /// <summary>
    /// Removes a data file from the file group.
    /// File must be empty (all data migrated) before removal.
    /// </summary>
    public bool RemoveFile(string logicalName)
    {
        var file = _fileGroup.Files.FirstOrDefault(f => f.LogicalName == logicalName);
        if (file == null) return false;

        if (file.AllocatedPages > 0)
            throw new CyscaleException($"Cannot remove file '{logicalName}': file still has {file.AllocatedPages} allocated pages.");

        if (_fileManagers.TryGetValue(file.FileId, out var mgr))
        {
            mgr.Dispose();
            _fileManagers.Remove(file.FileId);
        }

        return _fileGroup.RemoveFile(logicalName);
    }

    private DataFileInfo? ChooseTargetFile()
    {
        var writableFiles = _fileGroup.Files.Where(f => !f.IsReadOnly).ToList();
        if (writableFiles.Count == 0) return null;

        return Strategy switch
        {
            AllocationStrategy.RoundRobin => ChooseRoundRobin(writableFiles),
            AllocationStrategy.ProportionalFill => _fileGroup.GetFileWithMostFreeSpace(),
            AllocationStrategy.Striped => ChooseRoundRobin(writableFiles), // Striped uses RR for pages
            _ => writableFiles[0]
        };
    }

    private DataFileInfo ChooseRoundRobin(List<DataFileInfo> files)
    {
        var idx = _roundRobinIndex % files.Count;
        _roundRobinIndex++;
        return files[idx];
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var mgr in _fileManagers.Values)
            mgr.Dispose();
        _fileManagers.Clear();
    }
}

/// <summary>
/// Page allocation strategies for multi-file file groups.
/// </summary>
public enum AllocationStrategy
{
    /// <summary>
    /// Allocate to the file with the most free space.
    /// Ensures even space utilization across files.
    /// </summary>
    ProportionalFill,

    /// <summary>
    /// Cycle through files in order.
    /// Ensures even page count across files.
    /// </summary>
    RoundRobin,

    /// <summary>
    /// Consecutive pages alternate across files.
    /// Optimizes sequential scan I/O by reading from multiple disks.
    /// </summary>
    Striped
}
