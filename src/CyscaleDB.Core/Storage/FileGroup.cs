namespace CyscaleDB.Core.Storage;

/// <summary>
/// Describes a physical data file within a file group.
/// Each data file resides on a specific disk/path and has independent I/O.
/// </summary>
public sealed class DataFileInfo
{
    /// <summary>
    /// Global unique file ID within the database.
    /// </summary>
    public int FileId { get; }

    /// <summary>
    /// Logical name of the file (used in ALTER DATABASE ... MODIFY FILE).
    /// </summary>
    public string LogicalName { get; }

    /// <summary>
    /// Physical file absolute path (can be on any disk).
    /// </summary>
    public string FilePath { get; }

    /// <summary>
    /// Name of the file group this file belongs to.
    /// </summary>
    public string FileGroupName { get; }

    /// <summary>
    /// Maximum file size in bytes (0 = unlimited).
    /// </summary>
    public long MaxSizeBytes { get; set; }

    /// <summary>
    /// Auto-growth increment in bytes (default 64 MB).
    /// </summary>
    public long GrowthBytes { get; set; }

    /// <summary>
    /// Current file size in bytes.
    /// </summary>
    public long CurrentSizeBytes { get; set; }

    /// <summary>
    /// Whether this file is read-only.
    /// </summary>
    public bool IsReadOnly { get; set; }

    /// <summary>
    /// Number of pages currently allocated in this file.
    /// </summary>
    public int AllocatedPages { get; set; }

    public DataFileInfo(int fileId, string logicalName, string filePath, string fileGroupName,
        long maxSizeBytes = 0, long growthBytes = 64 * 1024 * 1024)
    {
        FileId = fileId;
        LogicalName = logicalName;
        FilePath = filePath;
        FileGroupName = fileGroupName;
        MaxSizeBytes = maxSizeBytes;
        GrowthBytes = growthBytes;
    }
}

/// <summary>
/// A file group is a logical container of one or more data files.
/// Tables and indexes are assigned to file groups; the file group distributes
/// data across its files for I/O parallelism.
///
/// Similar to SQL Server FileGroups and MySQL InnoDB Tablespaces.
/// </summary>
public sealed class FileGroupInfo
{
    /// <summary>
    /// File group name (e.g., "PRIMARY", "DATA", "INDEX").
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Whether this is the default file group for new tables.
    /// </summary>
    public bool IsDefault { get; set; }

    /// <summary>
    /// Whether this file group is read-only.
    /// </summary>
    public bool IsReadOnly { get; set; }

    /// <summary>
    /// Data files in this file group.
    /// </summary>
    public List<DataFileInfo> Files { get; } = new();

    /// <summary>
    /// Gets the total allocated pages across all files.
    /// </summary>
    public int TotalAllocatedPages => Files.Sum(f => f.AllocatedPages);

    /// <summary>
    /// Gets the total size across all files.
    /// </summary>
    public long TotalSizeBytes => Files.Sum(f => f.CurrentSizeBytes);

    public FileGroupInfo(string name, bool isDefault = false)
    {
        Name = name;
        IsDefault = isDefault;
    }

    /// <summary>
    /// Adds a data file to this file group.
    /// </summary>
    public void AddFile(DataFileInfo file)
    {
        Files.Add(file);
    }

    /// <summary>
    /// Removes a data file from this file group.
    /// </summary>
    public bool RemoveFile(string logicalName)
    {
        var file = Files.FirstOrDefault(f => f.LogicalName == logicalName);
        return file != null && Files.Remove(file);
    }

    /// <summary>
    /// Gets the file with the most free space (for proportional fill allocation).
    /// </summary>
    public DataFileInfo? GetFileWithMostFreeSpace()
    {
        DataFileInfo? best = null;
        long bestFree = -1;

        foreach (var file in Files)
        {
            if (file.IsReadOnly) continue;

            var free = file.MaxSizeBytes > 0
                ? file.MaxSizeBytes - file.CurrentSizeBytes
                : long.MaxValue;

            if (free > bestFree)
            {
                bestFree = free;
                best = file;
            }
        }

        return best;
    }
}
