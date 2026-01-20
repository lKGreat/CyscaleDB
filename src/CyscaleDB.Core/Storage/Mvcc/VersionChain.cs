namespace CyscaleDB.Core.Storage.Mvcc;

/// <summary>
/// Represents a version of a row in the version chain.
/// Each version contains the row data and a pointer to the next (older) version.
/// </summary>
public sealed class RowVersion
{
    /// <summary>
    /// The transaction ID that created this version.
    /// </summary>
    public long TransactionId { get; }

    /// <summary>
    /// Pointer to the previous version in the chain (via undo log).
    /// 0 indicates no previous version.
    /// </summary>
    public long RollPointer { get; }

    /// <summary>
    /// Whether this version represents a deletion.
    /// </summary>
    public bool IsDeleted { get; }

    /// <summary>
    /// The row data for this version (null if we only have the pointer).
    /// </summary>
    public Row? RowData { get; }

    /// <summary>
    /// Timestamp when this version was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// Creates a new row version.
    /// </summary>
    public RowVersion(long transactionId, long rollPointer, bool isDeleted, Row? rowData)
    {
        TransactionId = transactionId;
        RollPointer = rollPointer;
        IsDeleted = isDeleted;
        RowData = rowData;
        CreatedAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Creates a row version from an existing row.
    /// </summary>
    public static RowVersion FromRow(Row row)
    {
        return new RowVersion(row.TransactionId, row.RollPointer, row.IsDeleted, row);
    }

    /// <summary>
    /// Checks if there is a previous version in the chain.
    /// </summary>
    public bool HasPreviousVersion => RollPointerHelper.IsValid(RollPointer);

    public override string ToString()
    {
        return $"RowVersion(TrxId={TransactionId}, RollPtr={RollPointer}, Deleted={IsDeleted})";
    }
}

/// <summary>
/// Manages the version chain for MVCC.
/// The version chain links row versions through roll pointers that reference undo log records.
/// 
/// Version Chain Structure:
/// Current Row -> Undo Record 1 -> Undo Record 2 -> ... -> NULL
/// (newest)                                              (oldest)
/// 
/// Each row has:
/// - TRX_ID: Transaction that created this version
/// - ROLL_PTR: Points to undo log record containing the previous version
/// </summary>
public sealed class VersionChain
{
    private readonly IUndoLogReader? _undoLogReader;

    /// <summary>
    /// The current (newest) version of the row.
    /// </summary>
    public RowVersion CurrentVersion { get; private set; }

    /// <summary>
    /// Creates a new version chain starting with the given row.
    /// </summary>
    /// <param name="currentRow">The current version of the row</param>
    /// <param name="undoLogReader">Optional undo log reader for traversing history</param>
    public VersionChain(Row currentRow, IUndoLogReader? undoLogReader = null)
    {
        ArgumentNullException.ThrowIfNull(currentRow);
        CurrentVersion = RowVersion.FromRow(currentRow);
        _undoLogReader = undoLogReader;
    }

    /// <summary>
    /// Creates a version chain from a row version.
    /// </summary>
    public VersionChain(RowVersion currentVersion, IUndoLogReader? undoLogReader = null)
    {
        CurrentVersion = currentVersion ?? throw new ArgumentNullException(nameof(currentVersion));
        _undoLogReader = undoLogReader;
    }

    /// <summary>
    /// Finds the visible version of the row for the given ReadView.
    /// Traverses the version chain to find the first version that is visible.
    /// </summary>
    /// <param name="readView">The ReadView to check visibility against</param>
    /// <returns>The visible row, or null if no visible version exists</returns>
    public Row? FindVisibleVersion(ReadView readView)
    {
        ArgumentNullException.ThrowIfNull(readView);

        var version = CurrentVersion;

        while (version != null)
        {
            // Check if this version is visible
            if (readView.IsVisible(version.TransactionId))
            {
                // If this version is a delete marker, the row is not visible
                if (version.IsDeleted)
                    return null;

                // Return the row data for this version
                return version.RowData;
            }

            // Move to previous version if available
            version = GetPreviousVersion(version);
        }

        // No visible version found
        return null;
    }

    /// <summary>
    /// Enumerates all versions in the chain from newest to oldest.
    /// </summary>
    public IEnumerable<RowVersion> EnumerateVersions()
    {
        var version = CurrentVersion;

        while (version != null)
        {
            yield return version;
            version = GetPreviousVersion(version);
        }
    }

    /// <summary>
    /// Gets the previous version from the undo log.
    /// </summary>
    private RowVersion? GetPreviousVersion(RowVersion version)
    {
        if (!version.HasPreviousVersion)
            return null;

        if (_undoLogReader == null)
            return null;

        // Read the previous version from the undo log
        return _undoLogReader.ReadVersion(version.RollPointer);
    }

    /// <summary>
    /// Adds a new version to the chain (when a row is modified).
    /// The old current version becomes accessible through the roll pointer.
    /// </summary>
    /// <param name="newRow">The new version of the row</param>
    public void AddVersion(Row newRow)
    {
        ArgumentNullException.ThrowIfNull(newRow);
        CurrentVersion = RowVersion.FromRow(newRow);
    }

    /// <summary>
    /// Gets the number of versions in the chain (requires traversal).
    /// </summary>
    public int GetVersionCount()
    {
        int count = 0;
        foreach (var _ in EnumerateVersions())
        {
            count++;
        }
        return count;
    }

    /// <summary>
    /// Gets the oldest version in the chain.
    /// </summary>
    public RowVersion GetOldestVersion()
    {
        RowVersion oldest = CurrentVersion;
        
        foreach (var version in EnumerateVersions())
        {
            oldest = version;
        }

        return oldest;
    }

    public override string ToString()
    {
        return $"VersionChain(Current={CurrentVersion})";
    }
}

/// <summary>
/// Interface for reading row versions from the undo log.
/// </summary>
public interface IUndoLogReader
{
    /// <summary>
    /// Reads a row version from the undo log at the given roll pointer.
    /// </summary>
    /// <param name="rollPointer">The roll pointer pointing to the undo record</param>
    /// <returns>The row version, or null if not found</returns>
    RowVersion? ReadVersion(long rollPointer);
}

/// <summary>
/// In-memory implementation of undo log reader for testing and simple use cases.
/// </summary>
public sealed class InMemoryUndoLogReader : IUndoLogReader
{
    private readonly Dictionary<long, RowVersion> _versions = new();
    private long _nextRollPointer = 1;

    /// <summary>
    /// Stores a version and returns its roll pointer.
    /// </summary>
    public long StoreVersion(RowVersion version)
    {
        var rollPointer = _nextRollPointer++;
        _versions[rollPointer] = version;
        return rollPointer;
    }

    /// <summary>
    /// Stores a row as a version and returns its roll pointer.
    /// </summary>
    public long StoreVersion(Row row)
    {
        return StoreVersion(RowVersion.FromRow(row));
    }

    /// <summary>
    /// Reads a version from the in-memory store.
    /// </summary>
    public RowVersion? ReadVersion(long rollPointer)
    {
        return _versions.TryGetValue(rollPointer, out var version) ? version : null;
    }

    /// <summary>
    /// Removes a version from the store (for purging old versions).
    /// </summary>
    public bool RemoveVersion(long rollPointer)
    {
        return _versions.Remove(rollPointer);
    }

    /// <summary>
    /// Gets the count of stored versions.
    /// </summary>
    public int Count => _versions.Count;

    /// <summary>
    /// Clears all stored versions.
    /// </summary>
    public void Clear()
    {
        _versions.Clear();
        _nextRollPointer = 1;
    }
}

/// <summary>
/// Manages version chains for multiple rows.
/// Provides methods to find visible versions across the table.
/// </summary>
public sealed class VersionChainManager
{
    private readonly IUndoLogReader? _undoLogReader;

    /// <summary>
    /// Creates a new version chain manager.
    /// </summary>
    /// <param name="undoLogReader">The undo log reader for traversing history</param>
    public VersionChainManager(IUndoLogReader? undoLogReader = null)
    {
        _undoLogReader = undoLogReader;
    }

    /// <summary>
    /// Finds the visible version of a row for the given ReadView.
    /// </summary>
    /// <param name="row">The current row to check</param>
    /// <param name="readView">The ReadView to check visibility against</param>
    /// <returns>The visible row, or null if not visible</returns>
    public Row? FindVisibleVersion(Row row, ReadView readView)
    {
        var chain = new VersionChain(row, _undoLogReader);
        return chain.FindVisibleVersion(readView);
    }

    /// <summary>
    /// Checks if a row is visible for the given ReadView without traversing history.
    /// This is a fast path for rows that are clearly visible or invisible.
    /// </summary>
    /// <param name="row">The row to check</param>
    /// <param name="readView">The ReadView to check visibility against</param>
    /// <returns>True if the row is visible, false otherwise</returns>
    public bool IsRowVisible(Row row, ReadView readView)
    {
        // Fast path: if the current version is visible, use it
        if (readView.IsVisible(row.TransactionId))
        {
            return !row.IsDeleted;
        }

        // Need to traverse version chain if we have an undo log reader
        if (_undoLogReader != null && RollPointerHelper.IsValid(row.RollPointer))
        {
            var visibleRow = FindVisibleVersion(row, readView);
            return visibleRow != null;
        }

        return false;
    }

    /// <summary>
    /// Filters an enumerable of rows to only those visible to the ReadView.
    /// </summary>
    /// <param name="rows">The rows to filter</param>
    /// <param name="readView">The ReadView to check visibility against</param>
    /// <returns>Enumerable of visible rows</returns>
    public IEnumerable<Row> FilterVisibleRows(IEnumerable<Row> rows, ReadView readView)
    {
        foreach (var row in rows)
        {
            var visibleRow = FindVisibleVersion(row, readView);
            if (visibleRow != null)
            {
                yield return visibleRow;
            }
        }
    }
}
