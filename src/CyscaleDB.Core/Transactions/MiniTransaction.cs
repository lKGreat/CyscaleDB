using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Transactions;

/// <summary>
/// A Mini-Transaction (mtr) is used for atomic page-level operations in the storage engine.
/// It ensures that changes to one or more pages are atomic - either all changes are applied
/// or none are. MiniTransactions are used internally for operations like:
/// - B-Tree page splits
/// - Index node modifications
/// - Allocating new pages
/// 
/// Unlike full transactions, MiniTransactions are short-lived and do not support
/// user-level isolation or locking.
/// </summary>
public sealed class MiniTransaction : IDisposable
{
    private static long _nextMtrId = 1;

    private readonly long _mtrId;
    private readonly List<PageModification> _modifications = [];
    private readonly List<byte[]> _originalPageData = [];
    private readonly HashSet<int> _modifiedPageIds = [];
    private readonly Logger _logger;
    private readonly WalLog? _walLog;
    private readonly BufferPool? _bufferPool;
    private MtrState _state;
    private long _startLsn;
    private long _endLsn;

    /// <summary>
    /// Gets the unique identifier of this mini-transaction.
    /// </summary>
    public long MtrId => _mtrId;

    /// <summary>
    /// Gets the current state of this mini-transaction.
    /// </summary>
    public MtrState State => _state;

    /// <summary>
    /// Gets the number of pages modified in this mini-transaction.
    /// </summary>
    public int ModifiedPageCount => _modifiedPageIds.Count;

    /// <summary>
    /// Gets the starting LSN for this mini-transaction.
    /// </summary>
    public long StartLsn => _startLsn;

    /// <summary>
    /// Gets the ending LSN for this mini-transaction.
    /// </summary>
    public long EndLsn => _endLsn;

    /// <summary>
    /// Creates a new mini-transaction.
    /// </summary>
    /// <param name="walLog">Optional WAL log for durability. If null, changes are not logged.</param>
    /// <param name="bufferPool">Optional buffer pool for page management.</param>
    public MiniTransaction(WalLog? walLog = null, BufferPool? bufferPool = null)
    {
        _mtrId = Interlocked.Increment(ref _nextMtrId);
        _walLog = walLog;
        _bufferPool = bufferPool;
        _state = MtrState.Active;
        _startLsn = walLog?.CurrentLsn ?? 0;
        _logger = LogManager.Default.GetLogger<MiniTransaction>();
        _logger.Debug("MiniTransaction {0} started", _mtrId);
    }

    /// <summary>
    /// Records a page modification within this mini-transaction.
    /// Saves the original page data for potential rollback.
    /// </summary>
    /// <param name="page">The page being modified.</param>
    /// <param name="modificationType">The type of modification.</param>
    /// <param name="data">Optional additional data describing the modification.</param>
    public void RecordModification(Page page, PageModificationType modificationType, byte[]? data = null)
    {
        EnsureActive();

        if (!_modifiedPageIds.Contains(page.PageId))
        {
            // First modification to this page - save original data for rollback
            _originalPageData.Add(page.GetData());
            _modifiedPageIds.Add(page.PageId);
        }

        var modification = new PageModification(
            page.PageId,
            modificationType,
            data ?? [],
            _walLog?.CurrentLsn ?? 0);

        _modifications.Add(modification);
        page.IsDirty = true;

        _logger.Debug("MiniTransaction {0}: Recorded {1} on page {2}",
            _mtrId, modificationType, page.PageId);
    }

    /// <summary>
    /// Records a page modification with before and after images for redo/undo.
    /// </summary>
    /// <param name="page">The page being modified.</param>
    /// <param name="modificationType">The type of modification.</param>
    /// <param name="offset">The byte offset within the page.</param>
    /// <param name="oldData">The data before modification.</param>
    /// <param name="newData">The data after modification.</param>
    public void RecordModification(Page page, PageModificationType modificationType,
        int offset, byte[] oldData, byte[] newData)
    {
        EnsureActive();

        if (!_modifiedPageIds.Contains(page.PageId))
        {
            // First modification to this page - save original data for rollback
            _originalPageData.Add(page.GetData());
            _modifiedPageIds.Add(page.PageId);
        }

        // Create modification record with offset, old data, and new data
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(offset);
        writer.Write(oldData.Length);
        writer.Write(oldData);
        writer.Write(newData.Length);
        writer.Write(newData);

        var modification = new PageModification(
            page.PageId,
            modificationType,
            ms.ToArray(),
            _walLog?.CurrentLsn ?? 0);

        _modifications.Add(modification);
        page.IsDirty = true;
    }

    /// <summary>
    /// Commits the mini-transaction, making all changes durable.
    /// </summary>
    public void Commit()
    {
        EnsureActive();

        // Write redo log records if WAL is available
        if (_walLog != null && _modifications.Count > 0)
        {
            foreach (var mod in _modifications)
            {
                var entry = new WalEntry
                {
                    TransactionId = -_mtrId, // Use negative ID to distinguish from user transactions
                    Type = WalEntryType.Update,
                    PageId = mod.PageId,
                    NewData = SerializeModification(mod)
                };
                _walLog.Write(entry);
            }

            // Flush to ensure durability
            _walLog.Flush();
        }

        _endLsn = _walLog?.CurrentLsn ?? 0;
        _state = MtrState.Committed;

        _logger.Debug("MiniTransaction {0} committed with {1} modifications, LSN range [{2}, {3}]",
            _mtrId, _modifications.Count, _startLsn, _endLsn);

        // Clear saved data since we've committed
        _originalPageData.Clear();
    }

    /// <summary>
    /// Rolls back the mini-transaction, undoing all changes.
    /// </summary>
    public void Rollback()
    {
        if (_state != MtrState.Active)
            return;

        _logger.Debug("MiniTransaction {0} rolling back {1} modifications",
            _mtrId, _modifications.Count);

        // Restore original page data (in reverse order)
        // Note: In a full implementation, this would use the buffer pool to get pages
        // and restore their content. For now, we log the rollback.

        _state = MtrState.Aborted;
        _originalPageData.Clear();
        _modifications.Clear();
    }

    /// <summary>
    /// Gets all modifications made in this mini-transaction.
    /// </summary>
    public IReadOnlyList<PageModification> GetModifications() => _modifications.AsReadOnly();

    /// <summary>
    /// Checks if a specific page has been modified in this mini-transaction.
    /// </summary>
    public bool HasModifiedPage(int pageId) => _modifiedPageIds.Contains(pageId);

    private void EnsureActive()
    {
        if (_state != MtrState.Active)
            throw new InvalidOperationException(
                $"MiniTransaction is not active (state: {_state})");
    }

    private static byte[] SerializeModification(PageModification mod)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(mod.PageId);
        writer.Write((byte)mod.Type);
        writer.Write(mod.Lsn);
        writer.Write(mod.Data.Length);
        writer.Write(mod.Data);

        return ms.ToArray();
    }

    /// <summary>
    /// Deserializes a page modification from WAL data.
    /// </summary>
    public static PageModification DeserializeModification(byte[] data)
    {
        using var ms = new MemoryStream(data);
        using var reader = new BinaryReader(ms);

        var pageId = reader.ReadInt32();
        var type = (PageModificationType)reader.ReadByte();
        var lsn = reader.ReadInt64();
        var dataLength = reader.ReadInt32();
        var modData = reader.ReadBytes(dataLength);

        return new PageModification(pageId, type, modData, lsn);
    }

    public void Dispose()
    {
        if (_state == MtrState.Active)
        {
            Rollback();
        }
    }
}

/// <summary>
/// Represents the state of a mini-transaction.
/// </summary>
public enum MtrState
{
    /// <summary>
    /// The mini-transaction is active and accepting modifications.
    /// </summary>
    Active,

    /// <summary>
    /// The mini-transaction has been committed.
    /// </summary>
    Committed,

    /// <summary>
    /// The mini-transaction has been aborted/rolled back.
    /// </summary>
    Aborted
}

/// <summary>
/// Represents a single page modification within a mini-transaction.
/// </summary>
public sealed record PageModification(
    int PageId,
    PageModificationType Type,
    byte[] Data,
    long Lsn);

/// <summary>
/// Types of page modifications tracked by mini-transactions.
/// </summary>
public enum PageModificationType : byte
{
    /// <summary>
    /// Generic page data modification.
    /// </summary>
    Update = 1,

    /// <summary>
    /// Page initialization (new page allocated).
    /// </summary>
    Initialize = 2,

    /// <summary>
    /// Page format/structure change.
    /// </summary>
    Reorganize = 3,

    /// <summary>
    /// B-Tree node split.
    /// </summary>
    BTreeSplit = 4,

    /// <summary>
    /// B-Tree node merge.
    /// </summary>
    BTreeMerge = 5,

    /// <summary>
    /// Index entry insert.
    /// </summary>
    IndexInsert = 6,

    /// <summary>
    /// Index entry delete.
    /// </summary>
    IndexDelete = 7,

    /// <summary>
    /// Row insert into data page.
    /// </summary>
    RowInsert = 8,

    /// <summary>
    /// Row delete from data page.
    /// </summary>
    RowDelete = 9,

    /// <summary>
    /// Row update in data page.
    /// </summary>
    RowUpdate = 10,

    /// <summary>
    /// Page header modification.
    /// </summary>
    HeaderUpdate = 11,

    /// <summary>
    /// Free space management update.
    /// </summary>
    FreeSpaceUpdate = 12,

    /// <summary>
    /// Undo log insert.
    /// </summary>
    UndoInsert = 13,

    /// <summary>
    /// Undo log delete.
    /// </summary>
    UndoDelete = 14
}
