namespace CyscaleDB.Core.Storage.Mvcc;

/// <summary>
/// Represents a consistent snapshot of the database at a point in time for MVCC.
/// Used to determine which row versions are visible to a transaction.
/// 
/// ReadView captures the state of active transactions when created:
/// - ActiveTransactionIds: IDs of transactions that were active (uncommitted)
/// - MinActiveTransactionId: The smallest ID among active transactions
/// - MaxTransactionId: The next transaction ID that will be assigned
/// - CreatorTransactionId: The transaction that created this ReadView
/// 
/// Visibility rules:
/// 1. If row's TRX_ID < MinActiveTransactionId, the row is visible (committed before snapshot)
/// 2. If row's TRX_ID >= MaxTransactionId, the row is NOT visible (started after snapshot)
/// 3. If row's TRX_ID is in ActiveTransactionIds, the row is NOT visible (uncommitted)
/// 4. If row's TRX_ID == CreatorTransactionId, the row IS visible (own changes)
/// 5. Otherwise, the row is visible (committed and not active)
/// </summary>
public sealed class ReadView
{
    /// <summary>
    /// The set of transaction IDs that were active (uncommitted) when this ReadView was created.
    /// </summary>
    public IReadOnlySet<long> ActiveTransactionIds { get; }

    /// <summary>
    /// The smallest transaction ID that was active when this ReadView was created.
    /// Any transaction ID less than this is guaranteed to be committed.
    /// </summary>
    public long MinActiveTransactionId { get; }

    /// <summary>
    /// The next transaction ID that will be assigned after this ReadView was created.
    /// Any transaction ID >= this value started after the snapshot.
    /// </summary>
    public long MaxTransactionId { get; }

    /// <summary>
    /// The transaction ID that created this ReadView.
    /// Changes made by this transaction are always visible to itself.
    /// </summary>
    public long CreatorTransactionId { get; }

    /// <summary>
    /// The timestamp when this ReadView was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// Creates a new ReadView with the specified parameters.
    /// </summary>
    /// <param name="activeTransactionIds">IDs of active (uncommitted) transactions</param>
    /// <param name="minActiveTransactionId">Smallest active transaction ID</param>
    /// <param name="maxTransactionId">Next transaction ID to be assigned</param>
    /// <param name="creatorTransactionId">ID of the transaction creating this ReadView</param>
    public ReadView(
        IEnumerable<long> activeTransactionIds,
        long minActiveTransactionId,
        long maxTransactionId,
        long creatorTransactionId)
    {
        ActiveTransactionIds = activeTransactionIds.ToHashSet();
        MinActiveTransactionId = minActiveTransactionId;
        MaxTransactionId = maxTransactionId;
        CreatorTransactionId = creatorTransactionId;
        CreatedAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Creates a ReadView for a transaction from the current system state.
    /// </summary>
    /// <param name="activeTransactionIds">IDs of all currently active transactions</param>
    /// <param name="nextTransactionId">The next transaction ID that will be assigned</param>
    /// <param name="creatorTransactionId">ID of the transaction creating this ReadView</param>
    public static ReadView Create(
        IEnumerable<long> activeTransactionIds,
        long nextTransactionId,
        long creatorTransactionId)
    {
        var activeList = activeTransactionIds.ToList();
        
        // Remove the creator from active list (own changes should be visible)
        activeList.Remove(creatorTransactionId);
        
        var minActive = activeList.Count > 0 ? activeList.Min() : nextTransactionId;

        return new ReadView(activeList, minActive, nextTransactionId, creatorTransactionId);
    }

    /// <summary>
    /// Determines if a row version created by the given transaction is visible to this ReadView.
    /// </summary>
    /// <param name="rowTransactionId">The transaction ID that created/modified the row version</param>
    /// <returns>True if the row version is visible, false otherwise</returns>
    public bool IsVisible(long rowTransactionId)
    {
        // Rule 4: Own changes are always visible
        if (rowTransactionId == CreatorTransactionId)
            return true;

        // Rule 2: Transactions that started after the snapshot are not visible
        if (rowTransactionId >= MaxTransactionId)
            return false;

        // Rule 1: Transactions committed before any active transaction are visible
        if (rowTransactionId < MinActiveTransactionId)
            return true;

        // Rule 3: Active (uncommitted) transactions are not visible
        if (ActiveTransactionIds.Contains(rowTransactionId))
            return false;

        // Rule 5: Otherwise, the transaction was committed and is visible
        return true;
    }

    /// <summary>
    /// Determines if a row is visible considering both visibility and deletion status.
    /// </summary>
    /// <param name="row">The row to check</param>
    /// <returns>True if the row is visible and not deleted, false otherwise</returns>
    public bool IsRowVisible(Row row)
    {
        // First check if this version is visible
        if (!IsVisible(row.TransactionId))
            return false;

        // If deleted, need to check if the deletion is visible
        if (row.IsDeleted)
        {
            // If this is our own deletion, it's not visible (we deleted it)
            if (row.TransactionId == CreatorTransactionId)
                return false;
            
            // Otherwise, the row was deleted by a committed transaction
            return false;
        }

        return true;
    }

    public override string ToString()
    {
        return $"ReadView(Creator={CreatorTransactionId}, MinActive={MinActiveTransactionId}, Max={MaxTransactionId}, ActiveCount={ActiveTransactionIds.Count})";
    }
}

/// <summary>
/// Factory for creating ReadViews from the transaction manager.
/// </summary>
public interface IReadViewFactory
{
    /// <summary>
    /// Creates a new ReadView for the given transaction.
    /// </summary>
    ReadView CreateReadView(long transactionId);
}
