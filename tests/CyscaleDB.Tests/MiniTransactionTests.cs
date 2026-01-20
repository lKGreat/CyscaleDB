using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Transactions;
using Xunit;

namespace CyscaleDB.Tests;

public class MiniTransactionTests : IDisposable
{
    private readonly string _testDir;
    private readonly WalLog _walLog;

    public MiniTransactionTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"cyscaledb_mtr_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
        _walLog = new WalLog(_testDir);
        _walLog.Open();
    }

    public void Dispose()
    {
        _walLog.Dispose();
        if (Directory.Exists(_testDir))
        {
            try { Directory.Delete(_testDir, true); }
            catch { /* ignore cleanup errors */ }
        }
    }

    #region Basic Lifecycle Tests

    [Fact]
    public void MiniTransaction_NewTransaction_IsActive()
    {
        using var mtr = new MiniTransaction();

        Assert.Equal(MtrState.Active, mtr.State);
        Assert.True(mtr.MtrId > 0);
    }

    [Fact]
    public void MiniTransaction_Commit_ChangesStateToCommitted()
    {
        using var mtr = new MiniTransaction();

        mtr.Commit();

        Assert.Equal(MtrState.Committed, mtr.State);
    }

    [Fact]
    public void MiniTransaction_Rollback_ChangesStateToAborted()
    {
        using var mtr = new MiniTransaction();

        mtr.Rollback();

        Assert.Equal(MtrState.Aborted, mtr.State);
    }

    [Fact]
    public void MiniTransaction_Dispose_RollsBackIfActive()
    {
        var mtr = new MiniTransaction();
        Assert.Equal(MtrState.Active, mtr.State);

        mtr.Dispose();

        Assert.Equal(MtrState.Aborted, mtr.State);
    }

    [Fact]
    public void MiniTransaction_CommitAfterCommit_ThrowsException()
    {
        using var mtr = new MiniTransaction();
        mtr.Commit();

        Assert.Throws<InvalidOperationException>(() => mtr.Commit());
    }

    #endregion

    #region Modification Recording Tests

    [Fact]
    public void RecordModification_SinglePage_IncrementsModifiedCount()
    {
        using var mtr = new MiniTransaction();
        var page = new Page(1, PageType.Data);

        mtr.RecordModification(page, PageModificationType.Update);

        Assert.Equal(1, mtr.ModifiedPageCount);
        Assert.True(mtr.HasModifiedPage(1));
    }

    [Fact]
    public void RecordModification_SamePage_DoesNotIncrementCount()
    {
        using var mtr = new MiniTransaction();
        var page = new Page(1, PageType.Data);

        mtr.RecordModification(page, PageModificationType.Update);
        mtr.RecordModification(page, PageModificationType.RowInsert);
        mtr.RecordModification(page, PageModificationType.HeaderUpdate);

        // Same page modified multiple times - count should still be 1
        Assert.Equal(1, mtr.ModifiedPageCount);
        Assert.Equal(3, mtr.GetModifications().Count);
    }

    [Fact]
    public void RecordModification_MultiplePages_TracksAll()
    {
        using var mtr = new MiniTransaction();
        var page1 = new Page(1, PageType.Data);
        var page2 = new Page(2, PageType.Data);
        var page3 = new Page(3, PageType.Index);

        mtr.RecordModification(page1, PageModificationType.RowInsert);
        mtr.RecordModification(page2, PageModificationType.RowInsert);
        mtr.RecordModification(page3, PageModificationType.IndexInsert);

        Assert.Equal(3, mtr.ModifiedPageCount);
        Assert.True(mtr.HasModifiedPage(1));
        Assert.True(mtr.HasModifiedPage(2));
        Assert.True(mtr.HasModifiedPage(3));
        Assert.False(mtr.HasModifiedPage(4));
    }

    [Fact]
    public void RecordModification_MarksPageDirty()
    {
        using var mtr = new MiniTransaction();
        var page = new Page(1, PageType.Data);
        Assert.False(page.IsDirty);

        mtr.RecordModification(page, PageModificationType.Update);

        Assert.True(page.IsDirty);
    }

    [Fact]
    public void RecordModification_WithData_StoresModificationData()
    {
        using var mtr = new MiniTransaction();
        var page = new Page(1, PageType.Data);
        var testData = new byte[] { 1, 2, 3, 4, 5 };

        mtr.RecordModification(page, PageModificationType.Update, testData);

        var modifications = mtr.GetModifications();
        Assert.Single(modifications);
        Assert.Equal(testData, modifications[0].Data);
    }

    [Fact]
    public void RecordModification_AfterCommit_ThrowsException()
    {
        using var mtr = new MiniTransaction();
        var page = new Page(1, PageType.Data);
        mtr.Commit();

        Assert.Throws<InvalidOperationException>(() =>
            mtr.RecordModification(page, PageModificationType.Update));
    }

    [Fact]
    public void RecordModification_WithBeforeAfterImages()
    {
        using var mtr = new MiniTransaction();
        var page = new Page(1, PageType.Data);
        var oldData = new byte[] { 0, 0, 0 };
        var newData = new byte[] { 1, 2, 3 };

        mtr.RecordModification(page, PageModificationType.RowUpdate, 100, oldData, newData);

        var modifications = mtr.GetModifications();
        Assert.Single(modifications);
        Assert.Equal(PageModificationType.RowUpdate, modifications[0].Type);
    }

    #endregion

    #region WAL Integration Tests

    [Fact]
    public void Commit_WithWal_WritesLogEntries()
    {
        var initialLsn = _walLog.CurrentLsn;

        using var mtr = new MiniTransaction(_walLog);
        var page = new Page(1, PageType.Data);

        mtr.RecordModification(page, PageModificationType.RowInsert);
        mtr.Commit();

        // WAL should have advanced
        Assert.True(_walLog.CurrentLsn > initialLsn);
        Assert.True(mtr.EndLsn >= mtr.StartLsn);
    }

    [Fact]
    public void Commit_NoWal_DoesNotThrow()
    {
        // MiniTransaction without WAL should still work
        using var mtr = new MiniTransaction(walLog: null);
        var page = new Page(1, PageType.Data);

        mtr.RecordModification(page, PageModificationType.Update);

        var ex = Record.Exception(() => mtr.Commit());
        Assert.Null(ex);
    }

    [Fact]
    public void Commit_EmptyTransaction_Succeeds()
    {
        using var mtr = new MiniTransaction(_walLog);

        // No modifications
        mtr.Commit();

        Assert.Equal(MtrState.Committed, mtr.State);
    }

    #endregion

    #region Serialization Tests

    [Fact]
    public void PageModification_SerializeDeserialize_RoundTrips()
    {
        var original = new PageModification(
            PageId: 42,
            Type: PageModificationType.BTreeSplit,
            Data: [1, 2, 3, 4, 5],
            Lsn: 12345
        );

        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(original.PageId);
        writer.Write((byte)original.Type);
        writer.Write(original.Lsn);
        writer.Write(original.Data.Length);
        writer.Write(original.Data);

        var serialized = ms.ToArray();
        var deserialized = MiniTransaction.DeserializeModification(serialized);

        Assert.Equal(original.PageId, deserialized.PageId);
        Assert.Equal(original.Type, deserialized.Type);
        Assert.Equal(original.Lsn, deserialized.Lsn);
        Assert.Equal(original.Data, deserialized.Data);
    }

    #endregion

    #region B-Tree Operation Tests

    [Fact]
    public void BTreeSplit_RecordsMultiplePageModifications()
    {
        using var mtr = new MiniTransaction(_walLog);

        // Simulate a B-Tree split that touches multiple pages
        var originalPage = new Page(1, PageType.Index);
        var newPage = new Page(2, PageType.Index);
        var parentPage = new Page(0, PageType.Index);

        // Split operations
        mtr.RecordModification(originalPage, PageModificationType.BTreeSplit);
        mtr.RecordModification(newPage, PageModificationType.Initialize);
        mtr.RecordModification(parentPage, PageModificationType.IndexInsert);

        mtr.Commit();

        Assert.Equal(3, mtr.ModifiedPageCount);
        Assert.Equal(MtrState.Committed, mtr.State);

        var mods = mtr.GetModifications();
        Assert.Equal(PageModificationType.BTreeSplit, mods[0].Type);
        Assert.Equal(PageModificationType.Initialize, mods[1].Type);
        Assert.Equal(PageModificationType.IndexInsert, mods[2].Type);
    }

    #endregion

    #region Concurrent MiniTransaction Tests

    [Fact]
    public void ConcurrentMiniTransactions_HaveUniqueIds()
    {
        var mtrIds = new List<long>();
        var lockObj = new object();

        Parallel.For(0, 100, _ =>
        {
            using var mtr = new MiniTransaction();
            lock (lockObj)
            {
                mtrIds.Add(mtr.MtrId);
            }
            mtr.Commit();
        });

        // All IDs should be unique
        Assert.Equal(100, mtrIds.Distinct().Count());
    }

    #endregion
}
