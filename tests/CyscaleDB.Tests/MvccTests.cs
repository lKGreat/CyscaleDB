using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.Mvcc;

namespace CyscaleDB.Tests;

public class MvccTests
{
    #region ReadView Tests

    [Fact]
    public void ReadView_Create_SetsCorrectValues()
    {
        var activeIds = new List<long> { 100, 102, 105 };
        var readView = ReadView.Create(activeIds, nextTransactionId: 110, creatorTransactionId: 103);

        Assert.Equal(103L, readView.CreatorTransactionId);
        Assert.Equal(110L, readView.MaxTransactionId);
        Assert.Equal(100L, readView.MinActiveTransactionId);
        Assert.Contains(100L, readView.ActiveTransactionIds);
        Assert.Contains(102L, readView.ActiveTransactionIds);
        Assert.Contains(105L, readView.ActiveTransactionIds);
        // Creator should be removed from active list
        Assert.DoesNotContain(103L, readView.ActiveTransactionIds);
    }

    [Fact]
    public void ReadView_Create_WithEmptyActiveList_UsesNextTransactionIdAsMin()
    {
        var readView = ReadView.Create([], nextTransactionId: 50, creatorTransactionId: 49);

        Assert.Equal(50L, readView.MinActiveTransactionId);
        Assert.Empty(readView.ActiveTransactionIds);
    }

    [Fact]
    public void IsVisible_OwnTransaction_ReturnsTrue()
    {
        var readView = ReadView.Create([100, 102], nextTransactionId: 110, creatorTransactionId: 105);

        // Own changes should always be visible
        Assert.True(readView.IsVisible(105));
    }

    [Fact]
    public void IsVisible_TransactionAfterSnapshot_ReturnsFalse()
    {
        var readView = ReadView.Create([100], nextTransactionId: 110, creatorTransactionId: 105);

        // Transactions started after snapshot should not be visible
        Assert.False(readView.IsVisible(110));
        Assert.False(readView.IsVisible(111));
        Assert.False(readView.IsVisible(1000));
    }

    [Fact]
    public void IsVisible_CommittedBeforeAllActive_ReturnsTrue()
    {
        var readView = ReadView.Create([100, 102, 105], nextTransactionId: 110, creatorTransactionId: 103);

        // Transactions committed before the minimum active transaction are visible
        Assert.True(readView.IsVisible(50));
        Assert.True(readView.IsVisible(99));
    }

    [Fact]
    public void IsVisible_ActiveTransaction_ReturnsFalse()
    {
        var readView = ReadView.Create([100, 102, 105], nextTransactionId: 110, creatorTransactionId: 103);

        // Active (uncommitted) transactions should not be visible
        Assert.False(readView.IsVisible(100));
        Assert.False(readView.IsVisible(102));
        Assert.False(readView.IsVisible(105));
    }

    [Fact]
    public void IsVisible_CommittedTransactionInRange_ReturnsTrue()
    {
        // Active: 100, 102, 105
        // Committed (not in active list): 101, 104, 106, 107, 108, 109
        var readView = ReadView.Create([100, 102, 105], nextTransactionId: 110, creatorTransactionId: 103);

        // Transactions between min active and max that are not in the active list are visible
        Assert.True(readView.IsVisible(101)); // Committed between active transactions
        Assert.True(readView.IsVisible(104)); // Committed between active transactions
        Assert.True(readView.IsVisible(106)); // Committed after 105 but before max
        Assert.True(readView.IsVisible(109)); // Just before max, committed
    }

    [Fact]
    public void IsVisible_ComplexScenario()
    {
        // Simulate a snapshot at time when:
        // - Transactions < 100 are all committed
        // - Transactions 100, 105 are active (uncommitted)
        // - Transactions 101, 102, 103, 104 are committed
        // - Transaction 106 is creating this ReadView
        // - Next transaction ID is 110
        var readView = ReadView.Create([100, 105, 106], nextTransactionId: 110, creatorTransactionId: 106);

        Assert.True(readView.IsVisible(50));   // Committed long ago
        Assert.True(readView.IsVisible(99));   // Committed just before min active
        Assert.False(readView.IsVisible(100)); // Active
        Assert.True(readView.IsVisible(101));  // Committed
        Assert.True(readView.IsVisible(102));  // Committed
        Assert.True(readView.IsVisible(103));  // Committed
        Assert.True(readView.IsVisible(104));  // Committed
        Assert.False(readView.IsVisible(105)); // Active
        Assert.True(readView.IsVisible(106));  // Own transaction
        Assert.True(readView.IsVisible(107));  // Committed
        Assert.True(readView.IsVisible(109));  // Committed
        Assert.False(readView.IsVisible(110)); // Started after snapshot
        Assert.False(readView.IsVisible(111)); // Started after snapshot
    }

    [Fact]
    public void IsRowVisible_VisibleNonDeletedRow_ReturnsTrue()
    {
        var readView = ReadView.Create([100], nextTransactionId: 110, creatorTransactionId: 105);
        var schema = CreateTestSchema();
        var values = CreateTestValues();
        
        // Row created by committed transaction
        var row = new Row(schema, values, transactionId: 50, isDeleted: false);

        Assert.True(readView.IsRowVisible(row));
    }

    [Fact]
    public void IsRowVisible_DeletedRow_ReturnsFalse()
    {
        var readView = ReadView.Create([100], nextTransactionId: 110, creatorTransactionId: 105);
        var schema = CreateTestSchema();
        var values = CreateTestValues();
        
        // Row deleted by committed transaction
        var row = new Row(schema, values, transactionId: 50, isDeleted: true);

        Assert.False(readView.IsRowVisible(row));
    }

    [Fact]
    public void IsRowVisible_OwnDeletedRow_ReturnsFalse()
    {
        var readView = ReadView.Create([100], nextTransactionId: 110, creatorTransactionId: 105);
        var schema = CreateTestSchema();
        var values = CreateTestValues();
        
        // Row deleted by own transaction
        var row = new Row(schema, values, transactionId: 105, isDeleted: true);

        Assert.False(readView.IsRowVisible(row));
    }

    [Fact]
    public void IsRowVisible_InvisibleTransaction_ReturnsFalse()
    {
        var readView = ReadView.Create([100], nextTransactionId: 110, creatorTransactionId: 105);
        var schema = CreateTestSchema();
        var values = CreateTestValues();
        
        // Row created by active transaction
        var row = new Row(schema, values, transactionId: 100, isDeleted: false);

        Assert.False(readView.IsRowVisible(row));
    }

    [Fact]
    public void ReadView_ToString_ContainsRelevantInfo()
    {
        var readView = ReadView.Create([100, 102], nextTransactionId: 110, creatorTransactionId: 105);

        var str = readView.ToString();

        Assert.Contains("105", str);  // Creator
        Assert.Contains("110", str);  // Max
        Assert.Contains("2", str);    // Active count
    }

    #endregion

    #region Helper Methods

    private static TableSchema CreateTestSchema()
    {
        var columns = new[]
        {
            new ColumnDefinition("id", DataType.Int, isPrimaryKey: true),
            new ColumnDefinition("name", DataType.VarChar, maxLength: 100),
        };
        return new TableSchema(1, "test_db", "test_table", columns);
    }

    private static DataValue[] CreateTestValues()
    {
        return
        [
            DataValue.FromInt(1),
            DataValue.FromVarChar("Test")
        ];
    }

    #endregion
}
