using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;
using Xunit;

namespace CyscaleDB.Tests;

public class RowLazyColumnTests
{
    private TableSchema CreateTestSchema()
    {
        var columns = new List<ColumnDefinition>
        {
            new ColumnDefinition("id", DataType.Int, isPrimaryKey: true),
            new ColumnDefinition("name", DataType.VarChar, maxLength: 50),
            new ColumnDefinition("age", DataType.Int)
        };

        return new TableSchema(1, "test", "users", columns);
    }

    [Fact]
    public void MarkColumnAsLazy_ShouldMarkColumn()
    {
        // Arrange
        var schema = CreateTestSchema();
        var values = new DataValue[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Alice"),
            DataValue.FromInt(30)
        };
        var row = new Row(schema, values);

        // Act
        row.MarkColumnAsLazy(2);  // Mark 'age' as lazy

        // Assert
        Assert.True(row.IsColumnLazy(2));
        Assert.Equal(1, row.LazyColumnCount);
    }

    [Fact]
    public void GetValue_OnLazyColumn_ShouldReturnDefaultValue()
    {
        // Arrange
        var columns = new List<ColumnDefinition>
        {
            new ColumnDefinition("id", DataType.Int, isPrimaryKey: true),
            new ColumnDefinition("name", DataType.VarChar, maxLength: 50),
            new ColumnDefinition("email", DataType.VarChar, maxLength: 100, defaultValue: DataValue.FromVarChar("default@example.com"))
        };
        var schema = new TableSchema(1, "test", "users", columns);

        var values = new DataValue[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Alice"),
            DataValue.FromVarChar("")  // Will be marked as lazy
        };
        var row = new Row(schema, values);

        // Mark email as lazy (simulating online ADD COLUMN)
        row.MarkColumnAsLazy(2);

        // Act
        var email = row.GetValue("email");

        // Assert
        Assert.Equal("default@example.com", email.AsString());
    }

    [Fact]
    public void GetValue_OnLazyColumnWithoutDefault_ShouldReturnNull()
    {
        // Arrange
        var columns = new List<ColumnDefinition>
        {
            new ColumnDefinition("id", DataType.Int, isPrimaryKey: true),
            new ColumnDefinition("name", DataType.VarChar, maxLength: 50),
            new ColumnDefinition("status", DataType.VarChar, maxLength: 20)  // No default
        };
        var schema = new TableSchema(1, "test", "users", columns);

        var values = new DataValue[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Alice"),
            DataValue.FromVarChar("")
        };
        var row = new Row(schema, values);

        row.MarkColumnAsLazy(2);

        // Act
        var status = row.GetValue("status");

        // Assert
        Assert.True(status.IsNull);
    }

    [Fact]
    public void BackfillColumn_ShouldFillLazyColumn()
    {
        // Arrange
        var schema = CreateTestSchema();
        var values = new DataValue[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Alice"),
            DataValue.FromInt(0)
        };
        var row = new Row(schema, values);
        
        row.MarkColumnAsLazy(2);
        Assert.True(row.IsColumnLazy(2));

        // Act
        row.BackfillColumn(2, DataValue.FromInt(30));

        // Assert
        Assert.False(row.IsColumnLazy(2));
        Assert.Equal(30, row.GetValue(2).AsInt());
        Assert.Equal(0, row.LazyColumnCount);
    }

    [Fact]
    public void Clone_ShouldCopyLazyColumns()
    {
        // Arrange
        var schema = CreateTestSchema();
        var values = new DataValue[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Alice"),
            DataValue.FromInt(30)
        };
        var row = new Row(schema, values);
        row.MarkColumnAsLazy(2);

        // Act
        var cloned = row.Clone();

        // Assert
        Assert.True(cloned.IsColumnLazy(2));
        Assert.Equal(1, cloned.LazyColumnCount);
    }

    [Fact]
    public void CloneWithNewTransaction_ShouldCopyLazyColumns()
    {
        // Arrange
        var schema = CreateTestSchema();
        var values = new DataValue[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Alice"),
            DataValue.FromInt(30)
        };
        var row = new Row(schema, values);
        row.MarkColumnAsLazy(2);

        // Act
        var cloned = row.CloneWithNewTransaction(100, 200);

        // Assert
        Assert.True(cloned.IsColumnLazy(2));
        Assert.Equal(1, cloned.LazyColumnCount);
        Assert.Equal(100, cloned.TransactionId);
        Assert.Equal(200, cloned.RollPointer);
    }

    [Fact]
    public void MultipleLazyColumns_ShouldWorkCorrectly()
    {
        // Arrange
        var columns = new List<ColumnDefinition>
        {
            new ColumnDefinition("id", DataType.Int, isPrimaryKey: true),
            new ColumnDefinition("col1", DataType.Int, defaultValue: DataValue.FromInt(1)),
            new ColumnDefinition("col2", DataType.Int, defaultValue: DataValue.FromInt(2)),
            new ColumnDefinition("col3", DataType.Int, defaultValue: DataValue.FromInt(3))
        };
        var schema = new TableSchema(1, "test", "table", columns);

        var values = new DataValue[]
        {
            DataValue.FromInt(100),
            DataValue.FromInt(0),
            DataValue.FromInt(0),
            DataValue.FromInt(0)
        };
        var row = new Row(schema, values);

        // Mark col1, col2, col3 as lazy
        row.MarkColumnAsLazy(1);
        row.MarkColumnAsLazy(2);
        row.MarkColumnAsLazy(3);

        // Assert initial state
        Assert.Equal(3, row.LazyColumnCount);

        // Act & Assert - Get lazy values
        Assert.Equal(1, row.GetValue(1).AsInt());  // Returns default
        Assert.Equal(2, row.GetValue(2).AsInt());  // Returns default
        Assert.Equal(3, row.GetValue(3).AsInt());  // Returns default

        // Backfill one column
        row.BackfillColumn(1, DataValue.FromInt(10));
        Assert.Equal(2, row.LazyColumnCount);
        Assert.Equal(10, row.GetValue(1).AsInt());  // Returns actual value now

        // Backfill remaining columns
        row.BackfillColumn(2, DataValue.FromInt(20));
        row.BackfillColumn(3, DataValue.FromInt(30));
        Assert.Equal(0, row.LazyColumnCount);
    }
}
