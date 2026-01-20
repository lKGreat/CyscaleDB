using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

public class RowTests
{
    private static TableSchema CreateTestSchema()
    {
        var columns = new[]
        {
            new ColumnDefinition("id", DataType.Int, isPrimaryKey: true),
            new ColumnDefinition("name", DataType.VarChar, maxLength: 100),
            new ColumnDefinition("age", DataType.Int, isNullable: true),
            new ColumnDefinition("active", DataType.Boolean),
            new ColumnDefinition("salary", DataType.Decimal, isNullable: true)
        };
        return new TableSchema(1, "test_db", "employees", columns);
    }

    [Fact]
    public void Constructor_ValidValues_CreatesRow()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John Doe"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.FromDecimal(50000.00m)
        };

        var row = new Row(schema, values);

        Assert.Equal(schema, row.Schema);
        Assert.Equal(5, row.Values.Length);
    }

    [Fact]
    public void Constructor_WrongValueCount_Throws()
    {
        var schema = CreateTestSchema();
        var values = new[] { DataValue.FromInt(1), DataValue.FromVarChar("John") };

        Assert.Throws<ArgumentException>(() => new Row(schema, values));
    }

    [Fact]
    public void GetValue_ByName_ReturnsCorrectValue()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John Doe"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.FromDecimal(50000.00m)
        };
        var row = new Row(schema, values);

        var nameValue = row.GetValue("name");

        Assert.Equal("John Doe", nameValue.AsString());
    }

    [Fact]
    public void GetValue_ByOrdinal_ReturnsCorrectValue()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John Doe"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.FromDecimal(50000.00m)
        };
        var row = new Row(schema, values);

        var idValue = row.GetValue(0);
        var nameValue = row.GetValue(1);

        Assert.Equal(1, idValue.AsInt());
        Assert.Equal("John Doe", nameValue.AsString());
    }

    [Fact]
    public void GetValue_InvalidColumnName_Throws()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.Null,
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var row = new Row(schema, values);

        Assert.Throws<ArgumentException>(() => row.GetValue("nonexistent"));
    }

    [Fact]
    public void SetValue_ByOrdinal_UpdatesValue()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var row = new Row(schema, values);

        row.SetValue(1, DataValue.FromVarChar("Jane"));

        Assert.Equal("Jane", row.GetValue("name").AsString());
    }

    [Fact]
    public void SetValue_ByName_UpdatesValue()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var row = new Row(schema, values);

        row.SetValue("age", DataValue.FromInt(31));

        Assert.Equal(31, row.GetValue("age").AsInt());
    }

    [Fact]
    public void Serialize_WithNulls_RoundTrips()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.Null, // age is null
            DataValue.FromBoolean(false),
            DataValue.Null  // salary is null
        };
        var original = new Row(schema, values);

        var bytes = original.Serialize();
        var restored = Row.Deserialize(bytes, schema);

        Assert.Equal(original.Values.Length, restored.Values.Length);
        Assert.Equal(1, restored.GetValue("id").AsInt());
        Assert.Equal("John", restored.GetValue("name").AsString());
        Assert.True(restored.GetValue("age").IsNull);
        Assert.False(restored.GetValue("active").AsBoolean());
        Assert.True(restored.GetValue("salary").IsNull);
    }

    [Fact]
    public void Serialize_AllValueTypes_RoundTrips()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(42),
            DataValue.FromVarChar("Test String"),
            DataValue.FromInt(25),
            DataValue.FromBoolean(true),
            DataValue.FromDecimal(12345.67m)
        };
        var original = new Row(schema, values);

        var bytes = original.Serialize();
        var restored = Row.Deserialize(bytes, schema);

        Assert.Equal(42, restored.GetValue(0).AsInt());
        Assert.Equal("Test String", restored.GetValue(1).AsString());
        Assert.Equal(25, restored.GetValue(2).AsInt());
        Assert.True(restored.GetValue(3).AsBoolean());
        Assert.Equal(12345.67m, restored.GetValue(4).AsDecimal());
    }

    [Fact]
    public void Clone_CreatesIndependentCopy()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Original"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var original = new Row(schema, values);
        original.RowId = new RowId(1, 0);

        var clone = original.Clone();

        // Clone should have same values
        Assert.Equal(original.GetValue("name").AsString(), clone.GetValue("name").AsString());
        Assert.Equal(original.RowId, clone.RowId);

        // Modifying clone should not affect original
        clone.SetValue("name", DataValue.FromVarChar("Modified"));
        Assert.Equal("Original", original.GetValue("name").AsString());
        Assert.Equal("Modified", clone.GetValue("name").AsString());
    }

    [Fact]
    public void RowId_InvalidStaticProperty_IsNotValid()
    {
        var rowId = RowId.Invalid;
        Assert.False(rowId.IsValid);
        Assert.Equal(-1, rowId.PageId);
        Assert.Equal(-1, rowId.SlotNumber);
    }

    [Fact]
    public void RowId_ValidValues_IsValid()
    {
        var rowId = new RowId(1, 5);
        Assert.True(rowId.IsValid);
        Assert.Equal(1, rowId.PageId);
        Assert.Equal(5, rowId.SlotNumber);
    }

    [Fact]
    public void RowId_Equality_Works()
    {
        var a = new RowId(1, 5);
        var b = new RowId(1, 5);
        var c = new RowId(1, 6);

        Assert.True(a == b);
        Assert.False(a == c);
        Assert.True(a != c);
    }

    #region MVCC Tests

    [Fact]
    public void Constructor_DefaultMvccValues_AreInitialized()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var row = new Row(schema, values);

        Assert.Equal(0L, row.TransactionId);
        Assert.Equal(RollPointerHelper.Invalid, row.RollPointer);
        Assert.False(row.IsDeleted);
    }

    [Fact]
    public void Constructor_WithMvccValues_SetsCorrectly()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var rollPointer = RollPointerHelper.Create(1, 100, 50);
        var row = new Row(schema, values, transactionId: 42, rollPointer: rollPointer, isDeleted: true);

        Assert.Equal(42L, row.TransactionId);
        Assert.Equal(rollPointer, row.RollPointer);
        Assert.True(row.IsDeleted);
    }

    [Fact]
    public void Serialize_WithMvcc_RoundTrips()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.FromDecimal(50000.00m)
        };
        var rollPointer = RollPointerHelper.Create(2, 500, 100);
        var original = new Row(schema, values, transactionId: 123, rollPointer: rollPointer, isDeleted: false);

        var bytes = original.Serialize();
        var restored = Row.Deserialize(bytes, schema);

        Assert.Equal(original.TransactionId, restored.TransactionId);
        Assert.Equal(original.RollPointer, restored.RollPointer);
        Assert.Equal(original.IsDeleted, restored.IsDeleted);
        Assert.Equal(original.Values.Length, restored.Values.Length);
        Assert.Equal(1, restored.GetValue("id").AsInt());
        Assert.Equal("John", restored.GetValue("name").AsString());
    }

    [Fact]
    public void Serialize_WithDeletedFlag_RoundTrips()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Deleted"),
            DataValue.Null,
            DataValue.FromBoolean(false),
            DataValue.Null
        };
        var original = new Row(schema, values, transactionId: 999, isDeleted: true);

        var bytes = original.Serialize();
        var restored = Row.Deserialize(bytes, schema);

        Assert.True(restored.IsDeleted);
        Assert.Equal(999L, restored.TransactionId);
    }

    [Fact]
    public void Clone_CopiesMvccFields()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Original"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var rollPointer = RollPointerHelper.Create(1, 200, 75);
        var original = new Row(schema, values, transactionId: 555, rollPointer: rollPointer, isDeleted: true);
        original.RowId = new RowId(1, 0);

        var clone = original.Clone();

        Assert.Equal(original.TransactionId, clone.TransactionId);
        Assert.Equal(original.RollPointer, clone.RollPointer);
        Assert.Equal(original.IsDeleted, clone.IsDeleted);
    }

    [Fact]
    public void CloneWithNewTransaction_UpdatesMvccFields()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Original"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var oldRollPointer = RollPointerHelper.Create(1, 100, 25);
        var original = new Row(schema, values, transactionId: 100, rollPointer: oldRollPointer);
        original.RowId = new RowId(1, 0);

        var newRollPointer = RollPointerHelper.Create(1, 200, 50);
        var updated = original.CloneWithNewTransaction(200, newRollPointer);

        Assert.Equal(200L, updated.TransactionId);
        Assert.Equal(newRollPointer, updated.RollPointer);
        Assert.Equal(original.RowId, updated.RowId);
        // Original should be unchanged
        Assert.Equal(100L, original.TransactionId);
    }

    [Fact]
    public void RollPointerHelper_Create_EncodesCorrectly()
    {
        var rollPointer = RollPointerHelper.Create(1, 12345, 100);

        Assert.Equal(1, RollPointerHelper.GetSegmentId(rollPointer));
        Assert.Equal(12345u, RollPointerHelper.GetPageNumber(rollPointer));
        Assert.Equal(100, RollPointerHelper.GetOffset(rollPointer));
    }

    [Fact]
    public void RollPointerHelper_Invalid_IsRecognized()
    {
        Assert.False(RollPointerHelper.IsValid(RollPointerHelper.Invalid));
        Assert.True(RollPointerHelper.IsValid(RollPointerHelper.Create(1, 1, 1)));
    }

    [Fact]
    public void RollPointerHelper_MaxValues_WorkCorrectly()
    {
        var rollPointer = RollPointerHelper.Create(ushort.MaxValue, uint.MaxValue, ushort.MaxValue);

        Assert.Equal(ushort.MaxValue, RollPointerHelper.GetSegmentId(rollPointer));
        Assert.Equal(uint.MaxValue, RollPointerHelper.GetPageNumber(rollPointer));
        Assert.Equal(ushort.MaxValue, RollPointerHelper.GetOffset(rollPointer));
    }

    [Fact]
    public void SerializeWithoutMvcc_DoesNotIncludeMvccHeader()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var row = new Row(schema, values, transactionId: 999);

        var withMvcc = row.Serialize();
        var withoutMvcc = row.SerializeWithoutMvcc();

        // Without MVCC should be smaller (by 17 bytes: 8 + 8 + 1)
        Assert.Equal(17, withMvcc.Length - withoutMvcc.Length);
    }

    [Fact]
    public void DeserializeWithoutMvcc_ReadsCorrectly()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Test"),
            DataValue.FromInt(25),
            DataValue.FromBoolean(false),
            DataValue.FromDecimal(1000m)
        };
        var original = new Row(schema, values);

        var bytes = original.SerializeWithoutMvcc();
        var restored = Row.DeserializeWithoutMvcc(bytes, schema);

        Assert.Equal(0L, restored.TransactionId);
        Assert.Equal(RollPointerHelper.Invalid, restored.RollPointer);
        Assert.False(restored.IsDeleted);
        Assert.Equal(1, restored.GetValue("id").AsInt());
        Assert.Equal("Test", restored.GetValue("name").AsString());
    }

    #endregion
}
