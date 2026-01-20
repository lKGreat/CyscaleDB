using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Transactions;

namespace CyscaleDB.Tests;

public class UndoLogTests : IDisposable
{
    private readonly string _testDirectory;

    public UndoLogTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "CyscaleDB_UndoTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, true);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    #region UndoRecord Tests

    [Fact]
    public void UndoRecord_CreateInsertUndo_SetsCorrectValues()
    {
        var rowId = new RowId(1, 0);
        var primaryKey = new[] { DataValue.FromInt(42) };

        var record = UndoRecord.CreateInsertUndo(
            transactionId: 100,
            tableId: 1,
            databaseName: "testdb",
            tableName: "users",
            rowId: rowId,
            primaryKeyValues: primaryKey);

        Assert.Equal(UndoRecordType.Insert, record.Type);
        Assert.Equal(100L, record.TransactionId);
        Assert.Equal(1, record.TableId);
        Assert.Equal("testdb", record.DatabaseName);
        Assert.Equal("users", record.TableName);
        Assert.Equal(rowId, record.RowId);
        Assert.Equal(0L, record.PreviousUndoPointer);
        Assert.NotEmpty(record.Data);
    }

    [Fact]
    public void UndoRecord_CreateUpdateUndo_SetsCorrectValues()
    {
        var rowId = new RowId(1, 0);
        var schema = CreateTestSchema();
        var oldRow = new Row(schema, new[] { DataValue.FromInt(1), DataValue.FromVarChar("OldName") });

        var record = UndoRecord.CreateUpdateUndo(
            transactionId: 100,
            tableId: 1,
            databaseName: "testdb",
            tableName: "users",
            rowId: rowId,
            oldRow: oldRow,
            previousUndoPointer: 50);

        Assert.Equal(UndoRecordType.Update, record.Type);
        Assert.Equal(100L, record.TransactionId);
        Assert.Equal(50L, record.PreviousUndoPointer);
    }

    [Fact]
    public void UndoRecord_CreateDeleteUndo_SetsCorrectValues()
    {
        var rowId = new RowId(1, 0);
        var schema = CreateTestSchema();
        var deletedRow = new Row(schema, new[] { DataValue.FromInt(1), DataValue.FromVarChar("DeletedName") });

        var record = UndoRecord.CreateDeleteUndo(
            transactionId: 100,
            tableId: 1,
            databaseName: "testdb",
            tableName: "users",
            rowId: rowId,
            deletedRow: deletedRow);

        Assert.Equal(UndoRecordType.Delete, record.Type);
    }

    [Fact]
    public void UndoRecord_SerializeDeserialize_Roundtrip()
    {
        var rowId = new RowId(5, 10);
        var primaryKey = new[] { DataValue.FromInt(42), DataValue.FromVarChar("key") };

        var original = UndoRecord.CreateInsertUndo(
            transactionId: 12345,
            tableId: 7,
            databaseName: "mydb",
            tableName: "mytable",
            rowId: rowId,
            primaryKeyValues: primaryKey,
            previousUndoPointer: 999);

        var serialized = original.Serialize();
        var deserialized = UndoRecord.Deserialize(serialized);

        Assert.Equal(original.Type, deserialized.Type);
        Assert.Equal(original.TransactionId, deserialized.TransactionId);
        Assert.Equal(original.TableId, deserialized.TableId);
        Assert.Equal(original.DatabaseName, deserialized.DatabaseName);
        Assert.Equal(original.TableName, deserialized.TableName);
        Assert.Equal(original.RowId, deserialized.RowId);
        Assert.Equal(original.PreviousUndoPointer, deserialized.PreviousUndoPointer);
        Assert.Equal(original.Data, deserialized.Data);
    }

    [Fact]
    public void UndoRecord_GetPrimaryKeyValues_ReturnsCorrectValues()
    {
        var primaryKey = new[] { DataValue.FromInt(42), DataValue.FromBigInt(9999L) };
        var keyTypes = new[] { DataType.Int, DataType.BigInt };

        var record = UndoRecord.CreateInsertUndo(
            transactionId: 100,
            tableId: 1,
            databaseName: "testdb",
            tableName: "users",
            rowId: new RowId(1, 0),
            primaryKeyValues: primaryKey);

        var retrievedKey = record.GetPrimaryKeyValues(keyTypes);

        Assert.Equal(2, retrievedKey.Length);
        Assert.Equal(42, retrievedKey[0].AsInt());
        Assert.Equal(9999L, retrievedKey[1].AsBigInt());
    }

    [Fact]
    public void UndoRecord_GetOldRow_ReturnsCorrectRow()
    {
        var schema = CreateTestSchema();
        var oldRow = new Row(schema, new[] { DataValue.FromInt(1), DataValue.FromVarChar("OldName") });

        var record = UndoRecord.CreateUpdateUndo(
            transactionId: 100,
            tableId: 1,
            databaseName: "testdb",
            tableName: "users",
            rowId: new RowId(1, 0),
            oldRow: oldRow);

        var retrievedRow = record.GetOldRow(schema);

        Assert.Equal(1, retrievedRow.Values[0].AsInt());
        Assert.Equal("OldName", retrievedRow.Values[1].AsString());
    }

    [Fact]
    public void UndoRecordBuilder_BuildsCorrectRecord()
    {
        var record = new UndoRecordBuilder()
            .WithType(UndoRecordType.Insert)
            .WithTransactionId(100)
            .WithTableId(1)
            .WithDatabaseName("testdb")
            .WithTableName("users")
            .WithRowId(new RowId(1, 0))
            .WithPreviousUndoPointer(50)
            .WithData(new byte[] { 1, 2, 3 })
            .Build();

        Assert.Equal(UndoRecordType.Insert, record.Type);
        Assert.Equal(100L, record.TransactionId);
        Assert.Equal("testdb", record.DatabaseName);
        Assert.Equal(new byte[] { 1, 2, 3 }, record.Data);
    }

    #endregion

    #region UndoLog Tests

    [Fact]
    public void UndoLog_OpenAndClose_Works()
    {
        using var undoLog = new UndoLog(_testDirectory);
        undoLog.Open();
        
        Assert.True(undoLog.CurrentUndoPointer > 0);
    }

    [Fact]
    public void UndoLog_WriteAndRead_InsertUndo()
    {
        using var undoLog = new UndoLog(_testDirectory);
        undoLog.Open();

        var primaryKey = new[] { DataValue.FromInt(42) };
        var pointer = undoLog.WriteInsertUndo(
            transactionId: 100,
            tableId: 1,
            databaseName: "testdb",
            tableName: "users",
            rowId: new RowId(1, 0),
            primaryKeyValues: primaryKey);

        Assert.True(pointer > 0);

        var record = undoLog.Read(pointer);

        Assert.NotNull(record);
        Assert.Equal(UndoRecordType.Insert, record.Type);
        Assert.Equal(100L, record.TransactionId);
    }

    [Fact]
    public void UndoLog_WriteAndRead_UpdateUndo()
    {
        using var undoLog = new UndoLog(_testDirectory);
        undoLog.Open();

        var schema = CreateTestSchema();
        var oldRow = new Row(schema, new[] { DataValue.FromInt(1), DataValue.FromVarChar("OldName") });

        var pointer = undoLog.WriteUpdateUndo(
            transactionId: 100,
            tableId: 1,
            databaseName: "testdb",
            tableName: "users",
            rowId: new RowId(1, 0),
            oldRow: oldRow);

        var record = undoLog.Read(pointer);

        Assert.NotNull(record);
        Assert.Equal(UndoRecordType.Update, record.Type);

        var retrievedRow = record.GetOldRow(schema);
        Assert.Equal("OldName", retrievedRow.Values[1].AsString());
    }

    [Fact]
    public void UndoLog_WriteAndRead_DeleteUndo()
    {
        using var undoLog = new UndoLog(_testDirectory);
        undoLog.Open();

        var schema = CreateTestSchema();
        var deletedRow = new Row(schema, new[] { DataValue.FromInt(1), DataValue.FromVarChar("DeletedName") });

        var pointer = undoLog.WriteDeleteUndo(
            transactionId: 100,
            tableId: 1,
            databaseName: "testdb",
            tableName: "users",
            rowId: new RowId(1, 0),
            deletedRow: deletedRow);

        var record = undoLog.Read(pointer);

        Assert.NotNull(record);
        Assert.Equal(UndoRecordType.Delete, record.Type);
    }

    [Fact]
    public void UndoLog_WriteMultipleRecords_CreatesChain()
    {
        using var undoLog = new UndoLog(_testDirectory);
        undoLog.Open();

        var schema = CreateTestSchema();
        var row1 = new Row(schema, new[] { DataValue.FromInt(1), DataValue.FromVarChar("Version1") });
        var row2 = new Row(schema, new[] { DataValue.FromInt(1), DataValue.FromVarChar("Version2") });

        // First update
        var ptr1 = undoLog.WriteUpdateUndo(100, 1, "testdb", "users", new RowId(1, 0), row1);

        // Second update with pointer to first
        var ptr2 = undoLog.WriteUpdateUndo(100, 1, "testdb", "users", new RowId(1, 0), row2, ptr1);

        var record2 = undoLog.Read(ptr2);
        Assert.NotNull(record2);
        Assert.Equal(ptr1, record2.PreviousUndoPointer);

        var record1 = undoLog.Read(record2.PreviousUndoPointer);
        Assert.NotNull(record1);
        Assert.Equal(0L, record1.PreviousUndoPointer);
    }

    [Fact]
    public void UndoLog_ReadTransactionUndos_ReturnsAllRecords()
    {
        using var undoLog = new UndoLog(_testDirectory);
        undoLog.Open();

        var schema = CreateTestSchema();
        var row1 = new Row(schema, new[] { DataValue.FromInt(1), DataValue.FromVarChar("V1") });
        var row2 = new Row(schema, new[] { DataValue.FromInt(1), DataValue.FromVarChar("V2") });
        var row3 = new Row(schema, new[] { DataValue.FromInt(1), DataValue.FromVarChar("V3") });

        var ptr1 = undoLog.WriteUpdateUndo(100, 1, "testdb", "users", new RowId(1, 0), row1);
        var ptr2 = undoLog.WriteUpdateUndo(100, 1, "testdb", "users", new RowId(1, 0), row2, ptr1);
        var ptr3 = undoLog.WriteUpdateUndo(100, 1, "testdb", "users", new RowId(1, 0), row3, ptr2);

        var records = undoLog.ReadTransactionUndos(100, ptr3);

        Assert.Equal(3, records.Count);
    }

    [Fact]
    public void UndoLog_Read_InvalidPointer_ReturnsNull()
    {
        using var undoLog = new UndoLog(_testDirectory);
        undoLog.Open();

        var record = undoLog.Read(999999);

        Assert.Null(record);
    }

    [Fact]
    public void UndoLog_CachesRecentRecords()
    {
        using var undoLog = new UndoLog(_testDirectory, cacheSize: 100);
        undoLog.Open();

        var primaryKey = new[] { DataValue.FromInt(42) };
        var pointer = undoLog.WriteInsertUndo(100, 1, "testdb", "users", new RowId(1, 0), primaryKey);

        // First read - from disk
        var record1 = undoLog.Read(pointer);
        // Second read - should be from cache
        var record2 = undoLog.Read(pointer);

        Assert.NotNull(record1);
        Assert.NotNull(record2);
        Assert.Equal(record1.TransactionId, record2.TransactionId);
    }

    [Fact]
    public void UndoLog_ReadVersionWithData_ReturnsRowVersion()
    {
        using var undoLog = new UndoLog(_testDirectory);
        undoLog.Open();

        var schema = CreateTestSchema();
        var oldRow = new Row(schema, new[] { DataValue.FromInt(1), DataValue.FromVarChar("TestName") }, transactionId: 50);

        var pointer = undoLog.WriteUpdateUndo(100, 1, "testdb", "users", new RowId(1, 0), oldRow, previousUndoPointer: 25);

        var version = undoLog.ReadVersionWithData(pointer, schema);

        Assert.NotNull(version);
        Assert.Equal(100L, version.TransactionId);
        Assert.Equal(25L, version.RollPointer);
        Assert.False(version.IsDeleted);
        Assert.NotNull(version.RowData);
        Assert.Equal("TestName", version.RowData.Values[1].AsString());
    }

    [Fact]
    public void UndoLog_Persistence_DataSurvivesReopen()
    {
        var primaryKey = new[] { DataValue.FromInt(42) };
        long pointer;

        // Write and close
        using (var undoLog = new UndoLog(_testDirectory))
        {
            undoLog.Open();
            pointer = undoLog.WriteInsertUndo(100, 1, "testdb", "users", new RowId(1, 0), primaryKey);
            undoLog.Flush();
        }

        // Reopen and read
        using (var undoLog = new UndoLog(_testDirectory))
        {
            undoLog.Open();
            var record = undoLog.Read(pointer);

            Assert.NotNull(record);
            Assert.Equal(UndoRecordType.Insert, record.Type);
            Assert.Equal(100L, record.TransactionId);
        }
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
        return new TableSchema(1, "testdb", "users", columns);
    }

    #endregion
}
