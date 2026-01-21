using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.OnlineDdl;
using Xunit;

namespace CyscaleDB.Tests;

public class OnlineDdlTests : IDisposable
{
    private readonly OnlineDdlManager _manager;

    public OnlineDdlTests()
    {
        _manager = new OnlineDdlManager();
    }

    [Fact]
    public void BeginOnlineDdl_ShouldCreateChangeLog()
    {
        // Arrange
        var dbName = "testdb";
        var tableName = "users";
        var operation = OnlineDdlOperation.AddColumn;

        // Act
        var result = _manager.BeginOnlineDdl(dbName, tableName, operation);

        // Assert
        Assert.True(result);
        Assert.True(_manager.IsOnlineDdlInProgress(dbName, tableName));
        
        var changeLog = _manager.GetChangeLog(dbName, tableName);
        Assert.NotNull(changeLog);
        Assert.Equal(dbName, changeLog.DatabaseName);
        Assert.Equal(tableName, changeLog.TableName);
        Assert.Equal(operation, changeLog.Operation);
    }

    [Fact]
    public void BeginOnlineDdl_WhenAlreadyInProgress_ShouldReturnFalse()
    {
        // Arrange
        var dbName = "testdb";
        var tableName = "users";
        _manager.BeginOnlineDdl(dbName, tableName, OnlineDdlOperation.AddColumn);

        // Act
        var result = _manager.BeginOnlineDdl(dbName, tableName, OnlineDdlOperation.AddIndex);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void LogDmlChange_ShouldRecordChange()
    {
        // Arrange
        var dbName = "testdb";
        var tableName = "users";
        _manager.BeginOnlineDdl(dbName, tableName, OnlineDdlOperation.AddColumn);

        var rowId = new RowId(1, 0);
        var newRowData = new byte[] { 1, 2, 3, 4, 5 };
        var change = DmlChange.CreateInsert(rowId, newRowData);

        // Act
        _manager.LogDmlChange(dbName, tableName, change);

        // Assert
        var changeLog = _manager.GetChangeLog(dbName, tableName);
        Assert.NotNull(changeLog);
        Assert.Equal(1, changeLog.ChangeCount);
    }

    [Fact]
    public void CommitOnlineDdl_ShouldReturnChangesAndCleanup()
    {
        // Arrange
        var dbName = "testdb";
        var tableName = "users";
        _manager.BeginOnlineDdl(dbName, tableName, OnlineDdlOperation.AddColumn);

        var change1 = DmlChange.CreateInsert(new RowId(1, 0), new byte[] { 1, 2, 3 });
        var change2 = DmlChange.CreateUpdate(new RowId(1, 1), new byte[] { 4, 5, 6 }, new byte[] { 7, 8, 9 });
        
        _manager.LogDmlChange(dbName, tableName, change1);
        _manager.LogDmlChange(dbName, tableName, change2);

        // Act
        var changes = _manager.CommitOnlineDdl(dbName, tableName);

        // Assert
        Assert.Equal(2, changes.Count);
        Assert.False(_manager.IsOnlineDdlInProgress(dbName, tableName));
    }

    [Fact]
    public void RollbackOnlineDdl_ShouldDiscardChanges()
    {
        // Arrange
        var dbName = "testdb";
        var tableName = "users";
        _manager.BeginOnlineDdl(dbName, tableName, OnlineDdlOperation.AddIndex);
        
        var change = DmlChange.CreateInsert(new RowId(1, 0), new byte[] { 1, 2, 3 });
        _manager.LogDmlChange(dbName, tableName, change);

        // Act
        _manager.RollbackOnlineDdl(dbName, tableName);

        // Assert
        Assert.False(_manager.IsOnlineDdlInProgress(dbName, tableName));
        Assert.Null(_manager.GetChangeLog(dbName, tableName));
    }

    [Fact]
    public void DmlChange_CreateInsert_ShouldHaveCorrectProperties()
    {
        // Arrange
        var rowId = new RowId(5, 10);
        var newData = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        var change = DmlChange.CreateInsert(rowId, newData);

        // Assert
        Assert.Equal(DmlChangeType.Insert, change.Type);
        Assert.Equal(rowId, change.RowId);
        Assert.Null(change.OldRowData);
        Assert.NotNull(change.NewRowData);
        Assert.Equal(newData, change.NewRowData);
    }

    [Fact]
    public void DmlChange_CreateUpdate_ShouldHaveBothOldAndNew()
    {
        // Arrange
        var rowId = new RowId(5, 10);
        var oldData = new byte[] { 1, 2, 3 };
        var newData = new byte[] { 4, 5, 6 };

        // Act
        var change = DmlChange.CreateUpdate(rowId, oldData, newData);

        // Assert
        Assert.Equal(DmlChangeType.Update, change.Type);
        Assert.Equal(rowId, change.RowId);
        Assert.NotNull(change.OldRowData);
        Assert.NotNull(change.NewRowData);
        Assert.Equal(oldData, change.OldRowData);
        Assert.Equal(newData, change.NewRowData);
    }

    [Fact]
    public void DmlChange_CreateDelete_ShouldHaveOnlyOldData()
    {
        // Arrange
        var rowId = new RowId(5, 10);
        var oldData = new byte[] { 1, 2, 3 };

        // Act
        var change = DmlChange.CreateDelete(rowId, oldData);

        // Assert
        Assert.Equal(DmlChangeType.Delete, change.Type);
        Assert.Equal(rowId, change.RowId);
        Assert.NotNull(change.OldRowData);
        Assert.Null(change.NewRowData);
        Assert.Equal(oldData, change.OldRowData);
    }

    public void Dispose()
    {
        _manager.Dispose();
    }
}
