using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

public class OptimizationTests : IDisposable
{
    private readonly string _testDir;

    public OptimizationTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"CyscaleDB_OptTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, true);
        }
    }

    private Table CreateTestTable(string tableName = "test_table")
    {
        var columns = new List<ColumnDefinition>
        {
            new("id", DataType.Int, nullable: false, isPrimaryKey: true),
            new("name", DataType.VarChar, 100),
            new("value", DataType.Int)
        };
        var schema = new TableSchema(1, "testdb", tableName, columns);

        var filePath = Path.Combine(_testDir, $"{tableName}.cdb");
        var pageManager = new PageManager(filePath);
        var table = new Table(schema, pageManager);
        table.Open(createIfNotExists: true);

        return table;
    }

    [Fact]
    public void Table_GetStatistics_EmptyTable()
    {
        using var table = CreateTestTable();

        var stats = table.GetStatistics();

        Assert.Equal(0, stats.RowCount);
        Assert.Equal(0, stats.PageCount);
        Assert.Equal(0, stats.DataSize);
    }

    [Fact]
    public void Table_GetStatistics_WithData()
    {
        using var table = CreateTestTable();

        // Insert some rows
        for (int i = 1; i <= 10; i++)
        {
            var row = new Row(table.Schema, [
                DataValue.FromInt(i),
                DataValue.FromVarChar($"Name{i}"),
                DataValue.FromInt(i * 100)
            ]);
            table.InsertRow(row);
        }

        var stats = table.GetStatistics();

        Assert.Equal(10, stats.RowCount);
        Assert.True(stats.PageCount > 0);
        Assert.True(stats.DataSize > 0);
    }

    [Fact]
    public void Table_Optimize_EmptyTable()
    {
        using var table = CreateTestTable();

        var result = table.Optimize();

        Assert.Equal(0, result.RowsProcessed);
        Assert.Equal(0, result.NewPageCount);
    }

    [Fact]
    public void Table_Optimize_WithDeletedRows()
    {
        using var table = CreateTestTable("optimize_test");

        // Insert rows
        var rowIds = new List<RowId>();
        for (int i = 1; i <= 20; i++)
        {
            var row = new Row(table.Schema, [
                DataValue.FromInt(i),
                DataValue.FromVarChar($"Name{i}"),
                DataValue.FromInt(i * 100)
            ]);
            rowIds.Add(table.InsertRow(row));
        }

        // Delete every other row
        for (int i = 0; i < rowIds.Count; i += 2)
        {
            table.DeleteRow(rowIds[i]);
        }

        var statsBefore = table.GetStatistics();

        // Optimize
        var result = table.Optimize();

        var statsAfter = table.GetStatistics();

        Assert.Equal(10, result.RowsProcessed); // Half deleted
        Assert.True(result.SpaceReclaimed >= 0);
    }

    [Fact]
    public void Table_CompactPages()
    {
        using var table = CreateTestTable();

        // Insert and delete to create fragmentation
        var rowIds = new List<RowId>();
        for (int i = 1; i <= 5; i++)
        {
            var row = new Row(table.Schema, [
                DataValue.FromInt(i),
                DataValue.FromVarChar($"Name{i}"),
                DataValue.FromInt(i)
            ]);
            rowIds.Add(table.InsertRow(row));
        }

        // Delete some rows
        table.DeleteRow(rowIds[1]);
        table.DeleteRow(rowIds[3]);

        // Compact
        var compactedCount = table.CompactPages();

        // Verify rows still accessible
        var remaining = table.ScanTable().ToList();
        Assert.Equal(3, remaining.Count);
    }

    [Fact]
    public void OptimizeResult_ToString()
    {
        var result = new OptimizeResult(
            rowsProcessed: 100,
            originalPageCount: 10,
            newPageCount: 5,
            spaceReclaimed: 20480,
            duration: TimeSpan.FromMilliseconds(150));

        var str = result.ToString();

        Assert.Contains("100 rows", str);
        Assert.Contains("10 -> 5 pages", str);
        Assert.Contains("20480 bytes", str);
    }

    [Fact]
    public void TableStatistics_ToString()
    {
        var stats = new TableStatistics(
            rowCount: 1000,
            pageCount: 50,
            dataSize: 102400,
            freeSpace: 10240,
            emptyPages: 2,
            fragmentedPages: 5,
            fragmentationPercent: 10.0);

        var str = stats.ToString();

        Assert.Contains("1000 rows", str);
        Assert.Contains("50 pages", str);
    }

    [Fact]
    public void PageManager_Truncate()
    {
        var filePath = Path.Combine(_testDir, "truncate_test.cdb");
        using var pm = new PageManager(filePath);
        pm.Open(createIfNotExists: true);

        // Allocate some pages
        pm.AllocatePage();
        pm.AllocatePage();
        pm.AllocatePage();

        Assert.Equal(3, pm.PageCount);

        // Truncate to 1 page
        pm.Truncate(1);

        Assert.Equal(1, pm.PageCount);
    }
}
