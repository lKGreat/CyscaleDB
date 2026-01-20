using NUnit.Framework;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

[TestFixture]
public class OptimizationTests
{
    private string _testDir = null!;

    [SetUp]
    public void SetUp()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"CyscaleDB_OptTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    [TearDown]
    public void TearDown()
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

    [Test]
    public void Table_GetStatistics_EmptyTable()
    {
        using var table = CreateTestTable();

        var stats = table.GetStatistics();

        Assert.Multiple(() =>
        {
            Assert.That(stats.RowCount, Is.EqualTo(0));
            Assert.That(stats.PageCount, Is.EqualTo(0));
            Assert.That(stats.DataSize, Is.EqualTo(0));
        });
    }

    [Test]
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

        Assert.Multiple(() =>
        {
            Assert.That(stats.RowCount, Is.EqualTo(10));
            Assert.That(stats.PageCount, Is.GreaterThan(0));
            Assert.That(stats.DataSize, Is.GreaterThan(0));
        });
    }

    [Test]
    public void Table_Optimize_EmptyTable()
    {
        using var table = CreateTestTable();

        var result = table.Optimize();

        Assert.Multiple(() =>
        {
            Assert.That(result.RowsProcessed, Is.EqualTo(0));
            Assert.That(result.NewPageCount, Is.EqualTo(0));
        });
    }

    [Test]
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

        Assert.Multiple(() =>
        {
            Assert.That(result.RowsProcessed, Is.EqualTo(10)); // Half deleted
            Assert.That(result.SpaceReclaimed, Is.GreaterThanOrEqualTo(0));
        });
    }

    [Test]
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
        Assert.That(remaining, Has.Count.EqualTo(3));
    }

    [Test]
    public void OptimizeResult_ToString()
    {
        var result = new OptimizeResult(
            rowsProcessed: 100,
            originalPageCount: 10,
            newPageCount: 5,
            spaceReclaimed: 20480,
            duration: TimeSpan.FromMilliseconds(150));

        var str = result.ToString();

        Assert.That(str, Does.Contain("100 rows"));
        Assert.That(str, Does.Contain("10 -> 5 pages"));
        Assert.That(str, Does.Contain("20480 bytes"));
    }

    [Test]
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

        Assert.That(str, Does.Contain("1000 rows"));
        Assert.That(str, Does.Contain("50 pages"));
    }

    [Test]
    public void PageManager_Truncate()
    {
        var filePath = Path.Combine(_testDir, "truncate_test.cdb");
        using var pm = new PageManager(filePath);
        pm.Open(createIfNotExists: true);

        // Allocate some pages
        pm.AllocatePage();
        pm.AllocatePage();
        pm.AllocatePage();

        Assert.That(pm.PageCount, Is.EqualTo(3));

        // Truncate to 1 page
        pm.Truncate(1);

        Assert.That(pm.PageCount, Is.EqualTo(1));
    }
}
