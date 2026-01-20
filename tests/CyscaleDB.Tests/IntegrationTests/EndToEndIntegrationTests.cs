using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Transactions;

namespace CyscaleDB.Tests.IntegrationTests;

/// <summary>
/// End-to-end integration tests covering complete SQL workflows.
/// </summary>
public class EndToEndIntegrationTests : IDisposable
{
    private readonly string _testDir;
    private readonly StorageEngine _storageEngine;
    private readonly TransactionManager _transactionManager;
    private readonly Executor _executor;
    private bool _disposed;

    public EndToEndIntegrationTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "CyscaleDB_E2ETests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDir);

        _storageEngine = new StorageEngine(_testDir);
        _storageEngine.Catalog.Initialize();
        _storageEngine.Catalog.CreateDatabase("testdb");

        _transactionManager = new TransactionManager(_testDir);
        _transactionManager.Initialize();

        _executor = new Executor(_storageEngine.Catalog, "testdb");
    }

    [Fact]
    public void CreateInsertSelect_ShouldWork()
    {
        _executor.Execute("CREATE TABLE users (id INT, name VARCHAR(100), age INT)");
        _executor.Execute("INSERT INTO users VALUES (1, 'Alice', 25)");
        _executor.Execute("INSERT INTO users VALUES (2, 'Bob', 30)");

        var result = _executor.Execute("SELECT id, name, age FROM users ORDER BY id");
        
        Assert.Equal(ResultType.Query, result.Type);
        Assert.Equal(3, result.ResultSet!.ColumnCount);
        Assert.Equal(2, result.ResultSet.RowCount);
        Assert.Equal("1", result.ResultSet.Rows[0][0].ToString());
        Assert.Equal("Alice", result.ResultSet.Rows[0][1].ToString());
        Assert.Equal("25", result.ResultSet.Rows[0][2].ToString());
    }

    [Fact]
    public void SelectWithWhere_ShouldFilter()
    {
        _executor.Execute("CREATE TABLE products (id INT, name VARCHAR(100), price INT)");
        _executor.Execute("INSERT INTO products VALUES (1, 'Apple', 10)");
        _executor.Execute("INSERT INTO products VALUES (2, 'Banana', 5)");
        _executor.Execute("INSERT INTO products VALUES (3, 'Cherry', 15)");

        var result = _executor.Execute("SELECT name FROM products WHERE price > 10");
        
        Assert.Equal(ResultType.Query, result.Type);
        Assert.Equal(1, result.ResultSet!.RowCount);
        Assert.Equal("Cherry", result.ResultSet.Rows[0][0].ToString());
    }

    [Fact]
    public void Update_ShouldModifyData()
    {
        _executor.Execute("CREATE TABLE users (id INT, name VARCHAR(100))");
        _executor.Execute("INSERT INTO users VALUES (1, 'Alice')");
        _executor.Execute("UPDATE users SET name = 'Alicia' WHERE id = 1");

        var result = _executor.Execute("SELECT name FROM users WHERE id = 1");
        
        Assert.Equal(ResultType.Query, result.Type);
        Assert.Equal("Alicia", result.ResultSet!.Rows[0][0].ToString());
    }

    [Fact]
    public void Delete_ShouldRemoveData()
    {
        _executor.Execute("CREATE TABLE users (id INT, name VARCHAR(100))");
        _executor.Execute("INSERT INTO users VALUES (1, 'Alice')");
        _executor.Execute("INSERT INTO users VALUES (2, 'Bob')");
        _executor.Execute("DELETE FROM users WHERE id = 1");

        var result = _executor.Execute("SELECT * FROM users");
        
        Assert.Equal(ResultType.Query, result.Type);
        Assert.Equal(1, result.ResultSet!.RowCount);
        Assert.Equal("2", result.ResultSet.Rows[0][0].ToString());
    }

    [Fact]
    public void Join_ShouldCombineTables()
    {
        _executor.Execute("CREATE TABLE users (id INT, name VARCHAR(100))");
        _executor.Execute("CREATE TABLE orders (id INT, user_id INT, product VARCHAR(100))");
        
        _executor.Execute("INSERT INTO users VALUES (1, 'Alice')");
        _executor.Execute("INSERT INTO users VALUES (2, 'Bob')");
        _executor.Execute("INSERT INTO orders VALUES (1, 1, 'Apple')");
        _executor.Execute("INSERT INTO orders VALUES (2, 1, 'Banana')");
        _executor.Execute("INSERT INTO orders VALUES (3, 2, 'Cherry')");

        var result = _executor.Execute(
            "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.name, o.product");
        
        Assert.Equal(ResultType.Query, result.Type);
        Assert.Equal(3, result.ResultSet!.RowCount);
        Assert.Equal("Alice", result.ResultSet.Rows[0][0].ToString());
        Assert.Equal("Apple", result.ResultSet.Rows[0][1].ToString());
    }

    [Fact]
    public void MultipleDatabases_ShouldWork()
    {
        _storageEngine.Catalog.CreateDatabase("db1");
        _storageEngine.Catalog.CreateDatabase("db2");

        var executor1 = new Executor(_storageEngine.Catalog, "db1");
        var executor2 = new Executor(_storageEngine.Catalog, "db2");

        executor1.Execute("CREATE TABLE table1 (id INT)");
        executor2.Execute("CREATE TABLE table1 (id INT)");

        executor1.Execute("INSERT INTO table1 VALUES (1)");
        executor2.Execute("INSERT INTO table1 VALUES (2)");

        var result1 = executor1.Execute("SELECT * FROM table1");
        var result2 = executor2.Execute("SELECT * FROM table1");

        Assert.Equal(1, result1.ResultSet!.RowCount);
        Assert.Equal(1, result2.ResultSet!.RowCount);
        Assert.Equal("1", result1.ResultSet.Rows[0][0].ToString());
        Assert.Equal("2", result2.ResultSet.Rows[0][0].ToString());
    }

    [Fact]
    public void ShowTables_ShouldListTables()
    {
        _executor.Execute("CREATE TABLE users (id INT)");
        _executor.Execute("CREATE TABLE products (id INT)");

        var result = _executor.Execute("SHOW TABLES");
        
        Assert.Equal(ResultType.Query, result.Type);
        Assert.True(result.ResultSet!.RowCount >= 2);
        
        var tableNames = result.ResultSet.Rows.Select(r => r[0].ToString()).ToList();
        Assert.Contains("users", tableNames);
        Assert.Contains("products", tableNames);
    }

    [Fact]
    public void ShowDatabases_ShouldListDatabases()
    {
        _storageEngine.Catalog.CreateDatabase("db1");
        _storageEngine.Catalog.CreateDatabase("db2");

        var executor = new Executor(_storageEngine.Catalog);
        var result = executor.Execute("SHOW DATABASES");
        
        Assert.Equal(ResultType.Query, result.Type);
        Assert.True(result.ResultSet!.RowCount >= 2);
        
        var dbNames = result.ResultSet.Rows.Select(r => r[0].ToString()).ToList();
        Assert.Contains("db1", dbNames);
        Assert.Contains("db2", dbNames);
    }

    [Fact]
    public void DescribeTable_ShouldShowSchema()
    {
        _executor.Execute("CREATE TABLE users (id INT, name VARCHAR(100), age INT)");

        var result = _executor.Execute("DESCRIBE users");
        
        Assert.Equal(ResultType.Query, result.Type);
        Assert.Equal(3, result.ResultSet!.RowCount);
        
        var columnNames = result.ResultSet.Rows.Select(r => r[0].ToString()).ToList();
        Assert.Contains("id", columnNames);
        Assert.Contains("name", columnNames);
        Assert.Contains("age", columnNames);
    }

    [Fact]
    public void ComplexQuery_ShouldExecute()
    {
        _executor.Execute("CREATE TABLE users (id INT, name VARCHAR(100), age INT)");
        _executor.Execute("INSERT INTO users VALUES (1, 'Alice', 25)");
        _executor.Execute("INSERT INTO users VALUES (2, 'Bob', 30)");
        _executor.Execute("INSERT INTO users VALUES (3, 'Charlie', 25)");

        var result = _executor.Execute(
            "SELECT name FROM users WHERE age = 25 ORDER BY name");
        
        Assert.Equal(ResultType.Query, result.Type);
        Assert.Equal(2, result.ResultSet!.RowCount);
        Assert.Equal("Alice", result.ResultSet.Rows[0][0].ToString());
        Assert.Equal("Charlie", result.ResultSet.Rows[1][0].ToString());
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _transactionManager?.Dispose();
        _storageEngine?.Dispose();

        if (Directory.Exists(_testDir))
        {
            try
            {
                Directory.Delete(_testDir, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        _disposed = true;
    }
}
