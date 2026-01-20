using CyscaleDB.Core.Protocol;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Transactions;
using MySqlConnector;

namespace CyscaleDB.Tests.IntegrationTests;

/// <summary>
/// Integration tests using the official MySqlConnector library.
/// These tests verify compatibility with standard MySQL client libraries.
/// </summary>
public class MySqlConnectorIntegrationTests : IDisposable
{
    private readonly string _testDir;
    private readonly StorageEngine _storageEngine;
    private readonly TransactionManager _transactionManager;
    private readonly MySqlServer _server;
    private readonly int _testPort;
    private readonly string _connectionString;
    private bool _disposed;

    public MySqlConnectorIntegrationTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "CyscaleDB_MySqlConnectorTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDir);

        _storageEngine = new StorageEngine(_testDir);
        _storageEngine.Catalog.Initialize();
        _storageEngine.Catalog.CreateDatabase("testdb");

        _transactionManager = new TransactionManager(_testDir);
        _transactionManager.Initialize();

        // Use a random port to avoid conflicts
        _testPort = 33060 + Random.Shared.Next(100, 999);
        _server = new MySqlServer(_storageEngine, _transactionManager, _testPort);
        _server.Start();

        // Give server time to start
        Thread.Sleep(200);

        _connectionString = $"Server=localhost;Port={_testPort};Database=testdb;User=root;Password=;";
    }

    [Fact]
    public async Task Connect_WithMySqlConnector_ShouldSucceed()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        Assert.Equal(System.Data.ConnectionState.Open, connection.State);
    }

    [Fact]
    public async Task Ping_ShouldSucceed()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        var result = await connection.PingAsync();
        Assert.True(result);
    }

    [Fact]
    public async Task CreateTable_ShouldSucceed()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var cmd = new MySqlCommand("CREATE TABLE users (id INT, name VARCHAR(100))", connection);
        var result = await cmd.ExecuteNonQueryAsync();

        Assert.True(result >= 0);
    }

    [Fact]
    public async Task InsertAndSelect_ShouldReturnData()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        // Create table
        await using (var createCmd = new MySqlCommand("CREATE TABLE products (id INT, name VARCHAR(100), price INT)", connection))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        // Insert data
        await using (var insertCmd = new MySqlCommand("INSERT INTO products VALUES (1, 'Widget', 100)", connection))
        {
            var rowsAffected = await insertCmd.ExecuteNonQueryAsync();
            Assert.Equal(1, rowsAffected);
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO products VALUES (2, 'Gadget', 200)", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        // Select data
        await using var selectCmd = new MySqlCommand("SELECT id, name, price FROM products", connection);
        await using var reader = await selectCmd.ExecuteReaderAsync();

        var rows = new List<(int id, string name, int price)>();
        while (await reader.ReadAsync())
        {
            rows.Add((
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetInt32(2)
            ));
        }

        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => r.id == 1 && r.name == "Widget" && r.price == 100);
        Assert.Contains(rows, r => r.id == 2 && r.name == "Gadget" && r.price == 200);
    }

    [Fact]
    public async Task SelectWithWhere_ShouldFilterResults()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        await using (var createCmd = new MySqlCommand("CREATE TABLE items (id INT, category VARCHAR(50))", connection))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO items VALUES (1, 'A')", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO items VALUES (2, 'B')", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO items VALUES (3, 'A')", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        await using var selectCmd = new MySqlCommand("SELECT id FROM items WHERE category = 'A'", connection);
        await using var reader = await selectCmd.ExecuteReaderAsync();

        var ids = new List<int>();
        while (await reader.ReadAsync())
        {
            ids.Add(reader.GetInt32(0));
        }

        Assert.Equal(2, ids.Count);
        Assert.Contains(1, ids);
        Assert.Contains(3, ids);
    }

    [Fact]
    public async Task SelectOrderBy_ShouldSortResults()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        await using (var createCmd = new MySqlCommand("CREATE TABLE scores (name VARCHAR(50), score INT)", connection))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO scores VALUES ('Charlie', 75)", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO scores VALUES ('Alice', 90)", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO scores VALUES ('Bob', 85)", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        // Order by score descending
        await using var selectCmd = new MySqlCommand("SELECT name, score FROM scores ORDER BY score DESC", connection);
        await using var reader = await selectCmd.ExecuteReaderAsync();

        var results = new List<(string name, int score)>();
        while (await reader.ReadAsync())
        {
            results.Add((reader.GetString(0), reader.GetInt32(1)));
        }

        Assert.Equal(3, results.Count);
        Assert.Equal("Alice", results[0].name);
        Assert.Equal(90, results[0].score);
        Assert.Equal("Bob", results[1].name);
        Assert.Equal("Charlie", results[2].name);
    }

    [Fact]
    public async Task SelectDistinct_ShouldRemoveDuplicates()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        await using (var createCmd = new MySqlCommand("CREATE TABLE colors (color VARCHAR(50))", connection))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO colors VALUES ('Red')", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO colors VALUES ('Blue')", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO colors VALUES ('Red')", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        await using var selectCmd = new MySqlCommand("SELECT DISTINCT color FROM colors", connection);
        await using var reader = await selectCmd.ExecuteReaderAsync();

        var colors = new List<string>();
        while (await reader.ReadAsync())
        {
            colors.Add(reader.GetString(0));
        }

        Assert.Equal(2, colors.Count);
        Assert.Contains("Red", colors);
        Assert.Contains("Blue", colors);
    }

    [Fact]
    public async Task SelectWithLimit_ShouldLimitResults()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        await using (var createCmd = new MySqlCommand("CREATE TABLE numbers (n INT)", connection))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        for (int i = 1; i <= 10; i++)
        {
            await using var insertCmd = new MySqlCommand($"INSERT INTO numbers VALUES ({i})", connection);
            await insertCmd.ExecuteNonQueryAsync();
        }

        await using var selectCmd = new MySqlCommand("SELECT n FROM numbers LIMIT 3", connection);
        await using var reader = await selectCmd.ExecuteReaderAsync();

        var numbers = new List<int>();
        while (await reader.ReadAsync())
        {
            numbers.Add(reader.GetInt32(0));
        }

        Assert.Equal(3, numbers.Count);
    }

    [Fact]
    public async Task SelectWithoutFrom_ShouldWork()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var selectCmd = new MySqlCommand("SELECT 1", connection);
        var result = await selectCmd.ExecuteScalarAsync();

        Assert.Equal("1", result?.ToString());
    }

    [Fact]
    public async Task Update_ShouldModifyData()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        await using (var createCmd = new MySqlCommand("CREATE TABLE counters (name VARCHAR(50), value INT)", connection))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO counters VALUES ('hits', 100)", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        await using (var updateCmd = new MySqlCommand("UPDATE counters SET value = 101 WHERE name = 'hits'", connection))
        {
            var rowsAffected = await updateCmd.ExecuteNonQueryAsync();
            Assert.Equal(1, rowsAffected);
        }

        await using var selectCmd = new MySqlCommand("SELECT value FROM counters WHERE name = 'hits'", connection);
        var result = await selectCmd.ExecuteScalarAsync();

        Assert.Equal("101", result?.ToString());
    }

    [Fact]
    public async Task Delete_ShouldRemoveData()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        await using (var createCmd = new MySqlCommand("CREATE TABLE temp (id INT)", connection))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO temp VALUES (1)", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        await using (var insertCmd = new MySqlCommand("INSERT INTO temp VALUES (2)", connection))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        await using (var deleteCmd = new MySqlCommand("DELETE FROM temp WHERE id = 1", connection))
        {
            var rowsAffected = await deleteCmd.ExecuteNonQueryAsync();
            Assert.Equal(1, rowsAffected);
        }

        await using var countCmd = new MySqlCommand("SELECT id FROM temp", connection);
        await using var reader = await countCmd.ExecuteReaderAsync();

        var ids = new List<int>();
        while (await reader.ReadAsync())
        {
            ids.Add(reader.GetInt32(0));
        }

        Assert.Single(ids);
        Assert.Equal(2, ids[0]);
    }

    [Fact]
    public async Task ShowTables_ShouldListTables()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        await using (var createCmd = new MySqlCommand("CREATE TABLE table_a (id INT)", connection))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        await using (var createCmd = new MySqlCommand("CREATE TABLE table_b (id INT)", connection))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        await using var showCmd = new MySqlCommand("SHOW TABLES", connection);
        await using var reader = await showCmd.ExecuteReaderAsync();

        var tables = new List<string>();
        while (await reader.ReadAsync())
        {
            tables.Add(reader.GetString(0));
        }

        Assert.Contains("table_a", tables);
        Assert.Contains("table_b", tables);
    }

    [Fact]
    public async Task ShowDatabases_ShouldListDatabases()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var showCmd = new MySqlCommand("SHOW DATABASES", connection);
        await using var reader = await showCmd.ExecuteReaderAsync();

        var databases = new List<string>();
        while (await reader.ReadAsync())
        {
            databases.Add(reader.GetString(0));
        }

        Assert.Contains("testdb", databases);
        Assert.Contains("default", databases);
    }

    [Fact]
    public async Task DescribeTable_ShouldReturnSchema()
    {
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync();

        await using (var createCmd = new MySqlCommand("CREATE TABLE schema_test (id INT, name VARCHAR(100))", connection))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        await using var descCmd = new MySqlCommand("DESCRIBE schema_test", connection);
        await using var reader = await descCmd.ExecuteReaderAsync();

        var columns = new List<string>();
        while (await reader.ReadAsync())
        {
            columns.Add(reader.GetString(0)); // Field name
        }

        Assert.Equal(2, columns.Count);
        Assert.Contains("id", columns);
        Assert.Contains("name", columns);
    }

    [Fact]
    public async Task MultipleConnections_ShouldWork()
    {
        await using var connection1 = new MySqlConnection(_connectionString);
        await using var connection2 = new MySqlConnection(_connectionString);

        await connection1.OpenAsync();
        await connection2.OpenAsync();

        // Create table on connection 1
        await using (var createCmd = new MySqlCommand("CREATE TABLE shared (data VARCHAR(50))", connection1))
        {
            await createCmd.ExecuteNonQueryAsync();
        }

        // Insert on connection 1
        await using (var insertCmd = new MySqlCommand("INSERT INTO shared VALUES ('from conn1')", connection1))
        {
            await insertCmd.ExecuteNonQueryAsync();
        }

        // Read on connection 2
        await using var selectCmd = new MySqlCommand("SELECT data FROM shared", connection2);
        await using var reader = await selectCmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("from conn1", reader.GetString(0));
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _server?.Stop();
        _server?.Dispose();
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
