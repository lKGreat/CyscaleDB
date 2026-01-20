using CyscaleDB.Core.Common;
using CyscaleDB.Core.Protocol;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Transactions;
using CyscaleDB.Tests.Helpers;

namespace CyscaleDB.Tests.IntegrationTests;

/// <summary>
/// Integration tests for MySQL protocol compatibility.
/// </summary>
public class MySqlProtocolIntegrationTests : IDisposable
{
    private readonly string _testDir;
    private readonly StorageEngine _storageEngine;
    private readonly TransactionManager _transactionManager;
    private readonly MySqlServer _server;
    private readonly int _testPort;
    private bool _disposed;

    public MySqlProtocolIntegrationTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "CyscaleDB_IntegrationTests", Guid.NewGuid().ToString());
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
        Thread.Sleep(100);
    }

    [Fact]
    public async Task Connect_ShouldSucceed()
    {
        using var client = new MySqlTestClient("localhost", _testPort);
        await client.AuthenticateAsync();
    }

    [Fact]
    public async Task CreateTable_ShouldSucceed()
    {
        using var client = new MySqlTestClient("localhost", _testPort);
        await client.AuthenticateAsync(database: "testdb");

        var result = await client.QueryAsync("CREATE TABLE users (id INT, name VARCHAR(100))");
        Assert.Equal(QueryResultType.Ok, result.Type);
    }

    [Fact]
    public async Task Insert_ShouldSucceed()
    {
        using var client = new MySqlTestClient("localhost", _testPort);
        await client.AuthenticateAsync(database: "testdb");

        await client.QueryAsync("CREATE TABLE users (id INT, name VARCHAR(100))");
        var result = await client.QueryAsync("INSERT INTO users VALUES (1, 'Alice')");
        Assert.Equal(QueryResultType.Ok, result.Type);
    }

    [Fact]
    public async Task Select_ShouldReturnResults()
    {
        using var client = new MySqlTestClient("localhost", _testPort);
        await client.AuthenticateAsync(database: "testdb");

        await client.QueryAsync("CREATE TABLE users (id INT, name VARCHAR(100))");
        await client.QueryAsync("INSERT INTO users VALUES (1, 'Alice')");
        await client.QueryAsync("INSERT INTO users VALUES (2, 'Bob')");

        var result = await client.QueryAsync("SELECT id, name FROM users");
        Assert.Equal(QueryResultType.ResultSet, result.Type);
        Assert.Equal(2, result.Columns.Count);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("1", result.Rows[0][0].ToString());
        Assert.Equal("Alice", result.Rows[0][1].ToString());
        Assert.Equal("2", result.Rows[1][0].ToString());
        Assert.Equal("Bob", result.Rows[1][1].ToString());
    }

    [Fact]
    public async Task SelectWithWhere_ShouldFilterResults()
    {
        using var client = new MySqlTestClient("localhost", _testPort);
        await client.AuthenticateAsync(database: "testdb");

        await client.QueryAsync("CREATE TABLE users (id INT, name VARCHAR(100))");
        await client.QueryAsync("INSERT INTO users VALUES (1, 'Alice')");
        await client.QueryAsync("INSERT INTO users VALUES (2, 'Bob')");

        var result = await client.QueryAsync("SELECT id, name FROM users WHERE id = 1");
        Assert.Equal(QueryResultType.ResultSet, result.Type);
        Assert.Single(result.Rows);
        Assert.Equal("1", result.Rows[0][0].ToString());
        Assert.Equal("Alice", result.Rows[0][1].ToString());
    }

    [Fact]
    public async Task UnknownDatabase_ShouldReturnError()
    {
        using var client = new MySqlTestClient("localhost", _testPort);
        
        try
        {
            await client.AuthenticateAsync(database: "nonexistent");
            Assert.Fail("Should have thrown exception");
        }
        catch (MySqlException ex)
        {
            Assert.True(ex.ErrorCode > 0);
            Assert.Contains("Unknown database", ex.Message);
        }
    }

    [Fact]
    public async Task InvalidSQL_ShouldReturnError()
    {
        using var client = new MySqlTestClient("localhost", _testPort);
        await client.AuthenticateAsync(database: "testdb");

        try
        {
            await client.QueryAsync("SELECT * FROM nonexistent_table");
            Assert.Fail("Should have thrown exception");
        }
        catch (MySqlException)
        {
            // Expected
        }
    }

    [Fact]
    public async Task MultipleConnections_ShouldWork()
    {
        using var client1 = new MySqlTestClient("localhost", _testPort);
        using var client2 = new MySqlTestClient("localhost", _testPort);
        
        await client1.AuthenticateAsync(database: "testdb");
        await client2.AuthenticateAsync(database: "testdb");

        await client1.QueryAsync("CREATE TABLE users (id INT, name VARCHAR(100))");
        await client1.QueryAsync("INSERT INTO users VALUES (1, 'Alice')");

        var result = await client2.QueryAsync("SELECT * FROM users");
        Assert.Equal(QueryResultType.ResultSet, result.Type);
        Assert.Single(result.Rows);
    }

    [Fact]
    public async Task DropTable_ShouldSucceed()
    {
        using var client = new MySqlTestClient("localhost", _testPort);
        await client.AuthenticateAsync(database: "testdb");

        await client.QueryAsync("CREATE TABLE users (id INT)");
        var result = await client.QueryAsync("DROP TABLE users");
        Assert.Equal(QueryResultType.Ok, result.Type);
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
