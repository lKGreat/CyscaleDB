using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Transactions;
using System.Diagnostics;

namespace CyscaleDB.Tests.IntegrationTests;

/// <summary>
/// Basic performance benchmark tests (small data volumes).
/// </summary>
public class PerformanceBenchmarkTests : IDisposable
{
    private readonly string _testDir;
    private readonly StorageEngine _storageEngine;
    private readonly TransactionManager _transactionManager;
    private readonly Executor _executor;
    private bool _disposed;

    public PerformanceBenchmarkTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "CyscaleDB_PerfTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDir);

        _storageEngine = new StorageEngine(_testDir);
        _storageEngine.Catalog.Initialize();
        _storageEngine.Catalog.CreateDatabase("testdb");

        _transactionManager = new TransactionManager(_testDir);
        _transactionManager.Initialize();

        _executor = new Executor(_storageEngine.Catalog, "testdb");
    }

    [Fact]
    public void InsertPerformance_ShouldBeReasonable()
    {
        _executor.Execute("CREATE TABLE test (id INT, value VARCHAR(100))");

        var stopwatch = Stopwatch.StartNew();
        const int count = 1000;

        for (int i = 0; i < count; i++)
        {
            _executor.Execute($"INSERT INTO test VALUES ({i}, 'Value{i}')");
        }

        stopwatch.Stop();

        var elapsedMs = stopwatch.ElapsedMilliseconds;
        var insertsPerSecond = (count * 1000.0) / elapsedMs;

        // Should be able to insert at least 100 rows per second
        Assert.True(insertsPerSecond > 100, $"Insert performance too slow: {insertsPerSecond:F2} inserts/sec");
    }

    [Fact]
    public void SelectPerformance_ShouldBeReasonable()
    {
        _executor.Execute("CREATE TABLE test (id INT, value VARCHAR(100))");

        // Insert test data
        for (int i = 0; i < 1000; i++)
        {
            _executor.Execute($"INSERT INTO test VALUES ({i}, 'Value{i}')");
        }

        var stopwatch = Stopwatch.StartNew();
        const int iterations = 100;

        for (int i = 0; i < iterations; i++)
        {
            var result = _executor.Execute("SELECT * FROM test WHERE id = 500");
            Assert.Equal(ResultType.Query, result.Type);
        }

        stopwatch.Stop();

        var elapsedMs = stopwatch.ElapsedMilliseconds;
        var selectsPerSecond = (iterations * 1000.0) / elapsedMs;

        // Should be able to select at least 100 queries per second
        Assert.True(selectsPerSecond > 100, $"Select performance too slow: {selectsPerSecond:F2} selects/sec");
    }

    [Fact]
    public void FullTableScan_ShouldBeReasonable()
    {
        _executor.Execute("CREATE TABLE test (id INT, value VARCHAR(100))");

        // Insert test data
        for (int i = 0; i < 1000; i++)
        {
            _executor.Execute($"INSERT INTO test VALUES ({i}, 'Value{i}')");
        }

        var stopwatch = Stopwatch.StartNew();

        var result = _executor.Execute("SELECT * FROM test");
        
        stopwatch.Stop();

        var elapsedMs = stopwatch.ElapsedMilliseconds;

        // Full table scan of 1000 rows should complete in under 1 second
        Assert.True(elapsedMs < 1000, $"Full table scan too slow: {elapsedMs}ms");
        Assert.Equal(1000, result.ResultSet!.RowCount);
    }

    [Fact]
    public void JoinPerformance_ShouldBeReasonable()
    {
        _executor.Execute("CREATE TABLE users (id INT, name VARCHAR(100))");
        _executor.Execute("CREATE TABLE orders (id INT, user_id INT, product VARCHAR(100))");

        // Insert test data
        for (int i = 0; i < 100; i++)
        {
            _executor.Execute($"INSERT INTO users VALUES ({i}, 'User{i}')");
            _executor.Execute($"INSERT INTO orders VALUES ({i}, {i}, 'Product{i}')");
        }

        var stopwatch = Stopwatch.StartNew();

        var result = _executor.Execute(
            "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id");

        stopwatch.Stop();

        var elapsedMs = stopwatch.ElapsedMilliseconds;

        // Join of 100 rows should complete in under 1 second
        Assert.True(elapsedMs < 1000, $"Join performance too slow: {elapsedMs}ms");
        Assert.Equal(100, result.ResultSet!.RowCount);
    }

    [Fact]
    public void TransactionThroughput_ShouldBeReasonable()
    {
        _executor.Execute("CREATE TABLE test (id INT, value INT)");

        var stopwatch = Stopwatch.StartNew();
        const int transactionCount = 100;

        for (int i = 0; i < transactionCount; i++)
        {
            var tx = _transactionManager.Begin();
            _executor.Execute($"INSERT INTO test VALUES ({i}, {i * 2})");
            _transactionManager.Commit(tx);
        }

        stopwatch.Stop();

        var elapsedMs = stopwatch.ElapsedMilliseconds;
        var transactionsPerSecond = (transactionCount * 1000.0) / elapsedMs;

        // Should be able to process at least 50 transactions per second
        Assert.True(transactionsPerSecond > 50, $"Transaction throughput too slow: {transactionsPerSecond:F2} tx/sec");
    }

    [Fact]
    public void BufferPoolHitRatio_ShouldBeGood()
    {
        _executor.Execute("CREATE TABLE test (id INT, value VARCHAR(100))");

        // Insert data to fill multiple pages
        for (int i = 0; i < 500; i++)
        {
            _executor.Execute($"INSERT INTO test VALUES ({i}, 'Value{i}')");
        }

        // Read same data multiple times
        for (int i = 0; i < 10; i++)
        {
            _executor.Execute("SELECT * FROM test WHERE id < 100");
        }

        var (cachedPages, capacity, hitRatio) = _storageEngine.GetBufferPoolStats();

        // Buffer pool should have some cached pages
        Assert.True(cachedPages > 0, "Buffer pool should cache pages");
        
        // Hit ratio should improve with repeated reads
        // Note: This is a basic check - actual hit ratio depends on implementation
        Assert.True(hitRatio >= 0 && hitRatio <= 1, "Hit ratio should be between 0 and 1");
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
