using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Transactions;

namespace CyscaleDB.Tests.IntegrationTests;

/// <summary>
/// Tests for crash recovery using WAL.
/// </summary>
public class CrashRecoveryTests : IDisposable
{
    private readonly string _testDir;
    private bool _disposed;

    public CrashRecoveryTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "CyscaleDB_RecoveryTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDir);
    }

    [Fact]
    public void CommittedTransaction_ShouldSurviveCrash()
    {
        // Create initial state
        StorageEngine? storageEngine = null;
        TransactionManager? transactionManager = null;
        Executor? executor = null;

        try
        {
            storageEngine = new StorageEngine(_testDir);
            storageEngine.Catalog.Initialize();
            storageEngine.Catalog.CreateDatabase("testdb");

            transactionManager = new TransactionManager(_testDir);
            transactionManager.Initialize();

            executor = new Executor(storageEngine.Catalog, "testdb");
            executor.Execute("CREATE TABLE users (id INT, name VARCHAR(100))");

            // Commit a transaction
            var tx = transactionManager.Begin();
            executor.Execute("INSERT INTO users VALUES (1, 'Alice')");
            transactionManager.Commit(tx);
            transactionManager.Flush();
            storageEngine.Flush();
        }
        finally
        {
            storageEngine?.Dispose();
            transactionManager?.Dispose();
        }

        // Simulate crash - create new instances
        using var storageEngine2 = new StorageEngine(_testDir);
        using var transactionManager2 = new TransactionManager(_testDir);
        
        storageEngine2.Catalog.Initialize();
        transactionManager2.Initialize(); // This should perform recovery

        var executor2 = new Executor(storageEngine2.Catalog, "testdb");
        var result = executor2.Execute("SELECT * FROM users");

        // Committed data should be present
        Assert.Equal(ResultType.Query, result.Type);
        Assert.Equal(1, result.ResultSet!.RowCount);
        Assert.Equal("1", result.ResultSet.Rows[0][0].ToString());
        Assert.Equal("Alice", result.ResultSet.Rows[0][1].ToString());
    }

    [Fact]
    public void UncommittedTransaction_ShouldBeRolledBack()
    {
        // Create initial state
        StorageEngine? storageEngine = null;
        TransactionManager? transactionManager = null;
        Executor? executor = null;

        try
        {
            storageEngine = new StorageEngine(_testDir);
            storageEngine.Catalog.Initialize();
            storageEngine.Catalog.CreateDatabase("testdb");

            transactionManager = new TransactionManager(_testDir);
            transactionManager.Initialize();

            executor = new Executor(storageEngine.Catalog, "testdb");
            executor.Execute("CREATE TABLE users (id INT, name VARCHAR(100))");

            // Start but don't commit a transaction
            var tx = transactionManager.Begin();
            executor.Execute("INSERT INTO users VALUES (1, 'Alice')");
            // Don't commit - simulate crash
            transactionManager.Flush();
            storageEngine.Flush();
        }
        finally
        {
            storageEngine?.Dispose();
            transactionManager?.Dispose();
        }

        // Simulate crash - create new instances
        using var storageEngine2 = new StorageEngine(_testDir);
        using var transactionManager2 = new TransactionManager(_testDir);
        
        storageEngine2.Catalog.Initialize();
        transactionManager2.Initialize(); // This should perform recovery and rollback

        var executor2 = new Executor(storageEngine2.Catalog, "testdb");
        var result = executor2.Execute("SELECT * FROM users");

        // Uncommitted data should NOT be present
        Assert.Equal(ResultType.Query, result.Type);
        Assert.Equal(0, result.ResultSet!.RowCount);
    }

    [Fact]
    public void MultipleCommittedTransactions_ShouldAllSurvive()
    {
        // Create initial state
        StorageEngine? storageEngine = null;
        TransactionManager? transactionManager = null;
        Executor? executor = null;

        try
        {
            storageEngine = new StorageEngine(_testDir);
            storageEngine.Catalog.Initialize();
            storageEngine.Catalog.CreateDatabase("testdb");

            transactionManager = new TransactionManager(_testDir);
            transactionManager.Initialize();

            executor = new Executor(storageEngine.Catalog, "testdb");
            executor.Execute("CREATE TABLE users (id INT, name VARCHAR(100))");

            // Commit multiple transactions
            for (int i = 1; i <= 5; i++)
            {
                var tx = transactionManager.Begin();
                executor.Execute($"INSERT INTO users VALUES ({i}, 'User{i}')");
                transactionManager.Commit(tx);
            }

            transactionManager.Flush();
            storageEngine.Flush();
        }
        finally
        {
            storageEngine?.Dispose();
            transactionManager?.Dispose();
        }

        // Simulate crash - create new instances
        using var storageEngine2 = new StorageEngine(_testDir);
        using var transactionManager2 = new TransactionManager(_testDir);
        
        storageEngine2.Catalog.Initialize();
        transactionManager2.Initialize(); // This should perform recovery

        var executor2 = new Executor(storageEngine2.Catalog, "testdb");
        var result = executor2.Execute("SELECT * FROM users ORDER BY id");

        // All committed data should be present
        Assert.Equal(ResultType.Query, result.Type);
        Assert.Equal(5, result.ResultSet!.RowCount);
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal((i + 1).ToString(), result.ResultSet.Rows[i][0].ToString());
            Assert.Equal($"User{i + 1}", result.ResultSet.Rows[i][1].ToString());
        }
    }

    [Fact]
    public void MixedCommittedAndUncommitted_ShouldRecoverCorrectly()
    {
        // Create initial state
        StorageEngine? storageEngine = null;
        TransactionManager? transactionManager = null;
        Executor? executor = null;

        try
        {
            storageEngine = new StorageEngine(_testDir);
            storageEngine.Catalog.Initialize();
            storageEngine.Catalog.CreateDatabase("testdb");

            transactionManager = new TransactionManager(_testDir);
            transactionManager.Initialize();

            executor = new Executor(storageEngine.Catalog, "testdb");
            executor.Execute("CREATE TABLE users (id INT, name VARCHAR(100))");

            // Commit transaction 1
            var tx1 = transactionManager.Begin();
            executor.Execute("INSERT INTO users VALUES (1, 'Alice')");
            transactionManager.Commit(tx1);

            // Start but don't commit transaction 2
            var tx2 = transactionManager.Begin();
            executor.Execute("INSERT INTO users VALUES (2, 'Bob')");
            // Don't commit

            // Commit transaction 3
            var tx3 = transactionManager.Begin();
            executor.Execute("INSERT INTO users VALUES (3, 'Charlie')");
            transactionManager.Commit(tx3);

            transactionManager.Flush();
            storageEngine.Flush();
        }
        finally
        {
            storageEngine?.Dispose();
            transactionManager?.Dispose();
        }

        // Simulate crash - create new instances
        using var storageEngine2 = new StorageEngine(_testDir);
        using var transactionManager2 = new TransactionManager(_testDir);
        
        storageEngine2.Catalog.Initialize();
        transactionManager2.Initialize(); // This should perform recovery

        var executor2 = new Executor(storageEngine2.Catalog, "testdb");
        var result = executor2.Execute("SELECT * FROM users ORDER BY id");

        // Only committed transactions should be present
        Assert.Equal(ResultType.Query, result.Type);
        Assert.Equal(2, result.ResultSet!.RowCount);
        Assert.Equal("1", result.ResultSet.Rows[0][0].ToString());
        Assert.Equal("Alice", result.ResultSet.Rows[0][1].ToString());
        Assert.Equal("3", result.ResultSet.Rows[1][0].ToString());
        Assert.Equal("Charlie", result.ResultSet.Rows[1][1].ToString());
    }

    public void Dispose()
    {
        if (_disposed)
            return;

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
