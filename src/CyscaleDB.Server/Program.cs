using CyscaleDB.Core.Common;
using CyscaleDB.Core.Protocol;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Transactions;

namespace CyscaleDB.Server;

/// <summary>
/// Entry point for the CyscaleDB server.
/// </summary>
public class Program
{
    public static async Task<int> Main(string[] args)
    {
        // Setup logging
        var logManager = LogManager.Default;
        logManager.AddSink(new ConsoleLogSink());
        logManager.MinimumLevel = LogLevel.Info;

        var logger = logManager.GetLogger("Server");

        StorageEngine? storageEngine = null;
        TransactionManager? transactionManager = null;
        MySqlServer? mysqlServer = null;

        try
        {
            logger.Info("CyscaleDB Server v{0}", Constants.ServerVersion);
            logger.Info("Starting server on port {0}...", Constants.DefaultPort);

            // Determine data directory
            var dataDirectory = args.Length > 0 ? args[0] : Path.Combine(Environment.CurrentDirectory, "data");
            logger.Info("Using data directory: {0}", dataDirectory);

            // Initialize storage engine
            logger.Info("Initializing storage engine...");
            storageEngine = new StorageEngine(dataDirectory);
            storageEngine.Catalog.Initialize();
            logger.Info("Storage engine initialized");

            // Initialize transaction manager
            logger.Info("Initializing transaction manager...");
            transactionManager = new TransactionManager(dataDirectory);
            transactionManager.Initialize();
            logger.Info("Transaction manager initialized");

            // Start MySQL protocol server
            logger.Info("Starting MySQL protocol server...");
            mysqlServer = new MySqlServer(storageEngine, transactionManager);
            mysqlServer.Start();
            logger.Info("MySQL protocol server started");

            logger.Info("Server is ready to accept connections.");
            logger.Info("Press Ctrl+C to shutdown.");

            // Wait for shutdown signal
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
                logger.Info("Shutdown signal received...");
            };

            try
            {
                await Task.Delay(Timeout.Infinite, cts.Token);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown
            }

            logger.Info("Shutting down server...");
            mysqlServer?.Stop();
            transactionManager?.Dispose();
            storageEngine?.Dispose();

            logger.Info("Server shutdown complete.");
            return 0;
        }
        catch (Exception ex)
        {
            logger.Fatal("Server failed to start", ex);
            return 1;
        }
        finally
        {
            mysqlServer?.Dispose();
            transactionManager?.Dispose();
            storageEngine?.Dispose();
            logManager.Dispose();
        }
    }
}
