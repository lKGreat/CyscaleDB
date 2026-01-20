using CyscaleDB.Core.Common;

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

        try
        {
            logger.Info("CyscaleDB Server v{0}", Constants.ServerVersion);
            logger.Info("Starting server on port {0}...", Constants.DefaultPort);

            // TODO: Initialize storage engine
            // TODO: Initialize transaction manager
            // TODO: Start MySQL protocol server

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
            logManager.Dispose();
        }
    }
}
