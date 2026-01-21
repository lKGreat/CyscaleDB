using CysRedis.Core.Common;

namespace CysRedis.Server;

/// <summary>
/// Entry point for the CysRedis server.
/// </summary>
public class Program
{
    public static async Task<int> Main(string[] args)
    {
        Logger.Info("CysRedis Server v{0}", Constants.ServerVersion);
        Logger.Info("Redis Protocol Compatible Server");
        Logger.Info("Starting server on port {0}...", Constants.DefaultPort);

        try
        {
            // TODO: Initialize Redis server
            // var server = new RedisServer();
            // server.Start();

            Logger.Info("Server is ready to accept connections.");
            Logger.Info("Press Ctrl+C to shutdown.");

            // Wait for shutdown signal
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
                Logger.Info("Shutdown signal received...");
            };

            try
            {
                await Task.Delay(Timeout.Infinite, cts.Token);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown
            }

            Logger.Info("Shutting down server...");
            Logger.Info("Server shutdown complete.");
            return 0;
        }
        catch (Exception ex)
        {
            Logger.Error("Server failed to start", ex);
            return 1;
        }
    }
}
