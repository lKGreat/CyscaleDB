using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Server;

/// <summary>
/// Entry point for the CysRedis server.
/// </summary>
public class Program
{
    public static async Task<int> Main(string[] args)
    {
        // Parse command line arguments
        int port = Constants.DefaultPort;
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "--port" && i + 1 < args.Length)
            {
                if (int.TryParse(args[i + 1], out var p))
                    port = p;
                i++;
            }
        }

        Logger.Info("===========================================");
        Logger.Info("  CysRedis Server v{0}", Constants.ServerVersion);
        Logger.Info("  Redis Protocol Compatible Server");
        Logger.Info("===========================================");
        Logger.Info("Starting server on port {0}...", port);

        RedisServer? server = null;

        try
        {
            // Initialize and start Redis server
            server = new RedisServer(port);
            server.Start();

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
            server?.Stop();

            Logger.Info("Server shutdown complete.");
            return 0;
        }
        catch (Exception ex)
        {
            Logger.Error("Server failed to start", ex);
            return 1;
        }
        finally
        {
            server?.Dispose();
        }
    }
}
