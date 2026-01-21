using System.Runtime;
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
        // Configure GC for optimal server performance
        ConfigureGarbageCollector();

        // Parse command line arguments into options
        var options = ParseCommandLineOptions(args);

        Logger.Info("===========================================");
        Logger.Info("  CysRedis Server v{0}", Constants.ServerVersion);
        Logger.Info("  Redis Protocol Compatible Server");
        Logger.Info("  with High-Performance Network Stack");
        Logger.Info("===========================================");
        Logger.Info("Starting server on port {0}...", options.Port);

        RedisServer? server = null;

        try
        {
            // Initialize and start Redis server with optimized options
            server = new RedisServer(options);
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

    /// <summary>
    /// Configures garbage collector for optimal server performance.
    /// </summary>
    private static void ConfigureGarbageCollector()
    {
        // 1. Enable Server GC mode (multi-threaded, parallel GC)
        // Note: This must be set in runtimeconfig.json or as environment variable before startup
        // We check and log the current mode
        var isServerGC = GCSettings.IsServerGC;
        Logger.Info("GC Mode: {0}", isServerGC ? "Server GC (Parallel)" : "Workstation GC");

        if (!isServerGC)
        {
            Logger.Warning("Server GC is not enabled. For best performance, add to CysRedis.Server.csproj:");
            Logger.Warning("  <PropertyGroup>");
            Logger.Warning("    <ServerGarbageCollection>true</ServerGarbageCollection>");
            Logger.Warning("  </PropertyGroup>");
        }

        // 2. Set sustained low latency mode
        GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
        Logger.Info("GC Latency Mode: SustainedLowLatency");

        // 3. Configure LOH compaction
        GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.Default;
        Logger.Info("LOH Compaction: Configured (will compact on demand)");

        // 4. Log GC info
        Logger.Info("GC Max Generation: {0}", GC.MaxGeneration);
        Logger.Info("GC Total Memory: {0:N0} bytes", GC.GetTotalMemory(false));
    }

    /// <summary>
    /// Parses command line arguments into server options.
    /// </summary>
    private static RedisServerOptions ParseCommandLineOptions(string[] args)
    {
        var options = new RedisServerOptions();

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--port" or "-p" when i + 1 < args.Length:
                    if (int.TryParse(args[++i], out var port))
                        options.Port = port;
                    break;

                case "--bind" when i + 1 < args.Length:
                    options.BindAddress = args[++i];
                    break;

                case "--datadir" when i + 1 < args.Length:
                    options.DataDir = args[++i];
                    break;

                case "--maxclients" when i + 1 < args.Length:
                    if (int.TryParse(args[++i], out var maxClients))
                        options.MaxClients = maxClients;
                    break;

                case "--timeout" when i + 1 < args.Length:
                    if (int.TryParse(args[++i], out var timeout))
                        options.ClientIdleTimeout = timeout == 0 
                            ? TimeSpan.Zero 
                            : TimeSpan.FromSeconds(timeout);
                    break;

                case "--tcp-keepalive" when i + 1 < args.Length:
                    if (int.TryParse(args[++i], out var keepAlive))
                    {
                        options.TcpKeepAlive = keepAlive > 0;
                        if (keepAlive > 0)
                            options.TcpKeepAliveTime = keepAlive;
                    }
                    break;

                case "--low-latency":
                    // Apply low latency preset
                    options.TcpNoDelay = true;
                    options.ReceiveBufferSize = 32 * 1024;
                    options.SendBufferSize = 32 * 1024;
                    break;

                case "--high-throughput":
                    // Apply high throughput preset
                    options.TcpNoDelay = false;
                    options.ReceiveBufferSize = 128 * 1024;
                    options.SendBufferSize = 128 * 1024;
                    break;

                case "--help" or "-h":
                    PrintUsage();
                    Environment.Exit(0);
                    break;
            }
        }

        return options;
    }

    /// <summary>
    /// Prints command line usage.
    /// </summary>
    private static void PrintUsage()
    {
        Console.WriteLine("CysRedis Server - Redis Protocol Compatible Server");
        Console.WriteLine();
        Console.WriteLine("Usage: CysRedis.Server [options]");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --port, -p <port>      Server port (default: 6379)");
        Console.WriteLine("  --bind <address>       Bind address (default: 0.0.0.0)");
        Console.WriteLine("  --datadir <path>       Data directory for persistence");
        Console.WriteLine("  --maxclients <num>     Max clients (default: 10000, 0=unlimited)");
        Console.WriteLine("  --timeout <seconds>    Client idle timeout (default: 300, 0=disabled)");
        Console.WriteLine("  --tcp-keepalive <sec>  TCP keep-alive interval (default: 60)");
        Console.WriteLine("  --low-latency          Optimize for low latency");
        Console.WriteLine("  --high-throughput      Optimize for high throughput");
        Console.WriteLine("  --help, -h             Show this help message");
    }
}
