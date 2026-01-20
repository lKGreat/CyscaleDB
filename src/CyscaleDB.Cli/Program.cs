using CyscaleDB.Core.Common;

namespace CyscaleDB.Cli;

/// <summary>
/// Entry point for the CyscaleDB command-line interface.
/// </summary>
public class Program
{
    private static readonly LogManager _logManager = LogManager.Default;
    private static Logger _logger = null!;

    public static int Main(string[] args)
    {
        // Setup logging
        _logManager.AddSink(new ConsoleLogSink(useColors: true));
        _logManager.MinimumLevel = LogLevel.Warning; // CLI should be quiet by default
        _logger = _logManager.GetLogger("CLI");

        try
        {
            PrintBanner();

            // TODO (Phase 7): Implement interactive SQL shell
            // - Read user input (support multi-line SQL)
            // - Direct call to Core library executor (no network)
            // - Format and display results as table
            // - Support .exit, quit commands
            // - Command history

            Console.WriteLine("CyscaleDB CLI is not yet implemented.");
            Console.WriteLine("This will be implemented in Phase 7.");
            Console.WriteLine();
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey(true);

            return 0;
        }
        catch (Exception ex)
        {
            _logger.Fatal("CLI failed", ex);
            Console.Error.WriteLine($"Error: {ex.Message}");
            return 1;
        }
        finally
        {
            _logManager.Dispose();
        }
    }

    private static void PrintBanner()
    {
        Console.WriteLine();
        Console.WriteLine("  ╔═══════════════════════════════════════════╗");
        Console.WriteLine("  ║           CyscaleDB CLI                   ║");
        Console.WriteLine($"  ║           Version {Constants.ServerVersion,-14}        ║");
        Console.WriteLine("  ╚═══════════════════════════════════════════╝");
        Console.WriteLine();
    }
}
