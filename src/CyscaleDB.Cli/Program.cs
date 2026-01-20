using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Cli;

/// <summary>
/// Entry point for the CyscaleDB command-line interface.
/// </summary>
public class Program
{
    private static readonly LogManager _logManager = LogManager.Default;
    private static Logger _logger = null!;
    private static StorageEngine? _storageEngine;
    private static Executor? _executor;
    private static readonly List<string> _commandHistory = new();
    private const int MaxHistorySize = 100;

    public static int Main(string[] args)
    {
        // Setup logging
        _logManager.AddSink(new ConsoleLogSink(useColors: true));
        _logManager.MinimumLevel = LogLevel.Warning; // CLI should be quiet by default
        _logger = _logManager.GetLogger("CLI");

        try
        {
            PrintBanner();

            // Determine data directory
            var dataDirectory = args.Length > 0 ? args[0] : Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "CyscaleDB");
            
            Console.WriteLine($"Data directory: {dataDirectory}");
            Console.WriteLine();

            // Initialize storage engine
            _storageEngine = new StorageEngine(dataDirectory);
            _storageEngine.Catalog.Initialize();

            // Initialize executor
            _executor = new Executor(_storageEngine.Catalog);

            // Run interactive shell
            RunInteractiveShell();

            return 0;
        }
        catch (Exception ex)
        {
            _logger.Fatal("CLI failed", ex);
            Console.Error.WriteLine($"Error: {ex.Message}");
            if (ex.InnerException != null)
            {
                Console.Error.WriteLine($"Inner exception: {ex.InnerException.Message}");
            }
            return 1;
        }
        finally
        {
            _storageEngine?.Dispose();
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

    private static void RunInteractiveShell()
    {
        Console.WriteLine("Type SQL commands or '.exit' / 'quit' to exit.");
        Console.WriteLine("Multi-line SQL is supported. End statements with ';'");
        Console.WriteLine();

        while (true)
        {
            try
            {
                // Show prompt
                var currentDb = _executor?.CurrentDatabase ?? "none";
                Console.Write($"cyscale [{currentDb}]> ");

                // Read input (support multi-line)
                var sql = ReadMultiLineInput();
                
                if (string.IsNullOrWhiteSpace(sql))
                    continue;

                // Check for exit commands
                var trimmed = sql.Trim();
                if (trimmed.Equals(".exit", StringComparison.OrdinalIgnoreCase) ||
                    trimmed.Equals("quit", StringComparison.OrdinalIgnoreCase) ||
                    trimmed.Equals("exit", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Goodbye!");
                    break;
                }

                // Add to history
                AddToHistory(sql);

                // Execute SQL
                ExecuteSql(sql);
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error: {ex.Message}");
                Console.ResetColor();
                
                if (ex.InnerException != null)
                {
                    Console.ForegroundColor = ConsoleColor.DarkRed;
                    Console.WriteLine($"  {ex.InnerException.Message}");
                    Console.ResetColor();
                }
            }
        }
    }

    private static string ReadMultiLineInput()
    {
        var lines = new List<string>();
        string? line;

        while ((line = Console.ReadLine()) != null)
        {
            lines.Add(line);
            
            // Check if the input is complete (ends with semicolon)
            var combined = string.Join("\n", lines).Trim();
            if (combined.EndsWith(';'))
            {
                break;
            }

            // Show continuation prompt for multi-line input
            Console.Write("    -> ");
        }

        return string.Join("\n", lines).Trim();
    }

    private static void AddToHistory(string command)
    {
        _commandHistory.Add(command);
        if (_commandHistory.Count > MaxHistorySize)
        {
            _commandHistory.RemoveAt(0);
        }
    }

    private static void ExecuteSql(string sql)
    {
        if (_executor == null)
            throw new InvalidOperationException("Executor not initialized");

        var result = _executor.Execute(sql);

        switch (result.Type)
        {
            case ResultType.Query:
                DisplayResultSet(result.ResultSet!);
                break;

            case ResultType.Modification:
                Console.WriteLine($"Query OK, {result.AffectedRows} row(s) affected");
                break;

            case ResultType.Ddl:
                if (!string.IsNullOrEmpty(result.Message))
                {
                    Console.WriteLine(result.Message);
                }
                else
                {
                    Console.WriteLine("Query OK");
                }
                break;

            case ResultType.Empty:
                Console.WriteLine("Query OK");
                break;
        }
    }

    private static void DisplayResultSet(ResultSet resultSet)
    {
        if (resultSet.ColumnCount == 0)
        {
            Console.WriteLine("Empty result set");
            return;
        }

        // Calculate column widths
        var columnWidths = new int[resultSet.ColumnCount];
        for (int i = 0; i < resultSet.ColumnCount; i++)
        {
            var col = resultSet.Columns[i];
            columnWidths[i] = Math.Max(col.Name.Length, 10); // Minimum width

            // Check all rows for this column
            foreach (var row in resultSet.Rows)
            {
                var valueStr = FormatValue(row[i]);
                columnWidths[i] = Math.Max(columnWidths[i], valueStr.Length);
            }
        }

        // Print header
        PrintSeparator(columnWidths);
        Console.Write("|");
        for (int i = 0; i < resultSet.ColumnCount; i++)
        {
            Console.Write($" {resultSet.Columns[i].Name.PadRight(columnWidths[i])} |");
        }
        Console.WriteLine();
        PrintSeparator(columnWidths);

        // Print rows
        if (resultSet.RowCount == 0)
        {
            var emptyMsg = "Empty result set";
            var totalWidth = GetTotalWidth(columnWidths);
            if (emptyMsg.Length <= totalWidth)
            {
                Console.WriteLine($"| {emptyMsg.PadRight(totalWidth - 1)}|");
            }
            else
            {
                Console.WriteLine($"| {emptyMsg} |");
            }
        }
        else
        {
            foreach (var row in resultSet.Rows)
            {
                Console.Write("|");
                for (int i = 0; i < resultSet.ColumnCount; i++)
                {
                    var valueStr = FormatValue(row[i]);
                    Console.Write($" {valueStr.PadRight(columnWidths[i])} |");
                }
                Console.WriteLine();
            }
        }

        PrintSeparator(columnWidths);
        Console.WriteLine($"{resultSet.RowCount} row(s) in set");
        Console.WriteLine();
    }

    private static string FormatValue(DataValue value)
    {
        if (value.IsNull)
            return "NULL";

        // Remove quotes from ToString() for display
        var str = value.ToString();
        if (str.StartsWith("'") && str.EndsWith("'"))
        {
            return str.Substring(1, str.Length - 2);
        }
        return str;
    }

    private static void PrintSeparator(int[] widths)
    {
        Console.Write("+");
        foreach (var width in widths)
        {
            Console.Write(new string('-', width + 2) + "+");
        }
        Console.WriteLine();
    }

    private static int GetTotalWidth(int[] widths)
    {
        return widths.Sum() + (widths.Length * 3) - 1; // +3 for " | " between columns, -1 for last
    }
}
