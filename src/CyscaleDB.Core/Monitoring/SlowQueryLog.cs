using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Monitoring;

/// <summary>
/// Logs slow queries similar to MySQL's slow query log format.
/// Thread-safe for concurrent logging from multiple connections.
/// </summary>
public sealed class SlowQueryLog : IDisposable
{
    private readonly string _logFilePath;
    private readonly StreamWriter? _writer;
    private readonly object _lock = new();
    private readonly Logger _logger;
    private bool _disposed;
    private long _totalSlowQueries;

    public SlowQueryLog(string logFilePath)
    {
        _logFilePath = logFilePath ?? throw new ArgumentNullException(nameof(logFilePath));
        _logger = LogManager.Default.GetLogger<SlowQueryLog>();

        try
        {
            var directory = Path.GetDirectoryName(_logFilePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            _writer = new StreamWriter(_logFilePath, append: true)
            {
                AutoFlush = true
            };

            _logger.Info("Slow query log initialized: {0}", _logFilePath);
        }
        catch (Exception ex)
        {
            _logger.Error("Failed to initialize slow query log: {0}", ex.Message);
        }
    }

    /// <summary>
    /// Gets the total number of slow queries logged.
    /// </summary>
    public long TotalSlowQueries => Interlocked.Read(ref _totalSlowQueries);

    /// <summary>
    /// Logs a slow query with execution details.
    /// </summary>
    public void LogSlowQuery(string sql, TimeSpan duration, ExecutionPlan? plan, string? user = null, string? database = null)
    {
        if (_writer == null)
            return;

        var entry = new SlowQueryEntry
        {
            Timestamp = DateTime.UtcNow,
            Sql = sql,
            DurationMs = duration.TotalMilliseconds,
            RowsExamined = plan?.RowsExamined ?? 0,
            RowsReturned = plan?.RowsReturned ?? 0,
            IndexesUsed = plan?.IndexesUsed ?? [],
            User = user ?? "unknown",
            Database = database ?? "unknown"
        };

        WriteToLog(entry);
        Interlocked.Increment(ref _totalSlowQueries);

        // Also record in metrics
        MetricsCollector.Instance.RecordSlowQuery(sql, duration, plan);
    }

    private void WriteToLog(SlowQueryEntry entry)
    {
        lock (_lock)
        {
            if (_disposed || _writer == null)
                return;

            try
            {
                // MySQL-style slow query log format
                _writer.WriteLine($"# Time: {entry.Timestamp:yyyy-MM-dd HH:mm:ss}");
                _writer.WriteLine($"# User@Host: {entry.User}[{entry.User}] @ [{entry.Database}]");
                _writer.WriteLine($"# Query_time: {entry.DurationMs / 1000.0:F6}  Lock_time: 0.000000  Rows_sent: {entry.RowsReturned}  Rows_examined: {entry.RowsExamined}");
                
                if (entry.IndexesUsed.Count > 0)
                {
                    _writer.WriteLine($"# Indexes_used: {string.Join(", ", entry.IndexesUsed)}");
                }
                
                _writer.WriteLine($"SET timestamp={new DateTimeOffset(entry.Timestamp).ToUnixTimeSeconds()};");
                _writer.WriteLine(entry.Sql);
                _writer.WriteLine();  // Blank line between entries
            }
            catch (Exception ex)
            {
                _logger.Error("Error writing to slow query log: {0}", ex.Message);
            }
        }
    }

    /// <summary>
    /// Rotates the log file by renaming it with a timestamp and creating a new one.
    /// </summary>
    public void Rotate()
    {
        lock (_lock)
        {
            if (_disposed || _writer == null)
                return;

            try
            {
                _writer.Flush();
                _writer.Close();

                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                var directory = Path.GetDirectoryName(_logFilePath);
                var fileName = Path.GetFileNameWithoutExtension(_logFilePath);
                var extension = Path.GetExtension(_logFilePath);
                var rotatedPath = Path.Combine(directory ?? "", $"{fileName}_{timestamp}{extension}");

                if (File.Exists(_logFilePath))
                {
                    File.Move(_logFilePath, rotatedPath);
                    _logger.Info("Rotated slow query log to: {0}", rotatedPath);
                }
            }
            catch (Exception ex)
            {
                _logger.Error("Error rotating slow query log: {0}", ex.Message);
            }
        }
    }

    /// <summary>
    /// Clears the slow query log file.
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            if (_disposed || _writer == null)
                return;

            try
            {
                _writer.Flush();
                _writer.BaseStream.SetLength(0);
                Interlocked.Exchange(ref _totalSlowQueries, 0);
                _logger.Info("Cleared slow query log");
            }
            catch (Exception ex)
            {
                _logger.Error("Error clearing slow query log: {0}", ex.Message);
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        lock (_lock)
        {
            _writer?.Flush();
            _writer?.Dispose();
        }

        _logger.Info("Slow query log closed. Total slow queries: {0}", _totalSlowQueries);
    }
}

/// <summary>
/// Represents a slow query log entry.
/// </summary>
internal sealed class SlowQueryEntry
{
    public DateTime Timestamp { get; set; }
    public string Sql { get; set; } = null!;
    public double DurationMs { get; set; }
    public long RowsExamined { get; set; }
    public long RowsReturned { get; set; }
    public List<string> IndexesUsed { get; set; } = [];
    public string User { get; set; } = "unknown";
    public string Database { get; set; } = "unknown";
}
