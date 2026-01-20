using System.Collections.Concurrent;

namespace CyscaleDB.Core.Common;

/// <summary>
/// Log levels for the logging framework.
/// </summary>
public enum LogLevel
{
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warning = 3,
    Error = 4,
    Fatal = 5,
    None = 6
}

/// <summary>
/// Interface for log sinks that receive log messages.
/// </summary>
public interface ILogSink
{
    /// <summary>
    /// Writes a log entry to this sink.
    /// </summary>
    void Write(LogEntry entry);

    /// <summary>
    /// Flushes any buffered log entries.
    /// </summary>
    void Flush();
}

/// <summary>
/// Represents a single log entry.
/// </summary>
public readonly struct LogEntry
{
    public DateTime Timestamp { get; }
    public LogLevel Level { get; }
    public string Category { get; }
    public string Message { get; }
    public Exception? Exception { get; }
    public int ThreadId { get; }

    public LogEntry(LogLevel level, string category, string message, Exception? exception = null)
    {
        Timestamp = DateTime.UtcNow;
        Level = level;
        Category = category;
        Message = message;
        Exception = exception;
        ThreadId = Environment.CurrentManagedThreadId;
    }

    public override string ToString()
    {
        var levelStr = Level.ToString().ToUpper().PadRight(5);
        var timeStr = Timestamp.ToString("HH:mm:ss.fff");
        var exStr = Exception != null ? $"\n{Exception}" : "";
        return $"[{timeStr}] [{levelStr}] [{Category}] {Message}{exStr}";
    }
}

/// <summary>
/// A logger instance for a specific category.
/// </summary>
public sealed class Logger
{
    private readonly string _category;
    private readonly LogManager _manager;

    internal Logger(string category, LogManager manager)
    {
        _category = category;
        _manager = manager;
    }

    public bool IsEnabled(LogLevel level) => _manager.IsEnabled(level);

    public void Log(LogLevel level, string message, Exception? exception = null)
    {
        if (!IsEnabled(level)) return;

        var entry = new LogEntry(level, _category, message, exception);
        _manager.Write(entry);
    }

    public void Trace(string message) => Log(LogLevel.Trace, message);
    public void Debug(string message) => Log(LogLevel.Debug, message);
    public void Info(string message) => Log(LogLevel.Info, message);
    public void Warning(string message) => Log(LogLevel.Warning, message);
    public void Warning(string message, Exception ex) => Log(LogLevel.Warning, message, ex);
    public void Error(string message) => Log(LogLevel.Error, message);
    public void Error(string message, Exception ex) => Log(LogLevel.Error, message, ex);
    public void Fatal(string message) => Log(LogLevel.Fatal, message);
    public void Fatal(string message, Exception ex) => Log(LogLevel.Fatal, message, ex);

    // Formatted logging
    public void Trace(string format, params object[] args) => Log(LogLevel.Trace, string.Format(format, args));
    public void Debug(string format, params object[] args) => Log(LogLevel.Debug, string.Format(format, args));
    public void Info(string format, params object[] args) => Log(LogLevel.Info, string.Format(format, args));
    public void Warning(string format, params object[] args) => Log(LogLevel.Warning, string.Format(format, args));
    public void Error(string format, params object[] args) => Log(LogLevel.Error, string.Format(format, args));
}

/// <summary>
/// Central log manager that coordinates logging across the application.
/// </summary>
public sealed class LogManager : IDisposable
{
    private static readonly Lazy<LogManager> _default = new(() => new LogManager());
    
    /// <summary>
    /// The default log manager instance.
    /// </summary>
    public static LogManager Default => _default.Value;

    private readonly ConcurrentDictionary<string, Logger> _loggers = new();
    private readonly List<ILogSink> _sinks = [];
    private readonly object _sinkLock = new();
    private LogLevel _minimumLevel = LogLevel.Info;
    private bool _disposed;

    /// <summary>
    /// Gets or sets the minimum log level. Messages below this level are ignored.
    /// </summary>
    public LogLevel MinimumLevel
    {
        get => _minimumLevel;
        set => _minimumLevel = value;
    }

    /// <summary>
    /// Gets a logger for the specified category.
    /// </summary>
    public Logger GetLogger(string category)
    {
        return _loggers.GetOrAdd(category, cat => new Logger(cat, this));
    }

    /// <summary>
    /// Gets a logger for the specified type.
    /// </summary>
    public Logger GetLogger<T>() => GetLogger(typeof(T).Name);

    /// <summary>
    /// Gets a logger for the specified type.
    /// </summary>
    public Logger GetLogger(Type type) => GetLogger(type.Name);

    /// <summary>
    /// Adds a log sink.
    /// </summary>
    public void AddSink(ILogSink sink)
    {
        lock (_sinkLock)
        {
            _sinks.Add(sink);
        }
    }

    /// <summary>
    /// Removes a log sink.
    /// </summary>
    public void RemoveSink(ILogSink sink)
    {
        lock (_sinkLock)
        {
            _sinks.Remove(sink);
        }
    }

    /// <summary>
    /// Checks if the specified log level is enabled.
    /// </summary>
    public bool IsEnabled(LogLevel level) => level >= _minimumLevel;

    /// <summary>
    /// Writes a log entry to all sinks.
    /// </summary>
    internal void Write(LogEntry entry)
    {
        if (_disposed) return;

        ILogSink[] sinks;
        lock (_sinkLock)
        {
            sinks = [.. _sinks];
        }

        foreach (var sink in sinks)
        {
            try
            {
                sink.Write(entry);
            }
            catch
            {
                // Ignore sink errors
            }
        }
    }

    /// <summary>
    /// Flushes all sinks.
    /// </summary>
    public void Flush()
    {
        ILogSink[] sinks;
        lock (_sinkLock)
        {
            sinks = [.. _sinks];
        }

        foreach (var sink in sinks)
        {
            try
            {
                sink.Flush();
            }
            catch
            {
                // Ignore sink errors
            }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        Flush();

        lock (_sinkLock)
        {
            foreach (var sink in _sinks)
            {
                if (sink is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _sinks.Clear();
        }
    }
}

/// <summary>
/// A log sink that writes to the console.
/// </summary>
public sealed class ConsoleLogSink : ILogSink
{
    private readonly object _lock = new();
    private readonly bool _useColors;

    public ConsoleLogSink(bool useColors = true)
    {
        _useColors = useColors;
    }

    public void Write(LogEntry entry)
    {
        lock (_lock)
        {
            if (_useColors)
            {
                Console.ForegroundColor = GetColor(entry.Level);
            }

            Console.WriteLine(entry.ToString());

            if (_useColors)
            {
                Console.ResetColor();
            }
        }
    }

    public void Flush()
    {
        // Console is unbuffered
    }

    private static ConsoleColor GetColor(LogLevel level) => level switch
    {
        LogLevel.Trace => ConsoleColor.DarkGray,
        LogLevel.Debug => ConsoleColor.Gray,
        LogLevel.Info => ConsoleColor.White,
        LogLevel.Warning => ConsoleColor.Yellow,
        LogLevel.Error => ConsoleColor.Red,
        LogLevel.Fatal => ConsoleColor.DarkRed,
        _ => ConsoleColor.White
    };
}

/// <summary>
/// A log sink that writes to a file.
/// </summary>
public sealed class FileLogSink : ILogSink, IDisposable
{
    private readonly StreamWriter _writer;
    private readonly object _lock = new();
    private bool _disposed;

    public FileLogSink(string filePath, bool append = true)
    {
        var directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        _writer = new StreamWriter(filePath, append)
        {
            AutoFlush = false
        };
    }

    public void Write(LogEntry entry)
    {
        if (_disposed) return;

        lock (_lock)
        {
            _writer.WriteLine(entry.ToString());
        }
    }

    public void Flush()
    {
        if (_disposed) return;

        lock (_lock)
        {
            _writer.Flush();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        lock (_lock)
        {
            _writer.Dispose();
        }
    }
}
