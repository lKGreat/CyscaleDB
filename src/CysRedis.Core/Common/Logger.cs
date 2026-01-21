namespace CysRedis.Core.Common;

/// <summary>
/// Log levels.
/// </summary>
public enum LogLevel
{
    Debug,
    Info,
    Warning,
    Error
}

/// <summary>
/// Simple logger for CysRedis.
/// </summary>
public static class Logger
{
    private static LogLevel _minLevel = LogLevel.Info;
    private static readonly object _lock = new();

    /// <summary>
    /// Gets or sets the minimum log level.
    /// </summary>
    public static LogLevel MinLevel
    {
        get => _minLevel;
        set => _minLevel = value;
    }

    public static void Debug(string message) => Log(LogLevel.Debug, message);
    public static void Debug(string format, params object[] args) => Log(LogLevel.Debug, string.Format(format, args));

    public static void Info(string message) => Log(LogLevel.Info, message);
    public static void Info(string format, params object[] args) => Log(LogLevel.Info, string.Format(format, args));

    public static void Warning(string message) => Log(LogLevel.Warning, message);
    public static void Warning(string format, params object[] args) => Log(LogLevel.Warning, string.Format(format, args));

    public static void Error(string message) => Log(LogLevel.Error, message);
    public static void Error(string format, params object[] args) => Log(LogLevel.Error, string.Format(format, args));
    public static void Error(string message, Exception ex) => Log(LogLevel.Error, $"{message}: {ex}");

    private static void Log(LogLevel level, string message)
    {
        if (level < _minLevel) return;

        var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
        var levelStr = level switch
        {
            LogLevel.Debug => "DEBUG",
            LogLevel.Info => "INFO ",
            LogLevel.Warning => "WARN ",
            LogLevel.Error => "ERROR",
            _ => "?????"
        };

        lock (_lock)
        {
            var originalColor = Console.ForegroundColor;
            Console.ForegroundColor = level switch
            {
                LogLevel.Debug => ConsoleColor.Gray,
                LogLevel.Info => ConsoleColor.White,
                LogLevel.Warning => ConsoleColor.Yellow,
                LogLevel.Error => ConsoleColor.Red,
                _ => ConsoleColor.White
            };

            Console.WriteLine($"[{timestamp}] [{levelStr}] {message}");
            Console.ForegroundColor = originalColor;
        }
    }
}
