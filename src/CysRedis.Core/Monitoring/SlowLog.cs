using System.Collections.Concurrent;

namespace CysRedis.Core.Monitoring;

/// <summary>
/// Slow log entry representing a command that exceeded the threshold.
/// </summary>
public sealed class SlowLogEntry
{
    /// <summary>
    /// Unique entry identifier.
    /// </summary>
    public long Id { get; }

    /// <summary>
    /// Unix timestamp when the command was executed.
    /// </summary>
    public DateTime Timestamp { get; }

    /// <summary>
    /// Command execution duration in microseconds.
    /// </summary>
    public long DurationMicroseconds { get; }

    /// <summary>
    /// Command arguments.
    /// </summary>
    public string[] Arguments { get; }

    /// <summary>
    /// Client name (from CLIENT SETNAME).
    /// </summary>
    public string? ClientName { get; }

    /// <summary>
    /// Client IP address and port.
    /// </summary>
    public string ClientAddress { get; }

    public SlowLogEntry(
        long id,
        DateTime timestamp,
        long durationMicroseconds,
        string[] arguments,
        string? clientName,
        string clientAddress)
    {
        Id = id;
        Timestamp = timestamp;
        DurationMicroseconds = durationMicroseconds;
        Arguments = arguments;
        ClientName = clientName;
        ClientAddress = clientAddress;
    }
}

/// <summary>
/// Slow log manager that records commands exceeding a time threshold.
/// Thread-safe implementation compatible with Redis SLOWLOG command.
/// </summary>
public sealed class SlowLog
{
    /// <summary>
    /// Maximum arguments to store per entry (prevents memory issues with large commands).
    /// </summary>
    public const int MaxArgumentsPerEntry = 32;

    /// <summary>
    /// Maximum string length for each argument.
    /// </summary>
    public const int MaxArgumentLength = 128;

    private readonly ConcurrentQueue<SlowLogEntry> _entries;
    private readonly object _lock = new();
    private long _nextId;
    private int _maxLength;
    private long _thresholdMicroseconds;

    /// <summary>
    /// Gets or sets the slow log threshold in microseconds.
    /// Commands taking longer than this will be logged.
    /// Set to -1 to disable, 0 to log all commands.
    /// </summary>
    public long ThresholdMicroseconds
    {
        get => Interlocked.Read(ref _thresholdMicroseconds);
        set => Interlocked.Exchange(ref _thresholdMicroseconds, value);
    }

    /// <summary>
    /// Gets or sets the maximum number of entries to keep.
    /// </summary>
    public int MaxLength
    {
        get => _maxLength;
        set => Interlocked.Exchange(ref _maxLength, value);
    }

    /// <summary>
    /// Gets the current number of entries.
    /// </summary>
    public int Count => _entries.Count;

    /// <summary>
    /// Creates a new slow log instance.
    /// </summary>
    /// <param name="thresholdMicroseconds">Threshold in microseconds (-1 to disable).</param>
    /// <param name="maxLength">Maximum entries to keep.</param>
    public SlowLog(long thresholdMicroseconds = 10000, int maxLength = 128)
    {
        _entries = new ConcurrentQueue<SlowLogEntry>();
        _thresholdMicroseconds = thresholdMicroseconds;
        _maxLength = maxLength;
        _nextId = 0;
    }

    /// <summary>
    /// Records a command execution if it exceeds the threshold.
    /// </summary>
    /// <param name="durationMicroseconds">Command execution time in microseconds.</param>
    /// <param name="arguments">Command arguments.</param>
    /// <param name="clientName">Client name (optional).</param>
    /// <param name="clientAddress">Client address.</param>
    public void Record(
        long durationMicroseconds,
        string[] arguments,
        string? clientName,
        string clientAddress)
    {
        var threshold = ThresholdMicroseconds;

        // Disabled if threshold is -1
        if (threshold < 0)
            return;

        // Only log if duration exceeds threshold (0 means log all)
        if (threshold > 0 && durationMicroseconds < threshold)
            return;

        // Truncate arguments to prevent memory issues
        var truncatedArgs = TruncateArguments(arguments);

        var entry = new SlowLogEntry(
            Interlocked.Increment(ref _nextId),
            DateTime.UtcNow,
            durationMicroseconds,
            truncatedArgs,
            clientName,
            clientAddress);

        _entries.Enqueue(entry);

        // Trim if exceeds max length
        TrimToMaxLength();
    }

    /// <summary>
    /// Gets the most recent entries.
    /// </summary>
    /// <param name="count">Maximum number of entries to return.</param>
    /// <returns>Most recent entries, newest first.</returns>
    public SlowLogEntry[] Get(int count)
    {
        if (count <= 0)
            return Array.Empty<SlowLogEntry>();

        var entries = _entries.ToArray();
        var result = entries
            .OrderByDescending(e => e.Id)
            .Take(count)
            .ToArray();

        return result;
    }

    /// <summary>
    /// Gets the total number of entries.
    /// </summary>
    public int Length() => _entries.Count;

    /// <summary>
    /// Resets the slow log, removing all entries.
    /// </summary>
    public void Reset()
    {
        lock (_lock)
        {
            while (_entries.TryDequeue(out _)) { }
            _nextId = 0;
        }
    }

    /// <summary>
    /// Trims entries to the maximum length.
    /// </summary>
    private void TrimToMaxLength()
    {
        var maxLen = MaxLength;
        while (_entries.Count > maxLen && _entries.TryDequeue(out _))
        {
            // Keep removing oldest until within limit
        }
    }

    /// <summary>
    /// Truncates arguments to prevent memory issues.
    /// </summary>
    private static string[] TruncateArguments(string[] arguments)
    {
        var count = Math.Min(arguments.Length, MaxArgumentsPerEntry);
        var result = new string[count];

        for (int i = 0; i < count; i++)
        {
            var arg = arguments[i];
            if (arg.Length > MaxArgumentLength)
            {
                result[i] = arg[..MaxArgumentLength] + "...";
            }
            else
            {
                result[i] = arg;
            }
        }

        return result;
    }
}
