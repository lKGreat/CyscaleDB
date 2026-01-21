using System.Collections.Concurrent;

namespace CysRedis.Core.Monitoring;

/// <summary>
/// Latency sample representing a single measurement.
/// </summary>
public sealed class LatencySample
{
    /// <summary>
    /// Unix timestamp when the sample was taken.
    /// </summary>
    public long Timestamp { get; }

    /// <summary>
    /// Latency in milliseconds.
    /// </summary>
    public long LatencyMs { get; }

    public LatencySample(long timestamp, long latencyMs)
    {
        Timestamp = timestamp;
        LatencyMs = latencyMs;
    }
}

/// <summary>
/// Time series for a specific latency event type.
/// </summary>
public sealed class LatencyTimeSeries
{
    /// <summary>
    /// Maximum number of samples to keep per event type.
    /// </summary>
    public const int MaxSamples = 160;

    private readonly object _lock = new();
    private readonly LatencySample[] _samples;
    private int _index;
    private int _count;
    private long _maxLatency;

    /// <summary>
    /// Event type name.
    /// </summary>
    public string EventType { get; }

    /// <summary>
    /// Gets the maximum latency ever observed for this event.
    /// </summary>
    public long MaxLatency => Interlocked.Read(ref _maxLatency);

    /// <summary>
    /// Gets the number of samples stored.
    /// </summary>
    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _count;
            }
        }
    }

    public LatencyTimeSeries(string eventType)
    {
        EventType = eventType;
        _samples = new LatencySample[MaxSamples];
        _index = 0;
        _count = 0;
        _maxLatency = 0;
    }

    /// <summary>
    /// Adds a sample to the time series.
    /// </summary>
    public void AddSample(long latencyMs)
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        lock (_lock)
        {
            _samples[_index] = new LatencySample(timestamp, latencyMs);
            _index = (_index + 1) % MaxSamples;
            if (_count < MaxSamples) _count++;
        }

        // Update max (lock-free)
        long currentMax;
        do
        {
            currentMax = _maxLatency;
            if (latencyMs <= currentMax) break;
        } while (Interlocked.CompareExchange(ref _maxLatency, latencyMs, currentMax) != currentMax);
    }

    /// <summary>
    /// Gets all samples in chronological order (oldest first).
    /// </summary>
    public LatencySample[] GetSamples()
    {
        lock (_lock)
        {
            if (_count == 0)
                return Array.Empty<LatencySample>();

            var result = new LatencySample[_count];
            var startIndex = _count < MaxSamples ? 0 : _index;

            for (int i = 0; i < _count; i++)
            {
                var idx = (startIndex + i) % MaxSamples;
                result[i] = _samples[idx];
            }

            return result;
        }
    }

    /// <summary>
    /// Gets the latest sample.
    /// </summary>
    public LatencySample? GetLatest()
    {
        lock (_lock)
        {
            if (_count == 0)
                return null;

            var idx = (_index - 1 + MaxSamples) % MaxSamples;
            return _samples[idx];
        }
    }

    /// <summary>
    /// Resets the time series.
    /// </summary>
    public void Reset()
    {
        lock (_lock)
        {
            Array.Clear(_samples);
            _index = 0;
            _count = 0;
        }
        Interlocked.Exchange(ref _maxLatency, 0);
    }
}

/// <summary>
/// Latency monitor that tracks various event types.
/// Compatible with Redis LATENCY command.
/// </summary>
public sealed class LatencyMonitor
{
    private readonly ConcurrentDictionary<string, LatencyTimeSeries> _timeSeries;
    private long _thresholdMs;

    /// <summary>
    /// Gets or sets the latency threshold in milliseconds.
    /// Only events exceeding this threshold will be recorded.
    /// Set to 0 to disable monitoring.
    /// </summary>
    public long ThresholdMs
    {
        get => Interlocked.Read(ref _thresholdMs);
        set => Interlocked.Exchange(ref _thresholdMs, value);
    }

    /// <summary>
    /// Known event types.
    /// </summary>
    public static class EventTypes
    {
        public const string Command = "command";
        public const string Network = "network";
        public const string Fork = "fork";
        public const string Rdb = "rdb";
        public const string Aof = "aof";
        public const string FastCommand = "fast-command";
        public const string SlowCommand = "slow-command";
    }

    /// <summary>
    /// Creates a new latency monitor.
    /// </summary>
    /// <param name="thresholdMs">Threshold in milliseconds (0 to disable).</param>
    public LatencyMonitor(long thresholdMs = 0)
    {
        _timeSeries = new ConcurrentDictionary<string, LatencyTimeSeries>(StringComparer.OrdinalIgnoreCase);
        _thresholdMs = thresholdMs;
    }

    /// <summary>
    /// Records a latency sample if it exceeds the threshold.
    /// </summary>
    /// <param name="eventType">Type of event (e.g., "command", "network").</param>
    /// <param name="latencyMs">Latency in milliseconds.</param>
    public void Record(string eventType, long latencyMs)
    {
        var threshold = ThresholdMs;

        // Disabled if threshold is 0
        if (threshold == 0)
            return;

        // Only record if exceeds threshold
        if (latencyMs < threshold)
            return;

        var series = _timeSeries.GetOrAdd(eventType, t => new LatencyTimeSeries(t));
        series.AddSample(latencyMs);
    }

    /// <summary>
    /// Gets the history for a specific event type.
    /// </summary>
    public LatencySample[] GetHistory(string eventType)
    {
        if (_timeSeries.TryGetValue(eventType, out var series))
        {
            return series.GetSamples();
        }
        return Array.Empty<LatencySample>();
    }

    /// <summary>
    /// Gets the latest sample for each event type.
    /// </summary>
    public Dictionary<string, LatencySample?> GetLatest()
    {
        var result = new Dictionary<string, LatencySample?>();
        foreach (var kvp in _timeSeries)
        {
            result[kvp.Key] = kvp.Value.GetLatest();
        }
        return result;
    }

    /// <summary>
    /// Gets all event type names that have samples.
    /// </summary>
    public IEnumerable<string> GetEventTypes()
    {
        return _timeSeries.Keys;
    }

    /// <summary>
    /// Gets the time series for a specific event type.
    /// </summary>
    public LatencyTimeSeries? GetTimeSeries(string eventType)
    {
        _timeSeries.TryGetValue(eventType, out var series);
        return series;
    }

    /// <summary>
    /// Resets all latency data.
    /// </summary>
    public void Reset()
    {
        foreach (var series in _timeSeries.Values)
        {
            series.Reset();
        }
    }

    /// <summary>
    /// Resets a specific event type.
    /// </summary>
    public void Reset(string eventType)
    {
        if (_timeSeries.TryGetValue(eventType, out var series))
        {
            series.Reset();
        }
    }

    /// <summary>
    /// Generates a diagnostic report (LATENCY DOCTOR).
    /// </summary>
    public string GenerateDoctorReport()
    {
        var threshold = ThresholdMs;
        if (threshold == 0)
        {
            return "I'm sorry, Dave, I can't do that. Latency monitoring is disabled. " +
                   "You may enable it using CONFIG SET latency-monitor-threshold <milliseconds>.";
        }

        var eventTypes = _timeSeries.Where(kvp => kvp.Value.Count > 0).ToList();
        if (eventTypes.Count == 0)
        {
            return "I have reports from no latency spike events. " +
                   $"The latency threshold is currently set to {threshold} ms. " +
                   "All operations complete within this threshold.";
        }

        var report = new System.Text.StringBuilder();
        report.AppendLine($"Latency threshold: {threshold} ms");
        report.AppendLine($"Events with latency spikes: {eventTypes.Count}");
        report.AppendLine();

        foreach (var (eventType, series) in eventTypes)
        {
            var latest = series.GetLatest();
            report.AppendLine($"Event: {eventType}");
            report.AppendLine($"  Samples: {series.Count}");
            report.AppendLine($"  Max latency: {series.MaxLatency} ms");
            if (latest != null)
            {
                var when = DateTimeOffset.FromUnixTimeSeconds(latest.Timestamp).ToString("yyyy-MM-dd HH:mm:ss");
                report.AppendLine($"  Latest: {latest.LatencyMs} ms at {when}");
            }
            report.AppendLine();
        }

        return report.ToString();
    }
}
