using System.Collections.Concurrent;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Monitoring;

/// <summary>
/// Collects performance metrics for the database engine.
/// Thread-safe singleton that tracks queries, transactions, locks, and I/O operations.
/// </summary>
public sealed class MetricsCollector
{
    private static readonly Lazy<MetricsCollector> _instance = new(() => new MetricsCollector());
    public static MetricsCollector Instance => _instance.Value;

    private readonly Logger _logger;
    private readonly ConcurrentDictionary<string, Counter> _counters;
    private readonly ConcurrentDictionary<string, Histogram> _histograms;
    private readonly ConcurrentDictionary<string, Gauge> _gauges;

    // Query Metrics
    public Counter QueriesExecuted => GetOrCreateCounter("queries_executed");
    public Histogram QueryExecutionTime => GetOrCreateHistogram("query_execution_time_ms");
    public Counter SlowQueries => GetOrCreateCounter("slow_queries");
    public Counter FailedQueries => GetOrCreateCounter("failed_queries");

    // Transaction Metrics
    public Counter TransactionsStarted => GetOrCreateCounter("transactions_started");
    public Counter TransactionsCommitted => GetOrCreateCounter("transactions_committed");
    public Counter TransactionsRolledBack => GetOrCreateCounter("transactions_rolled_back");

    // Lock Metrics
    public Counter LockWaits => GetOrCreateCounter("lock_waits");
    public Histogram LockWaitTime => GetOrCreateHistogram("lock_wait_time_ms");
    public Counter Deadlocks => GetOrCreateCounter("deadlocks");
    public Gauge ActiveLocks => GetOrCreateGauge("active_locks");

    // Buffer Pool Metrics
    public Gauge BufferPoolUsedPages => GetOrCreateGauge("buffer_pool_used_pages");
    public Counter BufferPoolHits => GetOrCreateCounter("buffer_pool_hits");
    public Counter BufferPoolMisses => GetOrCreateCounter("buffer_pool_misses");
    public Gauge BufferPoolHitRatio => GetOrCreateGauge("buffer_pool_hit_ratio");

    // I/O Metrics
    public Counter PagesRead => GetOrCreateCounter("pages_read");
    public Counter PagesWritten => GetOrCreateCounter("pages_written");
    public Histogram IoReadTime => GetOrCreateHistogram("io_read_time_ms");
    public Histogram IoWriteTime => GetOrCreateHistogram("io_write_time_ms");

    // MVCC Metrics
    public Counter ReadViewsCreated => GetOrCreateCounter("read_views_created");
    public Counter VersionChainTraversals => GetOrCreateCounter("version_chain_traversals");
    public Histogram VersionChainLength => GetOrCreateHistogram("version_chain_length");

    // Online DDL Metrics
    public Counter OnlineDdlOperations => GetOrCreateCounter("online_ddl_operations");
    public Counter OnlineDdlDmlChanges => GetOrCreateCounter("online_ddl_dml_changes");

    private MetricsCollector()
    {
        _logger = LogManager.Default.GetLogger<MetricsCollector>();
        _counters = new ConcurrentDictionary<string, Counter>();
        _histograms = new ConcurrentDictionary<string, Histogram>();
        _gauges = new ConcurrentDictionary<string, Gauge>();
    }

    private Counter GetOrCreateCounter(string name)
    {
        return _counters.GetOrAdd(name, _ => new Counter(name));
    }

    private Histogram GetOrCreateHistogram(string name)
    {
        return _histograms.GetOrAdd(name, _ => new Histogram(name));
    }

    private Gauge GetOrCreateGauge(string name)
    {
        return _gauges.GetOrAdd(name, _ => new Gauge(name));
    }

    /// <summary>
    /// Records a query execution.
    /// </summary>
    public void RecordQuery(string sql, TimeSpan duration, ExecutionPlan? plan, bool failed = false)
    {
        if (failed)
        {
            FailedQueries.Increment();
        }
        else
        {
            QueriesExecuted.Increment();
            QueryExecutionTime.Record(duration.TotalMilliseconds);
        }
    }

    /// <summary>
    /// Records a slow query.
    /// </summary>
    public void RecordSlowQuery(string sql, TimeSpan duration, ExecutionPlan? plan)
    {
        SlowQueries.Increment();
        _logger.Warning("Slow query detected: {0}ms - {1}", duration.TotalMilliseconds, sql.Substring(0, Math.Min(100, sql.Length)));
    }

    /// <summary>
    /// Gets all counters.
    /// </summary>
    public IDictionary<string, long> GetAllCounters()
    {
        return _counters.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.Value);
    }

    /// <summary>
    /// Gets all gauges.
    /// </summary>
    public IDictionary<string, double> GetAllGauges()
    {
        return _gauges.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.Value);
    }

    /// <summary>
    /// Gets all histograms with their statistics.
    /// </summary>
    public IDictionary<string, HistogramStats> GetAllHistograms()
    {
        return _histograms.ToDictionary(
            kvp => kvp.Key,
            kvp => new HistogramStats
            {
                Count = kvp.Value.Count,
                Min = kvp.Value.Min,
                Max = kvp.Value.Max,
                Mean = kvp.Value.Mean,
                P50 = kvp.Value.P50,
                P95 = kvp.Value.P95,
                P99 = kvp.Value.P99
            });
    }

    /// <summary>
    /// Resets all metrics.
    /// </summary>
    public void Reset()
    {
        foreach (var counter in _counters.Values)
        {
            counter.Reset();
        }
        foreach (var histogram in _histograms.Values)
        {
            histogram.Reset();
        }
        foreach (var gauge in _gauges.Values)
        {
            gauge.Set(0);
        }
        _logger.Info("All metrics reset");
    }
}

/// <summary>
/// A thread-safe counter for tracking cumulative counts.
/// </summary>
public sealed class Counter
{
    private long _value;
    public string Name { get; }

    public Counter(string name)
    {
        Name = name;
    }

    public long Value => Interlocked.Read(ref _value);

    public void Increment(long amount = 1)
    {
        Interlocked.Add(ref _value, amount);
    }

    public void Reset()
    {
        Interlocked.Exchange(ref _value, 0);
    }
}

/// <summary>
/// A thread-safe gauge for tracking current values.
/// </summary>
public sealed class Gauge
{
    private double _value;
    public string Name { get; }

    public Gauge(string name)
    {
        Name = name;
    }

    public double Value
    {
        get => Interlocked.CompareExchange(ref _value, 0, 0);
    }

    public void Set(double value)
    {
        Interlocked.Exchange(ref _value, value);
    }

    public void Increment(double amount = 1.0)
    {
        double newValue, currentValue;
        do
        {
            currentValue = _value;
            newValue = currentValue + amount;
        } while (Interlocked.CompareExchange(ref _value, newValue, currentValue) != currentValue);
    }

    public void Decrement(double amount = 1.0)
    {
        Increment(-amount);
    }
}

/// <summary>
/// A thread-safe histogram for tracking value distributions.
/// </summary>
public sealed class Histogram
{
    private readonly ConcurrentBag<double> _values;
    private readonly object _lock = new();
    public string Name { get; }

    public Histogram(string name)
    {
        Name = name;
        _values = new ConcurrentBag<double>();
    }

    public void Record(double value)
    {
        _values.Add(value);
    }

    public int Count => _values.Count;

    public double Min => _values.IsEmpty ? 0 : _values.Min();
    public double Max => _values.IsEmpty ? 0 : _values.Max();
    public double Mean => _values.IsEmpty ? 0 : _values.Average();

    public double P50 => CalculatePercentile(0.5);
    public double P95 => CalculatePercentile(0.95);
    public double P99 => CalculatePercentile(0.99);

    private double CalculatePercentile(double percentile)
    {
        if (_values.IsEmpty)
            return 0;

        var sorted = _values.OrderBy(v => v).ToArray();
        int index = (int)Math.Ceiling(percentile * sorted.Length) - 1;
        return sorted[Math.Max(0, Math.Min(index, sorted.Length - 1))];
    }

    public void Reset()
    {
        lock (_lock)
        {
            while (_values.TryTake(out _)) { }
        }
    }
}

/// <summary>
/// Statistics for a histogram.
/// </summary>
public sealed class HistogramStats
{
    public int Count { get; init; }
    public double Min { get; init; }
    public double Max { get; init; }
    public double Mean { get; init; }
    public double P50 { get; init; }
    public double P95 { get; init; }
    public double P99 { get; init; }
}

/// <summary>
/// Represents an execution plan for metrics purposes.
/// </summary>
public sealed class ExecutionPlan
{
    public long RowsExamined { get; set; }
    public long RowsReturned { get; set; }
    public List<string> IndexesUsed { get; set; } = [];
    public string? OperatorTree { get; set; }
}
