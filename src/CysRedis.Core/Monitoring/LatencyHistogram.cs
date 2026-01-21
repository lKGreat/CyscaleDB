using System.Diagnostics;

namespace CysRedis.Core.Monitoring;

/// <summary>
/// Latency histogram for tracking response time distribution.
/// Uses predefined buckets for efficient percentile calculation.
/// </summary>
public sealed class LatencyHistogram
{
    // Bucket boundaries in milliseconds
    // [0-1ms, 1-5ms, 5-10ms, 10-50ms, 50-100ms, 100-500ms, 500ms-1s, 1s+]
    private static readonly double[] BucketBoundaries = { 1, 5, 10, 50, 100, 500, 1000, double.MaxValue };

    private readonly long[] _buckets;
    private readonly object _lock = new();

    private long _count;
    private double _sum;
    private double _min = double.MaxValue;
    private double _max = double.MinValue;

    /// <summary>
    /// Gets the total number of recorded samples.
    /// </summary>
    public long Count => _count;

    /// <summary>
    /// Gets the sum of all recorded latencies in milliseconds.
    /// </summary>
    public double Sum => _sum;

    /// <summary>
    /// Gets the minimum recorded latency in milliseconds.
    /// </summary>
    public double Min => _count > 0 ? _min : 0;

    /// <summary>
    /// Gets the maximum recorded latency in milliseconds.
    /// </summary>
    public double Max => _count > 0 ? _max : 0;

    /// <summary>
    /// Gets the average latency in milliseconds.
    /// </summary>
    public double Average => _count > 0 ? _sum / _count : 0;

    /// <summary>
    /// Creates a new latency histogram.
    /// </summary>
    public LatencyHistogram()
    {
        _buckets = new long[BucketBoundaries.Length];
    }

    /// <summary>
    /// Records a latency value.
    /// </summary>
    /// <param name="latency">The latency to record.</param>
    public void Record(TimeSpan latency)
    {
        Record(latency.TotalMilliseconds);
    }

    /// <summary>
    /// Records a latency value in milliseconds.
    /// </summary>
    /// <param name="latencyMs">The latency in milliseconds.</param>
    public void Record(double latencyMs)
    {
        // Find the appropriate bucket
        int bucketIndex = 0;
        for (int i = 0; i < BucketBoundaries.Length; i++)
        {
            if (latencyMs < BucketBoundaries[i])
            {
                bucketIndex = i;
                break;
            }
        }

        lock (_lock)
        {
            _buckets[bucketIndex]++;
            _count++;
            _sum += latencyMs;

            if (latencyMs < _min) _min = latencyMs;
            if (latencyMs > _max) _max = latencyMs;
        }
    }

    /// <summary>
    /// Creates a timed scope that records latency when disposed.
    /// </summary>
    public TimedScope StartTimer()
    {
        return new TimedScope(this);
    }

    /// <summary>
    /// Gets the estimated percentile value.
    /// </summary>
    /// <param name="percentile">Percentile (0-100).</param>
    /// <returns>Estimated latency at the given percentile.</returns>
    public double GetPercentile(double percentile)
    {
        if (_count == 0) return 0;

        var targetCount = (long)Math.Ceiling(_count * percentile / 100.0);
        long cumulative = 0;

        lock (_lock)
        {
            for (int i = 0; i < _buckets.Length; i++)
            {
                cumulative += _buckets[i];
                if (cumulative >= targetCount)
                {
                    // Return the upper bound of this bucket
                    return i == 0 ? BucketBoundaries[i] / 2 : BucketBoundaries[i];
                }
            }
        }

        return _max;
    }

    /// <summary>
    /// Gets common percentiles.
    /// </summary>
    public LatencyPercentiles GetPercentiles()
    {
        return new LatencyPercentiles
        {
            P50 = GetPercentile(50),
            P75 = GetPercentile(75),
            P90 = GetPercentile(90),
            P95 = GetPercentile(95),
            P99 = GetPercentile(99),
            P999 = GetPercentile(99.9),
            Min = Min,
            Max = Max,
            Average = Average,
            Count = _count
        };
    }

    /// <summary>
    /// Resets the histogram.
    /// </summary>
    public void Reset()
    {
        lock (_lock)
        {
            Array.Clear(_buckets);
            _count = 0;
            _sum = 0;
            _min = double.MaxValue;
            _max = double.MinValue;
        }
    }

    /// <summary>
    /// Timed scope for automatic latency recording.
    /// </summary>
    public readonly struct TimedScope : IDisposable
    {
        private readonly LatencyHistogram _histogram;
        private readonly Stopwatch _stopwatch;

        public TimedScope(LatencyHistogram histogram)
        {
            _histogram = histogram;
            _stopwatch = Stopwatch.StartNew();
        }

        public void Dispose()
        {
            _stopwatch.Stop();
            _histogram.Record(_stopwatch.Elapsed);
        }
    }
}

/// <summary>
/// Common latency percentiles.
/// </summary>
public sealed class LatencyPercentiles
{
    public double P50 { get; init; }
    public double P75 { get; init; }
    public double P90 { get; init; }
    public double P95 { get; init; }
    public double P99 { get; init; }
    public double P999 { get; init; }
    public double Min { get; init; }
    public double Max { get; init; }
    public double Average { get; init; }
    public long Count { get; init; }

    public override string ToString()
    {
        return $"Latency(ms): p50={P50:F2}, p95={P95:F2}, p99={P99:F2}, avg={Average:F2}, count={Count}";
    }
}
