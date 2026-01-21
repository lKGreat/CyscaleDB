using System.Collections.Concurrent;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Protocol.Security;

/// <summary>
/// Rate limiting configuration options.
/// </summary>
public class RateLimitOptions
{
    /// <summary>
    /// Whether rate limiting is enabled.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Maximum new connections per second globally.
    /// </summary>
    public int MaxConnectionsPerSecond { get; set; } = 100;

    /// <summary>
    /// Maximum concurrent connections per IP address.
    /// </summary>
    public int MaxConnectionsPerIp { get; set; } = 50;

    /// <summary>
    /// Maximum new connections per IP per window.
    /// </summary>
    public int MaxNewConnectionsPerIpPerWindow { get; set; } = 10;

    /// <summary>
    /// Sliding window size for rate limiting.
    /// </summary>
    public TimeSpan WindowSize { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Cleanup interval for expired entries.
    /// </summary>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromMinutes(5);
}

/// <summary>
/// Connection rate limiter to prevent DDoS attacks.
/// Uses sliding window algorithm for accurate rate limiting.
/// </summary>
public sealed class ConnectionRateLimiter : IDisposable
{
    private readonly RateLimitOptions _options;
    private readonly ConcurrentDictionary<string, IpConnectionInfo> _ipConnections;
    private readonly Logger _logger;
    private readonly Timer? _cleanupTimer;
    private readonly SlidingWindowCounter _globalCounter;
    private bool _disposed;

    // Statistics
    private long _allowedConnections;
    private long _rejectedConnections;
    private long _rejectedByGlobalLimit;
    private long _rejectedByIpLimit;

    /// <summary>
    /// Gets whether rate limiting is enabled.
    /// </summary>
    public bool IsEnabled => _options.Enabled;

    /// <summary>
    /// Creates a new connection rate limiter.
    /// </summary>
    public ConnectionRateLimiter(RateLimitOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _ipConnections = new ConcurrentDictionary<string, IpConnectionInfo>();
        _logger = LogManager.Default.GetLogger<ConnectionRateLimiter>();
        _globalCounter = new SlidingWindowCounter(_options.WindowSize);

        if (_options.Enabled && _options.CleanupInterval > TimeSpan.Zero)
        {
            _cleanupTimer = new Timer(
                CleanupExpiredEntries,
                null,
                _options.CleanupInterval,
                _options.CleanupInterval);
        }
    }

    /// <summary>
    /// Tries to acquire a connection slot for the given IP.
    /// </summary>
    /// <param name="clientIp">Client IP address.</param>
    /// <returns>True if connection is allowed, false if rate limited.</returns>
    public bool TryAcquire(string clientIp)
    {
        if (!_options.Enabled)
        {
            return true;
        }

        // Check global rate limit
        if (_globalCounter.Count >= _options.MaxConnectionsPerSecond)
        {
            Interlocked.Increment(ref _rejectedConnections);
            Interlocked.Increment(ref _rejectedByGlobalLimit);
            _logger.Warning("Global rate limit exceeded: {0} connections/sec", _globalCounter.Count);
            return false;
        }

        // Get or create IP connection info
        var ipInfo = _ipConnections.GetOrAdd(clientIp, _ => new IpConnectionInfo(_options.WindowSize));

        // Check per-IP concurrent connection limit
        if (ipInfo.ActiveConnections >= _options.MaxConnectionsPerIp)
        {
            Interlocked.Increment(ref _rejectedConnections);
            Interlocked.Increment(ref _rejectedByIpLimit);
            _logger.Warning("Per-IP connection limit exceeded for {0}: {1} active",
                clientIp, ipInfo.ActiveConnections);
            return false;
        }

        // Check per-IP rate limit
        if (ipInfo.NewConnectionsInWindow >= _options.MaxNewConnectionsPerIpPerWindow)
        {
            Interlocked.Increment(ref _rejectedConnections);
            Interlocked.Increment(ref _rejectedByIpLimit);
            _logger.Warning("Per-IP rate limit exceeded for {0}: {1} new connections in window",
                clientIp, ipInfo.NewConnectionsInWindow);
            return false;
        }

        // Acquire slots
        _globalCounter.Increment();
        ipInfo.IncrementActive();
        ipInfo.RecordNewConnection();

        Interlocked.Increment(ref _allowedConnections);
        return true;
    }

    /// <summary>
    /// Releases a connection slot for the given IP.
    /// </summary>
    /// <param name="clientIp">Client IP address.</param>
    public void Release(string clientIp)
    {
        if (!_options.Enabled)
        {
            return;
        }

        if (_ipConnections.TryGetValue(clientIp, out var ipInfo))
        {
            ipInfo.DecrementActive();
        }
    }

    /// <summary>
    /// Gets the current number of active connections for an IP.
    /// </summary>
    public int GetActiveConnections(string clientIp)
    {
        if (_ipConnections.TryGetValue(clientIp, out var ipInfo))
        {
            return ipInfo.ActiveConnections;
        }
        return 0;
    }

    /// <summary>
    /// Cleans up expired entries.
    /// </summary>
    private void CleanupExpiredEntries(object? state)
    {
        var expiredIps = new List<string>();
        var now = DateTime.UtcNow;
        var expirationThreshold = now - TimeSpan.FromMinutes(10);

        foreach (var (ip, info) in _ipConnections)
        {
            if (info.ActiveConnections == 0 && info.LastActivity < expirationThreshold)
            {
                expiredIps.Add(ip);
            }
        }

        foreach (var ip in expiredIps)
        {
            _ipConnections.TryRemove(ip, out _);
        }

        if (expiredIps.Count > 0)
        {
            _logger.Trace("Cleaned up {0} expired IP entries", expiredIps.Count);
        }
    }

    /// <summary>
    /// Gets rate limiter statistics.
    /// </summary>
    public RateLimiterStats GetStats()
    {
        return new RateLimiterStats
        {
            Enabled = _options.Enabled,
            AllowedConnections = _allowedConnections,
            RejectedConnections = _rejectedConnections,
            RejectedByGlobalLimit = _rejectedByGlobalLimit,
            RejectedByIpLimit = _rejectedByIpLimit,
            TrackedIps = _ipConnections.Count,
            CurrentGlobalRate = _globalCounter.Count
        };
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _cleanupTimer?.Dispose();
        }
    }

    /// <summary>
    /// Per-IP connection information.
    /// </summary>
    private sealed class IpConnectionInfo
    {
        private int _activeConnections;
        private readonly SlidingWindowCounter _newConnectionsCounter;

        public int ActiveConnections => _activeConnections;
        public int NewConnectionsInWindow => _newConnectionsCounter.Count;
        public DateTime LastActivity { get; private set; }

        public IpConnectionInfo(TimeSpan windowSize)
        {
            _newConnectionsCounter = new SlidingWindowCounter(windowSize);
            LastActivity = DateTime.UtcNow;
        }

        public void IncrementActive()
        {
            Interlocked.Increment(ref _activeConnections);
            LastActivity = DateTime.UtcNow;
        }

        public void DecrementActive()
        {
            Interlocked.Decrement(ref _activeConnections);
            LastActivity = DateTime.UtcNow;
        }

        public void RecordNewConnection()
        {
            _newConnectionsCounter.Increment();
        }
    }

    /// <summary>
    /// Sliding window counter for rate limiting.
    /// </summary>
    private sealed class SlidingWindowCounter
    {
        private readonly TimeSpan _windowSize;
        private readonly ConcurrentQueue<DateTime> _timestamps;

        public SlidingWindowCounter(TimeSpan windowSize)
        {
            _windowSize = windowSize;
            _timestamps = new ConcurrentQueue<DateTime>();
        }

        public int Count
        {
            get
            {
                CleanupOld();
                return _timestamps.Count;
            }
        }

        public void Increment()
        {
            _timestamps.Enqueue(DateTime.UtcNow);
            CleanupOld();
        }

        private void CleanupOld()
        {
            var cutoff = DateTime.UtcNow - _windowSize;
            while (_timestamps.TryPeek(out var timestamp) && timestamp < cutoff)
            {
                _timestamps.TryDequeue(out _);
            }
        }
    }
}

/// <summary>
/// Rate limiter statistics.
/// </summary>
public sealed class RateLimiterStats
{
    public bool Enabled { get; init; }
    public long AllowedConnections { get; init; }
    public long RejectedConnections { get; init; }
    public long RejectedByGlobalLimit { get; init; }
    public long RejectedByIpLimit { get; init; }
    public int TrackedIps { get; init; }
    public int CurrentGlobalRate { get; init; }

    public double RejectionRate => AllowedConnections + RejectedConnections > 0
        ? (double)RejectedConnections / (AllowedConnections + RejectedConnections)
        : 0;

    public override string ToString()
    {
        return $"RateLimiter: allowed={AllowedConnections}, rejected={RejectedConnections}, rejection rate={RejectionRate:P2}";
    }
}
