using CysRedis.Core.Security;
using CysRedis.Core.Threading;

namespace CysRedis.Core.Common;

/// <summary>
/// Redis server configuration options.
/// </summary>
public class RedisServerOptions
{
    /// <summary>
    /// Server port. Default: 6379.
    /// </summary>
    public int Port { get; set; } = Constants.DefaultPort;

    /// <summary>
    /// Data directory for persistence.
    /// </summary>
    public string? DataDir { get; set; }

    /// <summary>
    /// Bind address. Default: any (0.0.0.0).
    /// </summary>
    public string BindAddress { get; set; } = "0.0.0.0";

    #region Connection Management

    /// <summary>
    /// Maximum number of connected clients. Default: 10000.
    /// Set to 0 for unlimited.
    /// </summary>
    public int MaxClients { get; set; } = 10000;

    /// <summary>
    /// Client idle timeout. Connections idle longer than this will be closed.
    /// Default: 5 minutes. Set to TimeSpan.Zero to disable.
    /// </summary>
    public TimeSpan ClientIdleTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Interval for health check task to scan for idle connections.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan HealthCheckInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Connection backlog size. Default: 128.
    /// </summary>
    public int Backlog { get; set; } = 128;

    #endregion

    #region Socket Options

    /// <summary>
    /// Disable Nagle's algorithm (TCP_NODELAY).
    /// Default: true (disabled for low latency).
    /// </summary>
    public bool TcpNoDelay { get; set; } = true;

    /// <summary>
    /// Enable TCP keep-alive.
    /// Default: true.
    /// </summary>
    public bool TcpKeepAlive { get; set; } = true;

    /// <summary>
    /// TCP keep-alive time in seconds.
    /// Default: 60 seconds.
    /// </summary>
    public int TcpKeepAliveTime { get; set; } = 60;

    /// <summary>
    /// TCP keep-alive interval in seconds.
    /// Default: 10 seconds.
    /// </summary>
    public int TcpKeepAliveInterval { get; set; } = 10;

    /// <summary>
    /// TCP keep-alive retry count.
    /// Default: 3.
    /// </summary>
    public int TcpKeepAliveRetryCount { get; set; } = 3;

    /// <summary>
    /// Socket receive buffer size in bytes.
    /// Default: 64KB.
    /// </summary>
    public int ReceiveBufferSize { get; set; } = 64 * 1024;

    /// <summary>
    /// Socket send buffer size in bytes.
    /// Default: 64KB.
    /// </summary>
    public int SendBufferSize { get; set; } = 64 * 1024;

    /// <summary>
    /// Allow socket address reuse.
    /// Default: true.
    /// </summary>
    public bool ReuseAddress { get; set; } = true;

    #endregion

    #region Buffer Options

    /// <summary>
    /// Minimum buffer segment size for PipeReader/PipeWriter.
    /// Default: 4KB.
    /// </summary>
    public int MinimumSegmentSize { get; set; } = 4 * 1024;

    /// <summary>
    /// Pause threshold for PipeWriter (backpressure).
    /// Default: 64KB.
    /// </summary>
    public int PauseWriterThreshold { get; set; } = 64 * 1024;

    /// <summary>
    /// Resume threshold for PipeWriter.
    /// Default: 32KB.
    /// </summary>
    public int ResumeWriterThreshold { get; set; } = 32 * 1024;

    #endregion

    #region Slow Log Options

    /// <summary>
    /// Slow log threshold in microseconds.
    /// Commands taking longer than this will be logged.
    /// Default: 10000 (10ms). Set to -1 to disable, 0 to log all commands.
    /// </summary>
    public long SlowLogThreshold { get; set; } = 10000;

    /// <summary>
    /// Maximum number of slow log entries to keep.
    /// Default: 128.
    /// </summary>
    public int SlowLogMaxLen { get; set; } = 128;

    #endregion

    #region Latency Monitoring Options

    /// <summary>
    /// Latency monitor threshold in milliseconds.
    /// Only latency spikes greater than this will be sampled.
    /// Default: 0 (disabled). Set to a positive value to enable.
    /// </summary>
    public long LatencyMonitorThreshold { get; set; } = 0;

    #endregion

    #region Security Options

    /// <summary>
    /// IP filter options.
    /// </summary>
    public IpFilterOptions IpFilter { get; set; } = new();

    /// <summary>
    /// Rate limit options.
    /// </summary>
    public RateLimitOptions RateLimit { get; set; } = new();

    /// <summary>
    /// TLS/SSL options.
    /// </summary>
    public TlsOptions Tls { get; set; } = new();

    #endregion

    #region Shutdown Options

    /// <summary>
    /// Timeout for graceful shutdown.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan GracefulShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Whether to save RDB before shutdown.
    /// </summary>
    public bool SaveOnShutdown { get; set; } = true;

    /// <summary>
    /// Whether to wait for clients to finish current commands before shutdown.
    /// </summary>
    public bool WaitForClientsOnShutdown { get; set; } = true;

    #endregion

    #region I/O Threading Options

    /// <summary>
    /// I/O threading options.
    /// </summary>
    public IoThreadOptions IoThreading { get; set; } = new();

    #endregion

    /// <summary>
    /// Creates default options.
    /// </summary>
    public static RedisServerOptions Default => new();

    /// <summary>
    /// Creates options optimized for low latency.
    /// </summary>
    public static RedisServerOptions LowLatency => new()
    {
        TcpNoDelay = true,
        ReceiveBufferSize = 32 * 1024,
        SendBufferSize = 32 * 1024,
        MinimumSegmentSize = 2 * 1024,
        PauseWriterThreshold = 32 * 1024,
        ResumeWriterThreshold = 16 * 1024
    };

    /// <summary>
    /// Creates options optimized for high throughput.
    /// </summary>
    public static RedisServerOptions HighThroughput => new()
    {
        TcpNoDelay = false,
        ReceiveBufferSize = 128 * 1024,
        SendBufferSize = 128 * 1024,
        MinimumSegmentSize = 8 * 1024,
        PauseWriterThreshold = 128 * 1024,
        ResumeWriterThreshold = 64 * 1024
    };
}
