using CyscaleDB.Core.Protocol.Security;
using CyscaleDB.Core.Protocol.Transport;

namespace CyscaleDB.Core.Common;

/// <summary>
/// MySQL server configuration options.
/// Provides comprehensive configuration for network, connection management, and performance tuning.
/// </summary>
public class MySqlServerOptions
{
    /// <summary>
    /// Server listening port.
    /// </summary>
    public int Port { get; set; } = Constants.DefaultPort;

    /// <summary>
    /// Server bind address.
    /// </summary>
    public string BindAddress { get; set; } = "0.0.0.0";

    #region Connection Management

    /// <summary>
    /// Maximum number of concurrent client connections.
    /// </summary>
    public int MaxConnections { get; set; } = 1000;

    /// <summary>
    /// Client connection idle timeout. Connections idle longer than this will be closed.
    /// Default is 8 hours (28800 seconds) like MySQL.
    /// </summary>
    public TimeSpan ConnectionIdleTimeout { get; set; } = TimeSpan.FromHours(8);

    /// <summary>
    /// Interval for health check task to scan and close idle connections.
    /// </summary>
    public TimeSpan HealthCheckInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Connection backlog size for the listener socket.
    /// </summary>
    public int Backlog { get; set; } = 128;

    #endregion

    #region Socket Configuration

    /// <summary>
    /// Enable TCP_NODELAY to disable Nagle's algorithm for lower latency.
    /// </summary>
    public bool TcpNoDelay { get; set; } = true;

    /// <summary>
    /// Enable TCP Keep-Alive to detect dead connections.
    /// </summary>
    public bool TcpKeepAlive { get; set; } = true;

    /// <summary>
    /// Time in seconds before sending first keep-alive probe.
    /// </summary>
    public int TcpKeepAliveTime { get; set; } = 60;

    /// <summary>
    /// Interval in seconds between keep-alive probes.
    /// </summary>
    public int TcpKeepAliveInterval { get; set; } = 10;

    /// <summary>
    /// Number of keep-alive probes before considering connection dead.
    /// </summary>
    public int TcpKeepAliveRetryCount { get; set; } = 3;

    /// <summary>
    /// Socket receive buffer size in bytes.
    /// </summary>
    public int ReceiveBufferSize { get; set; } = 64 * 1024;

    /// <summary>
    /// Socket send buffer size in bytes.
    /// </summary>
    public int SendBufferSize { get; set; } = 64 * 1024;

    /// <summary>
    /// Allow socket address reuse.
    /// </summary>
    public bool ReuseAddress { get; set; } = true;

    #endregion

    #region Pipeline Configuration

    /// <summary>
    /// Minimum segment size for PipeReader/PipeWriter.
    /// </summary>
    public int MinimumSegmentSize { get; set; } = 4 * 1024;

    /// <summary>
    /// Threshold to pause writer when buffer is too large.
    /// </summary>
    public int PauseWriterThreshold { get; set; } = 64 * 1024;

    /// <summary>
    /// Threshold to resume writer after pausing.
    /// </summary>
    public int ResumeWriterThreshold { get; set; } = 32 * 1024;

    #endregion

    #region Security Configuration

    /// <summary>
    /// SSL/TLS configuration options.
    /// </summary>
    public SslOptions Ssl { get; set; } = new();

    /// <summary>
    /// IP filter configuration options.
    /// </summary>
    public IpFilterOptions IpFilter { get; set; } = new();

    /// <summary>
    /// Rate limiting configuration options.
    /// </summary>
    public RateLimitOptions RateLimit { get; set; } = new();

    #endregion

    #region Compression Configuration

    /// <summary>
    /// Whether protocol compression is enabled.
    /// </summary>
    public bool EnableCompression { get; set; } = false;

    /// <summary>
    /// Preferred compression algorithm.
    /// </summary>
    public CompressionAlgorithm PreferredCompression { get; set; } = CompressionAlgorithm.Zlib;

    /// <summary>
    /// Minimum payload size to trigger compression (in bytes).
    /// </summary>
    public int CompressionThreshold { get; set; } = 50;

    /// <summary>
    /// Compression level (1-9 for zlib).
    /// </summary>
    public int CompressionLevel { get; set; } = 6;

    #endregion

    #region Advanced Configuration

    /// <summary>
    /// Size of the SocketAsyncEventArgs pool.
    /// </summary>
    public int SocketPoolSize { get; set; } = 100;

    /// <summary>
    /// Maximum size of the SocketAsyncEventArgs pool.
    /// </summary>
    public int SocketPoolMaxSize { get; set; } = 1000;

    /// <summary>
    /// Buffer size for each pooled socket.
    /// </summary>
    public int SocketBufferSize { get; set; } = 8192;

    /// <summary>
    /// Timeout for graceful shutdown.
    /// </summary>
    public TimeSpan GracefulShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Threshold for slow query logging (in milliseconds).
    /// </summary>
    public int SlowQueryThresholdMs { get; set; } = 1000;

    #endregion

    /// <summary>
    /// Gets the default options.
    /// </summary>
    public static MySqlServerOptions Default => new();

    /// <summary>
    /// Gets options optimized for low latency.
    /// </summary>
    public static MySqlServerOptions LowLatency => new()
    {
        TcpNoDelay = true,
        TcpKeepAlive = true,
        ReceiveBufferSize = 32 * 1024,
        SendBufferSize = 32 * 1024,
        MinimumSegmentSize = 2 * 1024,
        PauseWriterThreshold = 32 * 1024,
        ResumeWriterThreshold = 16 * 1024
    };

    /// <summary>
    /// Gets options optimized for high throughput.
    /// </summary>
    public static MySqlServerOptions HighThroughput => new()
    {
        TcpNoDelay = false,
        TcpKeepAlive = true,
        ReceiveBufferSize = 128 * 1024,
        SendBufferSize = 128 * 1024,
        MinimumSegmentSize = 8 * 1024,
        PauseWriterThreshold = 128 * 1024,
        ResumeWriterThreshold = 64 * 1024,
        MaxConnections = 5000
    };
}
