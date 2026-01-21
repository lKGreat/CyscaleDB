namespace CysRedis.Core.Security;

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
