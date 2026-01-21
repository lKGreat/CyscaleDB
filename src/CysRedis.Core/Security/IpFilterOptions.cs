namespace CysRedis.Core.Security;

/// <summary>
/// IP filter mode.
/// </summary>
public enum IpFilterMode
{
    /// <summary>
    /// Blacklist mode: deny listed IPs, allow all others.
    /// </summary>
    Blacklist,

    /// <summary>
    /// Whitelist mode: allow listed IPs, deny all others.
    /// </summary>
    Whitelist
}

/// <summary>
/// IP filter configuration options.
/// </summary>
public class IpFilterOptions
{
    /// <summary>
    /// Whether IP filtering is enabled.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Filter mode (blacklist or whitelist).
    /// </summary>
    public IpFilterMode Mode { get; set; } = IpFilterMode.Blacklist;

    /// <summary>
    /// List of whitelisted IP addresses or CIDR ranges.
    /// </summary>
    public List<string> Whitelist { get; set; } = new();

    /// <summary>
    /// List of blacklisted IP addresses or CIDR ranges.
    /// </summary>
    public List<string> Blacklist { get; set; } = new();

    /// <summary>
    /// Whether to always allow localhost connections.
    /// </summary>
    public bool AlwaysAllowLocalhost { get; set; } = true;
}
