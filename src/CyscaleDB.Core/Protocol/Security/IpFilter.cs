using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Protocol.Security;

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
/// Result of IP filter check.
/// </summary>
public enum IpFilterResult
{
    /// <summary>
    /// IP is explicitly allowed.
    /// </summary>
    Allowed,

    /// <summary>
    /// IP is explicitly denied.
    /// </summary>
    Denied,

    /// <summary>
    /// IP is not in any list (default action depends on mode).
    /// </summary>
    NotInList
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

/// <summary>
/// IP filter for connection access control.
/// Supports individual IPs and CIDR notation.
/// </summary>
public sealed class IpFilter
{
    private readonly IpFilterOptions _options;
    private readonly ConcurrentDictionary<IPAddress, bool> _whitelistIps;
    private readonly ConcurrentDictionary<IPAddress, bool> _blacklistIps;
    private readonly List<IpNetwork> _whitelistNetworks;
    private readonly List<IpNetwork> _blacklistNetworks;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Logger _logger;

    // Statistics
    private long _allowedCount;
    private long _deniedCount;

    /// <summary>
    /// Gets whether IP filtering is enabled.
    /// </summary>
    public bool IsEnabled => _options.Enabled;

    /// <summary>
    /// Gets the filter mode.
    /// </summary>
    public IpFilterMode Mode => _options.Mode;

    /// <summary>
    /// Creates a new IP filter.
    /// </summary>
    public IpFilter(IpFilterOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _whitelistIps = new ConcurrentDictionary<IPAddress, bool>();
        _blacklistIps = new ConcurrentDictionary<IPAddress, bool>();
        _whitelistNetworks = new List<IpNetwork>();
        _blacklistNetworks = new List<IpNetwork>();
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<IpFilter>();

        // Initialize from options
        foreach (var entry in options.Whitelist)
        {
            AddToWhitelist(entry);
        }

        foreach (var entry in options.Blacklist)
        {
            AddToBlacklist(entry);
        }

        if (_options.Enabled)
        {
            _logger.Info("IP filter initialized: mode={0}, whitelist={1}, blacklist={2}",
                _options.Mode, _whitelistIps.Count + _whitelistNetworks.Count,
                _blacklistIps.Count + _blacklistNetworks.Count);
        }
    }

    /// <summary>
    /// Checks if an IP address is allowed.
    /// </summary>
    /// <param name="address">The IP address to check.</param>
    /// <returns>True if allowed, false if denied.</returns>
    public bool IsAllowed(IPAddress address)
    {
        if (!_options.Enabled)
        {
            return true;
        }

        // Always allow localhost if configured
        if (_options.AlwaysAllowLocalhost && IsLocalhost(address))
        {
            Interlocked.Increment(ref _allowedCount);
            return true;
        }

        var result = Check(address);
        var allowed = result switch
        {
            IpFilterResult.Allowed => true,
            IpFilterResult.Denied => false,
            IpFilterResult.NotInList => _options.Mode == IpFilterMode.Blacklist,
            _ => false
        };

        if (allowed)
        {
            Interlocked.Increment(ref _allowedCount);
        }
        else
        {
            Interlocked.Increment(ref _deniedCount);
            _logger.Warning("Connection denied for IP: {0}", address);
        }

        return allowed;
    }

    /// <summary>
    /// Checks the filter result for an IP address.
    /// </summary>
    public IpFilterResult Check(IPAddress address)
    {
        _lock.EnterReadLock();
        try
        {
            // Check whitelist
            if (_whitelistIps.ContainsKey(address))
            {
                return IpFilterResult.Allowed;
            }

            foreach (var network in _whitelistNetworks)
            {
                if (network.Contains(address))
                {
                    return IpFilterResult.Allowed;
                }
            }

            // Check blacklist
            if (_blacklistIps.ContainsKey(address))
            {
                return IpFilterResult.Denied;
            }

            foreach (var network in _blacklistNetworks)
            {
                if (network.Contains(address))
                {
                    return IpFilterResult.Denied;
                }
            }

            return IpFilterResult.NotInList;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Adds an IP or CIDR range to the whitelist.
    /// </summary>
    public void AddToWhitelist(string ipOrCidr)
    {
        if (TryParseNetwork(ipOrCidr, out var network))
        {
            _lock.EnterWriteLock();
            try
            {
                _whitelistNetworks.Add(network);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
            _logger.Debug("Added network to whitelist: {0}", ipOrCidr);
        }
        else if (IPAddress.TryParse(ipOrCidr, out var ip))
        {
            _whitelistIps[ip] = true;
            _logger.Debug("Added IP to whitelist: {0}", ipOrCidr);
        }
        else
        {
            _logger.Warning("Invalid IP/CIDR format: {0}", ipOrCidr);
        }
    }

    /// <summary>
    /// Adds an IP or CIDR range to the blacklist.
    /// </summary>
    public void AddToBlacklist(string ipOrCidr)
    {
        if (TryParseNetwork(ipOrCidr, out var network))
        {
            _lock.EnterWriteLock();
            try
            {
                _blacklistNetworks.Add(network);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
            _logger.Debug("Added network to blacklist: {0}", ipOrCidr);
        }
        else if (IPAddress.TryParse(ipOrCidr, out var ip))
        {
            _blacklistIps[ip] = true;
            _logger.Debug("Added IP to blacklist: {0}", ipOrCidr);
        }
        else
        {
            _logger.Warning("Invalid IP/CIDR format: {0}", ipOrCidr);
        }
    }

    /// <summary>
    /// Removes an IP or CIDR range from the whitelist.
    /// </summary>
    public void RemoveFromWhitelist(string ipOrCidr)
    {
        if (TryParseNetwork(ipOrCidr, out var network))
        {
            _lock.EnterWriteLock();
            try
            {
                _whitelistNetworks.RemoveAll(n => n.Equals(network));
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
        else if (IPAddress.TryParse(ipOrCidr, out var ip))
        {
            _whitelistIps.TryRemove(ip, out _);
        }
    }

    /// <summary>
    /// Removes an IP or CIDR range from the blacklist.
    /// </summary>
    public void RemoveFromBlacklist(string ipOrCidr)
    {
        if (TryParseNetwork(ipOrCidr, out var network))
        {
            _lock.EnterWriteLock();
            try
            {
                _blacklistNetworks.RemoveAll(n => n.Equals(network));
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
        else if (IPAddress.TryParse(ipOrCidr, out var ip))
        {
            _blacklistIps.TryRemove(ip, out _);
        }
    }

    /// <summary>
    /// Checks if an address is localhost.
    /// </summary>
    private static bool IsLocalhost(IPAddress address)
    {
        return IPAddress.IsLoopback(address) ||
               address.Equals(IPAddress.Any) ||
               address.Equals(IPAddress.IPv6Any) ||
               address.Equals(IPAddress.IPv6Loopback);
    }

    /// <summary>
    /// Tries to parse a CIDR notation string.
    /// </summary>
    private static bool TryParseNetwork(string cidr, out IpNetwork network)
    {
        network = default;

        if (!cidr.Contains('/'))
        {
            return false;
        }

        var parts = cidr.Split('/');
        if (parts.Length != 2)
        {
            return false;
        }

        if (!IPAddress.TryParse(parts[0], out var address))
        {
            return false;
        }

        if (!int.TryParse(parts[1], out var prefixLength))
        {
            return false;
        }

        var maxPrefix = address.AddressFamily == AddressFamily.InterNetwork ? 32 : 128;
        if (prefixLength < 0 || prefixLength > maxPrefix)
        {
            return false;
        }

        network = new IpNetwork(address, prefixLength);
        return true;
    }

    /// <summary>
    /// Gets filter statistics.
    /// </summary>
    public IpFilterStats GetStats()
    {
        _lock.EnterReadLock();
        try
        {
            return new IpFilterStats
            {
                Enabled = _options.Enabled,
                Mode = _options.Mode,
                WhitelistCount = _whitelistIps.Count + _whitelistNetworks.Count,
                BlacklistCount = _blacklistIps.Count + _blacklistNetworks.Count,
                AllowedCount = _allowedCount,
                DeniedCount = _deniedCount
            };
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Represents an IP network (CIDR).
    /// </summary>
    private readonly struct IpNetwork : IEquatable<IpNetwork>
    {
        private readonly IPAddress _networkAddress;
        private readonly int _prefixLength;
        private readonly byte[] _maskBytes;

        public IpNetwork(IPAddress address, int prefixLength)
        {
            _prefixLength = prefixLength;
            _maskBytes = CreateMask(address.AddressFamily, prefixLength);
            _networkAddress = ApplyMask(address, _maskBytes);
        }

        public bool Contains(IPAddress address)
        {
            if (address.AddressFamily != _networkAddress.AddressFamily)
            {
                return false;
            }

            var maskedAddress = ApplyMask(address, _maskBytes);
            return maskedAddress.Equals(_networkAddress);
        }

        private static byte[] CreateMask(AddressFamily family, int prefixLength)
        {
            var length = family == AddressFamily.InterNetwork ? 4 : 16;
            var mask = new byte[length];

            for (int i = 0; i < length && prefixLength > 0; i++)
            {
                if (prefixLength >= 8)
                {
                    mask[i] = 0xFF;
                    prefixLength -= 8;
                }
                else
                {
                    mask[i] = (byte)(0xFF << (8 - prefixLength));
                    prefixLength = 0;
                }
            }

            return mask;
        }

        private static IPAddress ApplyMask(IPAddress address, byte[] mask)
        {
            var addressBytes = address.GetAddressBytes();
            var result = new byte[addressBytes.Length];

            for (int i = 0; i < addressBytes.Length; i++)
            {
                result[i] = (byte)(addressBytes[i] & mask[i]);
            }

            return new IPAddress(result);
        }

        public bool Equals(IpNetwork other)
        {
            return _prefixLength == other._prefixLength &&
                   _networkAddress.Equals(other._networkAddress);
        }

        public override bool Equals(object? obj) => obj is IpNetwork other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(_networkAddress, _prefixLength);
    }
}

/// <summary>
/// IP filter statistics.
/// </summary>
public sealed class IpFilterStats
{
    public bool Enabled { get; init; }
    public IpFilterMode Mode { get; init; }
    public int WhitelistCount { get; init; }
    public int BlacklistCount { get; init; }
    public long AllowedCount { get; init; }
    public long DeniedCount { get; init; }

    public override string ToString()
    {
        return $"IpFilter: mode={Mode}, allowed={AllowedCount}, denied={DeniedCount}";
    }
}
