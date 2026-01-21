using System.Collections.Concurrent;
using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Replication;

/// <summary>
/// Manages replication (master-slave synchronization).
/// </summary>
public class ReplicationManager
{
    private readonly RedisServer _server;
    private readonly ConcurrentDictionary<string, ReplicaInfo> _replicas;
    private string? _masterHost;
    private int _masterPort;
    private ReplicationRole _role = ReplicationRole.Master;
    private long _masterReplOffset;
    private string _replId;

    public ReplicationManager(RedisServer server)
    {
        _server = server ?? throw new ArgumentNullException(nameof(server));
        _replicas = new ConcurrentDictionary<string, ReplicaInfo>();
        _replId = GenerateReplId();
    }

    /// <summary>
    /// Current replication role.
    /// </summary>
    public ReplicationRole Role => _role;

    /// <summary>
    /// Master host (if replica).
    /// </summary>
    public string? MasterHost => _masterHost;

    /// <summary>
    /// Master port (if replica).
    /// </summary>
    public int MasterPort => _masterPort;

    /// <summary>
    /// Replication ID.
    /// </summary>
    public string ReplId => _replId;

    /// <summary>
    /// Current replication offset.
    /// </summary>
    public long MasterReplOffset => _masterReplOffset;

    /// <summary>
    /// Number of connected replicas.
    /// </summary>
    public int ReplicaCount => _replicas.Count;

    /// <summary>
    /// Sets this server as a replica of another server.
    /// </summary>
    public async Task ReplicaOfAsync(string host, int port, CancellationToken cancellationToken = default)
    {
        if (host.Equals("no", StringComparison.OrdinalIgnoreCase) && port == 0)
        {
            // REPLICAOF NO ONE - become master
            _role = ReplicationRole.Master;
            _masterHost = null;
            _masterPort = 0;
            Logger.Info("Stopped replication, now a master");
            return;
        }

        _masterHost = host;
        _masterPort = port;
        _role = ReplicationRole.Replica;

        Logger.Info("Replicating from {0}:{1}", host, port);

        // Connect to master and perform sync
        // Note: Full implementation would establish connection and perform PSYNC
        await Task.CompletedTask;
    }

    /// <summary>
    /// Registers a replica connection.
    /// </summary>
    public void RegisterReplica(RedisClient client)
    {
        var id = $"{client.Address}:{client.Id}";
        _replicas[id] = new ReplicaInfo(client);
        Logger.Info("Replica connected: {0}", id);
    }

    /// <summary>
    /// Unregisters a replica.
    /// </summary>
    public void UnregisterReplica(RedisClient client)
    {
        var id = $"{client.Address}:{client.Id}";
        _replicas.TryRemove(id, out _);
        Logger.Info("Replica disconnected: {0}", id);
    }

    /// <summary>
    /// Propagates a command to all replicas.
    /// </summary>
    public async Task PropagateAsync(string[] args, CancellationToken cancellationToken = default)
    {
        if (_role != ReplicationRole.Master || _replicas.IsEmpty)
            return;

        // Build RESP array
        var command = BuildRespCommand(args);
        _masterReplOffset += command.Length;

        foreach (var replica in _replicas.Values)
        {
            try
            {
                // Note: Real implementation would write to replica connection
                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                Logger.Error("Failed to propagate to replica", ex);
            }
        }
    }

    /// <summary>
    /// Gets replication info for INFO command.
    /// </summary>
    public Dictionary<string, string> GetInfo()
    {
        var info = new Dictionary<string, string>
        {
            ["role"] = _role.ToString().ToLowerInvariant(),
            ["master_replid"] = _replId,
            ["master_repl_offset"] = _masterReplOffset.ToString(),
            ["connected_slaves"] = _replicas.Count.ToString()
        };

        if (_role == ReplicationRole.Replica)
        {
            info["master_host"] = _masterHost ?? "";
            info["master_port"] = _masterPort.ToString();
            info["master_link_status"] = "up";
        }

        int i = 0;
        foreach (var replica in _replicas.Values)
        {
            info[$"slave{i}"] = $"ip={replica.Address},port=6379,state=online,offset={replica.Offset}";
            i++;
        }

        return info;
    }

    private static string GenerateReplId()
    {
        var chars = "0123456789abcdef";
        var random = new Random();
        var id = new char[40];
        for (int i = 0; i < 40; i++)
            id[i] = chars[random.Next(chars.Length)];
        return new string(id);
    }

    private static byte[] BuildRespCommand(string[] args)
    {
        // Simplified RESP encoding
        var sb = new System.Text.StringBuilder();
        sb.Append('*').Append(args.Length).Append("\r\n");
        foreach (var arg in args)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(arg);
            sb.Append('$').Append(bytes.Length).Append("\r\n").Append(arg).Append("\r\n");
        }
        return System.Text.Encoding.UTF8.GetBytes(sb.ToString());
    }
}

/// <summary>
/// Replication role.
/// </summary>
public enum ReplicationRole
{
    Master,
    Replica
}

/// <summary>
/// Information about a connected replica.
/// </summary>
public class ReplicaInfo
{
    public RedisClient Client { get; }
    public string Address { get; }
    public long Offset { get; set; }
    public DateTime ConnectedAt { get; }

    public ReplicaInfo(RedisClient client)
    {
        Client = client;
        Address = client.Address;
        Offset = 0;
        ConnectedAt = DateTime.UtcNow;
    }
}
