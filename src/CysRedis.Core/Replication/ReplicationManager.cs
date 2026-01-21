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
    private readonly ReplicationBacklog _backlog;
    private string? _masterHost;
    private int _masterPort;
    private ReplicationRole _role = ReplicationRole.Master;
    private long _masterReplOffset;
    private string _replId;
    private string _replId2 = "0000000000000000000000000000000000000000"; // secondary repl id
    private long _secondReplOffset = -1;

    public ReplicationManager(RedisServer server)
    {
        _server = server ?? throw new ArgumentNullException(nameof(server));
        _replicas = new ConcurrentDictionary<string, ReplicaInfo>();
        _replId = GenerateReplId();
        _backlog = new ReplicationBacklog(1024 * 1024); // 1MB backlog
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
    /// Secondary replication ID (for failover).
    /// </summary>
    public string ReplId2 => _replId2;

    /// <summary>
    /// Second replication offset.
    /// </summary>
    public long SecondReplOffset => _secondReplOffset;

    /// <summary>
    /// Number of connected replicas.
    /// </summary>
    public int ReplicaCount => _replicas.Count;

    /// <summary>
    /// Gets the replication backlog.
    /// </summary>
    public ReplicationBacklog Backlog => _backlog;

    /// <summary>
    /// Gets the number of replicas that are fully synchronized.
    /// </summary>
    public int GetSyncedReplicaCount()
    {
        // 简化实现：返回所有已连接的副本数
        // 完整实现应该检查每个副本的同步状态
        return _replicas.Count;
    }

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
        
        // Add to backlog
        _backlog.Append(command, _masterReplOffset);
        _masterReplOffset += command.Length;

        // Propagate to all replicas
        var tasks = new List<Task>();
        foreach (var replica in _replicas.Values)
        {
            tasks.Add(PropagateToReplicaAsync(replica, command, cancellationToken));
        }

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Propagates a command to a single replica.
    /// </summary>
    private async Task PropagateToReplicaAsync(ReplicaInfo replica, byte[] command, CancellationToken cancellationToken)
    {
        try
        {
            // 简化实现：使用BulkString包装命令数据
            if (replica.Client.PipeWriter != null)
            {
                replica.Client.PipeWriter.WriteBulkString(command);
                await replica.Client.PipeWriter.FlushAsync(cancellationToken);
                replica.Offset += command.Length;
            }
        }
        catch (Exception ex)
        {
            Logger.Error("Failed to propagate to replica {0}", replica.Address, ex);
        }
    }

    /// <summary>
    /// Handles PSYNC request from a replica.
    /// </summary>
    public async Task<PsyncResult> HandlePsyncAsync(
        string replId, 
        long offset, 
        RedisClient replica,
        CancellationToken cancellationToken = default)
    {
        // Case 1: Full resync (new replica or invalid replid)
        if (replId == "?" || replId != _replId && replId != _replId2)
        {
            return new PsyncResult
            {
                Type = PsyncType.FullResync,
                ReplId = _replId,
                Offset = _masterReplOffset
            };
        }

        // Case 2: Partial resync
        if (_backlog.CanResync(offset))
        {
            return new PsyncResult
            {
                Type = PsyncType.PartialResync,
                ReplId = _replId,
                Offset = offset
            };
        }

        // Case 3: Offset too old, need full resync
        return new PsyncResult
        {
            Type = PsyncType.FullResync,
            ReplId = _replId,
            Offset = _masterReplOffset
        };
    }

    /// <summary>
    /// Sends backlog data to replica for partial resync.
    /// </summary>
    public async Task SendBacklogAsync(RedisClient replica, long fromOffset, CancellationToken cancellationToken = default)
    {
        var data = _backlog.GetDataFrom(fromOffset);
        if (data.Length > 0 && replica.PipeWriter != null)
        {
            replica.PipeWriter.WriteBulkString(data);
            await replica.PipeWriter.FlushAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Sends RDB snapshot to replica for full resync.
    /// </summary>
    public async Task SendRdbAsync(RedisClient replica, CancellationToken cancellationToken = default)
    {
        if (_server.Persistence == null)
        {
            throw new RedisException("ERR no persistence configured");
        }

        // Generate RDB in memory
        var rdbData = await Task.Run(() => _server.Persistence.SaveToBytes(), cancellationToken);
        
        // Send RDB as bulk string
        if (replica.PipeWriter != null)
        {
            replica.PipeWriter.WriteBulkString(rdbData);
            await replica.PipeWriter.FlushAsync(cancellationToken);
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
        using var ms = new System.IO.MemoryStream();
        using var writer = new System.IO.BinaryWriter(ms);
        
        // *<count>\r\n
        var header = $"*{args.Length}\r\n";
        writer.Write(System.Text.Encoding.UTF8.GetBytes(header));
        
        foreach (var arg in args)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(arg);
            var bulkHeader = $"${bytes.Length}\r\n";
            writer.Write(System.Text.Encoding.UTF8.GetBytes(bulkHeader));
            writer.Write(bytes);
            writer.Write(System.Text.Encoding.UTF8.GetBytes("\r\n"));
        }
        
        return ms.ToArray();
    }
}

/// <summary>
/// Replication backlog buffer for partial resync.
/// </summary>
public class ReplicationBacklog
{
    private readonly byte[] _buffer;
    private readonly int _size;
    private long _offset;
    private long _historyOffset;
    private int _writePos;
    private int _dataLen;

    public ReplicationBacklog(int size)
    {
        _size = size;
        _buffer = new byte[size];
        _offset = 0;
        _historyOffset = 0;
        _writePos = 0;
        _dataLen = 0;
    }

    /// <summary>
    /// Current offset.
    /// </summary>
    public long Offset => _offset;

    /// <summary>
    /// First available offset in backlog.
    /// </summary>
    public long HistoryOffset => _historyOffset;

    /// <summary>
    /// Appends data to backlog.
    /// </summary>
    public void Append(byte[] data, long currentOffset)
    {
        lock (_buffer)
        {
            foreach (var b in data)
            {
                _buffer[_writePos] = b;
                _writePos = (_writePos + 1) % _size;
                
                if (_dataLen < _size)
                    _dataLen++;
                else
                    _historyOffset++;
            }
            
            _offset = currentOffset + data.Length;
        }
    }

    /// <summary>
    /// Checks if partial resync is possible from the given offset.
    /// </summary>
    public bool CanResync(long offset)
    {
        lock (_buffer)
        {
            return offset >= _historyOffset && offset <= _offset;
        }
    }

    /// <summary>
    /// Gets data from the given offset.
    /// </summary>
    public byte[] GetDataFrom(long fromOffset)
    {
        lock (_buffer)
        {
            if (!CanResync(fromOffset))
                return Array.Empty<byte>();

            var skip = (int)(fromOffset - _historyOffset);
            var length = (int)(_offset - fromOffset);
            
            if (length <= 0 || skip >= _dataLen)
                return Array.Empty<byte>();

            var result = new byte[length];
            var readPos = (_writePos - _dataLen + skip + _size) % _size;
            
            for (int i = 0; i < length; i++)
            {
                result[i] = _buffer[(readPos + i) % _size];
            }
            
            return result;
        }
    }
}

/// <summary>
/// PSYNC result type.
/// </summary>
public enum PsyncType
{
    FullResync,
    PartialResync
}

/// <summary>
/// PSYNC result.
/// </summary>
public class PsyncResult
{
    public PsyncType Type { get; set; }
    public string ReplId { get; set; } = string.Empty;
    public long Offset { get; set; }
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
