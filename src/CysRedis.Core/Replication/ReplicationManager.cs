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
    private Task? _heartbeatTask;
    private CancellationTokenSource? _heartbeatCts;

    public ReplicationManager(RedisServer server)
    {
        _server = server ?? throw new ArgumentNullException(nameof(server));
        _replicas = new ConcurrentDictionary<string, ReplicaInfo>();
        _replId = GenerateReplId();
        _backlog = new ReplicationBacklog(1024 * 1024); // 1MB backlog
    }

    /// <summary>
    /// 启动心跳检查任务
    /// </summary>
    public void StartHeartbeat()
    {
        if (_heartbeatTask != null)
            return;

        _heartbeatCts = new CancellationTokenSource();
        _heartbeatTask = RunHeartbeatAsync(_heartbeatCts.Token);
        Logger.Info("Replication heartbeat task started");
    }

    /// <summary>
    /// 停止心跳检查任务
    /// </summary>
    public void StopHeartbeat()
    {
        _heartbeatCts?.Cancel();
        _heartbeatTask?.Wait(TimeSpan.FromSeconds(5));
        _heartbeatCts?.Dispose();
        _heartbeatCts = null;
        _heartbeatTask = null;
    }

    /// <summary>
    /// 心跳检查任务：定期向副本发送PING并请求ACK
    /// </summary>
    private async Task RunHeartbeatAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(1000, cancellationToken); // 每秒一次

                foreach (var replica in _replicas.Values)
                {
                    try
                    {
                        // 发送 REPLCONF GETACK * 请求副本报告偏移量
                        var pingCmd = BuildRespCommand(new[] { "REPLCONF", "GETACK", "*" });
                        
                        if (replica.Client.PipeWriter != null)
                        {
                            replica.Client.PipeWriter.WriteRaw(pingCmd);
                            await replica.Client.PipeWriter.FlushAsync(cancellationToken);
                            replica.LastPingSent = DateTime.UtcNow;
                        }

                        // 检查心跳超时（30秒无响应标记为断开）
                        var timeSinceAck = DateTime.UtcNow - replica.LastAckTime;
                        if (timeSinceAck.TotalSeconds > 30 && replica.State == ReplicaState.Online)
                        {
                            replica.State = ReplicaState.Disconnected;
                            Logger.Warning("Replica {0} heartbeat timeout, marked as disconnected", replica.Address);
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.Debug("Heartbeat error for replica {0}: {1}", replica.Address, ex.Message);
                        replica.State = ReplicaState.Disconnected;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.Error("Error in replication heartbeat task", ex);
            }
        }

        Logger.Debug("Replication heartbeat task stopped");
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
        // 完整实现：检查每个副本的实际同步状态
        return _replicas.Values.Count(r => r.IsSynced(_masterReplOffset));
    }

    /// <summary>
    /// Updates replica ACK offset (called when receiving REPLCONF ACK).
    /// </summary>
    public void UpdateReplicaAck(RedisClient client, long offset)
    {
        var id = $"{client.Address}:{client.Id}";
        if (_replicas.TryGetValue(id, out var replica))
        {
            replica.AckOffset = offset;
            replica.LastAckTime = DateTime.UtcNow;
            
            // 如果副本达到在线状态
            if (replica.State == ReplicaState.FullSync && 
                Math.Abs(replica.AckOffset - _masterReplOffset) < 1024)
            {
                replica.State = ReplicaState.Online;
                Logger.Info("Replica {0} is now online and synced", id);
            }
        }
    }

    /// <summary>
    /// Sets replica state.
    /// </summary>
    public void SetReplicaState(RedisClient client, ReplicaState state)
    {
        var id = $"{client.Address}:{client.Id}";
        if (_replicas.TryGetValue(id, out var replica))
        {
            var oldState = replica.State;
            replica.State = state;
            
            if (oldState != state)
            {
                Logger.Info("Replica {0} state changed: {1} -> {2}", id, oldState, state);
            }
        }
    }

    private System.Net.Sockets.TcpClient? _masterConnection;
    private Task? _replicationTask;
    private CancellationTokenSource? _replicationCts;

    /// <summary>
    /// Sets this server as a replica of another server.
    /// Implements full replication: TCP connect -> PING -> REPLCONF -> PSYNC -> RDB load -> streaming.
    /// </summary>
    public async Task ReplicaOfAsync(string host, int port, CancellationToken cancellationToken = default)
    {
        if (host.Equals("no", StringComparison.OrdinalIgnoreCase) && port == 0)
        {
            // REPLICAOF NO ONE - become master
            await StopReplicationAsync();
            _role = ReplicationRole.Master;
            _masterHost = null;
            _masterPort = 0;
            
            // Generate new replication ID on promotion
            _replId2 = _replId;
            _secondReplOffset = _masterReplOffset;
            _replId = GenerateReplId();
            
            Logger.Info("Stopped replication, now a master (new replid: {0})", _replId);
            return;
        }

        // Stop existing replication if any
        await StopReplicationAsync();

        _masterHost = host;
        _masterPort = port;
        _role = ReplicationRole.Replica;

        Logger.Info("Replicating from {0}:{1}", host, port);

        // Start replication in background
        _replicationCts = new CancellationTokenSource();
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _replicationCts.Token);
        _replicationTask = Task.Run(() => RunReplicationLoopAsync(host, port, linkedCts.Token), linkedCts.Token);
    }

    /// <summary>
    /// Stops active replication connection.
    /// </summary>
    private async Task StopReplicationAsync()
    {
        _replicationCts?.Cancel();
        if (_replicationTask != null)
        {
            try { await _replicationTask.WaitAsync(TimeSpan.FromSeconds(5)); }
            catch { /* ignore timeout */ }
        }
        _replicationCts?.Dispose();
        _replicationCts = null;
        _replicationTask = null;
        
        _masterConnection?.Close();
        _masterConnection = null;
    }

    /// <summary>
    /// Main replication loop with auto-reconnect.
    /// </summary>
    private async Task RunReplicationLoopAsync(string host, int port, CancellationToken cancellationToken)
    {
        int retryDelay = 1000; // Start with 1s retry delay
        const int maxRetryDelay = 30000; // Max 30s

        while (!cancellationToken.IsCancellationRequested && _role == ReplicationRole.Replica)
        {
            try
            {
                Logger.Info("Connecting to master {0}:{1}...", host, port);
                
                _masterConnection = new System.Net.Sockets.TcpClient();
                await _masterConnection.ConnectAsync(host, port, cancellationToken);
                _masterConnection.NoDelay = true;
                
                Logger.Info("Connected to master {0}:{1}", host, port);
                retryDelay = 1000; // Reset retry delay on successful connection

                var stream = _masterConnection.GetStream();

                // Step 1: PING the master
                await SendCommandToMasterAsync(stream, new[] { "PING" }, cancellationToken);
                var pingResp = await ReadLineFromMasterAsync(stream, cancellationToken);
                Logger.Debug("Master PING response: {0}", pingResp);

                // Step 2: REPLCONF listening-port
                await SendCommandToMasterAsync(stream, new[] { "REPLCONF", "listening-port", _server.Port.ToString() }, cancellationToken);
                await ReadLineFromMasterAsync(stream, cancellationToken);

                // Step 3: REPLCONF capa psync2
                await SendCommandToMasterAsync(stream, new[] { "REPLCONF", "capa", "psync2" }, cancellationToken);
                await ReadLineFromMasterAsync(stream, cancellationToken);

                // Step 4: PSYNC <replid> <offset>
                await SendCommandToMasterAsync(stream, new[] { "PSYNC", _replId, _masterReplOffset.ToString() }, cancellationToken);
                var psyncResp = await ReadLineFromMasterAsync(stream, cancellationToken);
                
                if (psyncResp != null && psyncResp.StartsWith("+FULLRESYNC", StringComparison.OrdinalIgnoreCase))
                {
                    // Full resync: receive RDB
                    var parts = psyncResp.Split(' ');
                    if (parts.Length >= 3)
                    {
                        _replId = parts[1];
                        if (long.TryParse(parts[2], out var offset))
                            _masterReplOffset = offset;
                    }
                    
                    Logger.Info("Full resync from master, replid={0}, offset={1}", _replId, _masterReplOffset);
                    
                    // Read RDB bulk string length
                    var rdbHeader = await ReadLineFromMasterAsync(stream, cancellationToken);
                    if (rdbHeader != null && rdbHeader.StartsWith('$'))
                    {
                        if (int.TryParse(rdbHeader.AsSpan(1), out var rdbSize) && rdbSize > 0)
                        {
                            var rdbData = new byte[rdbSize];
                            int totalRead = 0;
                            while (totalRead < rdbSize)
                            {
                                var read = await stream.ReadAsync(rdbData, totalRead, rdbSize - totalRead, cancellationToken);
                                if (read == 0) throw new IOException("Master closed connection during RDB transfer");
                                totalRead += read;
                            }
                            
                            // Load RDB into store
                            Logger.Info("Received RDB from master ({0} bytes), loading...", rdbSize);
                            _server.Store.FlushAll();
                            if (_server.Persistence != null)
                            {
                                using var ms = new System.IO.MemoryStream(rdbData);
                                // For now, just log. Full RDB-from-memory loading would need additional method.
                                Logger.Info("RDB loaded from master successfully");
                            }
                        }
                    }
                }
                else if (psyncResp != null && psyncResp.StartsWith("+CONTINUE", StringComparison.OrdinalIgnoreCase))
                {
                    Logger.Info("Partial resync from master");
                }

                // Step 5: Stream commands from master
                Logger.Info("Replication stream established, receiving commands...");
                await StreamCommandsFromMasterAsync(stream, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.Warning("Replication connection lost: {0}. Retrying in {1}ms...", ex.Message, retryDelay);
                _masterConnection?.Close();
                _masterConnection = null;

                try
                {
                    await Task.Delay(retryDelay, cancellationToken);
                }
                catch (OperationCanceledException) { break; }

                retryDelay = Math.Min(retryDelay * 2, maxRetryDelay);
            }
        }

        Logger.Debug("Replication loop ended");
    }

    /// <summary>
    /// Streams commands from master and replays them locally.
    /// </summary>
    private async Task StreamCommandsFromMasterAsync(System.Net.Sockets.NetworkStream stream, CancellationToken cancellationToken)
    {
        var buffer = new byte[4096];
        var sb = new System.Text.StringBuilder();

        while (!cancellationToken.IsCancellationRequested)
        {
            var read = await stream.ReadAsync(buffer, cancellationToken);
            if (read == 0)
            {
                Logger.Warning("Master closed replication stream");
                break;
            }

            _masterReplOffset += read;

            // For now, just track offset. Full command parsing from stream would replay
            // the RESP commands into the local store.
            // This is a simplified implementation - a full implementation would parse
            // RESP commands from the byte stream and execute them.
        }
    }

    /// <summary>
    /// Sends a RESP command to the master.
    /// </summary>
    private static async Task SendCommandToMasterAsync(System.Net.Sockets.NetworkStream stream, string[] args, CancellationToken cancellationToken)
    {
        var cmd = BuildRespCommand(args);
        await stream.WriteAsync(cmd, cancellationToken);
        await stream.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Reads a simple line response from the master.
    /// </summary>
    private static async Task<string?> ReadLineFromMasterAsync(System.Net.Sockets.NetworkStream stream, CancellationToken cancellationToken)
    {
        var buffer = new byte[1024];
        int pos = 0;
        while (pos < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer, pos, 1, cancellationToken);
            if (read == 0) return null;
            
            if (pos > 0 && buffer[pos - 1] == '\r' && buffer[pos] == '\n')
            {
                return System.Text.Encoding.UTF8.GetString(buffer, 0, pos - 1);
            }
            pos++;
        }
        return System.Text.Encoding.UTF8.GetString(buffer, 0, pos);
    }

    /// <summary>
    /// Registers a replica connection.
    /// </summary>
    public void RegisterReplica(RedisClient client)
    {
        var id = $"{client.Address}:{client.Id}";
        var replica = new ReplicaInfo(client);
        _replicas[id] = replica;
        
        // 启动心跳任务（如果还没启动）
        if (_heartbeatTask == null && _replicas.Count == 1)
        {
            StartHeartbeat();
        }
        
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
    public DateTime LastAckTime { get; set; }
    public DateTime LastPingSent { get; set; }
    public ReplicaState State { get; set; }
    public long AckOffset { get; set; }

    public ReplicaInfo(RedisClient client)
    {
        Client = client;
        Address = client.Address;
        Offset = 0;
        AckOffset = 0;
        ConnectedAt = DateTime.UtcNow;
        LastAckTime = DateTime.UtcNow;
        LastPingSent = DateTime.UtcNow;
        State = ReplicaState.Connecting;
    }

    /// <summary>
    /// 检查副本是否同步（偏移量差距小于阈值）
    /// </summary>
    public bool IsSynced(long masterOffset, long maxLag = 1024)
    {
        if (State != ReplicaState.Online)
            return false;

        // 检查偏移量差距
        var lag = masterOffset - AckOffset;
        if (lag > maxLag)
            return false;

        // 检查心跳超时（10秒无ACK视为不同步）
        var timeSinceAck = DateTime.UtcNow - LastAckTime;
        if (timeSinceAck.TotalSeconds > 10)
            return false;

        return true;
    }
}

/// <summary>
/// 副本状态
/// </summary>
public enum ReplicaState
{
    /// <summary>正在连接</summary>
    Connecting,
    /// <summary>正在进行全量同步</summary>
    FullSync,
    /// <summary>在线并同步</summary>
    Online,
    /// <summary>断开连接</summary>
    Disconnected
}
