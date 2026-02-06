using System.Collections.Concurrent;
using System.Diagnostics;
using CysRedis.Core.Common;
using CysRedis.Core.Monitoring;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// Dispatches commands to their handlers.
/// </summary>
public class CommandDispatcher
{
    private readonly RedisServer _server;
    private readonly ConcurrentDictionary<string, ICommandHandler> _handlers;

    public CommandDispatcher(RedisServer server)
    {
        _server = server ?? throw new ArgumentNullException(nameof(server));
        _handlers = new ConcurrentDictionary<string, ICommandHandler>(StringComparer.OrdinalIgnoreCase);
        
        RegisterBuiltInCommands();
    }

    /// <summary>
    /// Registers a command handler.
    /// </summary>
    public void Register(string command, ICommandHandler handler)
    {
        _handlers[command.ToUpperInvariant()] = handler;
    }

    /// <summary>
    /// Gets a command handler by name.
    /// </summary>
    public bool TryGetHandler(string commandName, out ICommandHandler handler)
    {
        return _handlers.TryGetValue(commandName.ToUpperInvariant(), out handler!);
    }

    /// <summary>
    /// Executes a command.
    /// </summary>
    public async Task ExecuteAsync(RedisClient client, string[] args, CancellationToken cancellationToken)
    {
        if (args.Length == 0)
        {
            await client.WriteErrorAsync("ERR empty command", cancellationToken);
            return;
        }

        var commandName = args[0].ToUpperInvariant();

        // Handle transaction queueing
        if (client.InTransaction && !IsTransactionCommand(commandName))
        {
            if (!_handlers.ContainsKey(commandName))
            {
                await client.WriteErrorAsync($"ERR unknown command '{args[0]}'", cancellationToken);
                return;
            }
            client.QueueCommand(args);
            await client.WriteResponseAsync(RespValue.SimpleString("QUEUED"), cancellationToken);
            return;
        }

        if (!_handlers.TryGetValue(commandName, out var handler))
        {
            await client.WriteErrorAsync($"ERR unknown command '{args[0]}'", cancellationToken);
            return;
        }

        var context = new CommandContext(_server, client, commandName, args);

        // 检查集群槽位（如果启用了集群模式）
        if (_server.Cluster.IsEnabled && !IsClusterCommand(commandName) && !IsTransactionCommand(commandName))
        {
            // 检查命令涉及的键是否在当前节点上
            var keys = ExtractKeys(commandName, args);
            foreach (var key in keys)
            {
                if (!_server.Cluster.IsKeyInMySlots(key))
                {
                    var slot = Cluster.ClusterManager.GetSlot(key);
                    var node = _server.Cluster.GetNodeForSlot(slot);
                    
                    if (node != null)
                    {
                        // 返回MOVED重定向
                        await client.WriteErrorAsync(
                            $"MOVED {slot} {node.IpAddress}:{node.Port}", 
                            cancellationToken);
                        return;
                    }
                    else
                    {
                        // 槽位未分配
                        await client.WriteErrorAsync(
                            $"CLUSTERDOWN Hash slot not served", 
                            cancellationToken);
                        return;
                    }
                }
            }
        }

        // Start timing for slow log
        var startTime = Stopwatch.GetTimestamp();

        try
        {
            await handler.ExecuteAsync(context, cancellationToken);
        }
        catch (WrongArityException ex)
        {
            await client.WriteErrorAsync(ex.GetRespError(), cancellationToken);
        }
        catch (WrongTypeException ex)
        {
            await client.WriteErrorAsync(ex.GetRespError(), cancellationToken);
        }
        catch (RedisException ex)
        {
            await client.WriteErrorAsync(ex.GetRespError(), cancellationToken);
        }
        finally
        {
            // Calculate elapsed time
            var elapsedTicks = Stopwatch.GetTimestamp() - startTime;
            var elapsedMicroseconds = (long)(elapsedTicks * 1_000_000.0 / Stopwatch.Frequency);
            var elapsedMilliseconds = elapsedMicroseconds / 1000.0;

            // Record to slow log if threshold exceeded
            _server.SlowLog.Record(elapsedMicroseconds, args, client.Name, client.Address);

            // Record to latency monitor
            _server.LatencyMonitor.Record(Monitoring.LatencyMonitor.EventTypes.Command, (long)elapsedMilliseconds);

            // Record to command latency histogram
            _server.CommandLatency.Record(elapsedMilliseconds);
        }
    }

    private static bool IsTransactionCommand(string command)
    {
        return command switch
        {
            "MULTI" or "EXEC" or "DISCARD" or "WATCH" or "UNWATCH" => true,
            _ => false
        };
    }

    private static bool IsClusterCommand(string command)
    {
        return command switch
        {
            "CLUSTER" or "PING" or "INFO" or "COMMAND" or "AUTH" or "SELECT" or "CLIENT" => true,
            _ => false
        };
    }

    /// <summary>
    /// 提取命令涉及的键
    /// </summary>
    private static List<string> ExtractKeys(string commandName, string[] args)
    {
        var keys = new List<string>();
        
        // 简化实现：根据命令类型提取键
        // 第一个参数通常是键（对于大多数命令）
        if (args.Length > 1)
        {
            switch (commandName)
            {
                case "GET" or "SET" or "DEL" or "EXISTS" or "EXPIRE" or "TTL" or "TYPE":
                case "LPUSH" or "RPUSH" or "LPOP" or "RPOP" or "LRANGE" or "LLEN":
                case "SADD" or "SREM" or "SMEMBERS" or "SISMEMBER":
                case "ZADD" or "ZREM" or "ZRANGE" or "ZSCORE":
                case "HSET" or "HGET" or "HDEL" or "HGETALL":
                    keys.Add(args[1]); // 第一个参数是键
                    break;
                
                case "MGET" or "MSET":
                    // 多键命令
                    for (int i = 1; i < args.Length; i += (commandName == "MSET" ? 2 : 1))
                    {
                        keys.Add(args[i]);
                    }
                    break;
                
                case "RENAME":
                    // 两个键
                    if (args.Length >= 3)
                    {
                        keys.Add(args[1]);
                        keys.Add(args[2]);
                    }
                    break;
            }
        }
        
        return keys;
    }

    private void RegisterBuiltInCommands()
    {
        // Connection commands
        Register("PING", new PingCommand());
        Register("ECHO", new EchoCommand());
        Register("SELECT", new SelectCommand());
        Register("QUIT", new QuitCommand());
        Register("AUTH", new AuthCommand());
        Register("CLIENT", new ClientCommand());

        // String commands
        Register("SET", new SetCommand());
        Register("GET", new GetCommand());
        Register("MSET", new MSetCommand());
        Register("MGET", new MGetCommand());
        Register("SETNX", new SetNxCommand());
        Register("SETEX", new SetExCommand());
        Register("PSETEX", new PSetExCommand());
        Register("GETSET", new GetSetCommand());
        Register("GETEX", new GetExCommand());
        Register("GETDEL", new GetDelCommand());
        Register("INCR", new IncrCommand());
        Register("DECR", new DecrCommand());
        Register("INCRBY", new IncrByCommand());
        Register("DECRBY", new DecrByCommand());
        Register("INCRBYFLOAT", new IncrByFloatCommand());
        Register("APPEND", new AppendCommand());
        Register("STRLEN", new StrLenCommand());
        Register("GETRANGE", new GetRangeCommand());
        Register("SETRANGE", new SetRangeCommand());

        // Key commands
        Register("DEL", new DelCommand());
        Register("EXISTS", new ExistsCommand());
        Register("TYPE", new TypeCommand());
        Register("KEYS", new KeysCommand());
        Register("SCAN", new ScanCommand());
        Register("RENAME", new RenameCommand());
        Register("RENAMENX", new RenameNxCommand());
        Register("RANDOMKEY", new RandomKeyCommand());
        Register("DBSIZE", new DbSizeCommand());
        Register("FLUSHDB", new FlushDbCommand());
        Register("FLUSHALL", new FlushAllCommand());
        Register("EXPIRE", new ExpireCommand());
        Register("PEXPIRE", new PExpireCommand());
        Register("EXPIREAT", new ExpireAtCommand());
        Register("PEXPIREAT", new PExpireAtCommand());
        Register("TTL", new TtlCommand());
        Register("PTTL", new PTtlCommand());
        Register("PERSIST", new PersistCommand());
        Register("EXPIRETIME", new ExpireTimeCommand());
        Register("COPY", new CopyCommand());

        // Hash commands
        Register("HSET", new HSetCommand());
        Register("HGET", new HGetCommand());
        Register("HMSET", new HMSetCommand());
        Register("HMGET", new HMGetCommand());
        Register("HGETALL", new HGetAllCommand());
        Register("HDEL", new HDelCommand());
        Register("HEXISTS", new HExistsCommand());
        Register("HLEN", new HLenCommand());
        Register("HKEYS", new HKeysCommand());
        Register("HVALS", new HValsCommand());
        Register("HINCRBY", new HIncrByCommand());
        Register("HINCRBYFLOAT", new HIncrByFloatCommand());
        Register("HSETNX", new HSetNxCommand());
        Register("HEXPIRE", new HExpireCommand());
        Register("HPEXPIRE", new HPExpireCommand());
        Register("HTTL", new HTtlCommand());
        Register("HPTTL", new HPTtlCommand());
        Register("HPERSIST", new HPersistCommand());

        // List commands
        Register("LPUSH", new LPushCommand());
        Register("RPUSH", new RPushCommand());
        Register("LPOP", new LPopCommand());
        Register("RPOP", new RPopCommand());
        Register("LRANGE", new LRangeCommand());
        Register("LINDEX", new LIndexCommand());
        Register("LSET", new LSetCommand());
        Register("LLEN", new LLenCommand());
        Register("LTRIM", new LTrimCommand());
        Register("LPOS", new LPosCommand());
        
        // Blocking list commands
        Register("BLPOP", new BLPopCommand());
        Register("BRPOP", new BRPopCommand());
        Register("BLMOVE", new BLMoveCommand());

        // Set commands
        Register("SADD", new SAddCommand());
        Register("SREM", new SRemCommand());
        Register("SMEMBERS", new SMembersCommand());
        Register("SISMEMBER", new SIsMemberCommand());
        Register("SMISMEMBER", new SMIsMemberCommand());
        Register("SCARD", new SCardCommand());
        Register("SPOP", new SPopCommand());
        Register("SRANDMEMBER", new SRandMemberCommand());
        Register("SUNION", new SUnionCommand());
        Register("SINTER", new SInterCommand());
        Register("SDIFF", new SDiffCommand());

        // Sorted Set commands
        Register("ZADD", new ZAddCommand());
        Register("ZREM", new ZRemCommand());
        Register("ZSCORE", new ZScoreCommand());
        Register("ZMSCORE", new ZMScoreCommand());
        Register("ZRANK", new ZRankCommand());
        Register("ZREVRANK", new ZRevRankCommand());
        Register("ZRANGE", new ZRangeCommand());
        Register("ZREVRANGE", new ZRevRangeCommand());
        Register("ZINCRBY", new ZIncrByCommand());
        Register("ZCARD", new ZCardCommand());
        Register("ZCOUNT", new ZCountCommand());
        Register("ZRANGEBYSCORE", new ZRangeByScoreCommand());
        
        // Blocking sorted set commands
        Register("BZPOPMIN", new BZPopMinCommand());
        Register("BZPOPMAX", new BZPopMaxCommand());

        // Transaction commands
        Register("MULTI", new MultiCommand());
        Register("EXEC", new ExecCommand());
        Register("DISCARD", new DiscardCommand());
        Register("WATCH", new WatchCommand());
        Register("UNWATCH", new UnwatchCommand());

        // Pub/Sub commands
        Register("SUBSCRIBE", new SubscribeCommand());
        Register("UNSUBSCRIBE", new UnsubscribeCommand());
        Register("PSUBSCRIBE", new PSubscribeCommand());
        Register("PUNSUBSCRIBE", new PUnsubscribeCommand());
        Register("PUBLISH", new PublishCommand());
        Register("PUBSUB", new PubSubInfoCommand());

        // Scripting commands
        Register("EVAL", new EvalCommand());
        Register("EVALSHA", new EvalShaCommand());
        Register("SCRIPT", new ScriptCommand());

        // Persistence commands
        Register("SAVE", new SaveCommand());
        Register("BGSAVE", new BgSaveCommand());
        Register("BGREWRITEAOF", new BgRewriteAofCommand());
        Register("LASTSAVE", new LastSaveCommand());

        // Stream commands
        Register("XADD", new XAddCommand());
        Register("XREAD", new XReadCommand());
        Register("XRANGE", new XRangeCommand());
        Register("XLEN", new XLenCommand());
        Register("XGROUP", new XGroupCommand());
        Register("XACK", new XAckCommand());
        Register("XTRIM", new XTrimCommand());
        Register("XINFO", new XInfoCommand());
        Register("XCLAIM", new XClaimCommand());
        Register("XAUTOCLAIM", new XAutoClaimCommand());
        Register("XPENDING", new XPendingCommand());
        Register("XSETID", new XSetIdCommand());

        // HyperLogLog commands
        Register("PFADD", new PfAddCommand());
        Register("PFCOUNT", new PfCountCommand());
        Register("PFMERGE", new PfMergeCommand());

        // Geo commands
        Register("GEOADD", new GeoAddCommand());
        Register("GEOPOS", new GeoPosCommand());
        Register("GEODIST", new GeoDistCommand());
        Register("GEOSEARCH", new GeoSearchCommand());
        Register("GEOHASH", new GeoHashCommand());

        // Bitmap commands
        Register("SETBIT", new SetBitCommand());
        Register("GETBIT", new GetBitCommand());
        Register("BITCOUNT", new BitCountCommand());
        Register("BITOP", new BitOpCommand());

        // Replication commands
        Register("REPLICAOF", new ReplicaOfCommand());
        Register("SLAVEOF", new ReplicaOfCommand());
        Register("PSYNC", new PsyncCommand());
        Register("REPLCONF", new ReplConfCommand());
        Register("WAIT", new WaitCommand());

        // ACL commands
        Register("ACL", new AclCommand());

        // Cluster commands
        Register("CLUSTER", new ClusterCommand());

        // Server commands
        Register("INFO", new InfoCommand());
        Register("COMMAND", new CommandInfoCommand());
        Register("CONFIG", new ConfigCommand());
        Register("DEBUG", new DebugCommand());
        Register("TIME", new TimeCommand());
        
        // Memory commands
        Register("MEMORY", new MemoryCommand());

        // Monitoring commands
        Register("SLOWLOG", new SlowLogCommand());
        Register("LATENCY", new LatencyCommand());
    }

    /// <summary>
    /// Gets all registered command names.
    /// </summary>
    public IEnumerable<string> GetCommandNames() => _handlers.Keys;
}

/// <summary>
/// Command handler interface.
/// </summary>
public interface ICommandHandler
{
    /// <summary>
    /// Executes the command.
    /// </summary>
    Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken);
}

/// <summary>
/// Command execution context.
/// </summary>
public class CommandContext
{
    public RedisServer Server { get; }
    public RedisClient Client { get; }
    public string CommandName { get; }
    public string[] Args { get; }

    public CommandContext(RedisServer server, RedisClient client, string commandName, string[] args)
    {
        Server = server;
        Client = client;
        CommandName = commandName;
        Args = args;
    }

    /// <summary>
    /// Gets the current database for this client.
    /// </summary>
    public DataStructures.RedisDatabase Database => Server.Store.GetDatabase(Client.DatabaseIndex);

    /// <summary>
    /// Gets argument count (excluding command name).
    /// </summary>
    public int ArgCount => Args.Length - 1;

    /// <summary>
    /// Gets an argument by index (0 = first argument after command).
    /// </summary>
    public string GetArg(int index) => Args[index + 1];

    /// <summary>
    /// Gets an argument or default if not present.
    /// </summary>
    public string? GetArgOrDefault(int index, string? defaultValue = null)
        => index + 1 < Args.Length ? Args[index + 1] : defaultValue;

    /// <summary>
    /// Ensures minimum argument count.
    /// </summary>
    public void EnsureMinArgs(int min)
    {
        if (ArgCount < min)
            throw new WrongArityException(CommandName);
    }

    /// <summary>
    /// Ensures exact argument count.
    /// </summary>
    public void EnsureArgCount(int count)
    {
        if (ArgCount != count)
            throw new WrongArityException(CommandName);
    }

    /// <summary>
    /// Parses an argument as an integer.
    /// </summary>
    public long GetArgAsInt(int index)
    {
        var str = GetArg(index);
        if (!long.TryParse(str, out var value))
            throw new NotIntegerException();
        return value;
    }

    /// <summary>
    /// Parses an argument as a double.
    /// </summary>
    public double GetArgAsDouble(int index)
    {
        var str = GetArg(index);
        if (!double.TryParse(str, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var value))
            throw new NotFloatException();
        return value;
    }
    
    /// <summary>
    /// Gets an argument as bytes.
    /// </summary>
    public byte[] GetArgBytes(int index)
    {
        var str = GetArg(index);
        return System.Text.Encoding.UTF8.GetBytes(str);
    }
}
