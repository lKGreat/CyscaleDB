using System.Collections.Concurrent;
using CysRedis.Core.Common;
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

        if (!_handlers.TryGetValue(commandName, out var handler))
        {
            await client.WriteErrorAsync($"ERR unknown command '{args[0]}'", cancellationToken);
            return;
        }

        var context = new CommandContext(_server, client, commandName, args);

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

        // Set commands
        Register("SADD", new SAddCommand());
        Register("SREM", new SRemCommand());
        Register("SMEMBERS", new SMembersCommand());
        Register("SISMEMBER", new SIsMemberCommand());
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
        Register("ZRANK", new ZRankCommand());
        Register("ZREVRANK", new ZRevRankCommand());
        Register("ZRANGE", new ZRangeCommand());
        Register("ZREVRANGE", new ZRevRangeCommand());
        Register("ZINCRBY", new ZIncrByCommand());
        Register("ZCARD", new ZCardCommand());
        Register("ZCOUNT", new ZCountCommand());
        Register("ZRANGEBYSCORE", new ZRangeByScoreCommand());

        // Server commands
        Register("INFO", new InfoCommand());
        Register("COMMAND", new CommandInfoCommand());
        Register("CONFIG", new ConfigCommand());
        Register("DEBUG", new DebugCommand());
        Register("TIME", new TimeCommand());
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
}
