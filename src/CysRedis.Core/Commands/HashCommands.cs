using System.Text;
using CysRedis.Core.DataStructures;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// HSET command.
/// </summary>
public class HSetCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 3 || (context.ArgCount - 1) % 2 != 0)
            throw new Common.WrongArityException("HSET");

        var key = context.GetArg(0);
        var hash = context.Database.GetOrCreate(key, () => new RedisHash());
        
        int newFields = 0;
        for (int i = 1; i < context.ArgCount; i += 2)
        {
            var field = context.GetArg(i);
            var value = Encoding.UTF8.GetBytes(context.GetArg(i + 1));
            if (!hash.Exists(field))
                newFields++;
            hash.Set(field, value);
        }
        
        return context.Client.WriteIntegerAsync(newFields, cancellationToken);
    }
}

/// <summary>
/// HGET command.
/// </summary>
public class HGetCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var field = context.GetArg(1);
        
        var hash = context.Database.Get<RedisHash>(key);
        var value = hash?.Get(field);
        
        if (value == null)
            return context.Client.WriteNullAsync(cancellationToken);
            
        return context.Client.WriteBulkStringAsync(Encoding.UTF8.GetString(value), cancellationToken);
    }
}

/// <summary>
/// HMSET command.
/// </summary>
public class HMSetCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 3 || (context.ArgCount - 1) % 2 != 0)
            throw new Common.WrongArityException("HMSET");

        var key = context.GetArg(0);
        var hash = context.Database.GetOrCreate(key, () => new RedisHash());
        
        for (int i = 1; i < context.ArgCount; i += 2)
        {
            var field = context.GetArg(i);
            var value = Encoding.UTF8.GetBytes(context.GetArg(i + 1));
            hash.Set(field, value);
        }
        
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// HMGET command.
/// </summary>
public class HMGetCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var hash = context.Database.Get<RedisHash>(key);
        
        var values = new RespValue[context.ArgCount - 1];
        for (int i = 1; i < context.ArgCount; i++)
        {
            var field = context.GetArg(i);
            var value = hash?.Get(field);
            values[i - 1] = value != null 
                ? RespValue.BulkString(Encoding.UTF8.GetString(value)) 
                : RespValue.Null;
        }
        
        await context.Client.WriteResponseAsync(RespValue.Array(values), cancellationToken);
    }
}

/// <summary>
/// HGETALL command.
/// </summary>
public class HGetAllCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var hash = context.Database.Get<RedisHash>(key);
        
        if (hash == null || hash.Count == 0)
        {
            await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
            return;
        }

        var values = new List<RespValue>();
        foreach (var entry in hash.Entries)
        {
            values.Add(RespValue.BulkString(entry.Key));
            values.Add(RespValue.BulkString(Encoding.UTF8.GetString(entry.Value)));
        }
        
        await context.Client.WriteResponseAsync(RespValue.Array(values.ToArray()), cancellationToken);
    }
}

/// <summary>
/// HDEL command.
/// </summary>
public class HDelCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var hash = context.Database.Get<RedisHash>(key);
        
        if (hash == null)
            return context.Client.WriteIntegerAsync(0, cancellationToken);

        int deleted = 0;
        for (int i = 1; i < context.ArgCount; i++)
        {
            if (hash.Delete(context.GetArg(i)))
                deleted++;
        }
        
        return context.Client.WriteIntegerAsync(deleted, cancellationToken);
    }
}

/// <summary>
/// HEXISTS command.
/// </summary>
public class HExistsCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var field = context.GetArg(1);
        
        var hash = context.Database.Get<RedisHash>(key);
        var exists = hash?.Exists(field) ?? false;
        
        return context.Client.WriteIntegerAsync(exists ? 1 : 0, cancellationToken);
    }
}

/// <summary>
/// HLEN command.
/// </summary>
public class HLenCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var hash = context.Database.Get<RedisHash>(key);
        return context.Client.WriteIntegerAsync(hash?.Count ?? 0, cancellationToken);
    }
}

/// <summary>
/// HKEYS command.
/// </summary>
public class HKeysCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var hash = context.Database.Get<RedisHash>(key);
        
        var keys = hash?.Keys.Select(k => RespValue.BulkString(k)).ToArray() ?? Array.Empty<RespValue>();
        await context.Client.WriteResponseAsync(RespValue.Array(keys), cancellationToken);
    }
}

/// <summary>
/// HVALS command.
/// </summary>
public class HValsCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var hash = context.Database.Get<RedisHash>(key);
        
        var values = hash?.Values.Select(v => RespValue.BulkString(Encoding.UTF8.GetString(v))).ToArray() 
            ?? Array.Empty<RespValue>();
        await context.Client.WriteResponseAsync(RespValue.Array(values), cancellationToken);
    }
}

/// <summary>
/// HINCRBY command.
/// </summary>
public class HIncrByCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var field = context.GetArg(1);
        var increment = context.GetArgAsInt(2);
        
        var hash = context.Database.GetOrCreate(key, () => new RedisHash());
        var result = hash.IncrBy(field, increment);
        
        return context.Client.WriteIntegerAsync(result, cancellationToken);
    }
}

/// <summary>
/// HINCRBYFLOAT command.
/// </summary>
public class HIncrByFloatCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var field = context.GetArg(1);
        var increment = context.GetArgAsDouble(2);
        
        var hash = context.Database.GetOrCreate(key, () => new RedisHash());
        var result = hash.IncrByFloat(field, increment);
        
        return context.Client.WriteBulkStringAsync(
            result.ToString("G17", System.Globalization.CultureInfo.InvariantCulture), 
            cancellationToken);
    }
}

/// <summary>
/// HSETNX command.
/// </summary>
public class HSetNxCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var field = context.GetArg(1);
        var value = Encoding.UTF8.GetBytes(context.GetArg(2));
        
        var hash = context.Database.GetOrCreate(key, () => new RedisHash());
        var set = hash.SetNx(field, value);
        
        return context.Client.WriteIntegerAsync(set ? 1 : 0, cancellationToken);
    }
}

/// <summary>
/// HEXPIRE command - sets field expiration in seconds (Redis 8.0+).
/// </summary>
public class HExpireCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        var key = context.GetArg(0);
        var seconds = context.GetArgAsInt(1);
        var hash = context.Database.Get<RedisHash>(key);
        
        if (hash == null)
        {
            await context.Client.WriteIntegerAsync(-2, cancellationToken);
            return;
        }
        
        var expireAt = DateTime.UtcNow.AddSeconds(seconds);
        var results = new RespValue[context.ArgCount - 2];
        
        for (int i = 2; i < context.ArgCount; i++)
        {
            var field = context.GetArg(i);
            if (!hash.Exists(field))
                results[i - 2] = new RespValue(-2); // Field doesn't exist
            else
            {
                hash.SetFieldExpire(field, expireAt);
                results[i - 2] = new RespValue(1);
            }
        }
        
        await context.Client.WriteResponseAsync(RespValue.Array(results), cancellationToken);
    }
}

/// <summary>
/// HPEXPIRE command - sets field expiration in milliseconds (Redis 8.0+).
/// </summary>
public class HPExpireCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        var key = context.GetArg(0);
        var milliseconds = context.GetArgAsInt(1);
        var hash = context.Database.Get<RedisHash>(key);
        
        if (hash == null)
        {
            await context.Client.WriteIntegerAsync(-2, cancellationToken);
            return;
        }
        
        var expireAt = DateTime.UtcNow.AddMilliseconds(milliseconds);
        var results = new RespValue[context.ArgCount - 2];
        
        for (int i = 2; i < context.ArgCount; i++)
        {
            var field = context.GetArg(i);
            if (!hash.Exists(field))
                results[i - 2] = new RespValue(-2);
            else
            {
                hash.SetFieldExpire(field, expireAt);
                results[i - 2] = new RespValue(1);
            }
        }
        
        await context.Client.WriteResponseAsync(RespValue.Array(results), cancellationToken);
    }
}

/// <summary>
/// HTTL command - gets field TTL in seconds (Redis 8.0+).
/// </summary>
public class HTtlCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var hash = context.Database.Get<RedisHash>(key);
        
        if (hash == null)
        {
            await context.Client.WriteIntegerAsync(-2, cancellationToken);
            return;
        }
        
        var results = new RespValue[context.ArgCount - 1];
        
        for (int i = 1; i < context.ArgCount; i++)
        {
            var field = context.GetArg(i);
            var ttl = hash.GetFieldTtl(field) ?? -2;
            results[i - 1] = new RespValue(ttl);
        }
        
        await context.Client.WriteResponseAsync(RespValue.Array(results), cancellationToken);
    }
}

/// <summary>
/// HPTTL command - gets field TTL in milliseconds (Redis 8.0+).
/// </summary>
public class HPTtlCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var hash = context.Database.Get<RedisHash>(key);
        
        if (hash == null)
        {
            await context.Client.WriteIntegerAsync(-2, cancellationToken);
            return;
        }
        
        var results = new RespValue[context.ArgCount - 1];
        
        for (int i = 1; i < context.ArgCount; i++)
        {
            var field = context.GetArg(i);
            var pttl = hash.GetFieldPttl(field) ?? -2;
            results[i - 1] = new RespValue(pttl);
        }
        
        await context.Client.WriteResponseAsync(RespValue.Array(results), cancellationToken);
    }
}

/// <summary>
/// HPERSIST command - removes field expiration (Redis 8.0+).
/// </summary>
public class HPersistCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var hash = context.Database.Get<RedisHash>(key);
        
        if (hash == null)
        {
            await context.Client.WriteIntegerAsync(-2, cancellationToken);
            return;
        }
        
        var results = new RespValue[context.ArgCount - 1];
        
        for (int i = 1; i < context.ArgCount; i++)
        {
            var field = context.GetArg(i);
            if (!hash.Exists(field))
                results[i - 1] = new RespValue(-2); // Field doesn't exist
            else if (hash.GetFieldExpire(field) == null)
                results[i - 1] = new RespValue(-1); // No expiration set
            else
            {
                hash.PersistField(field);
                results[i - 1] = new RespValue(1);
            }
        }
        
        await context.Client.WriteResponseAsync(RespValue.Array(results), cancellationToken);
    }
}
