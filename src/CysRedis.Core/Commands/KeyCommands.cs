using System.Text;
using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// DEL command.
/// </summary>
public class DelCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        
        var db = context.Database;
        int deleted = 0;
        
        for (int i = 0; i < context.ArgCount; i++)
        {
            var key = context.GetArg(i);
            if (db.Delete(key))
            {
                deleted++;
                // 发送Keyspace通知
                context.Server.KeyspaceNotifier.Notify(context.Client.DatabaseIndex, key, "del");
            }
        }
        
        return context.Client.WriteIntegerAsync(deleted, cancellationToken);
    }
}

/// <summary>
/// EXISTS command.
/// </summary>
public class ExistsCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        
        var db = context.Database;
        int count = 0;
        
        for (int i = 0; i < context.ArgCount; i++)
        {
            if (db.Exists(context.GetArg(i)))
                count++;
        }
        
        return context.Client.WriteIntegerAsync(count, cancellationToken);
    }
}

/// <summary>
/// TYPE command.
/// </summary>
public class TypeCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        
        var typeName = context.Database.GetType(context.GetArg(0)) ?? "none";
        return context.Client.WriteResponseAsync(RespValue.SimpleString(typeName), cancellationToken);
    }
}

/// <summary>
/// KEYS command.
/// </summary>
public class KeysCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var pattern = context.GetArg(0);
        
        var keys = context.Database.Keys(pattern).ToArray();
        var values = keys.Select(k => RespValue.BulkString(k)).ToArray();
        
        await context.Client.WriteResponseAsync(RespValue.Array(values), cancellationToken);
    }
}

/// <summary>
/// SCAN command.
/// </summary>
public class ScanCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var cursor = context.GetArgAsInt(0);
        
        string pattern = "*";
        int count = 10;

        // Parse options
        for (int i = 1; i < context.ArgCount; i++)
        {
            var opt = context.GetArg(i).ToUpperInvariant();
            switch (opt)
            {
                case "MATCH":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    pattern = context.GetArg(i);
                    break;
                case "COUNT":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    count = (int)context.GetArgAsInt(i);
                    break;
            }
        }

        var allKeys = context.Database.Keys(pattern).ToList();
        
        // Simple cursor implementation
        int startIndex = (int)cursor;
        int endIndex = Math.Min(startIndex + count, allKeys.Count);
        long nextCursor = endIndex >= allKeys.Count ? 0 : endIndex;

        var resultKeys = allKeys.Skip(startIndex).Take(count)
            .Select(k => RespValue.BulkString(k))
            .ToArray();

        var response = RespValue.Array(
            RespValue.BulkString(nextCursor.ToString()),
            RespValue.Array(resultKeys)
        );

        await context.Client.WriteResponseAsync(response, cancellationToken);
    }
}

/// <summary>
/// RENAME command.
/// </summary>
public class RenameCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var oldKey = context.GetArg(0);
        var newKey = context.GetArg(1);
        
        if (!context.Database.Exists(oldKey))
            throw new RedisException("no such key");
            
        context.Database.Rename(oldKey, newKey);
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// RENAMENX command.
/// </summary>
public class RenameNxCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var oldKey = context.GetArg(0);
        var newKey = context.GetArg(1);
        
        if (!context.Database.Exists(oldKey))
            throw new RedisException("no such key");
            
        if (context.Database.Exists(newKey))
            return context.Client.WriteIntegerAsync(0, cancellationToken);
            
        context.Database.Rename(oldKey, newKey);
        return context.Client.WriteIntegerAsync(1, cancellationToken);
    }
}

/// <summary>
/// RANDOMKEY command.
/// </summary>
public class RandomKeyCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        var key = context.Database.RandomKey();
        return context.Client.WriteBulkStringAsync(key, cancellationToken);
    }
}

/// <summary>
/// DBSIZE command.
/// </summary>
public class DbSizeCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        return context.Client.WriteIntegerAsync(context.Database.KeyCount, cancellationToken);
    }
}

/// <summary>
/// FLUSHDB command.
/// </summary>
public class FlushDbCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.Database.Flush();
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// FLUSHALL command.
/// </summary>
public class FlushAllCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.Server.Store.FlushAll();
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// EXPIRE command.
/// </summary>
public class ExpireCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var seconds = context.GetArgAsInt(1);
        
        var set = context.Database.SetExpire(key, DateTime.UtcNow.AddSeconds(seconds));
        return context.Client.WriteIntegerAsync(set ? 1 : 0, cancellationToken);
    }
}

/// <summary>
/// PEXPIRE command.
/// </summary>
public class PExpireCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var milliseconds = context.GetArgAsInt(1);
        
        var set = context.Database.SetExpire(key, DateTime.UtcNow.AddMilliseconds(milliseconds));
        return context.Client.WriteIntegerAsync(set ? 1 : 0, cancellationToken);
    }
}

/// <summary>
/// EXPIREAT command.
/// </summary>
public class ExpireAtCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var timestamp = context.GetArgAsInt(1);
        
        var expireAt = DateTimeOffset.FromUnixTimeSeconds(timestamp).UtcDateTime;
        var set = context.Database.SetExpire(key, expireAt);
        return context.Client.WriteIntegerAsync(set ? 1 : 0, cancellationToken);
    }
}

/// <summary>
/// PEXPIREAT command.
/// </summary>
public class PExpireAtCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var timestamp = context.GetArgAsInt(1);
        
        var expireAt = DateTimeOffset.FromUnixTimeMilliseconds(timestamp).UtcDateTime;
        var set = context.Database.SetExpire(key, expireAt);
        return context.Client.WriteIntegerAsync(set ? 1 : 0, cancellationToken);
    }
}

/// <summary>
/// TTL command.
/// </summary>
public class TtlCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var ttl = context.Database.GetTtl(key) ?? -2;
        return context.Client.WriteIntegerAsync(ttl, cancellationToken);
    }
}

/// <summary>
/// PTTL command.
/// </summary>
public class PTtlCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var pttl = context.Database.GetPttl(key) ?? -2;
        return context.Client.WriteIntegerAsync(pttl, cancellationToken);
    }
}

/// <summary>
/// PERSIST command.
/// </summary>
public class PersistCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var removed = context.Database.Persist(key);
        return context.Client.WriteIntegerAsync(removed ? 1 : 0, cancellationToken);
    }
}

/// <summary>
/// EXPIRETIME command.
/// </summary>
public class ExpireTimeCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        
        if (!context.Database.Exists(key))
            return context.Client.WriteIntegerAsync(-2, cancellationToken);
            
        var expiry = context.Database.GetExpire(key);
        if (expiry == null)
            return context.Client.WriteIntegerAsync(-1, cancellationToken);
            
        var timestamp = new DateTimeOffset(expiry.Value).ToUnixTimeSeconds();
        return context.Client.WriteIntegerAsync(timestamp, cancellationToken);
    }
}

/// <summary>
/// COPY command - copies a key to another key.
/// </summary>
public class CopyCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var sourceKey = context.GetArg(0);
        var destKey = context.GetArg(1);
        bool replace = false;
        int destDb = context.Client.DatabaseIndex;
        
        // Parse REPLACE/DB parameters
        for (int i = 2; i < context.ArgCount; i++)
        {
            var opt = context.GetArg(i).ToUpperInvariant();
            if (opt == "REPLACE")
            {
                replace = true;
            }
            else if (opt == "DB")
            {
                i++;
                if (i >= context.ArgCount) throw new SyntaxErrorException();
                destDb = (int)context.GetArgAsInt(i);
            }
        }
        
        var sourceDb = context.Database;
        var targetDb = context.Server.Store.GetDatabase(destDb);
        var source = sourceDb.Get(sourceKey);
        
        if (source == null)
            return context.Client.WriteIntegerAsync(0, cancellationToken);
        
        if (!replace && targetDb.Exists(destKey))
            return context.Client.WriteIntegerAsync(0, cancellationToken);
        
        // Deep copy object
        var copy = DeepCopyRedisObject(source);
        targetDb.Set(destKey, copy);
        
        // Copy expiration time
        var expire = sourceDb.GetExpire(sourceKey);
        if (expire.HasValue)
            targetDb.SetExpire(destKey, expire.Value);
        
        return context.Client.WriteIntegerAsync(1, cancellationToken);
    }
    
    private static RedisObject DeepCopyRedisObject(RedisObject obj)
    {
        return obj switch
        {
            RedisString s => new RedisString((byte[])s.Value.Clone()),
            RedisList l => CopyList(l),
            RedisSet s => CopySet(s),
            RedisSortedSet z => CopySortedSet(z),
            RedisHash h => CopyHash(h),
            RedisStream stream => CopyStream(stream),
            _ => throw new NotSupportedException($"Cannot copy object of type {obj.GetType().Name}")
        };
    }
    
    private static RedisList CopyList(RedisList source)
    {
        var copy = new RedisList();
        for (int i = 0; i < source.Count; i++)
        {
            var item = source.GetByIndex(i);
            if (item != null)
                copy.PushRight((byte[])item.Clone());
        }
        return copy;
    }
    
    private static RedisSet CopySet(RedisSet source)
    {
        var copy = new RedisSet();
        foreach (var member in source.Members)
        {
            copy.Add(member);
        }
        return copy;
    }
    
    private static RedisSortedSet CopySortedSet(RedisSortedSet source)
    {
        var copy = new RedisSortedSet();
        foreach (var member in source.Members)
        {
            var score = source.GetScore(member);
            if (score.HasValue)
                copy.Add(member, score.Value);
        }
        return copy;
    }
    
    private static RedisHash CopyHash(RedisHash source)
    {
        var copy = new RedisHash();
        foreach (var entry in source.Entries)
        {
            copy.Set(entry.Key, (byte[])entry.Value.Clone());
        }
        return copy;
    }
    
    private static RedisStream CopyStream(RedisStream source)
    {
        // For stream, we create a new instance
        // Stream copying is complex due to consumer groups, so we do a shallow copy
        var copy = new RedisStream();
        // Note: Full stream copying with consumer groups would require more work
        return copy;
    }
}
