using System.Text;
using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// SET command.
/// </summary>
public class SetCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var value = context.GetArg(1);

        bool nx = false, xx = false, get = false;
        TimeSpan? expiry = null;
        string? ifeqValue = null, ifneValue = null;
        string? ifdeqDigest = null, ifdneDigest = null;

        // Parse options
        for (int i = 2; i < context.ArgCount; i++)
        {
            var opt = context.GetArg(i).ToUpperInvariant();
            switch (opt)
            {
                case "NX":
                    nx = true;
                    break;
                case "XX":
                    xx = true;
                    break;
                case "GET":
                    get = true;
                    break;
                case "IFEQ":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    ifeqValue = context.GetArg(i);
                    break;
                case "IFNE":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    ifneValue = context.GetArg(i);
                    break;
                case "IFDEQ":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    ifdeqDigest = context.GetArg(i);
                    break;
                case "IFDNE":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    ifdneDigest = context.GetArg(i);
                    break;
                case "EX":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    expiry = TimeSpan.FromSeconds(context.GetArgAsInt(i - 1));
                    break;
                case "PX":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    expiry = TimeSpan.FromMilliseconds(context.GetArgAsInt(i - 1));
                    break;
                case "EXAT":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    var exatSec = context.GetArgAsInt(i - 1);
                    expiry = DateTimeOffset.FromUnixTimeSeconds(exatSec).UtcDateTime - DateTime.UtcNow;
                    break;
                case "PXAT":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    var pxatMs = context.GetArgAsInt(i - 1);
                    expiry = DateTimeOffset.FromUnixTimeMilliseconds(pxatMs).UtcDateTime - DateTime.UtcNow;
                    break;
                case "KEEPTTL":
                    // Keep existing TTL - handled below
                    break;
                default:
                    throw new SyntaxErrorException();
            }
        }

        var db = context.Database;
        string? oldValue = null;

        if (get)
        {
            var existing = db.Get<RedisString>(key);
            oldValue = existing?.GetString();
        }

        // Check conditional set requirements
        if (ifeqValue != null || ifneValue != null || ifdeqDigest != null || ifdneDigest != null)
        {
            var existing = db.Get<RedisString>(key);
            if (existing == null)
            {
                // Key doesn't exist - conditional check fails for IFEQ/IFDEQ
                if (ifeqValue != null || ifdeqDigest != null)
                {
                    if (get)
                        await context.Client.WriteBulkStringAsync(oldValue, cancellationToken);
                    else
                        await context.Client.WriteNullAsync(cancellationToken);
                    return;
                }
            }
            else
            {
                var currentValue = existing.GetString();
                
                // IFEQ: Set only if current value equals specified value
                if (ifeqValue != null && currentValue != ifeqValue)
                {
                    if (get)
                        await context.Client.WriteBulkStringAsync(oldValue, cancellationToken);
                    else
                        await context.Client.WriteNullAsync(cancellationToken);
                    return;
                }
                
                // IFNE: Set only if current value doesn't equal specified value
                if (ifneValue != null && currentValue == ifneValue)
                {
                    if (get)
                        await context.Client.WriteBulkStringAsync(oldValue, cancellationToken);
                    else
                        await context.Client.WriteNullAsync(cancellationToken);
                    return;
                }
                
                // IFDEQ: Set only if current digest equals specified digest
                if (ifdeqDigest != null)
                {
                    var currentDigest = ComputeXxh3Digest(currentValue);
                    if (!currentDigest.Equals(ifdeqDigest, StringComparison.OrdinalIgnoreCase))
                    {
                        if (get)
                            await context.Client.WriteBulkStringAsync(oldValue, cancellationToken);
                        else
                            await context.Client.WriteNullAsync(cancellationToken);
                        return;
                    }
                }
                
                // IFDNE: Set only if current digest doesn't equal specified digest
                if (ifdneDigest != null)
                {
                    var currentDigest = ComputeXxh3Digest(currentValue);
                    if (currentDigest.Equals(ifdneDigest, StringComparison.OrdinalIgnoreCase))
                    {
                        if (get)
                            await context.Client.WriteBulkStringAsync(oldValue, cancellationToken);
                        else
                            await context.Client.WriteNullAsync(cancellationToken);
                        return;
                    }
                }
            }
        }

        var newObj = new RedisString(value);

        bool set = true;
        if (nx)
        {
            set = db.SetNx(key, newObj);
        }
        else if (xx)
        {
            set = db.SetXx(key, newObj);
        }
        else
        {
            db.Set(key, newObj);
        }

        if (set && expiry.HasValue && expiry.Value.TotalMilliseconds > 0)
        {
            db.SetExpire(key, DateTime.UtcNow.Add(expiry.Value));
        }

        // 发送Keyspace通知
        if (set)
        {
            context.Server.KeyspaceNotifier.Notify(context.Client.DatabaseIndex, key, "set");
        }

        if (get)
        {
            await context.Client.WriteBulkStringAsync(oldValue, cancellationToken);
        }
        else if (set)
        {
            await context.Client.WriteOkAsync(cancellationToken);
        }
        else
        {
            await context.Client.WriteNullAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Compute XXH3 64-bit hash digest for a string (formatted as hex).
    /// This is a simple implementation - for production, consider using System.IO.Hashing.XxHash3 (.NET 7+)
    /// </summary>
    private static string ComputeXxh3Digest(string value)
    {
        // Simple hash implementation (can be replaced with XXH3 in .NET 7+)
        var bytes = Encoding.UTF8.GetBytes(value);
        ulong hash = 0x9E3779B185EBCA87UL; // XXH3 seed
        
        foreach (var b in bytes)
        {
            hash ^= b;
            hash *= 0x100000001B3UL; // FNV prime
        }
        
        return hash.ToString("x16"); // 16 hex chars
    }
}

/// <summary>
/// GET command.
/// </summary>
public class GetCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        
        var obj = context.Database.Get<RedisString>(key);
        var value = obj?.GetString();
        
        return context.Client.WriteBulkStringAsync(value, cancellationToken);
    }
}

/// <summary>
/// MSET command.
/// </summary>
public class MSetCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 2 || context.ArgCount % 2 != 0)
            throw new WrongArityException("MSET");

        var db = context.Database;
        for (int i = 0; i < context.ArgCount; i += 2)
        {
            var key = context.GetArg(i);
            var value = context.GetArg(i + 1);
            db.Set(key, new RedisString(value));
        }

        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// MGET command.
/// </summary>
public class MGetCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);

        var db = context.Database;
        var values = new RespValue[context.ArgCount];

        for (int i = 0; i < context.ArgCount; i++)
        {
            var key = context.GetArg(i);
            var obj = db.Get<RedisString>(key);
            values[i] = obj != null ? RespValue.BulkString(obj.GetString()) : RespValue.Null;
        }

        await context.Client.WriteResponseAsync(RespValue.Array(values), cancellationToken);
    }
}

/// <summary>
/// SETNX command.
/// </summary>
public class SetNxCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var value = context.GetArg(1);
        
        var set = context.Database.SetNx(key, new RedisString(value));
        return context.Client.WriteIntegerAsync(set ? 1 : 0, cancellationToken);
    }
}

/// <summary>
/// SETEX command.
/// </summary>
public class SetExCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var seconds = context.GetArgAsInt(1);
        var value = context.GetArg(2);
        
        var db = context.Database;
        db.Set(key, new RedisString(value));
        db.SetExpire(key, DateTime.UtcNow.AddSeconds(seconds));
        
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// PSETEX command.
/// </summary>
public class PSetExCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var milliseconds = context.GetArgAsInt(1);
        var value = context.GetArg(2);
        
        var db = context.Database;
        db.Set(key, new RedisString(value));
        db.SetExpire(key, DateTime.UtcNow.AddMilliseconds(milliseconds));
        
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// GETSET command.
/// </summary>
public class GetSetCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var value = context.GetArg(1);
        
        var db = context.Database;
        var old = db.Get<RedisString>(key);
        db.Set(key, new RedisString(value));
        
        return context.Client.WriteBulkStringAsync(old?.GetString(), cancellationToken);
    }
}

/// <summary>
/// GETEX command.
/// </summary>
public class GetExCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var key = context.GetArg(0);
        var db = context.Database;
        
        var obj = db.Get<RedisString>(key);
        if (obj == null)
            return context.Client.WriteNullAsync(cancellationToken);

        // Handle options
        for (int i = 1; i < context.ArgCount; i++)
        {
            var opt = context.GetArg(i).ToUpperInvariant();
            switch (opt)
            {
                case "EX":
                    i++;
                    db.SetExpire(key, DateTime.UtcNow.AddSeconds(context.GetArgAsInt(i - 1)));
                    break;
                case "PX":
                    i++;
                    db.SetExpire(key, DateTime.UtcNow.AddMilliseconds(context.GetArgAsInt(i - 1)));
                    break;
                case "EXAT":
                    i++;
                    db.SetExpire(key, DateTimeOffset.FromUnixTimeSeconds(context.GetArgAsInt(i - 1)).UtcDateTime);
                    break;
                case "PXAT":
                    i++;
                    db.SetExpire(key, DateTimeOffset.FromUnixTimeMilliseconds(context.GetArgAsInt(i - 1)).UtcDateTime);
                    break;
                case "PERSIST":
                    db.Persist(key);
                    break;
            }
        }

        return context.Client.WriteBulkStringAsync(obj.GetString(), cancellationToken);
    }
}

/// <summary>
/// GETDEL command.
/// </summary>
public class GetDelCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var db = context.Database;
        
        var obj = db.Get<RedisString>(key);
        if (obj == null)
            return context.Client.WriteNullAsync(cancellationToken);

        db.Delete(key);
        return context.Client.WriteBulkStringAsync(obj.GetString(), cancellationToken);
    }
}

/// <summary>
/// INCR command.
/// </summary>
public class IncrCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var result = IncrByValue(context.Database, key, 1);
        return context.Client.WriteIntegerAsync(result, cancellationToken);
    }

    internal static long IncrByValue(RedisDatabase db, string key, long increment)
    {
        var obj = db.Get(key);
        long currentValue = 0;

        if (obj != null)
        {
            if (obj is not RedisString str)
                throw new WrongTypeException();
            if (!str.TryGetInt64(out currentValue))
                throw new NotIntegerException();
        }

        var newValue = currentValue + increment;
        // Use shared object if possible
        var newStr = Protocol.SharedObjects.CreateIntegerString(newValue);
        db.Set(key, newStr);
        return newValue;
    }
}

/// <summary>
/// DECR command.
/// </summary>
public class DecrCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var result = IncrCommand.IncrByValue(context.Database, key, -1);
        return context.Client.WriteIntegerAsync(result, cancellationToken);
    }
}

/// <summary>
/// INCRBY command.
/// </summary>
public class IncrByCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var increment = context.GetArgAsInt(1);
        var result = IncrCommand.IncrByValue(context.Database, key, increment);
        return context.Client.WriteIntegerAsync(result, cancellationToken);
    }
}

/// <summary>
/// DECRBY command.
/// </summary>
public class DecrByCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var decrement = context.GetArgAsInt(1);
        var result = IncrCommand.IncrByValue(context.Database, key, -decrement);
        return context.Client.WriteIntegerAsync(result, cancellationToken);
    }
}

/// <summary>
/// INCRBYFLOAT command.
/// </summary>
public class IncrByFloatCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var increment = context.GetArgAsDouble(1);
        
        var db = context.Database;
        var obj = db.Get(key);
        double currentValue = 0;

        if (obj != null)
        {
            if (obj is not RedisString str)
                throw new WrongTypeException();
            if (!str.TryGetDouble(out currentValue))
                throw new NotFloatException();
        }

        var newValue = currentValue + increment;
        var newStr = new RedisString(newValue.ToString("G17", System.Globalization.CultureInfo.InvariantCulture));
        db.Set(key, newStr);
        
        return context.Client.WriteBulkStringAsync(newStr.GetString(), cancellationToken);
    }
}

/// <summary>
/// APPEND command.
/// </summary>
public class AppendCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var appendValue = Encoding.UTF8.GetBytes(context.GetArg(1));
        
        var db = context.Database;
        var existing = db.Get<RedisString>(key);

        byte[] newValue;
        if (existing != null)
        {
            newValue = new byte[existing.Value.Length + appendValue.Length];
            Buffer.BlockCopy(existing.Value, 0, newValue, 0, existing.Value.Length);
            Buffer.BlockCopy(appendValue, 0, newValue, existing.Value.Length, appendValue.Length);
        }
        else
        {
            newValue = appendValue;
        }

        db.Set(key, new RedisString(newValue));
        return context.Client.WriteIntegerAsync(newValue.Length, cancellationToken);
    }
}

/// <summary>
/// STRLEN command.
/// </summary>
public class StrLenCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        
        var obj = context.Database.Get<RedisString>(key);
        var length = obj?.Length ?? 0;
        
        return context.Client.WriteIntegerAsync(length, cancellationToken);
    }
}

/// <summary>
/// GETRANGE command.
/// </summary>
public class GetRangeCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var start = (int)context.GetArgAsInt(1);
        var end = (int)context.GetArgAsInt(2);
        
        var obj = context.Database.Get<RedisString>(key);
        if (obj == null)
            return context.Client.WriteBulkStringAsync("", cancellationToken);

        var value = obj.Value;
        var len = value.Length;

        // Handle negative indices
        if (start < 0) start = Math.Max(0, len + start);
        if (end < 0) end = len + end;
        if (start > len || start > end)
            return context.Client.WriteBulkStringAsync("", cancellationToken);

        end = Math.Min(end, len - 1);
        var resultLen = end - start + 1;
        var result = new byte[resultLen];
        Buffer.BlockCopy(value, start, result, 0, resultLen);
        
        return context.Client.WriteBulkStringAsync(Encoding.UTF8.GetString(result), cancellationToken);
    }
}

/// <summary>
/// SETRANGE command.
/// </summary>
public class SetRangeCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var offset = (int)context.GetArgAsInt(1);
        var replacement = Encoding.UTF8.GetBytes(context.GetArg(2));
        
        if (offset < 0)
            throw new InvalidArgumentException("offset is out of range");

        var db = context.Database;
        var existing = db.Get<RedisString>(key);
        byte[] value = existing?.Value ?? Array.Empty<byte>();

        var newLength = Math.Max(value.Length, offset + replacement.Length);
        var newValue = new byte[newLength];
        
        Buffer.BlockCopy(value, 0, newValue, 0, value.Length);
        // Fill gaps with zeros
        for (int i = value.Length; i < offset; i++)
            newValue[i] = 0;
        Buffer.BlockCopy(replacement, 0, newValue, offset, replacement.Length);

        db.Set(key, new RedisString(newValue));
        return context.Client.WriteIntegerAsync(newValue.Length, cancellationToken);
    }
}
