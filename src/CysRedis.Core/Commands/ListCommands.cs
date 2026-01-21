using System.Text;
using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// LPUSH command.
/// </summary>
public class LPushCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var list = context.Database.GetOrCreateList(key);
        
        for (int i = 1; i < context.ArgCount; i++)
        {
            list.PushLeft(Encoding.UTF8.GetBytes(context.GetArg(i)));
        }
        
        // 唤醒阻塞在此键上的客户端
        context.Server.Blocking.SignalKeyReady(key);
        
        return context.Client.WriteIntegerAsync(list.Count, cancellationToken);
    }
}

/// <summary>
/// RPUSH command.
/// </summary>
public class RPushCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var list = context.Database.GetOrCreateList(key);
        
        for (int i = 1; i < context.ArgCount; i++)
        {
            list.PushRight(Encoding.UTF8.GetBytes(context.GetArg(i)));
        }
        
        // 唤醒阻塞在此键上的客户端
        context.Server.Blocking.SignalKeyReady(key);
        
        return context.Client.WriteIntegerAsync(list.Count, cancellationToken);
    }
}

/// <summary>
/// LPOP command.
/// </summary>
public class LPopCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var key = context.GetArg(0);
        var list = context.Database.Get<RedisList>(key);
        
        if (list == null || list.Count == 0)
        {
            await context.Client.WriteNullAsync(cancellationToken);
            return;
        }

        // Check for count argument
        if (context.ArgCount > 1)
        {
            var count = (int)context.GetArgAsInt(1);
            var values = new List<RespValue>();
            for (int i = 0; i < count && list.Count > 0; i++)
            {
                var value = list.PopLeft();
                if (value != null)
                    values.Add(RespValue.BulkString(Encoding.UTF8.GetString(value)));
            }
            await context.Client.WriteResponseAsync(RespValue.Array(values.ToArray()), cancellationToken);
        }
        else
        {
            var value = list.PopLeft();
            if (value != null)
                await context.Client.WriteBulkStringAsync(Encoding.UTF8.GetString(value), cancellationToken);
            else
                await context.Client.WriteNullAsync(cancellationToken);
        }

        // Remove list if empty
        if (list.Count == 0)
            context.Database.Delete(key);
    }
}

/// <summary>
/// RPOP command.
/// </summary>
public class RPopCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var key = context.GetArg(0);
        var list = context.Database.Get<RedisList>(key);
        
        if (list == null || list.Count == 0)
        {
            await context.Client.WriteNullAsync(cancellationToken);
            return;
        }

        if (context.ArgCount > 1)
        {
            var count = (int)context.GetArgAsInt(1);
            var values = new List<RespValue>();
            for (int i = 0; i < count && list.Count > 0; i++)
            {
                var value = list.PopRight();
                if (value != null)
                    values.Add(RespValue.BulkString(Encoding.UTF8.GetString(value)));
            }
            await context.Client.WriteResponseAsync(RespValue.Array(values.ToArray()), cancellationToken);
        }
        else
        {
            var value = list.PopRight();
            if (value != null)
                await context.Client.WriteBulkStringAsync(Encoding.UTF8.GetString(value), cancellationToken);
            else
                await context.Client.WriteNullAsync(cancellationToken);
        }

        if (list.Count == 0)
            context.Database.Delete(key);
    }
}

/// <summary>
/// LRANGE command.
/// </summary>
public class LRangeCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var start = (int)context.GetArgAsInt(1);
        var stop = (int)context.GetArgAsInt(2);
        
        var list = context.Database.Get<RedisList>(key);
        if (list == null)
        {
            await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
            return;
        }

        var range = list.GetRange(start, stop);
        var values = range.Select(v => RespValue.BulkString(Encoding.UTF8.GetString(v))).ToArray();
        await context.Client.WriteResponseAsync(RespValue.Array(values), cancellationToken);
    }
}

/// <summary>
/// LINDEX command.
/// </summary>
public class LIndexCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var index = (int)context.GetArgAsInt(1);
        
        var list = context.Database.Get<RedisList>(key);
        var value = list?.GetByIndex(index);
        
        if (value == null)
            return context.Client.WriteNullAsync(cancellationToken);
            
        return context.Client.WriteBulkStringAsync(Encoding.UTF8.GetString(value), cancellationToken);
    }
}

/// <summary>
/// LSET command.
/// </summary>
public class LSetCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var index = (int)context.GetArgAsInt(1);
        var value = Encoding.UTF8.GetBytes(context.GetArg(2));
        
        var list = context.Database.Get<RedisList>(key);
        if (list == null)
            throw new RedisException("no such key");
            
        if (!list.SetByIndex(index, value))
            throw new RedisException("index out of range");
            
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// LLEN command.
/// </summary>
public class LLenCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var list = context.Database.Get<RedisList>(key);
        return context.Client.WriteIntegerAsync(list?.Count ?? 0, cancellationToken);
    }
}

/// <summary>
/// LTRIM command.
/// </summary>
public class LTrimCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var start = (int)context.GetArgAsInt(1);
        var stop = (int)context.GetArgAsInt(2);
        
        var list = context.Database.Get<RedisList>(key);
        if (list != null)
        {
            list.Trim(start, stop);
            if (list.Count == 0)
                context.Database.Delete(key);
        }
        
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// LPOS command - finds the position(s) of an element in a list.
/// </summary>
public class LPosCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var element = Encoding.UTF8.GetBytes(context.GetArg(1));
        var list = context.Database.Get<RedisList>(key);
        
        if (list == null)
        {
            await context.Client.WriteNullAsync(cancellationToken);
            return;
        }
        
        int rank = 1, count = 1, maxlen = 0;
        
        // Parse optional parameters
        for (int i = 2; i < context.ArgCount; i++)
        {
            var opt = context.GetArg(i).ToUpperInvariant();
            switch (opt)
            {
                case "RANK":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    rank = (int)context.GetArgAsInt(i);
                    if (rank == 0) throw new Common.InvalidArgumentException("RANK can't be zero");
                    break;
                case "COUNT":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    count = (int)context.GetArgAsInt(i);
                    if (count < 0) throw new Common.InvalidArgumentException("COUNT can't be negative");
                    break;
                case "MAXLEN":
                    i++;
                    if (i >= context.ArgCount) throw new SyntaxErrorException();
                    maxlen = (int)context.GetArgAsInt(i);
                    if (maxlen < 0) throw new Common.InvalidArgumentException("MAXLEN can't be negative");
                    break;
            }
        }
        
        var positions = new List<long>();
        int matchCount = 0;
        int direction = rank > 0 ? 1 : -1;
        int absRank = Math.Abs(rank);
        int searchLen = maxlen > 0 ? Math.Min(maxlen, list.Count) : list.Count;
        
        if (direction > 0)
        {
            // Forward search
            for (int i = 0; i < searchLen; i++)
            {
                var item = list.GetByIndex(i);
                if (item != null && item.SequenceEqual(element))
                {
                    matchCount++;
                    if (matchCount >= absRank)
                    {
                        positions.Add(i);
                        if (count > 0 && positions.Count >= count) break;
                    }
                }
            }
        }
        else
        {
            // Backward search
            for (int i = list.Count - 1; i >= list.Count - searchLen; i--)
            {
                var item = list.GetByIndex(i);
                if (item != null && item.SequenceEqual(element))
                {
                    matchCount++;
                    if (matchCount >= absRank)
                    {
                        positions.Add(i);
                        if (count > 0 && positions.Count >= count) break;
                    }
                }
            }
        }
        
        // Return results
        if (count == 1)
        {
            if (positions.Count > 0)
                await context.Client.WriteIntegerAsync(positions[0], cancellationToken);
            else
                await context.Client.WriteNullAsync(cancellationToken);
        }
        else if (count == 0)
        {
            // COUNT 0 means return all matches
            var results = positions.Select(p => new RespValue(p)).ToArray();
            await context.Client.WriteResponseAsync(RespValue.Array(results), cancellationToken);
        }
        else
        {
            var results = positions.Select(p => new RespValue(p)).ToArray();
            await context.Client.WriteResponseAsync(RespValue.Array(results), cancellationToken);
        }
    }
}
