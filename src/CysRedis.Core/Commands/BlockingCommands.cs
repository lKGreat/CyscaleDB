using System.Text;
using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// BLPOP command - 阻塞式左侧弹出
/// </summary>
public class BLPopCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        
        // 最后一个参数是超时时间(秒)
        var timeoutArg = context.GetArg(context.ArgCount - 1);
        if (!double.TryParse(timeoutArg, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var timeoutSeconds))
        {
            throw new InvalidArgumentException("ERR timeout is not a float or out of range");
        }

        if (timeoutSeconds < 0)
            throw new InvalidArgumentException("ERR timeout is negative");

        // 获取所有键（最后一个是超时时间，不包括）
        var keys = new string[context.ArgCount - 1];
        for (int i = 0; i < keys.Length; i++)
        {
            keys[i] = context.GetArg(i);
        }

        // 首先尝试非阻塞式获取
        foreach (var key in keys)
        {
            var list = context.Database.Get<RedisList>(key);
            if (list != null && list.Count > 0)
            {
                var value = list.PopLeft();
                if (value != null)
                {
                    if (list.Count == 0)
                        context.Database.Delete(key);

                    // 返回 [key, value] 数组
                    await context.Client.WriteResponseAsync(RespValue.Array(
                        RespValue.BulkString(key),
                        RespValue.BulkString(Encoding.UTF8.GetString(value))
                    ), cancellationToken);
                    
                    return;
                }
            }
        }

        // 没有数据，需要阻塞
        if (timeoutSeconds == 0)
        {
            // 0 表示无限等待
            timeoutSeconds = int.MaxValue;
        }

        var timeout = TimeSpan.FromSeconds(timeoutSeconds);
        var result = await context.Server.Blocking.BlockOnListKeysAsync(
            context.Client, keys, timeout, popLeft: true, cancellationToken);

        if (result != null)
        {
            // 实际弹出数据
            var list = context.Database.Get<RedisList>(result.Key);
            if (list != null && list.Count > 0)
            {
                var value = list.PopLeft();
                if (value != null)
                {
                    if (list.Count == 0)
                        context.Database.Delete(result.Key);

                    await context.Client.WriteResponseAsync(RespValue.Array(
                        RespValue.BulkString(result.Key),
                        RespValue.BulkString(Encoding.UTF8.GetString(value))
                    ), cancellationToken);
                    return;
                }
            }
        }

        // 超时，返回 null
        await context.Client.WriteNullAsync(cancellationToken);
    }
}

/// <summary>
/// BRPOP command - 阻塞式右侧弹出
/// </summary>
public class BRPopCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        
        var timeoutArg = context.GetArg(context.ArgCount - 1);
        if (!double.TryParse(timeoutArg, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var timeoutSeconds))
        {
            throw new InvalidArgumentException("ERR timeout is not a float or out of range");
        }

        if (timeoutSeconds < 0)
            throw new InvalidArgumentException("ERR timeout is negative");

        var keys = new string[context.ArgCount - 1];
        for (int i = 0; i < keys.Length; i++)
        {
            keys[i] = context.GetArg(i);
        }

        // 首先尝试非阻塞式获取
        foreach (var key in keys)
        {
            var list = context.Database.Get<RedisList>(key);
            if (list != null && list.Count > 0)
            {
                var value = list.PopRight();
                if (value != null)
                {
                    if (list.Count == 0)
                        context.Database.Delete(key);

                    await context.Client.WriteResponseAsync(RespValue.Array(
                        RespValue.BulkString(key),
                        RespValue.BulkString(Encoding.UTF8.GetString(value))
                    ), cancellationToken);
                    return;
                }
            }
        }

        // 没有数据，需要阻塞
        if (timeoutSeconds == 0)
            timeoutSeconds = int.MaxValue;

        var timeout = TimeSpan.FromSeconds(timeoutSeconds);
        var result = await context.Server.Blocking.BlockOnListKeysAsync(
            context.Client, keys, timeout, popLeft: false, cancellationToken);

        if (result != null)
        {
            var list = context.Database.Get<RedisList>(result.Key);
            if (list != null && list.Count > 0)
            {
                var value = list.PopRight();
                if (value != null)
                {
                    if (list.Count == 0)
                        context.Database.Delete(result.Key);

                    await context.Client.WriteResponseAsync(RespValue.Array(
                        RespValue.BulkString(result.Key),
                        RespValue.BulkString(Encoding.UTF8.GetString(value))
                    ), cancellationToken);
                    return;
                }
            }
        }

        await context.Client.WriteNullAsync(cancellationToken);
    }
}

/// <summary>
/// BLMOVE command - 阻塞式列表移动
/// </summary>
public class BLMoveCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(5);
        
        var source = context.GetArg(0);
        var destination = context.GetArg(1);
        var whereFrom = context.GetArg(2).ToUpperInvariant();
        var whereTo = context.GetArg(3).ToUpperInvariant();
        
        if (whereFrom != "LEFT" && whereFrom != "RIGHT")
            throw new InvalidArgumentException("ERR syntax error");
        if (whereTo != "LEFT" && whereTo != "RIGHT")
            throw new InvalidArgumentException("ERR syntax error");
        
        var timeoutArg = context.GetArg(4);
        if (!double.TryParse(timeoutArg, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var timeoutSeconds))
        {
            throw new InvalidArgumentException("ERR timeout is not a float or out of range");
        }

        if (timeoutSeconds < 0)
            throw new InvalidArgumentException("ERR timeout is negative");

        // 首先尝试非阻塞式移动
        var sourceList = context.Database.Get<RedisList>(source);
        if (sourceList != null && sourceList.Count > 0)
        {
            var value = whereFrom == "LEFT" ? sourceList.PopLeft() : sourceList.PopRight();
            if (value != null)
            {
                if (sourceList.Count == 0 && source != destination)
                    context.Database.Delete(source);

                var destList = context.Database.GetOrCreateList(destination);
                if (whereTo == "LEFT")
                    destList.PushLeft(value);
                else
                    destList.PushRight(value);

                await context.Client.WriteBulkStringAsync(Encoding.UTF8.GetString(value), cancellationToken);
                return;
            }
        }

        // 没有数据，需要阻塞
        if (timeoutSeconds == 0)
            timeoutSeconds = int.MaxValue;

        var timeout = TimeSpan.FromSeconds(timeoutSeconds);
        var result = await context.Server.Blocking.BlockOnListKeysAsync(
            context.Client, new[] { source }, timeout, popLeft: whereFrom == "LEFT", cancellationToken);

        if (result != null)
        {
            sourceList = context.Database.Get<RedisList>(source);
            if (sourceList != null && sourceList.Count > 0)
            {
                var value = whereFrom == "LEFT" ? sourceList.PopLeft() : sourceList.PopRight();
                if (value != null)
                {
                    if (sourceList.Count == 0 && source != destination)
                        context.Database.Delete(source);

                    var destList = context.Database.GetOrCreateList(destination);
                    if (whereTo == "LEFT")
                        destList.PushLeft(value);
                    else
                        destList.PushRight(value);

                    await context.Client.WriteBulkStringAsync(Encoding.UTF8.GetString(value), cancellationToken);
                    return;
                }
            }
        }

        await context.Client.WriteNullAsync(cancellationToken);
    }
}

/// <summary>
/// BZPOPMIN command - 阻塞式弹出最小元素
/// </summary>
public class BZPopMinCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        
        var timeoutArg = context.GetArg(context.ArgCount - 1);
        if (!double.TryParse(timeoutArg, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var timeoutSeconds))
        {
            throw new InvalidArgumentException("ERR timeout is not a float or out of range");
        }

        if (timeoutSeconds < 0)
            throw new InvalidArgumentException("ERR timeout is negative");

        var keys = new string[context.ArgCount - 1];
        for (int i = 0; i < keys.Length; i++)
        {
            keys[i] = context.GetArg(i);
        }

        // 首先尝试非阻塞式获取
        foreach (var key in keys)
        {
            var zset = context.Database.Get<RedisSortedSet>(key);
            if (zset != null && zset.Count > 0)
            {
                var (member, score) = zset.GetRange(0, 0).First();
                zset.Remove(member);
                
                if (zset.Count == 0)
                    context.Database.Delete(key);

                await context.Client.WriteResponseAsync(RespValue.Array(
                    RespValue.BulkString(key),
                    RespValue.BulkString(member),
                    RespValue.BulkString(score.ToString("G17", System.Globalization.CultureInfo.InvariantCulture))
                ), cancellationToken);
                return;
            }
        }

        // 没有数据，需要阻塞
        if (timeoutSeconds == 0)
            timeoutSeconds = int.MaxValue;

        var timeout = TimeSpan.FromSeconds(timeoutSeconds);
        var result = await context.Server.Blocking.BlockOnSortedSetKeysAsync(
            context.Client, keys, timeout, min: true, cancellationToken);

        if (result != null)
        {
            var zset = context.Database.Get<RedisSortedSet>(result.Key);
            if (zset != null && zset.Count > 0)
            {
                var (member, score) = zset.GetRange(0, 0).First();
                zset.Remove(member);
                
                if (zset.Count == 0)
                    context.Database.Delete(result.Key);

                await context.Client.WriteResponseAsync(RespValue.Array(
                    RespValue.BulkString(result.Key),
                    RespValue.BulkString(member),
                    RespValue.BulkString(score.ToString("G17", System.Globalization.CultureInfo.InvariantCulture))
                ), cancellationToken);
                return;
            }
        }

        await context.Client.WriteNullAsync(cancellationToken);
    }
}

/// <summary>
/// BZPOPMAX command - 阻塞式弹出最大元素
/// </summary>
public class BZPopMaxCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        
        var timeoutArg = context.GetArg(context.ArgCount - 1);
        if (!double.TryParse(timeoutArg, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var timeoutSeconds))
        {
            throw new InvalidArgumentException("ERR timeout is not a float or out of range");
        }

        if (timeoutSeconds < 0)
            throw new InvalidArgumentException("ERR timeout is negative");

        var keys = new string[context.ArgCount - 1];
        for (int i = 0; i < keys.Length; i++)
        {
            keys[i] = context.GetArg(i);
        }

        // 首先尝试非阻塞式获取
        foreach (var key in keys)
        {
            var zset = context.Database.Get<RedisSortedSet>(key);
            if (zset != null && zset.Count > 0)
            {
                var (member, score) = zset.GetRange(zset.Count - 1, zset.Count - 1).First();
                zset.Remove(member);
                
                if (zset.Count == 0)
                    context.Database.Delete(key);

                await context.Client.WriteResponseAsync(RespValue.Array(
                    RespValue.BulkString(key),
                    RespValue.BulkString(member),
                    RespValue.BulkString(score.ToString("G17", System.Globalization.CultureInfo.InvariantCulture))
                ), cancellationToken);
                return;
            }
        }

        // 没有数据，需要阻塞
        if (timeoutSeconds == 0)
            timeoutSeconds = int.MaxValue;

        var timeout = TimeSpan.FromSeconds(timeoutSeconds);
        var result = await context.Server.Blocking.BlockOnSortedSetKeysAsync(
            context.Client, keys, timeout, min: false, cancellationToken);

        if (result != null)
        {
            var zset = context.Database.Get<RedisSortedSet>(result.Key);
            if (zset != null && zset.Count > 0)
            {
                var (member, score) = zset.GetRange(zset.Count - 1, zset.Count - 1).First();
                zset.Remove(member);
                
                if (zset.Count == 0)
                    context.Database.Delete(result.Key);

                await context.Client.WriteResponseAsync(RespValue.Array(
                    RespValue.BulkString(result.Key),
                    RespValue.BulkString(member),
                    RespValue.BulkString(score.ToString("G17", System.Globalization.CultureInfo.InvariantCulture))
                ), cancellationToken);
                return;
            }
        }

        await context.Client.WriteNullAsync(cancellationToken);
    }
}

