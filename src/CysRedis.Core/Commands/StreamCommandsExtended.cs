using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// XINFO command - 获取Stream信息
/// </summary>
public class XInfoCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var subcommand = context.GetArg(0).ToUpperInvariant();

        switch (subcommand)
        {
            case "STREAM":
                await HandleStreamInfo(context, cancellationToken);
                break;
            case "GROUPS":
                await HandleGroupsInfo(context, cancellationToken);
                break;
            case "CONSUMERS":
                await HandleConsumersInfo(context, cancellationToken);
                break;
            default:
                await context.Client.WriteErrorAsync($"ERR unknown subcommand '{subcommand}'", cancellationToken);
                break;
        }
    }

    private static async Task HandleStreamInfo(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(1);
        var stream = context.Database.Get<RedisStream>(key);

        if (stream == null)
        {
            await context.Client.WriteErrorAsync("ERR no such key", cancellationToken);
            return;
        }

        var info = new List<RespValue>
        {
            RespValue.BulkString("length"),
            new RespValue(stream.Length),
            RespValue.BulkString("radix-tree-keys"),
            new RespValue(1), // 简化实现
            RespValue.BulkString("radix-tree-nodes"),
            new RespValue(2),
            RespValue.BulkString("groups"),
            new RespValue(stream.GetGroups().Count()),
            RespValue.BulkString("last-generated-id"),
            RespValue.BulkString(stream.LastId?.ToString() ?? "0-0"),
            RespValue.BulkString("first-entry"),
            stream.FirstId.HasValue ? FormatStreamEntry(stream, stream.FirstId.Value) : RespValue.Null,
            RespValue.BulkString("last-entry"),
            stream.LastId.HasValue ? FormatStreamEntry(stream, stream.LastId.Value) : RespValue.Null
        };

        await context.Client.WriteResponseAsync(RespValue.Array(info.ToArray()), cancellationToken);
    }

    private static async Task HandleGroupsInfo(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(1);
        var stream = context.Database.Get<RedisStream>(key);

        if (stream == null)
        {
            await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
            return;
        }

        var groups = stream.GetGroups().Select(g =>
        {
            return RespValue.Array(
                RespValue.BulkString("name"),
                RespValue.BulkString(g.Name),
                RespValue.BulkString("consumers"),
                new RespValue(g.GetConsumers().Count()),
                RespValue.BulkString("pending"),
                new RespValue(g.PendingCount),
                RespValue.BulkString("last-delivered-id"),
                RespValue.BulkString(g.LastDeliveredId.ToString())
            );
        }).ToArray();

        await context.Client.WriteResponseAsync(RespValue.Array(groups), cancellationToken);
    }

    private static async Task HandleConsumersInfo(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(1);
        var groupName = context.GetArg(2);
        
        var stream = context.Database.Get<RedisStream>(key);
        if (stream == null)
        {
            await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
            return;
        }

        var group = stream.GetGroup(groupName);
        if (group == null)
        {
            await context.Client.WriteErrorAsync("NOGROUP No such consumer group", cancellationToken);
            return;
        }

        var consumers = group.GetConsumers().Select(c =>
        {
            return RespValue.Array(
                RespValue.BulkString("name"),
                RespValue.BulkString(c.Name),
                RespValue.BulkString("pending"),
                new RespValue(c.PendingCount),
                RespValue.BulkString("idle"),
                new RespValue((long)(DateTime.UtcNow - c.LastSeenTime).TotalMilliseconds)
            );
        }).ToArray();

        await context.Client.WriteResponseAsync(RespValue.Array(consumers), cancellationToken);
    }

    private static RespValue FormatStreamEntry(RedisStream stream, StreamEntryId id)
    {
        var entries = stream.Range(id, id, 1).ToList();
        if (entries.Count == 0)
            return RespValue.Null;

        var entry = entries[0];
        var fields = new List<RespValue>();
        foreach (var (key, value) in entry.Fields)
        {
            fields.Add(RespValue.BulkString(key));
            fields.Add(RespValue.BulkString(value));
        }

        return RespValue.Array(
            RespValue.BulkString(entry.Id.ToString()),
            RespValue.Array(fields.ToArray())
        );
    }
}

/// <summary>
/// XCLAIM command - 认领待处理消息
/// </summary>
public class XClaimCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(5);
        
        var key = context.GetArg(0);
        var groupName = context.GetArg(1);
        var consumerName = context.GetArg(2);
        var minIdleTime = context.GetArgAsInt(3);
        
        var stream = context.Database.Get<RedisStream>(key);
        if (stream == null)
        {
            await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
            return;
        }

        var group = stream.GetGroup(groupName);
        if (group == null)
        {
            await context.Client.WriteErrorAsync("NOGROUP No such consumer group", cancellationToken);
            return;
        }

        // 收集要认领的消息ID
        var entryIds = new List<string>();
        for (int i = 4; i < context.ArgCount; i++)
        {
            var arg = context.GetArg(i).ToUpperInvariant();
            if (arg == "IDLE" || arg == "TIME" || arg == "RETRYCOUNT" || arg == "FORCE" || arg == "JUSTID")
            {
                i++; // 跳过选项值
                continue;
            }
            entryIds.Add(context.GetArg(i));
        }

        var consumer = group.GetOrCreateConsumer(consumerName);
        var claimedEntries = new List<RespValue>();

        foreach (var entryIdStr in entryIds)
        {
            if (!StreamEntryId.TryParse(entryIdStr, out var entryId))
                continue;

            // 查找并认领消息
            var entries = stream.Range(entryId, entryId, 1).ToList();
            if (entries.Count > 0)
            {
                group.AddPending(entryIdStr, consumerName);
                consumer.PendingCount++;
                consumer.LastSeenTime = DateTime.UtcNow;

                var entry = entries[0];
                var fields = new List<RespValue>();
                foreach (var (k, v) in entry.Fields)
                {
                    fields.Add(RespValue.BulkString(k));
                    fields.Add(RespValue.BulkString(v));
                }

                claimedEntries.Add(RespValue.Array(
                    RespValue.BulkString(entry.Id.ToString()),
                    RespValue.Array(fields.ToArray())
                ));
            }
        }

        await context.Client.WriteResponseAsync(RespValue.Array(claimedEntries.ToArray()), cancellationToken);
    }
}

/// <summary>
/// XAUTOCLAIM command - 自动认领待处理消息
/// </summary>
public class XAutoClaimCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(5);
        
        var key = context.GetArg(0);
        var groupName = context.GetArg(1);
        var consumerName = context.GetArg(2);
        var minIdleTime = context.GetArgAsInt(3);
        var startId = context.GetArg(4);
        
        int count = 100; // 默认
        for (int i = 5; i < context.ArgCount; i++)
        {
            var opt = context.GetArg(i).ToUpperInvariant();
            if (opt == "COUNT" && i + 1 < context.ArgCount)
            {
                count = (int)context.GetArgAsInt(i + 1);
                i++;
            }
        }

        var stream = context.Database.Get<RedisStream>(key);
        if (stream == null)
        {
            await context.Client.WriteResponseAsync(RespValue.Array(
                RespValue.BulkString("0-0"),
                RespValue.EmptyArray
            ), cancellationToken);
            return;
        }

        var group = stream.GetGroup(groupName);
        if (group == null)
        {
            await context.Client.WriteErrorAsync("NOGROUP No such consumer group", cancellationToken);
            return;
        }

        var consumer = group.GetOrCreateConsumer(consumerName);
        var claimedEntries = new List<RespValue>();
        
        // 简化实现：随机选择一些待处理消息进行认领
        var nextId = "0-0";
        var claimed = 0;

        // 实际应该遍历PEL (Pending Entries List)
        foreach (var entry in stream.Range(StreamEntryId.Parse(startId), null, count))
        {
            if (claimed >= count)
                break;

            group.AddPending(entry.Id.ToString(), consumerName);
            consumer.PendingCount++;
            consumer.LastSeenTime = DateTime.UtcNow;

            var fields = new List<RespValue>();
            foreach (var (k, v) in entry.Fields)
            {
                fields.Add(RespValue.BulkString(k));
                fields.Add(RespValue.BulkString(v));
            }

            claimedEntries.Add(RespValue.Array(
                RespValue.BulkString(entry.Id.ToString()),
                RespValue.Array(fields.ToArray())
            ));

            nextId = entry.Id.ToString();
            claimed++;
        }

        await context.Client.WriteResponseAsync(RespValue.Array(
            RespValue.BulkString(nextId),
            RespValue.Array(claimedEntries.ToArray())
        ), cancellationToken);
    }
}

/// <summary>
/// XPENDING command - 查看待处理消息
/// </summary>
public class XPendingCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        
        var key = context.GetArg(0);
        var groupName = context.GetArg(1);

        var stream = context.Database.Get<RedisStream>(key);
        if (stream == null)
        {
            await context.Client.WriteNullAsync(cancellationToken);
            return;
        }

        var group = stream.GetGroup(groupName);
        if (group == null)
        {
            await context.Client.WriteErrorAsync("NOGROUP No such consumer group", cancellationToken);
            return;
        }

        // 简化摘要格式
        if (context.ArgCount == 2)
        {
            var summary = RespValue.Array(
                new RespValue(group.PendingCount),
                RespValue.BulkString("0-0"), // 最小ID (简化)
                RespValue.BulkString("9999999999999-0"), // 最大ID (简化)
                RespValue.EmptyArray // 消费者列表 (简化)
            );
            await context.Client.WriteResponseAsync(summary, cancellationToken);
            return;
        }

        // 详细格式 (简化实现)
        await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
    }
}

/// <summary>
/// XSETID command - 设置Stream ID
/// </summary>
public class XSetIdCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        
        var key = context.GetArg(0);
        var idStr = context.GetArg(1);

        var stream = context.Database.Get<RedisStream>(key);
        if (stream == null)
        {
            await context.Client.WriteErrorAsync("ERR no such key", cancellationToken);
            return;
        }

        // 简化实现：只验证ID格式
        if (!StreamEntryId.TryParse(idStr, out var _))
        {
            await context.Client.WriteErrorAsync("ERR invalid stream ID", cancellationToken);
            return;
        }

        await context.Client.WriteOkAsync(cancellationToken);
    }
}
