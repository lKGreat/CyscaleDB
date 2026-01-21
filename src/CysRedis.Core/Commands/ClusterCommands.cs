using CysRedis.Core.Cluster;
using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// CLUSTER command - 集群管理命令
/// </summary>
public class ClusterCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var subCommand = context.GetArg(0).ToUpperInvariant();

        if (!context.Server.Cluster.IsEnabled)
        {
            await context.Client.WriteErrorAsync("ERR This instance has cluster support disabled", cancellationToken);
            return;
        }

        switch (subCommand)
        {
            case "INFO":
                await HandleInfo(context, cancellationToken);
                break;
            case "NODES":
                await HandleNodes(context, cancellationToken);
                break;
            case "SLOTS":
                await HandleSlots(context, cancellationToken);
                break;
            case "MEET":
                await HandleMeet(context, cancellationToken);
                break;
            case "ADDSLOTS":
                await HandleAddSlots(context, cancellationToken);
                break;
            case "DELSLOTS":
                await HandleDelSlots(context, cancellationToken);
                break;
            case "ADDSLOTSRANGE":
                await HandleAddSlotsRange(context, cancellationToken);
                break;
            case "DELSLOTSRANGE":
                await HandleDelSlotsRange(context, cancellationToken);
                break;
            case "KEYSLOT":
                await HandleKeySlot(context, cancellationToken);
                break;
            case "COUNTKEYSINSLOT":
                await HandleCountKeysInSlot(context, cancellationToken);
                break;
            case "GETKEYSINSLOT":
                await HandleGetKeysInSlot(context, cancellationToken);
                break;
            case "MYID":
                await HandleMyId(context, cancellationToken);
                break;
            case "RESET":
                await HandleReset(context, cancellationToken);
                break;
            default:
                await context.Client.WriteErrorAsync($"ERR Unknown CLUSTER subcommand '{subCommand}'", cancellationToken);
                break;
        }
    }

    private static async Task HandleInfo(CommandContext context, CancellationToken cancellationToken)
    {
        var info = context.Server.Cluster.GetInfo();
        var sb = new System.Text.StringBuilder();
        
        foreach (var (key, value) in info)
        {
            sb.Append(key).Append(':').Append(value).Append("\r\n");
        }
        
        await context.Client.WriteBulkStringAsync(sb.ToString(), cancellationToken);
    }

    private static async Task HandleNodes(CommandContext context, CancellationToken cancellationToken)
    {
        var nodes = context.Server.Cluster.GetNodes();
        var sb = new System.Text.StringBuilder();
        
        foreach (var node in nodes)
        {
            sb.AppendLine(node.ToNodesString());
        }
        
        await context.Client.WriteBulkStringAsync(sb.ToString(), cancellationToken);
    }

    private static async Task HandleSlots(CommandContext context, CancellationToken cancellationToken)
    {
        var slotsInfo = context.Server.Cluster.GetSlotsInfo();
        var result = new List<RespValue>();

        foreach (var range in slotsInfo)
        {
            var nodeInfo = RespValue.Array(
                RespValue.BulkString(range.Node.IpAddress),
                new RespValue(range.Node.Port),
                RespValue.BulkString(range.Node.NodeId)
            );

            result.Add(RespValue.Array(
                new RespValue(range.Start),
                new RespValue(range.End),
                nodeInfo
            ));
        }

        await context.Client.WriteResponseAsync(RespValue.Array(result.ToArray()), cancellationToken);
    }

    private static async Task HandleMeet(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var ipAddress = context.GetArg(1);
        var port = (int)context.GetArgAsInt(2);

        context.Server.Cluster.MeetNode(ipAddress, port);
        await context.Client.WriteOkAsync(cancellationToken);
    }

    private static async Task HandleAddSlots(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        
        var slots = new List<int>();
        for (int i = 1; i < context.ArgCount; i++)
        {
            var slot = (int)context.GetArgAsInt(i);
            slots.Add(slot);
        }

        context.Server.Cluster.AddSlots(slots.ToArray());
        await context.Client.WriteOkAsync(cancellationToken);
    }

    private static async Task HandleDelSlots(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        
        var slots = new List<int>();
        for (int i = 1; i < context.ArgCount; i++)
        {
            var slot = (int)context.GetArgAsInt(i);
            slots.Add(slot);
        }

        context.Server.Cluster.DelSlots(slots.ToArray());
        await context.Client.WriteOkAsync(cancellationToken);
    }

    private static async Task HandleAddSlotsRange(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        
        // CLUSTER ADDSLOTSRANGE start1 end1 [start2 end2 ...]
        if ((context.ArgCount - 1) % 2 != 0)
            throw new WrongArityException("CLUSTER ADDSLOTSRANGE");

        for (int i = 1; i < context.ArgCount; i += 2)
        {
            var start = (int)context.GetArgAsInt(i);
            var end = (int)context.GetArgAsInt(i + 1);
            context.Server.Cluster.AddSlotsRange(start, end);
        }

        await context.Client.WriteOkAsync(cancellationToken);
    }

    private static async Task HandleDelSlotsRange(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        
        if ((context.ArgCount - 1) % 2 != 0)
            throw new WrongArityException("CLUSTER DELSLOTSRANGE");

        var slots = new List<int>();
        for (int i = 1; i < context.ArgCount; i += 2)
        {
            var start = (int)context.GetArgAsInt(i);
            var end = (int)context.GetArgAsInt(i + 1);
            
            for (int slot = start; slot <= end; slot++)
            {
                slots.Add(slot);
            }
        }

        context.Server.Cluster.DelSlots(slots.ToArray());
        await context.Client.WriteOkAsync(cancellationToken);
    }

    private static async Task HandleKeySlot(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(1);
        var slot = ClusterManager.GetSlot(key);
        await context.Client.WriteIntegerAsync(slot, cancellationToken);
    }

    private static async Task HandleCountKeysInSlot(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var slot = (int)context.GetArgAsInt(1);

        // 统计该槽位的键数量
        var count = context.Database.Keys()
            .Count(key => ClusterManager.GetSlot(key) == slot);

        await context.Client.WriteIntegerAsync(count, cancellationToken);
    }

    private static async Task HandleGetKeysInSlot(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var slot = (int)context.GetArgAsInt(1);
        var count = (int)context.GetArgAsInt(2);

        var keys = context.Database.Keys()
            .Where(key => ClusterManager.GetSlot(key) == slot)
            .Take(count)
            .Select(k => RespValue.BulkString(k))
            .ToArray();

        await context.Client.WriteResponseAsync(RespValue.Array(keys), cancellationToken);
    }

    private static async Task HandleMyId(CommandContext context, CancellationToken cancellationToken)
    {
        await context.Client.WriteBulkStringAsync(context.Server.Cluster.MyNodeId, cancellationToken);
    }

    private static async Task HandleReset(CommandContext context, CancellationToken cancellationToken)
    {
        // 简化实现：只清空节点列表
        await context.Client.WriteOkAsync(cancellationToken);
    }
}
