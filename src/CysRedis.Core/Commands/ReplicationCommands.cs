using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// REPLICAOF / SLAVEOF command - 设置主从复制
/// </summary>
public class ReplicaOfCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var host = context.GetArg(0);
        var portStr = context.GetArg(1);

        if (!int.TryParse(portStr, out var port) && !host.Equals("NO", StringComparison.OrdinalIgnoreCase))
            throw new InvalidArgumentException("ERR invalid port");

        // Handle "REPLICAOF NO ONE"
        if (host.Equals("NO", StringComparison.OrdinalIgnoreCase))
            port = 0;

        await context.Server.Replication.ReplicaOfAsync(host, port, cancellationToken);
        await context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// PSYNC command - 部分/全量同步
/// </summary>
public class PsyncCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        
        var replId = context.GetArg(0);
        var offsetStr = context.GetArg(1);
        
        if (!long.TryParse(offsetStr, out var offset))
            throw new InvalidArgumentException("ERR invalid offset");

        // 处理PSYNC请求
        var result = await context.Server.Replication.HandlePsyncAsync(
            replId, offset, context.Client, cancellationToken);

        // 标记客户端为副本
        context.Client.Flags |= ClientFlags.Slave;
        context.Server.Replication.RegisterReplica(context.Client);

        if (result.Type == Replication.PsyncType.FullResync)
        {
            // 全量同步: +FULLRESYNC <replid> <offset>
            await context.Client.WriteResponseAsync(
                RespValue.SimpleString($"FULLRESYNC {result.ReplId} {result.Offset}"),
                cancellationToken);

            // 更新副本状态为全量同步中
            context.Server.Replication.SetReplicaState(context.Client, Replication.ReplicaState.FullSync);

            // 发送RDB快照
            await context.Server.Replication.SendRdbAsync(context.Client, cancellationToken);
        }
        else
        {
            // 部分同步: +CONTINUE <replid>
            await context.Client.WriteResponseAsync(
                RespValue.SimpleString($"CONTINUE {result.ReplId}"),
                cancellationToken);

            // 更新副本状态为在线
            context.Server.Replication.SetReplicaState(context.Client, Replication.ReplicaState.Online);

            // 发送backlog数据
            await context.Server.Replication.SendBacklogAsync(context.Client, offset, cancellationToken);
        }
    }
}

/// <summary>
/// REPLCONF command - 复制配置
/// </summary>
public class ReplConfCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 2 || context.ArgCount % 2 != 0)
            throw new WrongArityException("REPLCONF");

        // 处理各种REPLCONF选项
        for (int i = 0; i < context.ArgCount; i += 2)
        {
            var option = context.GetArg(i).ToUpperInvariant();
            var value = context.GetArg(i + 1);

            switch (option)
            {
                case "LISTENING-PORT":
                    // 副本监听端口
                    Logger.Debug("Replica listening port: {0}", value);
                    break;

                case "IP-ADDRESS":
                    // 副本IP地址
                    Logger.Debug("Replica IP address: {0}", value);
                    break;

                case "CAPA":
                    // 副本能力 (eof, psync2等)
                    Logger.Debug("Replica capability: {0}", value);
                    break;

                case "ACK":
                    // 副本确认收到的偏移量
                    if (long.TryParse(value, out var ack))
                    {
                        // 更新副本的偏移量和同步状态
                        context.Server.Replication.UpdateReplicaAck(context.Client, ack);
                        Logger.Debug("Replica ACK offset: {0}", ack);
                    }
                    break;

                case "GETACK":
                    // 主服务器请求副本的偏移量
                    if (value == "*")
                    {
                        var offset = context.Server.Replication.MasterReplOffset;
                        await context.Client.WriteResponseAsync(
                            RespValue.Array(
                                RespValue.BulkString("REPLCONF"),
                                RespValue.BulkString("ACK"),
                                RespValue.BulkString(offset.ToString())
                            ),
                            cancellationToken);
                        return;
                    }
                    break;

                default:
                    Logger.Warning("Unknown REPLCONF option: {0}", option);
                    break;
            }
        }

        await context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// WAIT command - 等待复制同步
/// </summary>
public class WaitCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        
        var numReplicas = (int)context.GetArgAsInt(0);
        var timeoutMs = (int)context.GetArgAsInt(1);

        if (numReplicas < 0)
            throw new InvalidArgumentException("ERR numreplicas must be positive");
        if (timeoutMs < 0)
            throw new InvalidArgumentException("ERR timeout must be positive");

        // 获取当前同步的副本数量
        var syncedReplicas = context.Server.Replication.GetSyncedReplicaCount();

        // 如果已经有足够的副本同步，直接返回
        if (syncedReplicas >= numReplicas)
        {
            await context.Client.WriteIntegerAsync(syncedReplicas, cancellationToken);
            return;
        }

        // 等待更多副本同步
        if (timeoutMs > 0)
        {
            try
            {
                using var cts = new CancellationTokenSource(timeoutMs);
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, cts.Token);
                
                // 定期检查同步状态
                while (!linked.Token.IsCancellationRequested)
                {
                    await Task.Delay(100, linked.Token);
                    syncedReplicas = context.Server.Replication.GetSyncedReplicaCount();
                    if (syncedReplicas >= numReplicas)
                        break;
                }
            }
            catch (OperationCanceledException)
            {
                // 超时或取消
            }
        }

        // 返回实际同步的副本数
        syncedReplicas = context.Server.Replication.GetSyncedReplicaCount();
        await context.Client.WriteIntegerAsync(syncedReplicas, cancellationToken);
    }
}
