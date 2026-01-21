using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// REPLICAOF command (also SLAVEOF).
/// </summary>
public class ReplicaOfCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var host = context.GetArg(0);
        var portStr = context.GetArg(1);

        if (host.Equals("no", StringComparison.OrdinalIgnoreCase) && 
            portStr.Equals("one", StringComparison.OrdinalIgnoreCase))
        {
            await context.Server.Replication.ReplicaOfAsync("no", 0, cancellationToken);
            await context.Client.WriteOkAsync(cancellationToken);
            return;
        }

        var port = (int)context.GetArgAsInt(1);
        await context.Server.Replication.ReplicaOfAsync(host, port, cancellationToken);
        await context.Client.WriteOkAsync(cancellationToken);
    }
}
