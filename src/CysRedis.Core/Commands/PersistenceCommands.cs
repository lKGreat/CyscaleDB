using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// SAVE command - blocking save.
/// </summary>
public class SaveCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.Server.Persistence?.Save();
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// BGSAVE command - background save.
/// </summary>
public class BgSaveCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.Server.Persistence == null)
        {
            await context.Client.WriteErrorAsync("ERR persistence not configured", cancellationToken);
            return;
        }

        if (context.Server.Persistence.IsSaving)
        {
            await context.Client.WriteErrorAsync("ERR Background save already in progress", cancellationToken);
            return;
        }

        _ = context.Server.Persistence.SaveBackgroundAsync(cancellationToken);
        await context.Client.WriteResponseAsync(
            RespValue.SimpleString("Background saving started"), cancellationToken);
    }
}

/// <summary>
/// BGREWRITEAOF command - background AOF rewrite.
/// </summary>
public class BgRewriteAofCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.Server.Aof == null)
        {
            await context.Client.WriteErrorAsync("ERR AOF not configured", cancellationToken);
            return;
        }

        _ = context.Server.Aof.RewriteAsync(context.Server.Store, cancellationToken);
        await context.Client.WriteResponseAsync(
            RespValue.SimpleString("Background append only file rewriting started"), cancellationToken);
    }
}

/// <summary>
/// LASTSAVE command - get last save timestamp.
/// </summary>
public class LastSaveCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        var lastSave = context.Server.LastSaveTime;
        var timestamp = new DateTimeOffset(lastSave).ToUnixTimeSeconds();
        return context.Client.WriteIntegerAsync(timestamp, cancellationToken);
    }
}
