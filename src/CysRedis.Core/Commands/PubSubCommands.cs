using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// SUBSCRIBE command.
/// </summary>
public class SubscribeCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);

        var channels = new string[context.ArgCount];
        for (int i = 0; i < context.ArgCount; i++)
            channels[i] = context.GetArg(i);

        return context.Server.PubSub.SubscribeAsync(context.Client, channels, cancellationToken);
    }
}

/// <summary>
/// UNSUBSCRIBE command.
/// </summary>
public class UnsubscribeCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        string[]? channels = null;
        if (context.ArgCount > 0)
        {
            channels = new string[context.ArgCount];
            for (int i = 0; i < context.ArgCount; i++)
                channels[i] = context.GetArg(i);
        }

        return context.Server.PubSub.UnsubscribeAsync(context.Client, channels, cancellationToken);
    }
}

/// <summary>
/// PSUBSCRIBE command.
/// </summary>
public class PSubscribeCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);

        var patterns = new string[context.ArgCount];
        for (int i = 0; i < context.ArgCount; i++)
            patterns[i] = context.GetArg(i);

        return context.Server.PubSub.PSubscribeAsync(context.Client, patterns, cancellationToken);
    }
}

/// <summary>
/// PUNSUBSCRIBE command.
/// </summary>
public class PUnsubscribeCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        string[]? patterns = null;
        if (context.ArgCount > 0)
        {
            patterns = new string[context.ArgCount];
            for (int i = 0; i < context.ArgCount; i++)
                patterns[i] = context.GetArg(i);
        }

        return context.Server.PubSub.PUnsubscribeAsync(context.Client, patterns, cancellationToken);
    }
}

/// <summary>
/// PUBLISH command.
/// </summary>
public class PublishCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var channel = context.GetArg(0);
        var message = context.GetArg(1);

        var receivers = await context.Server.PubSub.PublishAsync(channel, message, cancellationToken);
        await context.Client.WriteIntegerAsync(receivers, cancellationToken);
    }
}

/// <summary>
/// PUBSUB command.
/// </summary>
public class PubSubInfoCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var subCommand = context.GetArg(0).ToUpperInvariant();

        switch (subCommand)
        {
            case "CHANNELS":
                await HandleChannels(context, cancellationToken);
                break;
            case "NUMSUB":
                await HandleNumSub(context, cancellationToken);
                break;
            case "NUMPAT":
                await HandleNumPat(context, cancellationToken);
                break;
            default:
                await context.Client.WriteErrorAsync($"ERR Unknown PUBSUB subcommand '{subCommand}'", cancellationToken);
                break;
        }
    }

    private async Task HandleChannels(CommandContext context, CancellationToken cancellationToken)
    {
        string? pattern = context.ArgCount > 1 ? context.GetArg(1) : null;
        var channels = context.Server.PubSub.GetChannels(pattern)
            .Select(c => RespValue.BulkString(c))
            .ToArray();
        await context.Client.WriteResponseAsync(RespValue.Array(channels), cancellationToken);
    }

    private async Task HandleNumSub(CommandContext context, CancellationToken cancellationToken)
    {
        var result = new List<RespValue>();
        for (int i = 1; i < context.ArgCount; i++)
        {
            var channel = context.GetArg(i);
            var count = context.Server.PubSub.GetChannelSubscriberCount(channel);
            result.Add(RespValue.BulkString(channel));
            result.Add(new RespValue(count));
        }
        await context.Client.WriteResponseAsync(RespValue.Array(result.ToArray()), cancellationToken);
    }

    private Task HandleNumPat(CommandContext context, CancellationToken cancellationToken)
    {
        var count = context.Server.PubSub.GetPatternCount();
        return context.Client.WriteIntegerAsync(count, cancellationToken);
    }
}
