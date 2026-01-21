using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// SLOWLOG command handler.
/// Manages the slow query log.
/// </summary>
public class SlowLogCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var subCommand = context.GetArg(0).ToUpperInvariant();

        switch (subCommand)
        {
            case "GET":
                await HandleGet(context, cancellationToken);
                break;
            case "LEN":
                await HandleLen(context, cancellationToken);
                break;
            case "RESET":
                await HandleReset(context, cancellationToken);
                break;
            case "HELP":
                await HandleHelp(context, cancellationToken);
                break;
            default:
                await context.Client.WriteErrorAsync(
                    $"ERR Unknown subcommand or wrong number of arguments for '{subCommand}'",
                    cancellationToken);
                break;
        }
    }

    private static async Task HandleGet(CommandContext context, CancellationToken cancellationToken)
    {
        // Default count is 10 if not specified
        int count = 10;
        if (context.ArgCount > 1)
        {
            count = (int)context.GetArgAsInt(1);
            if (count < 0)
            {
                // Negative count means get all entries
                count = int.MaxValue;
            }
        }

        var entries = context.Server.SlowLog.Get(count);
        var result = new RespValue[entries.Length];

        for (int i = 0; i < entries.Length; i++)
        {
            var entry = entries[i];
            var unixTimestamp = new DateTimeOffset(entry.Timestamp).ToUnixTimeSeconds();

            // Build arguments array
            var argsArray = new RespValue[entry.Arguments.Length];
            for (int j = 0; j < entry.Arguments.Length; j++)
            {
                argsArray[j] = RespValue.BulkString(entry.Arguments[j]);
            }

            // Entry format: [id, timestamp, duration, [args], client_addr, client_name]
            result[i] = RespValue.Array(
                new RespValue(entry.Id),
                new RespValue(unixTimestamp),
                new RespValue(entry.DurationMicroseconds),
                RespValue.Array(argsArray),
                RespValue.BulkString(entry.ClientAddress),
                entry.ClientName != null
                    ? RespValue.BulkString(entry.ClientName)
                    : RespValue.BulkString("")
            );
        }

        await context.Client.WriteResponseAsync(RespValue.Array(result), cancellationToken);
    }

    private static async Task HandleLen(CommandContext context, CancellationToken cancellationToken)
    {
        var length = context.Server.SlowLog.Length();
        await context.Client.WriteIntegerAsync(length, cancellationToken);
    }

    private static async Task HandleReset(CommandContext context, CancellationToken cancellationToken)
    {
        context.Server.SlowLog.Reset();
        await context.Client.WriteOkAsync(cancellationToken);
    }

    private static async Task HandleHelp(CommandContext context, CancellationToken cancellationToken)
    {
        var help = new[]
        {
            "SLOWLOG GET [<count>]",
            "    Return top <count> entries from the slowlog (default: 10).",
            "    Entries are made of:",
            "    id, timestamp, time in microseconds, arguments array, client IP and port,",
            "    client name.",
            "SLOWLOG LEN",
            "    Return the number of entries in the slowlog.",
            "SLOWLOG RESET",
            "    Reset the slowlog.",
            "SLOWLOG HELP",
            "    Print this help."
        };

        var result = help.Select(line => RespValue.BulkString(line)).ToArray();
        await context.Client.WriteResponseAsync(RespValue.Array(result), cancellationToken);
    }
}
