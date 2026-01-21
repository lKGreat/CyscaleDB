using System.Text;
using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// INFO command.
/// </summary>
public class InfoCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        var section = context.ArgCount > 0 ? context.GetArg(0).ToLowerInvariant() : "default";
        
        var sb = new StringBuilder();
        
        if (section == "default" || section == "all" || section == "server")
        {
            sb.AppendLine("# Server");
            sb.AppendLine($"redis_version:{Constants.RedisVersion}");
            sb.AppendLine($"cysredis_version:{Constants.ServerVersion}");
            sb.AppendLine($"tcp_port:{context.Server.Port}");
            sb.AppendLine($"uptime_in_seconds:{(long)(DateTime.UtcNow - context.Server.StartTime).TotalSeconds}");
            sb.AppendLine($"uptime_in_days:{(long)(DateTime.UtcNow - context.Server.StartTime).TotalDays}");
            sb.AppendLine($"process_id:{Environment.ProcessId}");
            sb.AppendLine();
        }

        if (section == "default" || section == "all" || section == "clients")
        {
            sb.AppendLine("# Clients");
            sb.AppendLine($"connected_clients:{context.Server.ClientCount}");
            sb.AppendLine();
        }

        if (section == "default" || section == "all" || section == "memory")
        {
            var memory = GC.GetTotalMemory(false);
            sb.AppendLine("# Memory");
            sb.AppendLine($"used_memory:{memory}");
            sb.AppendLine($"used_memory_human:{FormatBytes(memory)}");
            sb.AppendLine();
        }

        if (section == "default" || section == "all" || section == "stats")
        {
            sb.AppendLine("# Stats");
            sb.AppendLine($"total_connections_received:{context.Server.ClientCount}");
            sb.AppendLine($"total_commands_processed:{context.Server.TotalCommandsProcessed}");
            sb.AppendLine();
        }

        if (section == "default" || section == "all" || section == "keyspace")
        {
            sb.AppendLine("# Keyspace");
            foreach (var db in context.Server.Store.GetAllDatabases())
            {
                if (db.KeyCount > 0)
                {
                    sb.AppendLine($"db{db.Index}:keys={db.KeyCount},expires=0,avg_ttl=0");
                }
            }
            sb.AppendLine();
        }

        return context.Client.WriteBulkStringAsync(sb.ToString(), cancellationToken);
    }

    private static string FormatBytes(long bytes)
    {
        string[] sizes = { "B", "KB", "MB", "GB", "TB" };
        double len = bytes;
        int order = 0;
        while (len >= 1024 && order < sizes.Length - 1)
        {
            order++;
            len = len / 1024;
        }
        return $"{len:0.##}{sizes[order]}";
    }
}

/// <summary>
/// COMMAND command.
/// </summary>
public class CommandInfoCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount == 0)
        {
            // Return all commands
            var commands = context.Server.Dispatcher.GetCommandNames()
                .Select(CreateCommandInfo)
                .ToArray();
            await context.Client.WriteResponseAsync(RespValue.Array(commands), cancellationToken);
            return;
        }

        var subCommand = context.GetArg(0).ToUpperInvariant();
        switch (subCommand)
        {
            case "COUNT":
                var count = context.Server.Dispatcher.GetCommandNames().Count();
                await context.Client.WriteIntegerAsync(count, cancellationToken);
                break;
            case "DOCS":
                // Simplified: return empty array
                await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
                break;
            case "INFO":
                var infos = new List<RespValue>();
                for (int i = 1; i < context.ArgCount; i++)
                {
                    infos.Add(CreateCommandInfo(context.GetArg(i).ToUpperInvariant()));
                }
                await context.Client.WriteResponseAsync(RespValue.Array(infos.ToArray()), cancellationToken);
                break;
            default:
                await context.Client.WriteErrorAsync($"ERR Unknown subcommand '{subCommand}'", cancellationToken);
                break;
        }
    }

    private static RespValue CreateCommandInfo(string name)
    {
        // Simplified command info: [name, arity, flags, first_key, last_key, step]
        return RespValue.Array(
            RespValue.BulkString(name.ToLowerInvariant()),
            new RespValue(-1), // arity (variable)
            RespValue.Array(RespValue.SimpleString("fast")), // flags
            new RespValue(1), // first key
            new RespValue(1), // last key
            new RespValue(1)  // step
        );
    }
}

/// <summary>
/// CONFIG command.
/// </summary>
public class ConfigCommand : ICommandHandler
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
            case "SET":
                await HandleSet(context, cancellationToken);
                break;
            case "RESETSTAT":
                await context.Client.WriteOkAsync(cancellationToken);
                break;
            case "REWRITE":
                await context.Client.WriteOkAsync(cancellationToken);
                break;
            default:
                await context.Client.WriteErrorAsync($"ERR Unknown subcommand '{subCommand}'", cancellationToken);
                break;
        }
    }

    private async Task HandleGet(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 2)
            throw new WrongArityException("CONFIG GET");

        var pattern = context.GetArg(1);
        var result = new List<RespValue>();

        // Return some common config values
        var configs = new Dictionary<string, string>
        {
            ["maxclients"] = "10000",
            ["timeout"] = "0",
            ["tcp-keepalive"] = "300",
            ["databases"] = Constants.DefaultDatabaseCount.ToString(),
            ["port"] = context.Server.Port.ToString(),
            ["bind"] = "0.0.0.0",
            ["save"] = "",
            ["appendonly"] = "no",
        };

        foreach (var kvp in configs)
        {
            if (MatchPattern(kvp.Key, pattern))
            {
                result.Add(RespValue.BulkString(kvp.Key));
                result.Add(RespValue.BulkString(kvp.Value));
            }
        }

        await context.Client.WriteResponseAsync(RespValue.Array(result.ToArray()), cancellationToken);
    }

    private Task HandleSet(CommandContext context, CancellationToken cancellationToken)
    {
        // Accept but ignore config set commands
        return context.Client.WriteOkAsync(cancellationToken);
    }

    private static bool MatchPattern(string key, string pattern)
    {
        if (pattern == "*") return true;
        return key.Equals(pattern, StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
/// DEBUG command.
/// </summary>
public class DebugCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount == 0)
        {
            await context.Client.WriteErrorAsync("ERR wrong number of arguments for 'debug' command", cancellationToken);
            return;
        }

        var subCommand = context.GetArg(0).ToUpperInvariant();
        switch (subCommand)
        {
            case "SLEEP":
                if (context.ArgCount > 1)
                {
                    var seconds = context.GetArgAsDouble(1);
                    await Task.Delay(TimeSpan.FromSeconds(seconds), cancellationToken);
                }
                await context.Client.WriteOkAsync(cancellationToken);
                break;
            case "OBJECT":
                await context.Client.WriteBulkStringAsync("Value at:0x0 refcount:1 encoding:raw", cancellationToken);
                break;
            default:
                await context.Client.WriteOkAsync(cancellationToken);
                break;
        }
    }
}

/// <summary>
/// TIME command.
/// </summary>
public class TimeCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var unixSeconds = now.ToUnixTimeSeconds();
        var microseconds = (now.ToUnixTimeMilliseconds() % 1000) * 1000;

        await context.Client.WriteResponseAsync(RespValue.Array(
            RespValue.BulkString(unixSeconds.ToString()),
            RespValue.BulkString(microseconds.ToString())
        ), cancellationToken);
    }
}
