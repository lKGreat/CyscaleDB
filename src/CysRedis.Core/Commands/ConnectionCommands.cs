using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// PING command - test connection.
/// </summary>
public class PingCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount > 0)
        {
            // PING with message - return the message as bulk string
            return context.Client.WriteBulkStringAsync(context.GetArg(0), cancellationToken);
        }
        return context.Client.WriteResponseAsync(RespValue.Pong, cancellationToken);
    }
}

/// <summary>
/// ECHO command - echo message.
/// </summary>
public class EchoCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        return context.Client.WriteBulkStringAsync(context.GetArg(0), cancellationToken);
    }
}

/// <summary>
/// SELECT command - select database.
/// </summary>
public class SelectCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var index = (int)context.GetArgAsInt(0);
        
        // Validate and set
        _ = context.Server.Store.GetDatabase(index); // Will throw if invalid
        context.Client.DatabaseIndex = index;
        
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// QUIT command - close connection.
/// </summary>
public class QuitCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        await context.Client.WriteOkAsync(cancellationToken);
        context.Client.Close();
    }
}

/// <summary>
/// AUTH command - authenticate.
/// </summary>
public class AuthCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        // Simple implementation - always succeed (no password configured)
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// CLIENT command - full client management (LIST, KILL, GETNAME, SETNAME, ID, INFO, PAUSE, UNPAUSE).
/// </summary>
public class ClientCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var subCommand = context.GetArg(0).ToUpperInvariant();

        switch (subCommand)
        {
            case "LIST":
                await HandleList(context, cancellationToken);
                break;
            case "GETNAME":
                await HandleGetName(context, cancellationToken);
                break;
            case "SETNAME":
                await HandleSetName(context, cancellationToken);
                break;
            case "ID":
                await context.Client.WriteIntegerAsync(context.Client.Id, cancellationToken);
                break;
            case "INFO":
                await HandleInfo(context, cancellationToken);
                break;
            case "KILL":
                await HandleKill(context, cancellationToken);
                break;
            case "NO-EVICT":
                // Toggle no-evict flag
                if (context.ArgCount >= 2 && context.GetArg(1).Equals("ON", StringComparison.OrdinalIgnoreCase))
                    context.Client.Flags |= ClientFlags.NoEvict;
                else
                    context.Client.Flags &= ~ClientFlags.NoEvict;
                await context.Client.WriteOkAsync(cancellationToken);
                break;
            default:
                await context.Client.WriteErrorAsync($"ERR Unknown subcommand or wrong number of arguments for CLIENT {subCommand}", cancellationToken);
                break;
        }
    }

    private async Task HandleList(CommandContext context, CancellationToken cancellationToken)
    {
        var clients = context.Server.GetClients().ToList();
        var sb = new System.Text.StringBuilder();
        
        foreach (var client in clients)
        {
            var flags = BuildFlagsString(client);
            sb.AppendFormat(
                "id={0} addr={1} laddr=0.0.0.0:{2} fd=0 name={3} db={4} " +
                "sub={5} psub=0 multi={6} watch=0 qbuf=0 qbuf-free=0 " +
                "obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=unknown " +
                "age={7} idle={8} flags={9}\n",
                client.Id,
                client.Address,
                context.Server.Port,
                client.Name ?? "",
                client.DatabaseIndex,
                client.InPubSubMode ? 1 : 0,
                client.InTransaction ? (client.GetQueuedCommands().Count) : -1,
                (long)(DateTime.UtcNow - client.ConnectedAt).TotalSeconds,
                (long)client.IdleTime.TotalSeconds,
                flags);
        }
        
        await context.Client.WriteBulkStringAsync(sb.ToString().TrimEnd('\n'), cancellationToken);
    }

    private static string BuildFlagsString(RedisClient client)
    {
        var flags = new System.Text.StringBuilder();
        if (client.Flags.HasFlag(ClientFlags.Slave)) flags.Append('S');
        if (client.Flags.HasFlag(ClientFlags.Master)) flags.Append('M');
        if (client.Flags.HasFlag(ClientFlags.Multi)) flags.Append('x');
        if (client.Flags.HasFlag(ClientFlags.Blocked)) flags.Append('b');
        if (client.InPubSubMode) flags.Append('P');
        if (client.Flags.HasFlag(ClientFlags.ReadOnly)) flags.Append('r');
        if (flags.Length == 0) flags.Append('N'); // Normal
        return flags.ToString();
    }

    private async Task HandleKill(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 2)
        {
            throw new Common.WrongArityException("CLIENT KILL");
        }

        int killed = 0;
        
        // CLIENT KILL <addr> (old format)
        if (context.ArgCount == 2 && !context.GetArg(1).Contains('='))
        {
            var targetAddr = context.GetArg(1);
            foreach (var c in context.Server.GetClients())
            {
                if (c.Address == targetAddr && c.Id != context.Client.Id)
                {
                    context.Server.KillClient(c.Id);
                    killed++;
                }
            }
            await context.Client.WriteOkAsync(cancellationToken);
            return;
        }

        // CLIENT KILL <filter> <value> [<filter> <value> ...] (new format)
        for (int i = 1; i + 1 < context.Args.Length; i += 2)
        {
            var filter = context.Args[i].ToUpperInvariant();
            var value = context.Args[i + 1];

            foreach (var c in context.Server.GetClients())
            {
                if (c.Id == context.Client.Id) continue; // Don't kill self

                bool match = filter switch
                {
                    "ID" => long.TryParse(value, out var id) && c.Id == id,
                    "ADDR" => c.Address == value,
                    "SKIPME" => false, // Skip handling
                    _ => false
                };

                if (match)
                {
                    context.Server.KillClient(c.Id);
                    killed++;
                }
            }
        }

        await context.Client.WriteIntegerAsync(killed, cancellationToken);
    }

    private Task HandleGetName(CommandContext context, CancellationToken cancellationToken)
    {
        return context.Client.WriteBulkStringAsync(context.Client.Name, cancellationToken);
    }

    private Task HandleSetName(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 2)
            throw new Common.WrongArityException("CLIENT SETNAME");
            
        context.Client.Name = context.GetArg(1);
        return context.Client.WriteOkAsync(cancellationToken);
    }

    private async Task HandleInfo(CommandContext context, CancellationToken cancellationToken)
    {
        var client = context.Client;
        var flags = BuildFlagsString(client);
        var info = $"id={client.Id} addr={client.Address} name={client.Name ?? ""} " +
                   $"db={client.DatabaseIndex} flags={flags} " +
                   $"age={(long)(DateTime.UtcNow - client.ConnectedAt).TotalSeconds} " +
                   $"idle={(long)client.IdleTime.TotalSeconds} " +
                   $"sub={( client.InPubSubMode ? 1 : 0)} " +
                   $"multi={(client.InTransaction ? client.GetQueuedCommands().Count : -1)} " +
                   $"resp={client.ProtocolVersion}";
        await context.Client.WriteBulkStringAsync(info, cancellationToken);
    }
}

/// <summary>
/// MEMORY command - memory introspection (USAGE, STATS, DOCTOR).
/// </summary>
public class MemoryCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var subCommand = context.GetArg(0).ToUpperInvariant();

        switch (subCommand)
        {
            case "USAGE":
                await HandleUsage(context, cancellationToken);
                break;
            case "STATS":
                await HandleStats(context, cancellationToken);
                break;
            case "DOCTOR":
                await HandleDoctor(context, cancellationToken);
                break;
            case "HELP":
                var help = new[]
                {
                    RespValue.BulkString("MEMORY USAGE <key> [SAMPLES <count>] - Estimate memory usage of a key"),
                    RespValue.BulkString("MEMORY STATS - Show memory usage stats"),
                    RespValue.BulkString("MEMORY DOCTOR - Diagnose memory issues"),
                };
                await context.Client.WriteResponseAsync(RespValue.Array(help), cancellationToken);
                break;
            default:
                await context.Client.WriteErrorAsync($"ERR Unknown subcommand '{subCommand}'", cancellationToken);
                break;
        }
    }

    private async Task HandleUsage(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 2)
            throw new Common.WrongArityException("MEMORY USAGE");

        var key = context.GetArg(1);
        var obj = context.Database.Get(key);
        
        if (obj == null)
        {
            await context.Client.WriteNullAsync(cancellationToken);
            return;
        }

        var size = Memory.EvictionManager.EstimateSize(obj);
        // Add overhead: key string + dict entry + expire entry
        var keyOverhead = 56 + System.Text.Encoding.UTF8.GetByteCount(key);
        await context.Client.WriteIntegerAsync(size + keyOverhead, cancellationToken);
    }

    private async Task HandleStats(CommandContext context, CancellationToken cancellationToken)
    {
        var eviction = context.Server.Store.Eviction;
        var gcInfo = GC.GetGCMemoryInfo();
        var totalMem = GC.GetTotalMemory(false);

        var stats = new System.Text.StringBuilder();
        stats.AppendLine("# Memory");
        stats.AppendFormat("used_memory:{0}\r\n", totalMem);
        stats.AppendFormat("used_memory_human:{0}\r\n", FormatBytes(totalMem));
        stats.AppendFormat("used_memory_rss:{0}\r\n", Environment.WorkingSet);
        stats.AppendFormat("used_memory_rss_human:{0}\r\n", FormatBytes(Environment.WorkingSet));
        stats.AppendFormat("used_memory_peak:{0}\r\n", gcInfo.HighMemoryLoadThresholdBytes);
        stats.AppendFormat("maxmemory:{0}\r\n", eviction.MaxMemory);
        stats.AppendFormat("maxmemory_policy:{0}\r\n", eviction.Policy.ToString().ToLowerInvariant());
        stats.AppendFormat("mem_tracked:{0}\r\n", eviction.ApproximateMemoryUsage);
        stats.AppendFormat("gc_gen0_count:{0}\r\n", GC.CollectionCount(0));
        stats.AppendFormat("gc_gen1_count:{0}\r\n", GC.CollectionCount(1));
        stats.AppendFormat("gc_gen2_count:{0}\r\n", GC.CollectionCount(2));

        await context.Client.WriteBulkStringAsync(stats.ToString(), cancellationToken);
    }

    private async Task HandleDoctor(CommandContext context, CancellationToken cancellationToken)
    {
        var eviction = context.Server.Store.Eviction;
        var totalMem = GC.GetTotalMemory(false);
        var sb = new System.Text.StringBuilder();
        bool hasIssues = false;

        // Check memory usage
        if (eviction.MaxMemory > 0 && eviction.ApproximateMemoryUsage > eviction.MaxMemory * 0.9)
        {
            sb.AppendLine("WARNING: Memory usage is above 90% of maxmemory limit.");
            hasIssues = true;
        }

        // Check GC pressure
        if (GC.CollectionCount(2) > 100)
        {
            sb.AppendLine("WARNING: High Gen2 GC collection count. Consider increasing maxmemory or optimizing data structures.");
            hasIssues = true;
        }

        if (!hasIssues)
        {
            sb.AppendLine("Sam, I have no memory problems");
        }

        await context.Client.WriteBulkStringAsync(sb.ToString().TrimEnd(), cancellationToken);
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes >= 1073741824) return $"{bytes / 1073741824.0:F2}G";
        if (bytes >= 1048576) return $"{bytes / 1048576.0:F2}M";
        if (bytes >= 1024) return $"{bytes / 1024.0:F2}K";
        return $"{bytes}B";
    }
}
