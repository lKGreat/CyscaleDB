using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// EVAL command - evaluate Lua script.
/// </summary>
public class EvalCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var script = context.GetArg(0);
        var numKeys = (int)context.GetArgAsInt(1);

        if (context.ArgCount < 2 + numKeys)
            throw new WrongArityException("EVAL");

        var keys = new string[numKeys];
        var args = new string[context.ArgCount - 2 - numKeys];

        for (int i = 0; i < numKeys; i++)
            keys[i] = context.GetArg(2 + i);

        for (int i = 0; i < args.Length; i++)
            args[i] = context.GetArg(2 + numKeys + i);

        var result = context.Server.ScriptManager.Execute(script, keys, args, 
            cmdArgs => ExecuteRedisCommand(context, cmdArgs, cancellationToken));

        await WriteResult(context.Client, result, cancellationToken);
    }

    private static object? ExecuteRedisCommand(CommandContext context, string[] args, CancellationToken cancellationToken)
    {
        // Simplified: This would need to execute Redis commands from within Lua
        // For now, return null
        return null;
    }

    private static async Task WriteResult(RedisClient client, object? result, CancellationToken cancellationToken)
    {
        switch (result)
        {
            case null:
                await client.WriteNullAsync(cancellationToken);
                break;
            case bool b:
                await client.WriteIntegerAsync(b ? 1 : 0, cancellationToken);
                break;
            case int i:
                await client.WriteIntegerAsync(i, cancellationToken);
                break;
            case long l:
                await client.WriteIntegerAsync(l, cancellationToken);
                break;
            case string s:
                await client.WriteBulkStringAsync(s, cancellationToken);
                break;
            case object[] arr:
                var values = new RespValue[arr.Length];
                for (int i = 0; i < arr.Length; i++)
                {
                    values[i] = arr[i] switch
                    {
                        null => RespValue.Null,
                        bool b => new RespValue(b ? 1 : 0),
                        int n => new RespValue(n),
                        long n => new RespValue(n),
                        string s => RespValue.BulkString(s),
                        _ => RespValue.BulkString(arr[i].ToString() ?? "")
                    };
                }
                await client.WriteResponseAsync(RespValue.Array(values), cancellationToken);
                break;
            default:
                await client.WriteBulkStringAsync(result.ToString(), cancellationToken);
                break;
        }
    }
}

/// <summary>
/// EVALSHA command - evaluate script by SHA1.
/// </summary>
public class EvalShaCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var sha1 = context.GetArg(0);
        var numKeys = (int)context.GetArgAsInt(1);

        var script = context.Server.ScriptManager.GetScript(sha1);
        if (script == null)
        {
            await context.Client.WriteErrorAsync("NOSCRIPT No matching script. Please use EVAL.", cancellationToken);
            return;
        }

        // Reuse EVAL logic
        var keys = new string[numKeys];
        var args = new string[context.ArgCount - 2 - numKeys];

        for (int i = 0; i < numKeys; i++)
            keys[i] = context.GetArg(2 + i);

        for (int i = 0; i < args.Length; i++)
            args[i] = context.GetArg(2 + numKeys + i);

        var result = context.Server.ScriptManager.Execute(script, keys, args, null!);
        await WriteResult(context.Client, result, cancellationToken);
    }

    private static async Task WriteResult(RedisClient client, object? result, CancellationToken cancellationToken)
    {
        if (result == null)
            await client.WriteNullAsync(cancellationToken);
        else if (result is long l)
            await client.WriteIntegerAsync(l, cancellationToken);
        else
            await client.WriteBulkStringAsync(result.ToString(), cancellationToken);
    }
}

/// <summary>
/// SCRIPT command - script management.
/// </summary>
public class ScriptCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var subCommand = context.GetArg(0).ToUpperInvariant();

        switch (subCommand)
        {
            case "LOAD":
                await HandleLoad(context, cancellationToken);
                break;
            case "EXISTS":
                await HandleExists(context, cancellationToken);
                break;
            case "FLUSH":
                await HandleFlush(context, cancellationToken);
                break;
            case "KILL":
                // No-op for now (would require async script execution)
                await context.Client.WriteOkAsync(cancellationToken);
                break;
            default:
                await context.Client.WriteErrorAsync($"ERR Unknown SCRIPT subcommand '{subCommand}'", cancellationToken);
                break;
        }
    }

    private async Task HandleLoad(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 2)
            throw new WrongArityException("SCRIPT LOAD");

        var script = context.GetArg(1);
        var sha1 = context.Server.ScriptManager.Load(script);
        await context.Client.WriteBulkStringAsync(sha1, cancellationToken);
    }

    private async Task HandleExists(CommandContext context, CancellationToken cancellationToken)
    {
        var sha1s = new string[context.ArgCount - 1];
        for (int i = 1; i < context.ArgCount; i++)
            sha1s[i - 1] = context.GetArg(i);

        var exists = context.Server.ScriptManager.Exists(sha1s);
        var results = exists.Select(e => new RespValue(e ? 1 : 0)).ToArray();
        await context.Client.WriteResponseAsync(RespValue.Array(results), cancellationToken);
    }

    private Task HandleFlush(CommandContext context, CancellationToken cancellationToken)
    {
        context.Server.ScriptManager.Flush();
        return context.Client.WriteOkAsync(cancellationToken);
    }
}
