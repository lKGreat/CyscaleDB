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

        // 创建redis.call和redis.pcall的回调
        var result = context.Server.ScriptManager.Execute(
            script, 
            keys, 
            args,
            async cmdArgs => await ExecuteRedisCommand(context, cmdArgs, cancellationToken),
            async cmdArgs => await ExecuteRedisPcall(context, cmdArgs, cancellationToken),
            cancellationToken);

        await WriteResult(context.Client, result, cancellationToken);
    }

    internal static async Task<object?> ExecuteRedisCommand(CommandContext context, string[] args, CancellationToken cancellationToken)
    {
        if (args.Length == 0)
            throw new RedisException("ERR redis.call() requires at least one argument");

        try
        {
            // 执行Redis命令
            var resultCapture = new ResultCapture();
            var captureClient = new CaptureRedisClient(context.Client, resultCapture);
            
            await context.Server.Dispatcher.ExecuteAsync(captureClient, args, cancellationToken);
            
            return resultCapture.Result;
        }
        catch (Exception ex)
        {
            throw new RedisException($"ERR {ex.Message}");
        }
    }

    internal static async Task<object?> ExecuteRedisPcall(CommandContext context, string[] args, CancellationToken cancellationToken)
    {
        try
        {
            return await ExecuteRedisCommand(context, args, cancellationToken);
        }
        catch (Exception ex)
        {
            // redis.pcall 返回错误对象而不是抛出异常
            return new Dictionary<string, object?> { ["err"] = ex.Message };
        }
    }

    internal static async Task WriteResult(RedisClient client, object? result, CancellationToken cancellationToken)
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
            case double d:
                await client.WriteBulkStringAsync(d.ToString("G17", System.Globalization.CultureInfo.InvariantCulture), cancellationToken);
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
                        double d => RespValue.BulkString(d.ToString("G17", System.Globalization.CultureInfo.InvariantCulture)),
                        string s => RespValue.BulkString(s),
                        _ => RespValue.BulkString(arr[i].ToString() ?? "")
                    };
                }
                await client.WriteResponseAsync(RespValue.Array(values), cancellationToken);
                break;
            case Dictionary<string, object?> dict when dict.ContainsKey("ok"):
                // 状态回复
                await client.WriteResponseAsync(RespValue.SimpleString(dict["ok"]?.ToString() ?? "OK"), cancellationToken);
                break;
            case Dictionary<string, object?> dict when dict.ContainsKey("err"):
                // 错误回复
                await client.WriteErrorAsync(dict["err"]?.ToString() ?? "ERR", cancellationToken);
                break;
            default:
                await client.WriteBulkStringAsync(result.ToString() ?? "", cancellationToken);
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

        if (context.ArgCount < 2 + numKeys)
            throw new WrongArityException("EVALSHA");

        var keys = new string[numKeys];
        var args = new string[context.ArgCount - 2 - numKeys];

        for (int i = 0; i < numKeys; i++)
            keys[i] = context.GetArg(2 + i);

        for (int i = 0; i < args.Length; i++)
            args[i] = context.GetArg(2 + numKeys + i);

        // 使用与EVAL相同的回调
        var result = context.Server.ScriptManager.Execute(
            script, 
            keys, 
            args,
            async cmdArgs => await EvalCommand.ExecuteRedisCommand(context, cmdArgs, cancellationToken),
            async cmdArgs => await EvalCommand.ExecuteRedisPcall(context, cmdArgs, cancellationToken),
            cancellationToken);

        await EvalCommand.WriteResult(context.Client, result, cancellationToken);
    }
}

/// <summary>
/// Helper class to capture command result.
/// </summary>
internal class ResultCapture
{
    public object? Result { get; set; }
}

/// <summary>
/// Wrapper client to capture command results.
/// </summary>
internal class CaptureRedisClient : RedisClient
{
    private readonly ResultCapture _capture;

    public CaptureRedisClient(RedisClient other, ResultCapture capture) : base(other)
    {
        _capture = capture;
    }

    public override Task WriteResponseAsync(RespValue value, CancellationToken cancellationToken = default)
    {
        _capture.Result = ConvertRespValue(value);
        return Task.CompletedTask;
    }

    public override Task WriteOkAsync(CancellationToken cancellationToken = default)
    {
        _capture.Result = "OK";
        return Task.CompletedTask;
    }

    public override Task WriteErrorAsync(string message, CancellationToken cancellationToken = default)
    {
        throw new RedisException(message);
    }

    public override Task WriteIntegerAsync(long value, CancellationToken cancellationToken = default)
    {
        _capture.Result = value;
        return Task.CompletedTask;
    }

    public override Task WriteBulkStringAsync(string? value, CancellationToken cancellationToken = default)
    {
        _capture.Result = value;
        return Task.CompletedTask;
    }

    public override Task WriteNullAsync(CancellationToken cancellationToken = default)
    {
        _capture.Result = null;
        return Task.CompletedTask;
    }

    private static object? ConvertRespValue(RespValue value)
    {
        return value.Type switch
        {
            RespType.SimpleString => value.GetString(),
            RespType.BulkString => value.GetString(),
            RespType.Integer => value.Integer,
            RespType.Array => value.Elements?.Select(ConvertRespValue).ToArray(),
            RespType.Null => null,
            RespType.Error => throw new RedisException(value.GetString() ?? "ERR unknown error"),
            _ => null
        };
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
