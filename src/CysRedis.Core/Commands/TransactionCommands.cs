using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// MULTI command - start transaction.
/// </summary>
public class MultiCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.Client.InTransaction)
        {
            return context.Client.WriteErrorAsync("ERR MULTI calls can not be nested", cancellationToken);
        }

        context.Client.StartTransaction();
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// EXEC command - execute transaction.
/// </summary>
public class ExecCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (!context.Client.InTransaction)
        {
            await context.Client.WriteErrorAsync("ERR EXEC without MULTI", cancellationToken);
            return;
        }

        // Check if transaction was aborted (WATCH keys modified)
        if (context.Client.TransactionAborted)
        {
            context.Client.DiscardTransaction();
            await context.Client.WriteNullAsync(cancellationToken);
            return;
        }

        var commands = context.Client.GetQueuedCommands();
        context.Client.DiscardTransaction();

        // Execute all queued commands
        var results = new List<RespValue>();
        foreach (var cmd in commands)
        {
            try
            {
                var capturedResult = new CapturedRespWriter();
                var tempClient = new TransactionExecutionClient(context.Client, capturedResult);
                var tempContext = new CommandContext(context.Server, tempClient, cmd[0], cmd);

                if (context.Server.Dispatcher.TryGetHandler(cmd[0], out var handler))
                {
                    await handler.ExecuteAsync(tempContext, cancellationToken);
                }
                else
                {
                    capturedResult.Value = RespValue.Error($"ERR unknown command '{cmd[0]}'");
                }

                results.Add(capturedResult.Value);
            }
            catch (RedisException ex)
            {
                results.Add(RespValue.Error(ex.GetRespError()));
            }
            catch (Exception ex)
            {
                results.Add(RespValue.Error($"ERR {ex.Message}"));
            }
        }

        await context.Client.WriteResponseAsync(RespValue.Array(results.ToArray()), cancellationToken);
    }
}

/// <summary>
/// DISCARD command - discard transaction.
/// </summary>
public class DiscardCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (!context.Client.InTransaction)
        {
            return context.Client.WriteErrorAsync("ERR DISCARD without MULTI", cancellationToken);
        }

        context.Client.DiscardTransaction();
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// WATCH command - watch keys for modifications.
/// </summary>
public class WatchCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);

        if (context.Client.InTransaction)
        {
            return context.Client.WriteErrorAsync("ERR WATCH inside MULTI is not allowed", cancellationToken);
        }

        for (int i = 0; i < context.ArgCount; i++)
        {
            context.Client.Watch(context.GetArg(i), context.Database);
        }

        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// UNWATCH command - unwatch all keys.
/// </summary>
public class UnwatchCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.Client.Unwatch();
        return context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// Helper class to capture RESP response during transaction execution.
/// </summary>
internal class CapturedRespWriter
{
    public RespValue Value { get; set; } = RespValue.Null;
}

/// <summary>
/// Wrapper client for transaction execution that captures responses.
/// </summary>
internal class TransactionExecutionClient : RedisClient
{
    private readonly RedisClient _realClient;
    private readonly CapturedRespWriter _writer;

    public TransactionExecutionClient(RedisClient realClient, CapturedRespWriter writer)
        : base(realClient)
    {
        _realClient = realClient;
        _writer = writer;
    }

    public override Task WriteResponseAsync(RespValue value, CancellationToken cancellationToken = default)
    {
        _writer.Value = value;
        return Task.CompletedTask;
    }

    public override Task WriteOkAsync(CancellationToken cancellationToken = default)
    {
        _writer.Value = RespValue.Ok;
        return Task.CompletedTask;
    }

    public override Task WriteNullAsync(CancellationToken cancellationToken = default)
    {
        _writer.Value = RespValue.Null;
        return Task.CompletedTask;
    }

    public override Task WriteIntegerAsync(long value, CancellationToken cancellationToken = default)
    {
        _writer.Value = new RespValue(value);
        return Task.CompletedTask;
    }

    public override Task WriteBulkStringAsync(string? value, CancellationToken cancellationToken = default)
    {
        _writer.Value = value != null ? RespValue.BulkString(value) : RespValue.Null;
        return Task.CompletedTask;
    }

    public override Task WriteErrorAsync(string error, CancellationToken cancellationToken = default)
    {
        _writer.Value = RespValue.Error(error);
        return Task.CompletedTask;
    }
}
