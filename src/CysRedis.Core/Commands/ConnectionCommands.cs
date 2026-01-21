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
/// CLIENT command - client management.
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
            default:
                await context.Client.WriteErrorAsync($"ERR Unknown subcommand '{subCommand}'", cancellationToken);
                break;
        }
    }

    private async Task HandleList(CommandContext context, CancellationToken cancellationToken)
    {
        var clients = context.Server.GetClients().ToList();
        var lines = new List<string>();
        
        foreach (var client in clients)
        {
            lines.Add($"id={client.Id} addr={client.Address} name={client.Name ?? ""} db={client.DatabaseIndex}");
        }
        
        await context.Client.WriteBulkStringAsync(string.Join("\n", lines), cancellationToken);
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
        var info = $"id={client.Id} addr={client.Address} name={client.Name ?? ""} db={client.DatabaseIndex}";
        await context.Client.WriteBulkStringAsync(info, cancellationToken);
    }
}
