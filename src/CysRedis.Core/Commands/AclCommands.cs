using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// ACL command - access control list management.
/// </summary>
public class AclCommand : ICommandHandler
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
            case "USERS":
                await HandleUsers(context, cancellationToken);
                break;
            case "GETUSER":
                await HandleGetUser(context, cancellationToken);
                break;
            case "SETUSER":
                await HandleSetUser(context, cancellationToken);
                break;
            case "DELUSER":
                await HandleDelUser(context, cancellationToken);
                break;
            case "WHOAMI":
                await HandleWhoAmI(context, cancellationToken);
                break;
            case "CAT":
                await HandleCat(context, cancellationToken);
                break;
            default:
                await context.Client.WriteErrorAsync($"ERR Unknown ACL subcommand '{subCommand}'", cancellationToken);
                break;
        }
    }

    private async Task HandleList(CommandContext context, CancellationToken cancellationToken)
    {
        var users = context.Server.Acl.GetUsers().ToList();
        var result = users.Select(u => RespValue.BulkString(FormatUserRule(u))).ToArray();
        await context.Client.WriteResponseAsync(RespValue.Array(result), cancellationToken);
    }

    private async Task HandleUsers(CommandContext context, CancellationToken cancellationToken)
    {
        var names = context.Server.Acl.GetUserNames()
            .Select(n => RespValue.BulkString(n))
            .ToArray();
        await context.Client.WriteResponseAsync(RespValue.Array(names), cancellationToken);
    }

    private async Task HandleGetUser(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 2)
            throw new WrongArityException("ACL GETUSER");

        var username = context.GetArg(1);
        var user = context.Server.Acl.GetUser(username);

        if (user == null)
        {
            await context.Client.WriteNullAsync(cancellationToken);
            return;
        }

        var result = new List<RespValue>
        {
            RespValue.BulkString("flags"),
            RespValue.Array(user.GetFlags().Select(f => RespValue.BulkString(f)).ToArray()),
            RespValue.BulkString("passwords"),
            RespValue.Array(), // Don't expose passwords
            RespValue.BulkString("commands"),
            RespValue.BulkString("+@all"),
            RespValue.BulkString("keys"),
            RespValue.BulkString("~*")
        };

        await context.Client.WriteResponseAsync(RespValue.Array(result.ToArray()), cancellationToken);
    }

    private async Task HandleSetUser(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 2)
            throw new WrongArityException("ACL SETUSER");

        var username = context.GetArg(1);

        context.Server.Acl.SetUser(username, user =>
        {
            for (int i = 2; i < context.ArgCount; i++)
            {
                var rule = context.GetArg(i).ToLowerInvariant();
                ApplyRule(user, rule);
            }
        });

        await context.Client.WriteOkAsync(cancellationToken);
    }

    private async Task HandleDelUser(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 2)
            throw new WrongArityException("ACL DELUSER");

        int deleted = 0;
        for (int i = 1; i < context.ArgCount; i++)
        {
            if (context.Server.Acl.DeleteUser(context.GetArg(i)))
                deleted++;
        }

        await context.Client.WriteIntegerAsync(deleted, cancellationToken);
    }

    private Task HandleWhoAmI(CommandContext context, CancellationToken cancellationToken)
    {
        // Simplified: return "default" user
        return context.Client.WriteBulkStringAsync("default", cancellationToken);
    }

    private async Task HandleCat(CommandContext context, CancellationToken cancellationToken)
    {
        // Return command categories
        var categories = new[] { "read", "write", "set", "sortedset", "list", "hash", "string",
            "bitmap", "hyperloglog", "geo", "stream", "pubsub", "admin", "fast", "slow",
            "blocking", "dangerous", "connection", "transaction", "scripting" };

        if (context.ArgCount > 1)
        {
            // Return commands in category (simplified)
            await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
        }
        else
        {
            var result = categories.Select(c => RespValue.BulkString(c)).ToArray();
            await context.Client.WriteResponseAsync(RespValue.Array(result), cancellationToken);
        }
    }

    private static void ApplyRule(Auth.AclUser user, string rule)
    {
        switch (rule)
        {
            case "on":
                user.Enabled = true;
                break;
            case "off":
                user.Enabled = false;
                break;
            case "nopass":
                user.NoPassword = true;
                break;
            case "resetpass":
                user.NoPassword = false;
                break;
            case "allcommands":
            case "+@all":
                user.AllowAllCommands();
                break;
            case "allkeys":
            case "~*":
                user.AllowAllKeys();
                break;
            default:
                if (rule.StartsWith('>'))
                    user.AddPassword(rule[1..]);
                else if (rule.StartsWith('<'))
                    user.RemovePassword(rule[1..]);
                else if (rule.StartsWith('+'))
                    user.AllowCommand(rule[1..]);
                else if (rule.StartsWith('-'))
                    user.DisallowCommand(rule[1..]);
                else if (rule.StartsWith('~'))
                    user.AddKeyPattern(rule[1..]);
                break;
        }
    }

    private static string FormatUserRule(Auth.AclUser user)
    {
        var parts = new List<string> { $"user {user.Name}" };
        parts.AddRange(user.GetFlags());
        return string.Join(" ", parts);
    }
}
