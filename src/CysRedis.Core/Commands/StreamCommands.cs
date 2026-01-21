using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// XADD command - add entry to stream.
/// </summary>
public class XAddCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        var key = context.GetArg(0);
        
        int fieldStart = 1;
        string? id = null;
        long? maxLen = null;
        bool approximate = false;

        // Parse options
        while (fieldStart < context.ArgCount)
        {
            var opt = context.GetArg(fieldStart).ToUpperInvariant();
            if (opt == "MAXLEN")
            {
                fieldStart++;
                if (fieldStart < context.ArgCount && context.GetArg(fieldStart) == "~")
                {
                    approximate = true;
                    fieldStart++;
                }
                if (fieldStart >= context.ArgCount)
                    throw new SyntaxErrorException();
                maxLen = context.GetArgAsInt(fieldStart);
                fieldStart++;
            }
            else if (opt == "*" || opt.Contains('-'))
            {
                id = opt == "*" ? null : opt;
                fieldStart++;
                break;
            }
            else
            {
                break;
            }
        }

        // Parse fields
        if ((context.ArgCount - fieldStart) < 2 || (context.ArgCount - fieldStart) % 2 != 0)
            throw new WrongArityException("XADD");

        var fields = new Dictionary<string, string>();
        for (int i = fieldStart; i < context.ArgCount; i += 2)
        {
            fields[context.GetArg(i)] = context.GetArg(i + 1);
        }

        var stream = context.Database.GetOrCreate(key, () => new RedisStream());
        var entryId = stream.Add(id, fields);

        // Apply MAXLEN trim
        if (maxLen.HasValue)
        {
            stream.Trim(maxLen.Value, approximate);
        }

        await context.Client.WriteBulkStringAsync(entryId.ToString(), cancellationToken);
    }
}

/// <summary>
/// XREAD command - read from streams.
/// </summary>
public class XReadCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        
        int count = int.MaxValue;
        int argIndex = 0;

        // Parse options
        while (argIndex < context.ArgCount)
        {
            var opt = context.GetArg(argIndex).ToUpperInvariant();
            if (opt == "COUNT")
            {
                argIndex++;
                count = (int)context.GetArgAsInt(argIndex);
                argIndex++;
            }
            else if (opt == "BLOCK")
            {
                // Simplified: ignore blocking for now
                argIndex += 2;
            }
            else if (opt == "STREAMS")
            {
                argIndex++;
                break;
            }
            else
            {
                argIndex++;
            }
        }

        // Parse streams and IDs
        var remaining = context.ArgCount - argIndex;
        if (remaining < 2 || remaining % 2 != 0)
            throw new SyntaxErrorException();

        var numStreams = remaining / 2;
        var streamKeys = new string[numStreams];
        var startIds = new string[numStreams];

        for (int i = 0; i < numStreams; i++)
        {
            streamKeys[i] = context.GetArg(argIndex + i);
            startIds[i] = context.GetArg(argIndex + numStreams + i);
        }

        var results = new List<RespValue>();
        
        for (int i = 0; i < numStreams; i++)
        {
            var stream = context.Database.Get<RedisStream>(streamKeys[i]);
            if (stream == null) continue;

            StreamEntryId? startId = null;
            bool exclusive = false;
            
            if (startIds[i] == "$")
            {
                startId = stream.LastId;
                exclusive = true;
            }
            else if (startIds[i] != "0" && startIds[i] != "0-0")
            {
                startId = StreamEntryId.Parse(startIds[i]);
                exclusive = true;
            }

            var entries = stream.Read(startId, count, exclusive).ToList();
            if (entries.Count == 0) continue;

            var entryValues = entries.Select(e => RespValue.Array(
                RespValue.BulkString(e.Id.ToString()),
                RespValue.Array(e.Fields.SelectMany(f => new[] {
                    RespValue.BulkString(f.Key),
                    RespValue.BulkString(f.Value)
                }).ToArray())
            )).ToArray();

            results.Add(RespValue.Array(
                RespValue.BulkString(streamKeys[i]),
                RespValue.Array(entryValues)
            ));
        }

        if (results.Count == 0)
            await context.Client.WriteNullAsync(cancellationToken);
        else
            await context.Client.WriteResponseAsync(RespValue.Array(results.ToArray()), cancellationToken);
    }
}

/// <summary>
/// XRANGE command - get range of entries.
/// </summary>
public class XRangeCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        var key = context.GetArg(0);
        var startStr = context.GetArg(1);
        var endStr = context.GetArg(2);
        
        int count = int.MaxValue;
        if (context.ArgCount > 4 && context.GetArg(3).ToUpperInvariant() == "COUNT")
            count = (int)context.GetArgAsInt(4);

        var stream = context.Database.Get<RedisStream>(key);
        if (stream == null)
        {
            await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
            return;
        }

        StreamEntryId? start = startStr == "-" ? null : StreamEntryId.Parse(startStr);
        StreamEntryId? end = endStr == "+" ? null : StreamEntryId.Parse(endStr);

        var entries = stream.Range(start, end, count).ToList();
        var values = entries.Select(e => RespValue.Array(
            RespValue.BulkString(e.Id.ToString()),
            RespValue.Array(e.Fields.SelectMany(f => new[] {
                RespValue.BulkString(f.Key),
                RespValue.BulkString(f.Value)
            }).ToArray())
        )).ToArray();

        await context.Client.WriteResponseAsync(RespValue.Array(values), cancellationToken);
    }
}

/// <summary>
/// XLEN command - get stream length.
/// </summary>
public class XLenCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var stream = context.Database.Get<RedisStream>(key);
        return context.Client.WriteIntegerAsync(stream?.Length ?? 0, cancellationToken);
    }
}

/// <summary>
/// XGROUP command - manage consumer groups.
/// </summary>
public class XGroupCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var subCommand = context.GetArg(0).ToUpperInvariant();

        switch (subCommand)
        {
            case "CREATE":
                await HandleCreate(context, cancellationToken);
                break;
            case "DESTROY":
                await HandleDestroy(context, cancellationToken);
                break;
            case "SETID":
                await HandleSetId(context, cancellationToken);
                break;
            default:
                await context.Client.WriteErrorAsync($"ERR Unknown XGROUP subcommand '{subCommand}'", cancellationToken);
                break;
        }
    }

    private async Task HandleCreate(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 4)
            throw new WrongArityException("XGROUP CREATE");

        var key = context.GetArg(1);
        var groupName = context.GetArg(2);
        var idStr = context.GetArg(3);

        var stream = context.Database.GetOrCreate(key, () => new RedisStream());
        
        StreamEntryId? lastId = idStr == "$" ? stream.LastId : 
            (idStr == "0" ? new StreamEntryId(0, 0) : StreamEntryId.Parse(idStr));

        if (stream.CreateGroup(groupName, lastId))
            await context.Client.WriteOkAsync(cancellationToken);
        else
            await context.Client.WriteErrorAsync("BUSYGROUP Consumer Group name already exists", cancellationToken);
    }

    private async Task HandleDestroy(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 3)
            throw new WrongArityException("XGROUP DESTROY");

        var key = context.GetArg(1);
        var groupName = context.GetArg(2);

        var stream = context.Database.Get<RedisStream>(key);
        var destroyed = stream?.DestroyGroup(groupName) ?? false;

        await context.Client.WriteIntegerAsync(destroyed ? 1 : 0, cancellationToken);
    }

    private async Task HandleSetId(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 4)
            throw new WrongArityException("XGROUP SETID");

        var key = context.GetArg(1);
        var groupName = context.GetArg(2);
        var idStr = context.GetArg(3);

        var stream = context.Database.Get<RedisStream>(key);
        var group = stream?.GetGroup(groupName);

        if (group == null)
        {
            await context.Client.WriteErrorAsync("NOGROUP No such consumer group", cancellationToken);
            return;
        }

        group.LastDeliveredId = idStr == "$" ? (stream!.LastId ?? new StreamEntryId(0, 0)) : StreamEntryId.Parse(idStr);
        await context.Client.WriteOkAsync(cancellationToken);
    }
}

/// <summary>
/// XACK command - acknowledge messages.
/// </summary>
public class XAckCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        var key = context.GetArg(0);
        var groupName = context.GetArg(1);

        var stream = context.Database.Get<RedisStream>(key);
        var group = stream?.GetGroup(groupName);

        if (group == null)
            return context.Client.WriteIntegerAsync(0, cancellationToken);

        int acknowledged = 0;
        for (int i = 2; i < context.ArgCount; i++)
        {
            if (group.Acknowledge(context.GetArg(i)))
                acknowledged++;
        }

        return context.Client.WriteIntegerAsync(acknowledged, cancellationToken);
    }
}

/// <summary>
/// XTRIM command - trim stream.
/// </summary>
public class XTrimCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        var key = context.GetArg(0);
        var strategy = context.GetArg(1).ToUpperInvariant();

        if (strategy != "MAXLEN")
            throw new SyntaxErrorException();

        int argIndex = 2;
        bool approximate = false;
        
        if (context.GetArg(argIndex) == "~")
        {
            approximate = true;
            argIndex++;
        }

        var maxLen = context.GetArgAsInt(argIndex);

        var stream = context.Database.Get<RedisStream>(key);
        var trimmed = stream?.Trim(maxLen, approximate) ?? 0;

        return context.Client.WriteIntegerAsync(trimmed, cancellationToken);
    }
}
