using CysRedis.Core.DataStructures;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// SADD command.
/// </summary>
public class SAddCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var set = context.Database.GetOrCreateSet(key);
        
        int added = 0;
        for (int i = 1; i < context.ArgCount; i++)
        {
            if (set.Add(context.GetArg(i)))
                added++;
        }
        
        return context.Client.WriteIntegerAsync(added, cancellationToken);
    }
}

/// <summary>
/// SREM command.
/// </summary>
public class SRemCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var set = context.Database.Get<RedisSet>(key);
        
        if (set == null)
            return context.Client.WriteIntegerAsync(0, cancellationToken);

        int removed = 0;
        for (int i = 1; i < context.ArgCount; i++)
        {
            if (set.Remove(context.GetArg(i)))
                removed++;
        }

        if (set.Count == 0)
            context.Database.Delete(key);
        
        return context.Client.WriteIntegerAsync(removed, cancellationToken);
    }
}

/// <summary>
/// SMEMBERS command.
/// </summary>
public class SMembersCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var set = context.Database.Get<RedisSet>(key);
        
        var members = set?.Members.Select(m => RespValue.BulkString(m)).ToArray() 
            ?? Array.Empty<RespValue>();
        await context.Client.WriteResponseAsync(RespValue.Array(members), cancellationToken);
    }
}

/// <summary>
/// SISMEMBER command.
/// </summary>
public class SIsMemberCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var member = context.GetArg(1);
        
        var set = context.Database.Get<RedisSet>(key);
        var isMember = set?.Contains(member) ?? false;
        
        return context.Client.WriteIntegerAsync(isMember ? 1 : 0, cancellationToken);
    }
}

/// <summary>
/// SMISMEMBER command - checks multiple members for existence.
/// </summary>
public class SMIsMemberCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var set = context.Database.Get<RedisSet>(key);
        
        var results = new RespValue[context.ArgCount - 1];
        for (int i = 1; i < context.ArgCount; i++)
        {
            var member = context.GetArg(i);
            results[i - 1] = new RespValue(set?.Contains(member) == true ? 1 : 0);
        }
        
        await context.Client.WriteResponseAsync(RespValue.Array(results), cancellationToken);
    }
}

/// <summary>
/// SCARD command.
/// </summary>
public class SCardCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var set = context.Database.Get<RedisSet>(key);
        return context.Client.WriteIntegerAsync(set?.Count ?? 0, cancellationToken);
    }
}

/// <summary>
/// SPOP command.
/// </summary>
public class SPopCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var key = context.GetArg(0);
        var set = context.Database.Get<RedisSet>(key);
        
        if (set == null || set.Count == 0)
        {
            await context.Client.WriteNullAsync(cancellationToken);
            return;
        }

        if (context.ArgCount > 1)
        {
            var count = (int)context.GetArgAsInt(1);
            var values = new List<RespValue>();
            for (int i = 0; i < count && set.Count > 0; i++)
            {
                var member = set.Pop();
                if (member != null)
                    values.Add(RespValue.BulkString(member));
            }
            await context.Client.WriteResponseAsync(RespValue.Array(values.ToArray()), cancellationToken);
        }
        else
        {
            var member = set.Pop();
            if (member != null)
                await context.Client.WriteBulkStringAsync(member, cancellationToken);
            else
                await context.Client.WriteNullAsync(cancellationToken);
        }

        if (set.Count == 0)
            context.Database.Delete(key);
    }
}

/// <summary>
/// SRANDMEMBER command.
/// </summary>
public class SRandMemberCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var key = context.GetArg(0);
        var set = context.Database.Get<RedisSet>(key);
        
        if (set == null || set.Count == 0)
        {
            if (context.ArgCount > 1)
                await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
            else
                await context.Client.WriteNullAsync(cancellationToken);
            return;
        }

        if (context.ArgCount > 1)
        {
            var count = (int)context.GetArgAsInt(1);
            var members = set.Members.ToList();
            var result = new List<RespValue>();
            
            if (count > 0)
            {
                // Without repetition
                for (int i = 0; i < count && i < members.Count; i++)
                {
                    result.Add(RespValue.BulkString(members[i]));
                }
            }
            else
            {
                // With repetition
                count = Math.Abs(count);
                for (int i = 0; i < count; i++)
                {
                    var randomMember = set.RandomMember();
                    if (randomMember != null)
                        result.Add(RespValue.BulkString(randomMember));
                }
            }
            
            await context.Client.WriteResponseAsync(RespValue.Array(result.ToArray()), cancellationToken);
        }
        else
        {
            var member = set.RandomMember();
            if (member != null)
                await context.Client.WriteBulkStringAsync(member, cancellationToken);
            else
                await context.Client.WriteNullAsync(cancellationToken);
        }
    }
}

/// <summary>
/// SUNION command.
/// </summary>
public class SUnionCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        
        var result = new HashSet<string>(StringComparer.Ordinal);
        
        for (int i = 0; i < context.ArgCount; i++)
        {
            var set = context.Database.Get<RedisSet>(context.GetArg(i));
            if (set != null)
                result.UnionWith(set.Members);
        }
        
        var values = result.Select(m => RespValue.BulkString(m)).ToArray();
        await context.Client.WriteResponseAsync(RespValue.Array(values), cancellationToken);
    }
}

/// <summary>
/// SINTER command.
/// </summary>
public class SInterCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        
        HashSet<string>? result = null;
        
        for (int i = 0; i < context.ArgCount; i++)
        {
            var set = context.Database.Get<RedisSet>(context.GetArg(i));
            if (set == null)
            {
                result = new HashSet<string>();
                break;
            }
            
            if (result == null)
                result = new HashSet<string>(set.Members, StringComparer.Ordinal);
            else
                result.IntersectWith(set.Members);
        }
        
        var values = (result ?? new HashSet<string>()).Select(m => RespValue.BulkString(m)).ToArray();
        await context.Client.WriteResponseAsync(RespValue.Array(values), cancellationToken);
    }
}

/// <summary>
/// SDIFF command.
/// </summary>
public class SDiffCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        
        var firstSet = context.Database.Get<RedisSet>(context.GetArg(0));
        if (firstSet == null)
        {
            await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
            return;
        }
        
        var result = new HashSet<string>(firstSet.Members, StringComparer.Ordinal);
        
        for (int i = 1; i < context.ArgCount; i++)
        {
            var set = context.Database.Get<RedisSet>(context.GetArg(i));
            if (set != null)
                result.ExceptWith(set.Members);
        }
        
        var values = result.Select(m => RespValue.BulkString(m)).ToArray();
        await context.Client.WriteResponseAsync(RespValue.Array(values), cancellationToken);
    }
}
