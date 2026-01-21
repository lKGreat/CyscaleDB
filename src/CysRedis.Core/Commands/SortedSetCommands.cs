using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// ZADD command.
/// </summary>
public class ZAddCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        var key = context.GetArg(0);

        bool nx = false, xx = false, gt = false, lt = false, ch = false;
        int startIndex = 1;

        // Parse options
        while (startIndex < context.ArgCount)
        {
            var opt = context.GetArg(startIndex).ToUpperInvariant();
            switch (opt)
            {
                case "NX": nx = true; startIndex++; break;
                case "XX": xx = true; startIndex++; break;
                case "GT": gt = true; startIndex++; break;
                case "LT": lt = true; startIndex++; break;
                case "CH": ch = true; startIndex++; break;
                default: goto parseScoreMembers;
            }
        }

    parseScoreMembers:
        if ((context.ArgCount - startIndex) < 2 || (context.ArgCount - startIndex) % 2 != 0)
            throw new WrongArityException("ZADD");

        var zset = context.Database.GetOrCreate(key, () => new RedisSortedSet());
        int added = 0, changed = 0;

        for (int i = startIndex; i < context.ArgCount; i += 2)
        {
            var scoreStr = context.GetArg(i);
            var member = context.GetArg(i + 1);

            if (!double.TryParse(scoreStr, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var score))
                throw new NotFloatException();

            var exists = zset.Contains(member);
            var oldScore = zset.GetScore(member);

            // Check NX/XX conditions
            if (nx && exists) continue;
            if (xx && !exists) continue;

            // Check GT/LT conditions
            if (exists && oldScore.HasValue)
            {
                if (gt && score <= oldScore.Value) continue;
                if (lt && score >= oldScore.Value) continue;
            }

            var wasNew = zset.Add(member, score);
            if (wasNew) added++;
            else if (oldScore.HasValue && Math.Abs(oldScore.Value - score) > double.Epsilon)
                changed++;
        }

        // 唤醒阻塞在此键上的客户端
        if (added > 0)
            context.Server.Blocking.SignalKeyReady(key);

        return context.Client.WriteIntegerAsync(ch ? added + changed : added, cancellationToken);
    }
}

/// <summary>
/// ZREM command.
/// </summary>
public class ZRemCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var zset = context.Database.Get<RedisSortedSet>(key);

        if (zset == null)
            return context.Client.WriteIntegerAsync(0, cancellationToken);

        int removed = 0;
        for (int i = 1; i < context.ArgCount; i++)
        {
            if (zset.Remove(context.GetArg(i)))
                removed++;
        }

        if (zset.Count == 0)
            context.Database.Delete(key);

        return context.Client.WriteIntegerAsync(removed, cancellationToken);
    }
}

/// <summary>
/// ZSCORE command.
/// </summary>
public class ZScoreCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var member = context.GetArg(1);

        var zset = context.Database.Get<RedisSortedSet>(key);
        var score = zset?.GetScore(member);

        if (score == null)
            return context.Client.WriteNullAsync(cancellationToken);

        return context.Client.WriteBulkStringAsync(
            score.Value.ToString("G17", System.Globalization.CultureInfo.InvariantCulture),
            cancellationToken);
    }
}

/// <summary>
/// ZMSCORE command - returns the scores of multiple members.
/// </summary>
public class ZMScoreCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var zset = context.Database.Get<RedisSortedSet>(key);
        
        var results = new RespValue[context.ArgCount - 1];
        for (int i = 1; i < context.ArgCount; i++)
        {
            var member = context.GetArg(i);
            var score = zset?.GetScore(member);
            results[i - 1] = score.HasValue 
                ? RespValue.BulkString(score.Value.ToString("G17", System.Globalization.CultureInfo.InvariantCulture)) 
                : RespValue.Null;
        }
        
        await context.Client.WriteResponseAsync(RespValue.Array(results), cancellationToken);
    }
}

/// <summary>
/// ZRANK command.
/// </summary>
public class ZRankCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var member = context.GetArg(1);

        var zset = context.Database.Get<RedisSortedSet>(key);
        var rank = zset?.GetRank(member);

        if (rank == null)
            return context.Client.WriteNullAsync(cancellationToken);

        return context.Client.WriteIntegerAsync(rank.Value, cancellationToken);
    }
}

/// <summary>
/// ZREVRANK command.
/// </summary>
public class ZRevRankCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var member = context.GetArg(1);

        var zset = context.Database.Get<RedisSortedSet>(key);
        var rank = zset?.GetRank(member, reverse: true);

        if (rank == null)
            return context.Client.WriteNullAsync(cancellationToken);

        return context.Client.WriteIntegerAsync(rank.Value, cancellationToken);
    }
}

/// <summary>
/// ZRANGE command.
/// </summary>
public class ZRangeCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        var key = context.GetArg(0);
        var start = (int)context.GetArgAsInt(1);
        var stop = (int)context.GetArgAsInt(2);

        bool withScores = false;
        bool reverse = false;

        for (int i = 3; i < context.ArgCount; i++)
        {
            var opt = context.GetArg(i).ToUpperInvariant();
            if (opt == "WITHSCORES") withScores = true;
            else if (opt == "REV") reverse = true;
        }

        var zset = context.Database.Get<RedisSortedSet>(key);
        if (zset == null || zset.Count == 0)
        {
            await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
            return;
        }

        // Handle negative indices
        if (start < 0) start = Math.Max(0, zset.Count + start);
        if (stop < 0) stop = zset.Count + stop;

        var items = zset.GetRange(start, stop, reverse).ToList();
        var result = new List<RespValue>();

        foreach (var (member, score) in items)
        {
            result.Add(RespValue.BulkString(member));
            if (withScores)
                result.Add(RespValue.BulkString(score.ToString("G17", System.Globalization.CultureInfo.InvariantCulture)));
        }

        await context.Client.WriteResponseAsync(RespValue.Array(result.ToArray()), cancellationToken);
    }
}

/// <summary>
/// ZREVRANGE command.
/// </summary>
public class ZRevRangeCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        var key = context.GetArg(0);
        var start = (int)context.GetArgAsInt(1);
        var stop = (int)context.GetArgAsInt(2);

        bool withScores = context.ArgCount > 3 && 
            context.GetArg(3).Equals("WITHSCORES", StringComparison.OrdinalIgnoreCase);

        var zset = context.Database.Get<RedisSortedSet>(key);
        if (zset == null || zset.Count == 0)
        {
            await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
            return;
        }

        if (start < 0) start = Math.Max(0, zset.Count + start);
        if (stop < 0) stop = zset.Count + stop;

        var items = zset.GetRange(start, stop, reverse: true).ToList();
        var result = new List<RespValue>();

        foreach (var (member, score) in items)
        {
            result.Add(RespValue.BulkString(member));
            if (withScores)
                result.Add(RespValue.BulkString(score.ToString("G17", System.Globalization.CultureInfo.InvariantCulture)));
        }

        await context.Client.WriteResponseAsync(RespValue.Array(result.ToArray()), cancellationToken);
    }
}

/// <summary>
/// ZINCRBY command.
/// </summary>
public class ZIncrByCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var increment = context.GetArgAsDouble(1);
        var member = context.GetArg(2);

        var zset = context.Database.GetOrCreate(key, () => new RedisSortedSet());
        var newScore = zset.IncrBy(member, increment);

        return context.Client.WriteBulkStringAsync(
            newScore.ToString("G17", System.Globalization.CultureInfo.InvariantCulture),
            cancellationToken);
    }
}

/// <summary>
/// ZCARD command.
/// </summary>
public class ZCardCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(1);
        var key = context.GetArg(0);
        var zset = context.Database.Get<RedisSortedSet>(key);
        return context.Client.WriteIntegerAsync(zset?.Count ?? 0, cancellationToken);
    }
}

/// <summary>
/// ZCOUNT command.
/// </summary>
public class ZCountCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var min = ParseScoreBound(context.GetArg(1), double.NegativeInfinity);
        var max = ParseScoreBound(context.GetArg(2), double.PositiveInfinity);

        var zset = context.Database.Get<RedisSortedSet>(key);
        var count = zset?.CountByScore(min, max) ?? 0;

        return context.Client.WriteIntegerAsync(count, cancellationToken);
    }

    private static double ParseScoreBound(string str, double defaultValue)
    {
        if (str == "-inf") return double.NegativeInfinity;
        if (str == "+inf" || str == "inf") return double.PositiveInfinity;
        
        bool exclusive = str.StartsWith('(');
        if (exclusive) str = str[1..];
        
        if (double.TryParse(str, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var value))
        {
            return exclusive ? value + double.Epsilon : value;
        }
        
        return defaultValue;
    }
}

/// <summary>
/// ZRANGEBYSCORE command.
/// </summary>
public class ZRangeByScoreCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        var key = context.GetArg(0);
        var min = ParseScoreBound(context.GetArg(1));
        var max = ParseScoreBound(context.GetArg(2));

        bool withScores = false;
        int? offset = null, count = null;

        for (int i = 3; i < context.ArgCount; i++)
        {
            var opt = context.GetArg(i).ToUpperInvariant();
            if (opt == "WITHSCORES")
                withScores = true;
            else if (opt == "LIMIT" && i + 2 < context.ArgCount)
            {
                offset = (int)context.GetArgAsInt(i + 1);
                count = (int)context.GetArgAsInt(i + 2);
                i += 2;
            }
        }

        var zset = context.Database.Get<RedisSortedSet>(key);
        if (zset == null)
        {
            await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
            return;
        }

        var items = zset.GetRangeByScore(min, max)
            .Skip(offset ?? 0)
            .Take(count ?? int.MaxValue)
            .ToList();

        var result = new List<RespValue>();
        foreach (var (member, score) in items)
        {
            result.Add(RespValue.BulkString(member));
            if (withScores)
                result.Add(RespValue.BulkString(score.ToString("G17", System.Globalization.CultureInfo.InvariantCulture)));
        }

        await context.Client.WriteResponseAsync(RespValue.Array(result.ToArray()), cancellationToken);
    }

    private static double ParseScoreBound(string str)
    {
        if (str == "-inf") return double.NegativeInfinity;
        if (str == "+inf" || str == "inf") return double.PositiveInfinity;

        bool exclusive = str.StartsWith('(');
        if (exclusive) str = str[1..];

        if (double.TryParse(str, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var value))
        {
            return exclusive ? value + double.Epsilon : value;
        }

        throw new NotFloatException();
    }
}
