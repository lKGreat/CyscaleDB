using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

#region HyperLogLog Commands

/// <summary>
/// PFADD command - add to HyperLogLog.
/// </summary>
public class PfAddCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        
        var hll = context.Database.GetOrCreate(key, () => new RedisHyperLogLog());
        var elements = new string[context.ArgCount - 1];
        for (int i = 1; i < context.ArgCount; i++)
            elements[i - 1] = context.GetArg(i);

        var modified = hll.Add(elements);
        return context.Client.WriteIntegerAsync(modified ? 1 : 0, cancellationToken);
    }
}

/// <summary>
/// PFCOUNT command - count unique elements.
/// </summary>
public class PfCountCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);

        if (context.ArgCount == 1)
        {
            var hll = context.Database.Get<RedisHyperLogLog>(context.GetArg(0));
            return context.Client.WriteIntegerAsync(hll?.Count() ?? 0, cancellationToken);
        }

        // Multiple keys - merge and count
        var merged = new RedisHyperLogLog();
        for (int i = 0; i < context.ArgCount; i++)
        {
            var hll = context.Database.Get<RedisHyperLogLog>(context.GetArg(i));
            if (hll != null)
                merged.Merge(hll);
        }

        return context.Client.WriteIntegerAsync(merged.Count(), cancellationToken);
    }
}

/// <summary>
/// PFMERGE command - merge HyperLogLogs.
/// </summary>
public class PfMergeCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var destKey = context.GetArg(0);

        var dest = context.Database.GetOrCreate(destKey, () => new RedisHyperLogLog());

        for (int i = 1; i < context.ArgCount; i++)
        {
            var hll = context.Database.Get<RedisHyperLogLog>(context.GetArg(i));
            if (hll != null)
                dest.Merge(hll);
        }

        return context.Client.WriteOkAsync(cancellationToken);
    }
}

#endregion

#region Geo Commands

/// <summary>
/// GEOADD command - add geo points.
/// </summary>
public class GeoAddCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount < 4 || (context.ArgCount - 1) % 3 != 0)
            throw new WrongArityException("GEOADD");

        var key = context.GetArg(0);
        var geo = context.Database.GetOrCreate(key, () => new RedisGeo());

        int added = 0;
        for (int i = 1; i < context.ArgCount; i += 3)
        {
            var longitude = context.GetArgAsDouble(i);
            var latitude = context.GetArgAsDouble(i + 1);
            var member = context.GetArg(i + 2);
            added += geo.Add(member, longitude, latitude);
        }

        return context.Client.WriteIntegerAsync(added, cancellationToken);
    }
}

/// <summary>
/// GEOPOS command - get positions.
/// </summary>
public class GeoPosCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var geo = context.Database.Get<RedisGeo>(key);

        var results = new RespValue[context.ArgCount - 1];
        for (int i = 1; i < context.ArgCount; i++)
        {
            var pos = geo?.GetPosition(context.GetArg(i));
            if (pos == null)
            {
                results[i - 1] = RespValue.Null;
            }
            else
            {
                results[i - 1] = RespValue.Array(
                    RespValue.BulkString(pos.Value.Longitude.ToString("G17", System.Globalization.CultureInfo.InvariantCulture)),
                    RespValue.BulkString(pos.Value.Latitude.ToString("G17", System.Globalization.CultureInfo.InvariantCulture))
                );
            }
        }

        await context.Client.WriteResponseAsync(RespValue.Array(results), cancellationToken);
    }
}

/// <summary>
/// GEODIST command - get distance.
/// </summary>
public class GeoDistCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        var key = context.GetArg(0);
        var member1 = context.GetArg(1);
        var member2 = context.GetArg(2);
        var unit = context.ArgCount > 3 ? context.GetArg(3) : "m";

        var geo = context.Database.Get<RedisGeo>(key);
        var distance = geo?.GetDistance(member1, member2, unit);

        if (distance == null)
            return context.Client.WriteNullAsync(cancellationToken);

        return context.Client.WriteBulkStringAsync(
            distance.Value.ToString("G17", System.Globalization.CultureInfo.InvariantCulture),
            cancellationToken);
    }
}

/// <summary>
/// GEOSEARCH command - search within radius.
/// </summary>
public class GeoSearchCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(4);
        var key = context.GetArg(0);
        var geo = context.Database.Get<RedisGeo>(key);

        if (geo == null)
        {
            await context.Client.WriteResponseAsync(RespValue.EmptyArray, cancellationToken);
            return;
        }

        double? longitude = null, latitude = null;
        double radius = 0;
        string unit = "m";
        int count = int.MaxValue;
        bool withCoord = false, withDist = false;

        // Parse options
        for (int i = 1; i < context.ArgCount; i++)
        {
            var opt = context.GetArg(i).ToUpperInvariant();
            switch (opt)
            {
                case "FROMMEMBER":
                    i++;
                    var pos = geo.GetPosition(context.GetArg(i));
                    if (pos == null)
                        throw new RedisException("member not found");
                    longitude = pos.Value.Longitude;
                    latitude = pos.Value.Latitude;
                    break;
                case "FROMLONLAT":
                    longitude = context.GetArgAsDouble(++i);
                    latitude = context.GetArgAsDouble(++i);
                    break;
                case "BYRADIUS":
                    radius = context.GetArgAsDouble(++i);
                    unit = context.GetArg(++i);
                    break;
                case "COUNT":
                    count = (int)context.GetArgAsInt(++i);
                    break;
                case "WITHCOORD":
                    withCoord = true;
                    break;
                case "WITHDIST":
                    withDist = true;
                    break;
            }
        }

        if (longitude == null || latitude == null)
            throw new SyntaxErrorException();

        var results = geo.SearchRadius(longitude.Value, latitude.Value, radius, unit, count).ToList();
        var values = new List<RespValue>();

        foreach (var (member, distance) in results)
        {
            if (withCoord || withDist)
            {
                var items = new List<RespValue> { RespValue.BulkString(member) };
                if (withDist && distance.HasValue)
                    items.Add(RespValue.BulkString(distance.Value.ToString("G17", System.Globalization.CultureInfo.InvariantCulture)));
                if (withCoord)
                {
                    var pos = geo.GetPosition(member);
                    if (pos.HasValue)
                    {
                        items.Add(RespValue.Array(
                            RespValue.BulkString(pos.Value.Longitude.ToString("G17", System.Globalization.CultureInfo.InvariantCulture)),
                            RespValue.BulkString(pos.Value.Latitude.ToString("G17", System.Globalization.CultureInfo.InvariantCulture))
                        ));
                    }
                }
                values.Add(RespValue.Array(items.ToArray()));
            }
            else
            {
                values.Add(RespValue.BulkString(member));
            }
        }

        await context.Client.WriteResponseAsync(RespValue.Array(values.ToArray()), cancellationToken);
    }
}

/// <summary>
/// GEOHASH command - get geohash strings.
/// </summary>
public class GeoHashCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var geo = context.Database.Get<RedisGeo>(key);

        var results = new RespValue[context.ArgCount - 1];
        for (int i = 1; i < context.ArgCount; i++)
        {
            var hash = geo?.GetGeohash(context.GetArg(i));
            results[i - 1] = hash != null ? RespValue.BulkString(hash) : RespValue.Null;
        }

        await context.Client.WriteResponseAsync(RespValue.Array(results), cancellationToken);
    }
}

#endregion

#region Bitmap Commands

/// <summary>
/// SETBIT command - set bit value.
/// </summary>
public class SetBitCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(3);
        var key = context.GetArg(0);
        var offset = context.GetArgAsInt(1);
        var value = (int)context.GetArgAsInt(2);

        if (value != 0 && value != 1)
            throw new InvalidArgumentException("bit is not an integer or out of range");
        if (offset < 0)
            throw new InvalidArgumentException("bit offset is not an integer or out of range");

        var str = context.Database.GetOrCreate(key, () => new RedisString(Array.Empty<byte>()));
        var oldValue = GetBit(str.Value, offset);
        SetBit(ref str, offset, value == 1);
        context.Database.Set(key, str);

        return context.Client.WriteIntegerAsync(oldValue ? 1 : 0, cancellationToken);
    }

    private static bool GetBit(byte[] data, long offset)
    {
        var byteIndex = (int)(offset / 8);
        var bitIndex = (int)(7 - offset % 8);
        if (byteIndex >= data.Length) return false;
        return (data[byteIndex] & (1 << bitIndex)) != 0;
    }

    private static void SetBit(ref RedisString str, long offset, bool value)
    {
        var byteIndex = (int)(offset / 8);
        var bitIndex = (int)(7 - offset % 8);

        if (byteIndex >= str.Value.Length)
        {
            var newData = new byte[byteIndex + 1];
            Buffer.BlockCopy(str.Value, 0, newData, 0, str.Value.Length);
            str = new RedisString(newData);
        }

        if (value)
            str.Value[byteIndex] |= (byte)(1 << bitIndex);
        else
            str.Value[byteIndex] &= (byte)~(1 << bitIndex);
    }
}

/// <summary>
/// GETBIT command - get bit value.
/// </summary>
public class GetBitCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureArgCount(2);
        var key = context.GetArg(0);
        var offset = context.GetArgAsInt(1);

        if (offset < 0)
            throw new InvalidArgumentException("bit offset is not an integer or out of range");

        var str = context.Database.Get<RedisString>(key);
        if (str == null)
            return context.Client.WriteIntegerAsync(0, cancellationToken);

        var byteIndex = (int)(offset / 8);
        var bitIndex = (int)(7 - offset % 8);
        
        if (byteIndex >= str.Value.Length)
            return context.Client.WriteIntegerAsync(0, cancellationToken);

        var bit = (str.Value[byteIndex] & (1 << bitIndex)) != 0;
        return context.Client.WriteIntegerAsync(bit ? 1 : 0, cancellationToken);
    }
}

/// <summary>
/// BITCOUNT command - count set bits.
/// </summary>
public class BitCountCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var key = context.GetArg(0);
        var str = context.Database.Get<RedisString>(key);

        if (str == null)
            return context.Client.WriteIntegerAsync(0, cancellationToken);

        int start = 0, end = str.Value.Length - 1;

        if (context.ArgCount >= 3)
        {
            start = (int)context.GetArgAsInt(1);
            end = (int)context.GetArgAsInt(2);

            // Handle negative indices
            if (start < 0) start = str.Value.Length + start;
            if (end < 0) end = str.Value.Length + end;
            start = Math.Max(0, Math.Min(start, str.Value.Length - 1));
            end = Math.Max(0, Math.Min(end, str.Value.Length - 1));
        }

        if (start > end)
            return context.Client.WriteIntegerAsync(0, cancellationToken);

        long count = 0;
        for (int i = start; i <= end; i++)
        {
            count += BitCount(str.Value[i]);
        }

        return context.Client.WriteIntegerAsync(count, cancellationToken);
    }

    private static int BitCount(byte b)
    {
        int count = 0;
        while (b != 0)
        {
            count += b & 1;
            b >>= 1;
        }
        return count;
    }
}

/// <summary>
/// BITOP command - bitwise operations.
/// </summary>
public class BitOpCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(3);
        var operation = context.GetArg(0).ToUpperInvariant();
        var destKey = context.GetArg(1);

        var sources = new List<byte[]>();
        int maxLen = 0;

        for (int i = 2; i < context.ArgCount; i++)
        {
            var str = context.Database.Get<RedisString>(context.GetArg(i));
            var data = str?.Value ?? Array.Empty<byte>();
            sources.Add(data);
            maxLen = Math.Max(maxLen, data.Length);
        }

        var result = new byte[maxLen];

        for (int i = 0; i < maxLen; i++)
        {
            byte value = operation == "NOT" ? (byte)~GetByte(sources[0], i) : GetByte(sources[0], i);

            for (int j = 1; j < sources.Count; j++)
            {
                var b = GetByte(sources[j], i);
                value = operation switch
                {
                    "AND" => (byte)(value & b),
                    "OR" => (byte)(value | b),
                    "XOR" => (byte)(value ^ b),
                    _ => value
                };
            }

            result[i] = value;
        }

        context.Database.Set(destKey, new RedisString(result));
        return context.Client.WriteIntegerAsync(result.Length, cancellationToken);
    }

    private static byte GetByte(byte[] data, int index) => index < data.Length ? data[index] : (byte)0;
}

#endregion
