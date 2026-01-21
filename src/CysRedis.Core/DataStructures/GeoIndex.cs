namespace CysRedis.Core.DataStructures;

/// <summary>
/// Geospatial index using sorted set with geohash encoding.
/// </summary>
public class RedisGeo : RedisObject
{
    public override string TypeName => "zset"; // Geo uses sorted set internally

    private readonly RedisSortedSet _data = new();

    // Geohash constants
    private const double MaxLatitude = 85.05112878;
    private const double MinLatitude = -85.05112878;
    private const double MaxLongitude = 180.0;
    private const double MinLongitude = -180.0;

    public int Count => _data.Count;

    /// <summary>
    /// Adds a member with coordinates.
    /// </summary>
    public int Add(string member, double longitude, double latitude)
    {
        ValidateCoordinates(longitude, latitude);
        var score = EncodeGeohash(longitude, latitude);
        return _data.Add(member, score) ? 1 : 0;
    }

    /// <summary>
    /// Gets position of a member.
    /// </summary>
    public (double Longitude, double Latitude)? GetPosition(string member)
    {
        var score = _data.GetScore(member);
        if (score == null) return null;
        return DecodeGeohash(score.Value);
    }

    /// <summary>
    /// Gets distance between two members.
    /// </summary>
    public double? GetDistance(string member1, string member2, string unit = "m")
    {
        var pos1 = GetPosition(member1);
        var pos2 = GetPosition(member2);
        
        if (pos1 == null || pos2 == null) return null;

        var distance = HaversineDistance(
            pos1.Value.Latitude, pos1.Value.Longitude,
            pos2.Value.Latitude, pos2.Value.Longitude);

        return ConvertDistance(distance, unit);
    }

    /// <summary>
    /// Searches members within radius.
    /// </summary>
    public IEnumerable<(string Member, double? Distance)> SearchRadius(
        double longitude, double latitude, double radius, string unit, int count = int.MaxValue)
    {
        ValidateCoordinates(longitude, latitude);
        var radiusMeters = radius * GetUnitMultiplier(unit);

        var results = new List<(string Member, double Distance)>();

        foreach (var (member, score) in _data.GetRange(0, -1))
        {
            var (lon, lat) = DecodeGeohash(score);
            var distance = HaversineDistance(latitude, longitude, lat, lon);
            
            if (distance <= radiusMeters)
            {
                results.Add((member, ConvertDistance(distance, unit)));
            }
        }

        return results
            .OrderBy(r => r.Distance)
            .Take(count)
            .Select(r => ((string Member, double? Distance))(r.Member, r.Distance));
    }

    /// <summary>
    /// Gets geohash string for a member.
    /// </summary>
    public string? GetGeohash(string member)
    {
        var score = _data.GetScore(member);
        if (score == null) return null;
        return GeohashToString((long)score.Value);
    }

    /// <summary>
    /// Removes members.
    /// </summary>
    public int Remove(params string[] members)
    {
        int removed = 0;
        foreach (var member in members)
        {
            if (_data.Remove(member))
                removed++;
        }
        return removed;
    }

    private static void ValidateCoordinates(double longitude, double latitude)
    {
        if (longitude < MinLongitude || longitude > MaxLongitude)
            throw new Common.RedisException($"ERR invalid longitude {longitude}");
        if (latitude < MinLatitude || latitude > MaxLatitude)
            throw new Common.RedisException($"ERR invalid latitude {latitude}");
    }

    private static double EncodeGeohash(double longitude, double latitude)
    {
        // Simplified geohash encoding to 52-bit integer
        long latBits = EncodeRange(latitude, MinLatitude, MaxLatitude, 26);
        long lonBits = EncodeRange(longitude, MinLongitude, MaxLongitude, 26);

        long hash = 0;
        for (int i = 0; i < 26; i++)
        {
            hash |= ((lonBits >> (25 - i)) & 1) << (51 - 2 * i);
            hash |= ((latBits >> (25 - i)) & 1) << (50 - 2 * i);
        }

        return hash;
    }

    private static (double Longitude, double Latitude) DecodeGeohash(double score)
    {
        long hash = (long)score;
        long lonBits = 0, latBits = 0;

        for (int i = 0; i < 26; i++)
        {
            lonBits |= ((hash >> (51 - 2 * i)) & 1) << (25 - i);
            latBits |= ((hash >> (50 - 2 * i)) & 1) << (25 - i);
        }

        var longitude = DecodeRange(lonBits, MinLongitude, MaxLongitude, 26);
        var latitude = DecodeRange(latBits, MinLatitude, MaxLatitude, 26);

        return (longitude, latitude);
    }

    private static long EncodeRange(double value, double min, double max, int bits)
    {
        double range = max - min;
        double normalized = (value - min) / range;
        return (long)(normalized * ((1L << bits) - 1));
    }

    private static double DecodeRange(long value, double min, double max, int bits)
    {
        double range = max - min;
        double normalized = (double)value / ((1L << bits) - 1);
        return min + normalized * range;
    }

    private static double HaversineDistance(double lat1, double lon1, double lat2, double lon2)
    {
        const double R = 6372797.560856; // Earth radius in meters

        var dLat = ToRadians(lat2 - lat1);
        var dLon = ToRadians(lon2 - lon1);
        
        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                Math.Cos(ToRadians(lat1)) * Math.Cos(ToRadians(lat2)) *
                Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
        
        var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
        
        return R * c;
    }

    private static double ToRadians(double degrees) => degrees * Math.PI / 180;

    private static double GetUnitMultiplier(string unit)
    {
        return unit.ToLowerInvariant() switch
        {
            "km" => 1000,
            "mi" => 1609.34,
            "ft" => 0.3048,
            _ => 1 // meters
        };
    }

    private static double ConvertDistance(double meters, string unit)
    {
        return meters / GetUnitMultiplier(unit);
    }

    private static string GeohashToString(long hash)
    {
        const string Base32 = "0123456789bcdefghjkmnpqrstuvwxyz";
        var chars = new char[11];
        
        for (int i = 10; i >= 0; i--)
        {
            chars[i] = Base32[(int)(hash & 0x1F)];
            hash >>= 5;
        }
        
        return new string(chars);
    }
}
