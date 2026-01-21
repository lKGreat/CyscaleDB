using CysRedis.Core.DataStructures;

namespace CysRedis.Core.Protocol;

/// <summary>
/// Shared objects pool similar to Redis shared objects.
/// Preallocates commonly used objects to reduce allocations and GC pressure.
/// </summary>
public static class SharedObjects
{
    // Preallocated integer strings from 0 to 9999
    private static readonly RedisString[] _sharedIntegers;
    
    // Common string values
    private static readonly RedisString _emptyString;
    private static readonly RedisString _okString;
    private static readonly RedisString _pongString;
    private static readonly RedisString _queuedString;
    
    private const int MinSharedInteger = 0;
    private const int MaxSharedInteger = 9999;
    private const int SharedIntegerCount = MaxSharedInteger - MinSharedInteger + 1;

    static SharedObjects()
    {
        // Preallocate integer strings
        _sharedIntegers = new RedisString[SharedIntegerCount];
        for (int i = 0; i < SharedIntegerCount; i++)
        {
            var value = (i + MinSharedInteger).ToString();
            _sharedIntegers[i] = new RedisString(value);
        }

        // Preallocate common strings
        _emptyString = new RedisString("");
        _okString = new RedisString("OK");
        _pongString = new RedisString("PONG");
        _queuedString = new RedisString("QUEUED");
    }

    /// <summary>
    /// Gets a shared RedisString for an integer value (0-9999).
    /// Returns null if value is outside shared range.
    /// </summary>
    public static RedisString? GetSharedInteger(long value)
    {
        if (value >= MinSharedInteger && value <= MaxSharedInteger)
        {
            return _sharedIntegers[value - MinSharedInteger];
        }
        return null;
    }

    /// <summary>
    /// Gets a shared empty string.
    /// </summary>
    public static RedisString EmptyString => _emptyString;

    /// <summary>
    /// Gets a shared "OK" string.
    /// </summary>
    public static RedisString OkString => _okString;

    /// <summary>
    /// Gets a shared "PONG" string.
    /// </summary>
    public static RedisString PongString => _pongString;

    /// <summary>
    /// Gets a shared "QUEUED" string.
    /// </summary>
    public static RedisString QueuedString => _queuedString;

    /// <summary>
    /// Creates a RedisString, using shared object if applicable.
    /// </summary>
    public static RedisString CreateString(string value)
    {
        // Try to parse as integer
        if (long.TryParse(value, out var intValue))
        {
            var shared = GetSharedInteger(intValue);
            if (shared != null)
                return shared;
        }

        // Check common strings
        if (value.Length == 0) return _emptyString;
        if (value == "OK") return _okString;
        if (value == "PONG") return _pongString;
        if (value == "QUEUED") return _queuedString;

        // Create new string
        return new RedisString(value);
    }

    /// <summary>
    /// Creates a RedisString from an integer, using shared object if applicable.
    /// </summary>
    public static RedisString CreateIntegerString(long value)
    {
        var shared = GetSharedInteger(value);
        return shared ?? new RedisString(value.ToString());
    }
}
