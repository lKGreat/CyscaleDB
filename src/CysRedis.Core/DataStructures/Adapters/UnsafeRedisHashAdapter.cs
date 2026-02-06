using System.Text;
using CysRedis.Core.Unsafe.DataStructures;

namespace CysRedis.Core.DataStructures.Adapters;

/// <summary>
/// Adapter that wraps UnsafeRedisHash to implement RedisHash interface.
/// </summary>
public sealed class UnsafeRedisHashAdapter : RedisHash, IDisposable
{
    private readonly UnsafeRedisHash _unsafeHash;
    private readonly Dictionary<string, DateTime> _fieldExpires = new(StringComparer.Ordinal);
    private readonly Dictionary<string, byte[]> _keyCache = new(StringComparer.Ordinal);

    /// <summary>
    /// Creates a new adapter wrapping an unsafe hash.
    /// </summary>
    public UnsafeRedisHashAdapter()
    {
        _unsafeHash = new UnsafeRedisHash();
    }

    /// <summary>
    /// Gets the Redis type name.
    /// </summary>
    public override string TypeName => "hash";

    /// <summary>
    /// Number of fields in the hash.
    /// </summary>
    public new int Count => _unsafeHash.Count;

    /// <summary>
    /// Sets a field value.
    /// </summary>
    public void Set(string field, byte[] value)
    {
        var fieldBytes = System.Text.Encoding.UTF8.GetBytes(field);
        _unsafeHash.Set(fieldBytes, value);
        _keyCache[field] = value; // Cache for Keys/Values/Entries
    }

    /// <summary>
    /// Sets a field value only if it doesn't exist.
    /// </summary>
    public new bool SetNx(string field, byte[] value)
    {
        if (Exists(field))
            return false;
        Set(field, value);
        return true;
    }

    /// <summary>
    /// Gets a field value.
    /// </summary>
    public new byte[]? Get(string field)
    {
        // Check if field has expired
        if (IsFieldExpired(field))
        {
            Delete(field);
            return null;
        }

        var fieldBytes = System.Text.Encoding.UTF8.GetBytes(field);
        if (_unsafeHash.Get(fieldBytes, out var value))
        {
            return value.ToArray();
        }
        return null;
    }

    /// <summary>
    /// Deletes a field.
    /// </summary>
    public new bool Delete(string field)
    {
        _fieldExpires.Remove(field);
        _keyCache.Remove(field);
        var fieldBytes = System.Text.Encoding.UTF8.GetBytes(field);
        return _unsafeHash.Delete(fieldBytes);
    }

    /// <summary>
    /// Checks if a field exists.
    /// </summary>
    public new bool Exists(string field)
    {
        if (IsFieldExpired(field))
        {
            Delete(field);
            return false;
        }

        var fieldBytes = System.Text.Encoding.UTF8.GetBytes(field);
        return _unsafeHash.Get(fieldBytes, out _);
    }

    /// <summary>
    /// Gets all field names.
    /// </summary>
    public new IEnumerable<string> Keys => _keyCache.Keys.Where(k => !IsFieldExpired(k));

    /// <summary>
    /// Gets all field values.
    /// </summary>
    public new IEnumerable<byte[]> Values => _keyCache.Where(kvp => !IsFieldExpired(kvp.Key)).Select(kvp => kvp.Value);

    /// <summary>
    /// Gets all field-value pairs.
    /// </summary>
    public new IEnumerable<KeyValuePair<string, byte[]>> Entries => _keyCache.Where(kvp => !IsFieldExpired(kvp.Key));

    /// <summary>
    /// Increments a field value by an integer.
    /// </summary>
    public new long IncrBy(string field, long increment)
    {
        var current = Get(field);
        long newValue;
        if (current != null && long.TryParse(System.Text.Encoding.UTF8.GetString(current), out var oldValue))
        {
            newValue = oldValue + increment;
        }
        else
        {
            newValue = increment;
        }
        Set(field, System.Text.Encoding.UTF8.GetBytes(newValue.ToString()));
        return newValue;
    }

    /// <summary>
    /// Increments a field value by a float.
    /// </summary>
    public new double IncrByFloat(string field, double increment)
    {
        var current = Get(field);
        double newValue;
        if (current != null && double.TryParse(System.Text.Encoding.UTF8.GetString(current), 
            System.Globalization.NumberStyles.Float, 
            System.Globalization.CultureInfo.InvariantCulture, out var oldValue))
        {
            newValue = oldValue + increment;
        }
        else
        {
            newValue = increment;
        }
        Set(field, System.Text.Encoding.UTF8.GetBytes(newValue.ToString("G17", System.Globalization.CultureInfo.InvariantCulture)));
        return newValue;
    }

    /// <summary>
    /// Sets field expiration.
    /// </summary>
    public new bool SetFieldExpire(string field, DateTime expireAt)
    {
        if (!Exists(field))
            return false;
        _fieldExpires[field] = expireAt;
        return true;
    }

    /// <summary>
    /// Gets field expiration.
    /// </summary>
    public new DateTime? GetFieldExpire(string field)
    {
        return _fieldExpires.TryGetValue(field, out var expiry) ? expiry : null;
    }

    /// <summary>
    /// Removes field expiration.
    /// </summary>
    public new bool PersistField(string field)
    {
        return _fieldExpires.Remove(field);
    }

    /// <summary>
    /// Gets field TTL in seconds.
    /// </summary>
    public new long? GetFieldTtl(string field)
    {
        if (!Exists(field))
            return -2;
        if (!_fieldExpires.TryGetValue(field, out var expiry))
            return -1;
        var remaining = (expiry - DateTime.UtcNow).TotalSeconds;
        return remaining > 0 ? (long)remaining : -2;
    }

    /// <summary>
    /// Checks if a field is expired.
    /// </summary>
    private bool IsFieldExpired(string field)
    {
        if (_fieldExpires.TryGetValue(field, out var expiry))
            return DateTime.UtcNow >= expiry;
        return false;
    }

    /// <summary>
    /// Disposes the adapter and underlying unsafe hash.
    /// </summary>
    public void Dispose()
    {
        _unsafeHash?.Dispose();
    }
}
