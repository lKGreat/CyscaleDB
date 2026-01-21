using System.Collections.Concurrent;
using CysRedis.Core.Common;

namespace CysRedis.Core.DataStructures;

/// <summary>
/// Redis data store - manages multiple databases.
/// </summary>
public class RedisStore : IDisposable
{
    private readonly RedisDatabase[] _databases;
    private bool _disposed;

    /// <summary>
    /// Number of databases.
    /// </summary>
    public int DatabaseCount => _databases.Length;

    /// <summary>
    /// Creates a new Redis store.
    /// </summary>
    public RedisStore(int databaseCount = Constants.DefaultDatabaseCount)
    {
        _databases = new RedisDatabase[databaseCount];
        for (int i = 0; i < databaseCount; i++)
        {
            _databases[i] = new RedisDatabase(i);
        }
    }

    /// <summary>
    /// Gets a database by index.
    /// </summary>
    public RedisDatabase GetDatabase(int index)
    {
        if (index < 0 || index >= _databases.Length)
            throw new InvalidArgumentException($"ERR DB index is out of range");
        return _databases[index];
    }

    /// <summary>
    /// Gets all databases.
    /// </summary>
    public IEnumerable<RedisDatabase> GetAllDatabases() => _databases;

    /// <summary>
    /// Flushes all databases.
    /// </summary>
    public void FlushAll()
    {
        foreach (var db in _databases)
        {
            db.Flush();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var db in _databases)
        {
            db.Dispose();
        }

        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// A single Redis database.
/// </summary>
public class RedisDatabase : IDisposable
{
    private readonly ConcurrentDictionary<string, RedisObject> _data;
    private readonly ConcurrentDictionary<string, DateTime> _expires;
    private bool _disposed;

    /// <summary>
    /// Database index.
    /// </summary>
    public int Index { get; }

    /// <summary>
    /// Number of keys in the database.
    /// </summary>
    public int KeyCount => _data.Count;

    /// <summary>
    /// Creates a new Redis database.
    /// </summary>
    public RedisDatabase(int index)
    {
        Index = index;
        _data = new ConcurrentDictionary<string, RedisObject>(StringComparer.Ordinal);
        _expires = new ConcurrentDictionary<string, DateTime>(StringComparer.Ordinal);
    }

    /// <summary>
    /// Gets a value by key, checking expiration.
    /// </summary>
    public RedisObject? Get(string key)
    {
        if (IsExpired(key))
        {
            Delete(key);
            return null;
        }

        _data.TryGetValue(key, out var value);
        return value;
    }

    /// <summary>
    /// Gets a value with type check.
    /// </summary>
    public T? Get<T>(string key) where T : RedisObject
    {
        var obj = Get(key);
        if (obj == null) return null;
        
        if (obj is not T typed)
            throw new WrongTypeException();
        
        return typed;
    }

    /// <summary>
    /// Sets a value.
    /// </summary>
    public void Set(string key, RedisObject value)
    {
        _data[key] = value;
    }

    /// <summary>
    /// Sets a value only if key doesn't exist.
    /// </summary>
    public bool SetNx(string key, RedisObject value)
    {
        if (IsExpired(key))
            Delete(key);
            
        return _data.TryAdd(key, value);
    }

    /// <summary>
    /// Sets a value only if key exists.
    /// </summary>
    public bool SetXx(string key, RedisObject value)
    {
        if (IsExpired(key))
        {
            Delete(key);
            return false;
        }

        if (!_data.ContainsKey(key))
            return false;

        _data[key] = value;
        return true;
    }

    /// <summary>
    /// Deletes a key.
    /// </summary>
    public bool Delete(string key)
    {
        _expires.TryRemove(key, out _);
        return _data.TryRemove(key, out _);
    }

    /// <summary>
    /// Checks if a key exists.
    /// </summary>
    public bool Exists(string key)
    {
        if (IsExpired(key))
        {
            Delete(key);
            return false;
        }
        return _data.ContainsKey(key);
    }

    /// <summary>
    /// Gets the type of a key.
    /// </summary>
    public string? GetType(string key)
    {
        var obj = Get(key);
        return obj?.TypeName;
    }

    /// <summary>
    /// Renames a key.
    /// </summary>
    public bool Rename(string oldKey, string newKey)
    {
        if (!_data.TryRemove(oldKey, out var value))
            return false;

        _data[newKey] = value;

        if (_expires.TryRemove(oldKey, out var expiry))
            _expires[newKey] = expiry;

        return true;
    }

    /// <summary>
    /// Gets all keys matching a pattern.
    /// </summary>
    public IEnumerable<string> Keys(string pattern = "*")
    {
        // Remove expired keys first
        CleanupExpired();

        if (pattern == "*")
            return _data.Keys;

        return _data.Keys.Where(k => MatchPattern(k, pattern));
    }

    /// <summary>
    /// Gets a random key.
    /// </summary>
    public string? RandomKey()
    {
        CleanupExpired();
        var keys = _data.Keys.ToArray();
        if (keys.Length == 0) return null;
        return keys[Random.Shared.Next(keys.Length)];
    }

    /// <summary>
    /// Sets key expiration.
    /// </summary>
    public bool SetExpire(string key, DateTime expireAt)
    {
        if (!_data.ContainsKey(key))
            return false;
        _expires[key] = expireAt;
        return true;
    }

    /// <summary>
    /// Gets key expiration.
    /// </summary>
    public DateTime? GetExpire(string key)
    {
        if (_expires.TryGetValue(key, out var expiry))
            return expiry;
        return null;
    }

    /// <summary>
    /// Removes key expiration.
    /// </summary>
    public bool Persist(string key)
    {
        return _expires.TryRemove(key, out _);
    }

    /// <summary>
    /// Gets TTL in seconds.
    /// </summary>
    public long? GetTtl(string key)
    {
        if (!_data.ContainsKey(key))
            return -2; // Key doesn't exist

        if (!_expires.TryGetValue(key, out var expiry))
            return -1; // No expiration

        var remaining = (expiry - DateTime.UtcNow).TotalSeconds;
        return remaining > 0 ? (long)remaining : -2;
    }

    /// <summary>
    /// Gets TTL in milliseconds.
    /// </summary>
    public long? GetPttl(string key)
    {
        if (!_data.ContainsKey(key))
            return -2;

        if (!_expires.TryGetValue(key, out var expiry))
            return -1;

        var remaining = (expiry - DateTime.UtcNow).TotalMilliseconds;
        return remaining > 0 ? (long)remaining : -2;
    }

    /// <summary>
    /// Checks if a key is expired.
    /// </summary>
    public bool IsExpired(string key)
    {
        if (_expires.TryGetValue(key, out var expiry))
            return DateTime.UtcNow >= expiry;
        return false;
    }

    /// <summary>
    /// Cleans up expired keys.
    /// </summary>
    public int CleanupExpired()
    {
        int count = 0;
        var now = DateTime.UtcNow;

        foreach (var kvp in _expires)
        {
            if (now >= kvp.Value)
            {
                if (Delete(kvp.Key))
                    count++;
            }
        }

        return count;
    }

    /// <summary>
    /// Flushes the database.
    /// </summary>
    public void Flush()
    {
        _data.Clear();
        _expires.Clear();
    }

    /// <summary>
    /// Gets or creates a value.
    /// </summary>
    public T GetOrCreate<T>(string key, Func<T> factory) where T : RedisObject
    {
        var existing = Get(key);
        if (existing != null)
        {
            if (existing is not T typed)
                throw new WrongTypeException();
            return typed;
        }

        var newValue = factory();
        _data[key] = newValue;
        return newValue;
    }

    private static bool MatchPattern(string key, string pattern)
    {
        // Simple glob pattern matching
        int ki = 0, pi = 0;
        int starIdx = -1, matchIdx = 0;

        while (ki < key.Length)
        {
            if (pi < pattern.Length && (pattern[pi] == '?' || pattern[pi] == key[ki]))
            {
                ki++;
                pi++;
            }
            else if (pi < pattern.Length && pattern[pi] == '*')
            {
                starIdx = pi;
                matchIdx = ki;
                pi++;
            }
            else if (starIdx != -1)
            {
                pi = starIdx + 1;
                matchIdx++;
                ki = matchIdx;
            }
            else
            {
                return false;
            }
        }

        while (pi < pattern.Length && pattern[pi] == '*')
            pi++;

        return pi == pattern.Length;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Flush();
        GC.SuppressFinalize(this);
    }
}
