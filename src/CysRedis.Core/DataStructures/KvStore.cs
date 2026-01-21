using System.Collections.Concurrent;
using CysRedis.Core.Cluster;

namespace CysRedis.Core.DataStructures;

/// <summary>
/// Slot-based key-value store similar to Redis kvstore.
/// Shards keys across 16384 slots to reduce lock contention and enable cluster mode.
/// </summary>
public class KvStore : IDisposable
{
    private readonly ConcurrentDictionary<string, RedisObject>[] _slots;
    private readonly ConcurrentDictionary<string, DateTime>[] _expiresSlots;
    private const int SlotBits = 14; // 2^14 = 16384
    private const int NumSlots = 1 << SlotBits; // 16384 slots
    private bool _disposed;

    /// <summary>
    /// Number of slots in the store.
    /// </summary>
    public int SlotCount => NumSlots;

    /// <summary>
    /// Total number of keys across all slots.
    /// </summary>
    public int TotalKeys
    {
        get
        {
            int total = 0;
            for (int i = 0; i < NumSlots; i++)
            {
                total += _slots[i].Count;
            }
            return total;
        }
    }

    /// <summary>
    /// Creates a new sharded key-value store.
    /// </summary>
    public KvStore()
    {
        _slots = new ConcurrentDictionary<string, RedisObject>[NumSlots];
        _expiresSlots = new ConcurrentDictionary<string, DateTime>[NumSlots];

        for (int i = 0; i < NumSlots; i++)
        {
            _slots[i] = new ConcurrentDictionary<string, RedisObject>(StringComparer.Ordinal);
            _expiresSlots[i] = new ConcurrentDictionary<string, DateTime>(StringComparer.Ordinal);
        }
    }

    /// <summary>
    /// Gets a value by key.
    /// </summary>
    public RedisObject? Get(string key)
    {
        var slot = GetSlotForKey(key);
        return _slots[slot].TryGetValue(key, out var value) ? value : null;
    }

    /// <summary>
    /// Sets a value.
    /// </summary>
    public void Set(string key, RedisObject value)
    {
        var slot = GetSlotForKey(key);
        _slots[slot][key] = value;
    }

    /// <summary>
    /// Sets a value only if key doesn't exist.
    /// </summary>
    public bool SetNx(string key, RedisObject value)
    {
        var slot = GetSlotForKey(key);
        return _slots[slot].TryAdd(key, value);
    }

    /// <summary>
    /// Deletes a key.
    /// </summary>
    public bool Delete(string key)
    {
        var slot = GetSlotForKey(key);
        _expiresSlots[slot].TryRemove(key, out _);
        return _slots[slot].TryRemove(key, out _);
    }

    /// <summary>
    /// Checks if a key exists.
    /// </summary>
    public bool Exists(string key)
    {
        var slot = GetSlotForKey(key);
        return _slots[slot].ContainsKey(key);
    }

    /// <summary>
    /// Sets key expiration.
    /// </summary>
    public bool SetExpire(string key, DateTime expireAt)
    {
        var slot = GetSlotForKey(key);
        if (!_slots[slot].ContainsKey(key))
            return false;
        _expiresSlots[slot][key] = expireAt;
        return true;
    }

    /// <summary>
    /// Gets key expiration.
    /// </summary>
    public DateTime? GetExpire(string key)
    {
        var slot = GetSlotForKey(key);
        return _expiresSlots[slot].TryGetValue(key, out var expiry) ? expiry : null;
    }

    /// <summary>
    /// Removes key expiration.
    /// </summary>
    public bool Persist(string key)
    {
        var slot = GetSlotForKey(key);
        return _expiresSlots[slot].TryRemove(key, out _);
    }

    /// <summary>
    /// Checks if a key is expired.
    /// </summary>
    public bool IsExpired(string key)
    {
        var slot = GetSlotForKey(key);
        if (_expiresSlots[slot].TryGetValue(key, out var expiry))
            return DateTime.UtcNow >= expiry;
        return false;
    }

    /// <summary>
    /// Gets all keys across all slots.
    /// </summary>
    public IEnumerable<string> GetAllKeys()
    {
        for (int i = 0; i < NumSlots; i++)
        {
            foreach (var key in _slots[i].Keys)
            {
                yield return key;
            }
        }
    }

    /// <summary>
    /// Gets all keys in a specific slot.
    /// </summary>
    public IEnumerable<string> GetKeysInSlot(int slot)
    {
        if (slot < 0 || slot >= NumSlots)
            throw new ArgumentOutOfRangeException(nameof(slot));

        return _slots[slot].Keys;
    }

    /// <summary>
    /// Gets the number of keys in a specific slot.
    /// </summary>
    public int GetSlotSize(int slot)
    {
        if (slot < 0 || slot >= NumSlots)
            throw new ArgumentOutOfRangeException(nameof(slot));

        return _slots[slot].Count;
    }

    /// <summary>
    /// Flushes all keys.
    /// </summary>
    public void FlushAll()
    {
        for (int i = 0; i < NumSlots; i++)
        {
            _slots[i].Clear();
            _expiresSlots[i].Clear();
        }
    }

    /// <summary>
    /// Flushes a specific slot.
    /// </summary>
    public void FlushSlot(int slot)
    {
        if (slot < 0 || slot >= NumSlots)
            throw new ArgumentOutOfRangeException(nameof(slot));

        _slots[slot].Clear();
        _expiresSlots[slot].Clear();
    }

    /// <summary>
    /// Gets the slot number for a key using Redis CRC16 algorithm.
    /// </summary>
    private static int GetSlotForKey(string key)
    {
        return ClusterManager.GetSlot(key);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        FlushAll();
        GC.SuppressFinalize(this);
    }
}
