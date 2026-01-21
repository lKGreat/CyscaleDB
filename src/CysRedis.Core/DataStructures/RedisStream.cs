using System.Collections.Concurrent;

namespace CysRedis.Core.DataStructures;

/// <summary>
/// Redis Stream data type implementation.
/// </summary>
public class RedisStream : RedisObject
{
    public override string TypeName => "stream";

    private readonly List<StreamEntry> _entries = new();
    private readonly ConcurrentDictionary<string, ConsumerGroup> _groups = new();
    private long _lastTimestamp;
    private long _lastSequence;

    public int Count => _entries.Count;
    public StreamEntryId? FirstId => _entries.Count > 0 ? _entries[0].Id : null;
    public StreamEntryId? LastId => _entries.Count > 0 ? _entries[^1].Id : null;

    /// <summary>
    /// Adds an entry to the stream.
    /// </summary>
    public StreamEntryId Add(string? id, Dictionary<string, string> fields)
    {
        StreamEntryId entryId;
        
        if (id == null || id == "*")
        {
            entryId = GenerateId();
        }
        else if (id.EndsWith("-*"))
        {
            var timestamp = long.Parse(id[..^2]);
            entryId = GenerateIdWithTimestamp(timestamp);
        }
        else
        {
            entryId = StreamEntryId.Parse(id);
            ValidateId(entryId);
        }

        var entry = new StreamEntry(entryId, fields);
        _entries.Add(entry);
        
        _lastTimestamp = entryId.Timestamp;
        _lastSequence = entryId.Sequence;

        return entryId;
    }

    /// <summary>
    /// Reads entries from the stream.
    /// </summary>
    public IEnumerable<StreamEntry> Read(StreamEntryId? startId, int count, bool exclusive = false)
    {
        int startIndex = 0;
        
        if (startId != null)
        {
            var sid = startId.Value;
            startIndex = _entries.FindIndex(e => 
                exclusive ? e.Id.CompareTo(sid) > 0 : e.Id.CompareTo(sid) >= 0);
            if (startIndex < 0)
                yield break;
        }

        for (int i = startIndex; i < _entries.Count && count > 0; i++, count--)
        {
            yield return _entries[i];
        }
    }

    /// <summary>
    /// Gets entries in a range.
    /// </summary>
    public IEnumerable<StreamEntry> Range(StreamEntryId? start, StreamEntryId? end, int count = int.MaxValue)
    {
        foreach (var entry in _entries)
        {
            if (count <= 0) break;
            
            if (start != null && entry.Id.CompareTo(start.Value) < 0) continue;
            if (end != null && entry.Id.CompareTo(end.Value) > 0) break;
            
            yield return entry;
            count--;
        }
    }

    /// <summary>
    /// Creates a consumer group.
    /// </summary>
    public bool CreateGroup(string name, StreamEntryId? lastDeliveredId)
    {
        var group = new ConsumerGroup(name, lastDeliveredId ?? new StreamEntryId(0, 0));
        return _groups.TryAdd(name, group);
    }

    /// <summary>
    /// Gets a consumer group.
    /// </summary>
    public ConsumerGroup? GetGroup(string name)
    {
        return _groups.TryGetValue(name, out var group) ? group : null;
    }

    /// <summary>
    /// Destroys a consumer group.
    /// </summary>
    public bool DestroyGroup(string name)
    {
        return _groups.TryRemove(name, out _);
    }

    /// <summary>
    /// Gets all consumer groups.
    /// </summary>
    public IEnumerable<ConsumerGroup> GetGroups() => _groups.Values;

    /// <summary>
    /// Gets stream length.
    /// </summary>
    public long Length => _entries.Count;

    /// <summary>
    /// Trims stream to maxlen.
    /// </summary>
    public int Trim(long maxLen, bool approximate = false)
    {
        if (_entries.Count <= maxLen)
            return 0;

        var toRemove = (int)(_entries.Count - maxLen);
        _entries.RemoveRange(0, toRemove);
        return toRemove;
    }

    private StreamEntryId GenerateId()
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return GenerateIdWithTimestamp(timestamp);
    }

    private StreamEntryId GenerateIdWithTimestamp(long timestamp)
    {
        long sequence;
        if (timestamp == _lastTimestamp)
        {
            sequence = _lastSequence + 1;
        }
        else if (timestamp > _lastTimestamp)
        {
            sequence = 0;
        }
        else
        {
            // Timestamp in past, use last timestamp
            timestamp = _lastTimestamp;
            sequence = _lastSequence + 1;
        }
        
        return new StreamEntryId(timestamp, sequence);
    }

    private void ValidateId(StreamEntryId id)
    {
        if (_entries.Count > 0)
        {
            var lastId = _entries[^1].Id;
            if (id.CompareTo(lastId) <= 0)
                throw new Common.RedisException("The ID specified in XADD is equal or smaller than the target stream top item");
        }
    }
}

/// <summary>
/// Stream entry ID.
/// </summary>
public readonly struct StreamEntryId : IComparable<StreamEntryId>
{
    public long Timestamp { get; }
    public long Sequence { get; }

    public StreamEntryId(long timestamp, long sequence)
    {
        Timestamp = timestamp;
        Sequence = sequence;
    }

    public static StreamEntryId Parse(string s)
    {
        var parts = s.Split('-');
        if (parts.Length != 2)
            throw new ArgumentException($"Invalid stream ID: {s}");
        
        return new StreamEntryId(long.Parse(parts[0]), long.Parse(parts[1]));
    }

    public int CompareTo(StreamEntryId other)
    {
        var cmp = Timestamp.CompareTo(other.Timestamp);
        return cmp != 0 ? cmp : Sequence.CompareTo(other.Sequence);
    }

    public override string ToString() => $"{Timestamp}-{Sequence}";

    public static bool TryParse(string s, out StreamEntryId id)
    {
        id = default;
        var parts = s.Split('-');
        if (parts.Length != 2) return false;
        if (!long.TryParse(parts[0], out var ts)) return false;
        if (!long.TryParse(parts[1], out var seq)) return false;
        id = new StreamEntryId(ts, seq);
        return true;
    }
}

/// <summary>
/// Stream entry.
/// </summary>
public class StreamEntry
{
    public StreamEntryId Id { get; }
    public Dictionary<string, string> Fields { get; }

    public StreamEntry(StreamEntryId id, Dictionary<string, string> fields)
    {
        Id = id;
        Fields = fields;
    }
}

/// <summary>
/// Consumer group.
/// </summary>
public class ConsumerGroup
{
    public string Name { get; }
    public StreamEntryId LastDeliveredId { get; set; }
    private readonly ConcurrentDictionary<string, Consumer> _consumers = new();
    private readonly ConcurrentDictionary<string, PendingEntry> _pendingEntries = new();

    public ConsumerGroup(string name, StreamEntryId lastDeliveredId)
    {
        Name = name;
        LastDeliveredId = lastDeliveredId;
    }

    public Consumer GetOrCreateConsumer(string name)
    {
        return _consumers.GetOrAdd(name, n => new Consumer(n));
    }

    public IEnumerable<Consumer> GetConsumers() => _consumers.Values;

    public void AddPending(string entryId, string consumer)
    {
        _pendingEntries[entryId] = new PendingEntry(entryId, consumer, DateTime.UtcNow);
    }

    public bool Acknowledge(string entryId)
    {
        return _pendingEntries.TryRemove(entryId, out _);
    }

    public int PendingCount => _pendingEntries.Count;
}

/// <summary>
/// Stream consumer.
/// </summary>
public class Consumer
{
    public string Name { get; }
    public DateTime LastSeenTime { get; set; }
    public int PendingCount { get; set; }

    public Consumer(string name)
    {
        Name = name;
        LastSeenTime = DateTime.UtcNow;
    }
}

/// <summary>
/// Pending entry in a consumer group.
/// </summary>
public class PendingEntry
{
    public string EntryId { get; }
    public string ConsumerName { get; }
    public DateTime DeliveryTime { get; set; }
    public int DeliveryCount { get; set; }

    public PendingEntry(string entryId, string consumerName, DateTime deliveryTime)
    {
        EntryId = entryId;
        ConsumerName = consumerName;
        DeliveryTime = deliveryTime;
        DeliveryCount = 1;
    }
}
