using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using CysRedis.Core.Unsafe.Common;
using CysRedis.Core.Unsafe.DataStructures;

namespace CysRedis.Core.Unsafe.DataStructures;

/// <summary>
/// High-performance Redis stream using listpack for entries grouped by timestamp.
/// Simplified implementation focusing on performance.
/// </summary>
public unsafe sealed class UnsafeRedisStream : IDisposable
{
    private UnsafeListpack _entries;
    private int _count;
    private long _lastTimestamp;
    private long _lastSequence;
    
    /// <summary>
    /// Number of entries in the stream.
    /// </summary>
    public int Count => _count;
    
    /// <summary>
    /// Creates a new stream.
    /// </summary>
    public UnsafeRedisStream()
    {
        _entries = new UnsafeListpack(1024);
        _count = 0;
        _lastTimestamp = 0;
        _lastSequence = 0;
    }
    
    /// <summary>
    /// Adds an entry to the stream.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public StreamEntryId Add(long timestamp, long sequence, ReadOnlySpan<byte> field, ReadOnlySpan<byte> value)
    {
        // Generate ID if needed
        if (timestamp == 0)
        {
            timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        }
        
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
            timestamp = _lastTimestamp;
            sequence = _lastSequence + 1;
        }
        
        // Append to listpack: timestamp, sequence, field, value
        _entries.AppendInteger(timestamp);
        _entries.AppendInteger(sequence);
        _entries.Append(field);
        _entries.Append(value);
        
        _count++;
        _lastTimestamp = timestamp;
        _lastSequence = sequence;
        
        return new StreamEntryId(timestamp, sequence);
    }
    
    /// <summary>
    /// Reads entries from the stream.
    /// </summary>
    public IEnumerable<StreamEntry> Read(long? startTimestamp, long? startSequence, int count)
    {
        int readCount = 0;
        int index = 0;
        
        foreach (var entry in _entries.GetAll())
        {
            if (readCount >= count)
                yield break;
            
            if (index % 4 == 0) // Timestamp
            {
                if (!entry.IsInteger)
                {
                    index++;
                    continue;
                }
                
                long timestamp = entry.IntValue;
                
                // Check if we should start reading
                if (startTimestamp.HasValue && timestamp < startTimestamp.Value)
                {
                    index++;
                    continue;
                }
                
                // Get sequence
                index++;
                var seqEntry = _entries.GetAt(index);
                if (!seqEntry.IsInteger)
                {
                    index++;
                    continue;
                }
                
                long sequence = seqEntry.IntValue;
                
                if (startSequence.HasValue && timestamp == startTimestamp.Value && sequence <= startSequence.Value)
                {
                    index++;
                    continue;
                }
                
                // Get field and value
                index++;
                var fieldEntry = _entries.GetAt(index);
                index++;
                var valueEntry = _entries.GetAt(index);
                
                var fieldBytes = fieldEntry.GetBytes().ToArray();
                var valueBytes = valueEntry.GetBytes().ToArray();
                yield return new StreamEntry
                {
                    Id = new StreamEntryId(timestamp, sequence),
                    Field = fieldBytes,
                    Value = valueBytes
                };
                readCount++;
            }
            
            index++;
        }
    }
    
    public void Dispose()
    {
        _entries.Dispose();
        _count = 0;
    }
}

/// <summary>
/// Stream entry ID.
/// </summary>
public readonly struct StreamEntryId
{
    public long Timestamp { get; }
    public long Sequence { get; }
    
    public StreamEntryId(long timestamp, long sequence)
    {
        Timestamp = timestamp;
        Sequence = sequence;
    }
    
    public override string ToString() => $"{Timestamp}-{Sequence}";
}

/// <summary>
/// Stream entry.
/// </summary>
public struct StreamEntry
{
    public StreamEntryId Id { get; set; }
    public byte[] Field { get; set; }
    public byte[] Value { get; set; }
}
