using System.Buffers.Binary;

namespace CysRedis.Core.DataStructures;

/// <summary>
/// Compact integer set implementation similar to Redis intset.
/// Automatically upgrades encoding as needed (int16 -> int32 -> int64).
/// Uses sorted array with binary search for O(log n) lookups.
/// </summary>
public class IntSet
{
    private byte _encoding; // 2=int16, 4=int32, 8=int64
    private int _length;
    private byte[] _contents;
    
    private const byte EncodingInt16 = 2;
    private const byte EncodingInt32 = 4;
    private const byte EncodingInt64 = 8;
    
    /// <summary>
    /// Number of elements in the set.
    /// </summary>
    public int Count => _length;
    
    /// <summary>
    /// Creates a new empty IntSet.
    /// </summary>
    public IntSet()
    {
        _encoding = EncodingInt16; // Start with smallest encoding
        _length = 0;
        _contents = Array.Empty<byte>();
    }
    
    /// <summary>
    /// Adds a value to the set. Returns true if added, false if already exists.
    /// </summary>
    public bool Add(long value)
    {
        var reqEncoding = GetRequiredEncoding(value);
        
        // Upgrade encoding if needed
        if (reqEncoding > _encoding)
        {
            UpgradeAndAdd(value);
            return true;
        }
        
        // Check if value already exists
        var pos = BinarySearch(value);
        if (pos >= 0)
            return false; // Already exists
        
        // Insert at position
        var insertPos = ~pos;
        InsertAt(insertPos, value);
        return true;
    }
    
    /// <summary>
    /// Removes a value from the set. Returns true if removed, false if not found.
    /// </summary>
    public bool Remove(long value)
    {
        if (!CanEncode(value, _encoding))
            return false;
        
        var pos = BinarySearch(value);
        if (pos < 0)
            return false;
        
        RemoveAt(pos);
        return true;
    }
    
    /// <summary>
    /// Checks if a value exists in the set.
    /// </summary>
    public bool Contains(long value)
    {
        if (!CanEncode(value, _encoding))
            return false;
        
        return BinarySearch(value) >= 0;
    }
    
    /// <summary>
    /// Gets the value at a specific index.
    /// </summary>
    public long GetAt(int index)
    {
        if (index < 0 || index >= _length)
            throw new IndexOutOfRangeException();
        
        return GetValue(index);
    }
    
    /// <summary>
    /// Gets all values as an enumerable.
    /// </summary>
    public IEnumerable<long> GetAll()
    {
        for (int i = 0; i < _length; i++)
        {
            yield return GetValue(i);
        }
    }
    
    /// <summary>
    /// Gets the minimum value.
    /// </summary>
    public long? Min => _length > 0 ? GetValue(0) : null;
    
    /// <summary>
    /// Gets the maximum value.
    /// </summary>
    public long? Max => _length > 0 ? GetValue(_length - 1) : null;
    
    private byte GetRequiredEncoding(long value)
    {
        if (value >= short.MinValue && value <= short.MaxValue)
            return EncodingInt16;
        if (value >= int.MinValue && value <= int.MaxValue)
            return EncodingInt32;
        return EncodingInt64;
    }
    
    private bool CanEncode(long value, byte encoding)
    {
        return encoding switch
        {
            EncodingInt16 => value >= short.MinValue && value <= short.MaxValue,
            EncodingInt32 => value >= int.MinValue && value <= int.MaxValue,
            EncodingInt64 => true,
            _ => false
        };
    }
    
    private int BinarySearch(long value)
    {
        int min = 0, max = _length - 1, mid = -1;
        long cur;
        
        while (min <= max)
        {
            mid = (min + max) / 2;
            cur = GetValue(mid);
            
            if (value > cur)
                min = mid + 1;
            else if (value < cur)
                max = mid - 1;
            else
                return mid;
        }
        
        return ~min; // Return bitwise complement of insert position
    }
    
    private long GetValue(int index)
    {
        var offset = index * _encoding;
        var span = _contents.AsSpan(offset, _encoding);
        
        return _encoding switch
        {
            EncodingInt16 => BinaryPrimitives.ReadInt16LittleEndian(span),
            EncodingInt32 => BinaryPrimitives.ReadInt32LittleEndian(span),
            EncodingInt64 => BinaryPrimitives.ReadInt64LittleEndian(span),
            _ => throw new InvalidOperationException("Invalid encoding")
        };
    }
    
    private void SetValue(int index, long value)
    {
        var offset = index * _encoding;
        var span = _contents.AsSpan(offset, _encoding);
        
        switch (_encoding)
        {
            case EncodingInt16:
                BinaryPrimitives.WriteInt16LittleEndian(span, (short)value);
                break;
            case EncodingInt32:
                BinaryPrimitives.WriteInt32LittleEndian(span, (int)value);
                break;
            case EncodingInt64:
                BinaryPrimitives.WriteInt64LittleEndian(span, value);
                break;
        }
    }
    
    private void InsertAt(int pos, long value)
    {
        // Resize array
        var newContents = new byte[(_length + 1) * _encoding];
        
        // Copy elements before insert position
        if (pos > 0)
            Array.Copy(_contents, 0, newContents, 0, pos * _encoding);
        
        // Copy elements after insert position
        if (pos < _length)
            Array.Copy(_contents, pos * _encoding, newContents, (pos + 1) * _encoding, 
                (_length - pos) * _encoding);
        
        _contents = newContents;
        _length++;
        
        // Set the new value
        SetValue(pos, value);
    }
    
    private void RemoveAt(int pos)
    {
        if (_length == 1)
        {
            _contents = Array.Empty<byte>();
            _length = 0;
            return;
        }
        
        var newContents = new byte[(_length - 1) * _encoding];
        
        // Copy elements before remove position
        if (pos > 0)
            Array.Copy(_contents, 0, newContents, 0, pos * _encoding);
        
        // Copy elements after remove position
        if (pos < _length - 1)
            Array.Copy(_contents, (pos + 1) * _encoding, newContents, pos * _encoding, 
                (_length - pos - 1) * _encoding);
        
        _contents = newContents;
        _length--;
    }
    
    private void UpgradeAndAdd(long value)
    {
        var oldEncoding = _encoding;
        var newEncoding = GetRequiredEncoding(value);
        
        // Create new array with upgraded encoding
        var newContents = new byte[(_length + 1) * newEncoding];
        
        // Determine insert position
        int insertPos = value > 0 ? _length : 0;
        
        // Copy existing values to new array
        for (int i = 0; i < _length; i++)
        {
            var oldValue = GetValue(i);
            var targetPos = i >= insertPos ? i + 1 : i;
            var offset = targetPos * newEncoding;
            var span = newContents.AsSpan(offset, newEncoding);
            
            switch (newEncoding)
            {
                case EncodingInt16:
                    BinaryPrimitives.WriteInt16LittleEndian(span, (short)oldValue);
                    break;
                case EncodingInt32:
                    BinaryPrimitives.WriteInt32LittleEndian(span, (int)oldValue);
                    break;
                case EncodingInt64:
                    BinaryPrimitives.WriteInt64LittleEndian(span, oldValue);
                    break;
            }
        }
        
        // Set the new value
        {
            var offset = insertPos * newEncoding;
            var span = newContents.AsSpan(offset, newEncoding);
            
            switch (newEncoding)
            {
                case EncodingInt16:
                    BinaryPrimitives.WriteInt16LittleEndian(span, (short)value);
                    break;
                case EncodingInt32:
                    BinaryPrimitives.WriteInt32LittleEndian(span, (int)value);
                    break;
                case EncodingInt64:
                    BinaryPrimitives.WriteInt64LittleEndian(span, value);
                    break;
            }
        }
        
        _encoding = newEncoding;
        _contents = newContents;
        _length++;
    }
    
    /// <summary>
    /// Gets the memory usage in bytes.
    /// </summary>
    public int MemoryUsage => _contents.Length + sizeof(byte) + sizeof(int);
}
