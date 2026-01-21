using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using CysRedis.Core.Unsafe.Common;

namespace CysRedis.Core.Unsafe.DataStructures;

/// <summary>
/// High-performance integer set using unmanaged memory and SIMD optimizations.
/// Automatically upgrades encoding as needed (int16 -> int32 -> int64).
/// </summary>
public unsafe sealed class UnsafeIntSet : IDisposable
{
    private void* _data;
    private byte _encoding; // 2=int16, 4=int32, 8=int64
    private int _length;
    private int _capacity;
    
    private const byte EncodingInt16 = 2;
    private const byte EncodingInt32 = 4;
    private const byte EncodingInt64 = 8;
    
    /// <summary>
    /// Number of elements in the set.
    /// </summary>
    public int Count => _length;
    
    /// <summary>
    /// Gets the current encoding.
    /// </summary>
    public byte Encoding => _encoding;
    
    /// <summary>
    /// Creates a new empty IntSet.
    /// </summary>
    public UnsafeIntSet(int initialCapacity = 8)
    {
        _encoding = EncodingInt16; // Start with smallest encoding
        _length = 0;
        _capacity = initialCapacity;
        _data = UnsafeMemoryManager.AlignedAlloc((nuint)(_capacity * EncodingInt16), SimdHelpers.CacheLineSize, zeroInit: true);
    }
    
    /// <summary>
    /// Adds a value to the set. Returns true if added, false if already exists.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(long value)
    {
        if (!CanEncode(value, _encoding))
            return false;
        
        return BinarySearch(value) >= 0;
    }
    
    /// <summary>
    /// Gets the value at a specific index.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long GetAt(int index)
    {
        if (index < 0 || index >= _length)
            throw new IndexOutOfRangeException();
        
        return GetValueUnsafe(index);
    }
    
    /// <summary>
    /// Gets all values as an enumerable.
    /// </summary>
    public IEnumerable<long> GetAll()
    {
        for (int i = 0; i < _length; i++)
        {
            yield return GetValueUnsafe(i);
        }
    }
    
    /// <summary>
    /// Gets the minimum value.
    /// </summary>
    public long? Min => _length > 0 ? GetValueUnsafe(0) : null;
    
    /// <summary>
    /// Gets the maximum value.
    /// </summary>
    public long? Max => _length > 0 ? GetValueUnsafe(_length - 1) : null;
    
    /// <summary>
    /// Gets the memory usage in bytes.
    /// </summary>
    public int MemoryUsage => _capacity * _encoding + sizeof(byte) + sizeof(int) * 2;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private byte GetRequiredEncoding(long value)
    {
        if (value >= short.MinValue && value <= short.MaxValue)
            return EncodingInt16;
        if (value >= int.MinValue && value <= int.MaxValue)
            return EncodingInt32;
        return EncodingInt64;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int BinarySearch(long value)
    {
        int min = 0, max = _length - 1, mid = -1;
        long cur;
        
        while (min <= max)
        {
            mid = (min + max) / 2;
            cur = GetValueUnsafe(mid);
            
            if (value > cur)
                min = mid + 1;
            else if (value < cur)
                max = mid - 1;
            else
                return mid;
        }
        
        return ~min; // Return bitwise complement of insert position
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long GetValueUnsafe(int index)
    {
        byte* ptr = (byte*)_data + index * _encoding;
        
        return _encoding switch
        {
            EncodingInt16 => *(short*)ptr,
            EncodingInt32 => *(int*)ptr,
            EncodingInt64 => *(long*)ptr,
            _ => throw new InvalidOperationException("Invalid encoding")
        };
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SetValueUnsafe(int index, long value)
    {
        byte* ptr = (byte*)_data + index * _encoding;
        
        switch (_encoding)
        {
            case EncodingInt16:
                *(short*)ptr = (short)value;
                break;
            case EncodingInt32:
                *(int*)ptr = (int)value;
                break;
            case EncodingInt64:
                *(long*)ptr = value;
                break;
        }
    }
    
    private void InsertAt(int pos, long value)
    {
        // Ensure capacity
        if (_length >= _capacity)
        {
            GrowCapacity();
        }
        
        // Shift elements right
        if (pos < _length)
        {
            byte* src = (byte*)_data + pos * _encoding;
            byte* dst = (byte*)_data + (pos + 1) * _encoding;
            int bytesToMove = (_length - pos) * _encoding;
            
            SimdHelpers.CopyMemory(src, dst, bytesToMove);
        }
        
        _length++;
        SetValueUnsafe(pos, value);
    }
    
    private void RemoveAt(int pos)
    {
        if (_length == 1)
        {
            _length = 0;
            return;
        }
        
        // Shift elements left
        if (pos < _length - 1)
        {
            byte* src = (byte*)_data + (pos + 1) * _encoding;
            byte* dst = (byte*)_data + pos * _encoding;
            int bytesToMove = (_length - pos - 1) * _encoding;
            
            SimdHelpers.CopyMemory(src, dst, bytesToMove);
        }
        
        _length--;
    }
    
    private void GrowCapacity()
    {
        int newCapacity = _capacity * 2;
        void* newData = UnsafeMemoryManager.AlignedAlloc((nuint)(newCapacity * _encoding), SimdHelpers.CacheLineSize);
        
        SimdHelpers.CopyMemory((byte*)_data, (byte*)newData, _length * _encoding);
        
        UnsafeMemoryManager.AlignedFree(_data);
        _data = newData;
        _capacity = newCapacity;
    }
    
    private void UpgradeAndAdd(long value)
    {
        var oldEncoding = _encoding;
        var newEncoding = GetRequiredEncoding(value);
        
        // Create new array with upgraded encoding
        int newCapacity = Math.Max(_capacity, _length + 1);
        void* newData = UnsafeMemoryManager.AlignedAlloc((nuint)(newCapacity * newEncoding), SimdHelpers.CacheLineSize);
        
        // Determine insert position
        int insertPos = _length;
        if (_length > 0)
        {
            // Find insert position
            for (int i = 0; i < _length; i++)
            {
                long oldValue = GetValueUnsafe(i);
                if (value < oldValue)
                {
                    insertPos = i;
                    break;
                }
            }
            
            // Copy existing values to new array
            for (int i = 0; i < _length; i++)
            {
                long oldValue = GetValueUnsafe(i);
                int targetPos = i >= insertPos ? i + 1 : i;
                byte* targetPtr = (byte*)newData + targetPos * newEncoding;
                
                switch (newEncoding)
                {
                    case EncodingInt16:
                        *(short*)targetPtr = (short)oldValue;
                        break;
                    case EncodingInt32:
                        *(int*)targetPtr = (int)oldValue;
                        break;
                    case EncodingInt64:
                        *(long*)targetPtr = oldValue;
                        break;
                }
            }
        }
        
        // Set the new value
        byte* insertPtr = (byte*)newData + insertPos * newEncoding;
        switch (newEncoding)
        {
            case EncodingInt16:
                *(short*)insertPtr = (short)value;
                break;
            case EncodingInt32:
                *(int*)insertPtr = (int)value;
                break;
            case EncodingInt64:
                *(long*)insertPtr = value;
                break;
        }
        
        UnsafeMemoryManager.AlignedFree(_data);
        _encoding = newEncoding;
        _data = newData;
        _capacity = newCapacity;
        _length++;
    }
    
    public void Dispose()
    {
        if (_data != null)
        {
            UnsafeMemoryManager.AlignedFree(_data);
            _data = null;
            _length = 0;
            _capacity = 0;
        }
    }
}
