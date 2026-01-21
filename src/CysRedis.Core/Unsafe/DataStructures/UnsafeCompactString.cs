using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using CysRedis.Core.Unsafe.Common;

namespace CysRedis.Core.Unsafe.DataStructures;

/// <summary>
/// High-performance compact string implementation using unmanaged memory.
/// Similar to Redis SDS (Simple Dynamic String) with variable-size headers.
/// </summary>
public unsafe struct UnsafeCompactString : IDisposable
{
    private byte* _data;      // Header + data continuous storage
    private int _allocSize;
    
    // Type encodings
    private const byte SdsType5 = 0;   // String length <= 31, 1 byte header
    private const byte SdsType8 = 1;   // String length <= 255, 3 byte header
    private const byte SdsType16 = 2;  // String length <= 65535, 5 byte header
    private const byte SdsType32 = 3;  // String length <= 4GB, 9 byte header
    
    /// <summary>
    /// Gets the string length.
    /// </summary>
    public int Length { get; private set; }
    
    /// <summary>
    /// Gets the allocated capacity.
    /// </summary>
    public int Capacity { get; private set; }
    
    /// <summary>
    /// Gets the header size for this string.
    /// </summary>
    public int HeaderSize { get; private set; }
    
    /// <summary>
    /// Gets the type encoding.
    /// </summary>
    public byte Type { get; private set; }
    
    private UnsafeCompactString(byte* data, int length, int capacity, byte type, int headerSize)
    {
        _data = data;
        Length = length;
        Capacity = capacity;
        Type = type;
        HeaderSize = headerSize;
    }
    
    /// <summary>
    /// Creates a compact string from a byte array.
    /// </summary>
    public static UnsafeCompactString Create(ReadOnlySpan<byte> data)
    {
        var len = data.Length;
        
        if (len <= 31)
            return CreateType5(data);
        else if (len <= 255)
            return CreateType8(data);
        else if (len <= 65535)
            return CreateType16(data);
        else
            return CreateType32(data);
    }
    
    /// <summary>
    /// Creates a compact string from a regular string.
    /// </summary>
    public static UnsafeCompactString Create(string str)
    {
        var bytes = Encoding.UTF8.GetBytes(str);
        return Create(bytes);
    }
    
    /// <summary>
    /// Creates a small string using stack allocation when possible.
    /// </summary>
    public static UnsafeCompactString CreateSmall(ReadOnlySpan<byte> data)
    {
        if (data.Length <= 55)
        {
            // Use stack allocation for very small strings
            Span<byte> stack = stackalloc byte[64];
            data.CopyTo(stack);
            return CreateType5(stack.Slice(0, data.Length));
        }
        
        return Create(data);
    }
    
    /// <summary>
    /// Gets the string content as a span.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> AsSpan()
    {
        if (_data == null) return ReadOnlySpan<byte>.Empty;
        return new ReadOnlySpan<byte>(_data + HeaderSize, Length);
    }
    
    /// <summary>
    /// Gets the string content as UTF-8 text.
    /// </summary>
    public string GetString()
    {
        if (_data == null) return string.Empty;
        return Encoding.UTF8.GetString(_data + HeaderSize, Length);
    }
    
    /// <summary>
    /// Gets the available free space.
    /// </summary>
    public int AvailableSpace => Capacity - Length;
    
    /// <summary>
    /// Appends data to the string, growing if necessary.
    /// </summary>
    public void Append(ReadOnlySpan<byte> data)
    {
        var newLength = Length + data.Length;
        
        if (newLength > Capacity)
        {
            GrowCapacity(newLength);
        }
        
        fixed (byte* src = data)
        {
            SimdHelpers.CopyMemory(src, _data + HeaderSize + Length, data.Length);
        }
        
        Length = newLength;
        UpdateHeader();
    }
    
    /// <summary>
    /// Clears the string content but keeps the allocated buffer.
    /// </summary>
    public void Clear()
    {
        Length = 0;
        UpdateHeader();
    }
    
    /// <summary>
    /// Gets the total memory usage in bytes.
    /// </summary>
    public int MemoryUsage => _allocSize;
    
    private static UnsafeCompactString CreateType5(ReadOnlySpan<byte> data)
    {
        var len = data.Length;
        var headerSize = 1;
        var capacity = len; // Type5 doesn't have free space
        var allocSize = headerSize + capacity;
        var buffer = (byte*)UnsafeMemoryManager.AlignedAlloc((nuint)allocSize, SimdHelpers.CacheLineSize);
        
        // Header: 3 bits type + 5 bits length
        buffer[0] = (byte)((SdsType5 << 5) | len);
        
        // Copy data
        fixed (byte* src = data)
        {
            SimdHelpers.CopyMemory(src, buffer + headerSize, len);
        }
        
        return new UnsafeCompactString(buffer, len, capacity, SdsType5, headerSize) { _allocSize = allocSize };
    }
    
    private static UnsafeCompactString CreateType8(ReadOnlySpan<byte> data)
    {
        var len = data.Length;
        var headerSize = 3;
        var capacity = CalculateCapacity(len);
        var allocSize = headerSize + capacity;
        var buffer = (byte*)UnsafeMemoryManager.AlignedAlloc((nuint)allocSize, SimdHelpers.CacheLineSize);
        
        // Header: len (1 byte) + alloc (1 byte) + flags (1 byte)
        buffer[0] = (byte)len;
        buffer[1] = (byte)capacity;
        buffer[2] = SdsType8;
        
        // Copy data
        fixed (byte* src = data)
        {
            SimdHelpers.CopyMemory(src, buffer + headerSize, len);
        }
        
        return new UnsafeCompactString(buffer, len, capacity, SdsType8, headerSize) { _allocSize = allocSize };
    }
    
    private static UnsafeCompactString CreateType16(ReadOnlySpan<byte> data)
    {
        var len = data.Length;
        var headerSize = 5;
        var capacity = CalculateCapacity(len);
        var allocSize = headerSize + capacity;
        var buffer = (byte*)UnsafeMemoryManager.AlignedAlloc((nuint)allocSize, SimdHelpers.CacheLineSize);
        
        // Header: len (2 bytes) + alloc (2 bytes) + flags (1 byte)
        BinaryPrimitives.WriteUInt16LittleEndian(new Span<byte>(buffer, 2), (ushort)len);
        BinaryPrimitives.WriteUInt16LittleEndian(new Span<byte>(buffer + 2, 2), (ushort)capacity);
        buffer[4] = SdsType16;
        
        // Copy data
        fixed (byte* src = data)
        {
            SimdHelpers.CopyMemory(src, buffer + headerSize, len);
        }
        
        return new UnsafeCompactString(buffer, len, capacity, SdsType16, headerSize) { _allocSize = allocSize };
    }
    
    private static UnsafeCompactString CreateType32(ReadOnlySpan<byte> data)
    {
        var len = data.Length;
        var headerSize = 9;
        var capacity = CalculateCapacity(len);
        var allocSize = headerSize + capacity;
        var buffer = (byte*)UnsafeMemoryManager.AlignedAlloc((nuint)allocSize, SimdHelpers.CacheLineSize);
        
        // Header: len (4 bytes) + alloc (4 bytes) + flags (1 byte)
        BinaryPrimitives.WriteInt32LittleEndian(new Span<byte>(buffer, 4), len);
        BinaryPrimitives.WriteInt32LittleEndian(new Span<byte>(buffer + 4, 4), capacity);
        buffer[8] = SdsType32;
        
        // Copy data
        fixed (byte* src = data)
        {
            SimdHelpers.CopyMemory(src, buffer + headerSize, len);
        }
        
        return new UnsafeCompactString(buffer, len, capacity, SdsType32, headerSize) { _allocSize = allocSize };
    }
    
    private static int CalculateCapacity(int length)
    {
        // Redis uses preallocation similar to this
        const int MaxPrealloc = 1024 * 1024; // 1MB
        
        if (length < MaxPrealloc)
            return length * 2;
        else
            return length + MaxPrealloc;
    }
    
    private void GrowCapacity(int requiredLength)
    {
        var newCapacity = CalculateCapacity(requiredLength);
        var newType = DetermineType(newCapacity);
        
        if (newType != Type)
        {
            // Need to change type - recreate
            var currentSpan = AsSpan();
            UnsafeCompactString newString;
            
            if (newType == SdsType8)
                newString = CreateType8(currentSpan);
            else if (newType == SdsType16)
                newString = CreateType16(currentSpan);
            else
                newString = CreateType32(currentSpan);
            
            Dispose();
            this = newString;
        }
        else
        {
            // Same type, just extend buffer
            var newHeaderSize = GetHeaderSize(newType);
            var newAllocSize = newHeaderSize + newCapacity;
            var newBuffer = (byte*)UnsafeMemoryManager.AlignedAlloc((nuint)newAllocSize, SimdHelpers.CacheLineSize);
            
            // Copy header and existing data
            SimdHelpers.CopyMemory(_data, newBuffer, HeaderSize + Length);
            
            UnsafeMemoryManager.AlignedFree(_data);
            _data = newBuffer;
            Capacity = newCapacity;
            _allocSize = newAllocSize;
            UpdateHeader();
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void UpdateHeader()
    {
        switch (Type)
        {
            case SdsType5:
                _data[0] = (byte)((SdsType5 << 5) | Length);
                break;
            case SdsType8:
                _data[0] = (byte)Length;
                _data[1] = (byte)Capacity;
                break;
            case SdsType16:
                BinaryPrimitives.WriteUInt16LittleEndian(new Span<byte>(_data, 2), (ushort)Length);
                BinaryPrimitives.WriteUInt16LittleEndian(new Span<byte>(_data + 2, 2), (ushort)Capacity);
                break;
            case SdsType32:
                BinaryPrimitives.WriteInt32LittleEndian(new Span<byte>(_data, 4), Length);
                BinaryPrimitives.WriteInt32LittleEndian(new Span<byte>(_data + 4, 4), Capacity);
                break;
        }
    }
    
    private static byte DetermineType(int capacity)
    {
        if (capacity <= 31) return SdsType5;
        if (capacity <= 255) return SdsType8;
        if (capacity <= 65535) return SdsType16;
        return SdsType32;
    }
    
    private static int GetHeaderSize(byte type)
    {
        return type switch
        {
            SdsType5 => 1,
            SdsType8 => 3,
            SdsType16 => 5,
            SdsType32 => 9,
            _ => throw new ArgumentException("Invalid type")
        };
    }
    
    public void Dispose()
    {
        if (_data != null)
        {
            UnsafeMemoryManager.AlignedFree(_data);
            _data = null;
            Length = 0;
            Capacity = 0;
            _allocSize = 0;
        }
    }
}
