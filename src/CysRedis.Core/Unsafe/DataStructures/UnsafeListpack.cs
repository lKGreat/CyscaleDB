using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using CysRedis.Core.Unsafe.Common;

namespace CysRedis.Core.Unsafe.DataStructures;

/// <summary>
/// High-performance listpack implementation using unmanaged memory.
/// Zero-copy encoding/decoding with SIMD optimizations.
/// </summary>
public unsafe sealed class UnsafeListpack : IDisposable
{
    private byte* _buffer;
    private int _numElements;
    private int _capacity;
    
    // Header: total bytes (4 bytes) + num elements (2 bytes)
    private const int HeaderSize = 6;
    private const int EndMarker = 0xFF;
    private const int MaxSize = 65535; // Max 64KB
    
    // Encoding types
    private const byte Enc7BitUint = 0;      // 0xxxxxxx
    private const byte Enc6BitStr = 0x80;    // 10xxxxxx + data
    private const byte Enc13BitInt = 0xC0;   // 110xxxxx xxxxxxxx
    private const byte Enc12BitStr = 0xE0;   // 1110xxxx + 1byte len + data
    private const byte Enc16BitInt = 0xF1;   // 11110001 + 2 bytes
    private const byte Enc24BitInt = 0xF2;   // 11110010 + 3 bytes
    private const byte Enc32BitInt = 0xF3;   // 11110011 + 4 bytes
    private const byte Enc64BitInt = 0xF4;   // 11110100 + 8 bytes
    private const byte Enc32BitStr = 0xF0;   // 11110000 + 4bytes len + data
    
    /// <summary>
    /// Number of elements in the listpack.
    /// </summary>
    public int Count => _numElements;
    
    /// <summary>
    /// Total bytes used.
    /// </summary>
    public int TotalBytes => _buffer != null ? GetTotalBytes() : HeaderSize + 1;
    
    /// <summary>
    /// Creates a new empty listpack.
    /// </summary>
    public UnsafeListpack(int initialCapacity = 256)
    {
        _capacity = initialCapacity;
        _buffer = (byte*)UnsafeMemoryManager.AlignedAlloc((nuint)_capacity, SimdHelpers.CacheLineSize);
        _numElements = 0;
        _buffer[HeaderSize] = EndMarker;
        UpdateHeader();
    }
    
    /// <summary>
    /// Appends a string element to the end.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(ReadOnlySpan<byte> data)
    {
        // Try to parse as integer for compact encoding
        if (TryParseInteger(data, out var intVal))
        {
            AppendInteger(intVal);
        }
        else
        {
            AppendString(data);
        }
    }
    
    /// <summary>
    /// Appends an integer element to the end.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AppendInteger(long value)
    {
        var encodedSize = EstimateIntegerSize(value);
        var totalSize = encodedSize + 1; // +1 for backlen
        var currentTotal = TotalBytes;
        var required = currentTotal - 1 + totalSize + 1; // -1 for end marker, +1 for new end marker
        
        EnsureCapacity(required);
        
        var pos = currentTotal - 1; // Position before end marker
        EncodeIntegerFast(value, pos);
        
        _numElements++;
        UpdateHeader();
    }
    
    /// <summary>
    /// Appends a string element to the end.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AppendString(ReadOnlySpan<byte> data)
    {
        var encodedSize = EstimateStringSize(data.Length);
        var totalSize = encodedSize + data.Length + 1; // +1 for backlen
        var currentTotal = TotalBytes;
        var required = currentTotal - 1 + totalSize + 1;
        
        EnsureCapacity(required);
        
        var pos = currentTotal - 1;
        EncodeStringFast(data, pos);
        
        _numElements++;
        UpdateHeader();
    }
    
    /// <summary>
    /// Gets an element at a specific index.
    /// </summary>
    public ListpackEntry GetAt(int index)
    {
        if (index < 0 || index >= _numElements)
            throw new IndexOutOfRangeException();
        
        var pos = HeaderSize;
        for (int i = 0; i < index; i++)
        {
            pos = SkipEntryFast(pos);
        }
        
        return DecodeEntryFast(pos);
    }
    
    /// <summary>
    /// Gets all entries as an enumerable.
    /// </summary>
    public IEnumerable<ListpackEntry> GetAll()
    {
        var pos = HeaderSize;
        for (int i = 0; i < _numElements; i++)
        {
            yield return DecodeEntryFast(pos);
            pos = SkipEntryFast(pos);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetTotalBytes()
    {
        return BinaryPrimitives.ReadInt32LittleEndian(new ReadOnlySpan<byte>(_buffer, 4));
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryParseInteger(ReadOnlySpan<byte> data, out long value)
    {
        var str = Encoding.UTF8.GetString(data);
        return long.TryParse(str, out value);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int EstimateIntegerSize(long value)
    {
        if (value >= 0 && value <= 127) return 1; // 7-bit uint
        if (value >= -4096 && value <= 4095) return 2; // 13-bit int
        if (value >= short.MinValue && value <= short.MaxValue) return 3; // 16-bit
        if (value >= -(1L << 23) && value < (1L << 23)) return 4; // 24-bit
        if (value >= int.MinValue && value <= int.MaxValue) return 5; // 32-bit
        return 9; // 64-bit
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int EstimateStringSize(int length)
    {
        if (length <= 63) return 1; // 6-bit str
        if (length <= 4095) return 2; // 12-bit str
        return 5; // 32-bit str
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EncodeIntegerFast(long value, int pos)
    {
        byte* ptr = _buffer + pos;
        int startPos = pos;
        
        if (value >= 0 && value <= 127)
        {
            *ptr++ = (byte)value;
        }
        else if (value >= -4096 && value <= 4095)
        {
            *ptr++ = (byte)(Enc13BitInt | ((value >> 8) & 0x1F));
            *ptr++ = (byte)(value & 0xFF);
        }
        else if (value >= short.MinValue && value <= short.MaxValue)
        {
            *ptr++ = Enc16BitInt;
            BinaryPrimitives.WriteInt16LittleEndian(new Span<byte>(ptr, 2), (short)value);
            ptr += 2;
        }
        else if (value >= int.MinValue && value <= int.MaxValue)
        {
            *ptr++ = Enc32BitInt;
            BinaryPrimitives.WriteInt32LittleEndian(new Span<byte>(ptr, 4), (int)value);
            ptr += 4;
        }
        else
        {
            *ptr++ = Enc64BitInt;
            BinaryPrimitives.WriteInt64LittleEndian(new Span<byte>(ptr, 8), value);
            ptr += 8;
        }
        
        // Write backlen
        int entryLen = (int)(ptr - (_buffer + startPos));
        *ptr++ = (byte)entryLen;
        
        // Write end marker
        *ptr = EndMarker;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EncodeStringFast(ReadOnlySpan<byte> data, int pos)
    {
        byte* ptr = _buffer + pos;
        int startPos = pos;
        int len = data.Length;
        
        if (len <= 63)
        {
            *ptr++ = (byte)(Enc6BitStr | len);
        }
        else if (len <= 4095)
        {
            *ptr++ = (byte)(Enc12BitStr | (len >> 8));
            *ptr++ = (byte)(len & 0xFF);
        }
        else
        {
            *ptr++ = Enc32BitStr;
            BinaryPrimitives.WriteInt32LittleEndian(new Span<byte>(ptr, 4), len);
            ptr += 4;
        }
        
        // Write data using SIMD if available
        if (len > 0)
        {
            fixed (byte* src = data)
            {
                SimdHelpers.CopyMemory(src, ptr, len);
            }
            ptr += len;
        }
        
        // Write backlen
        int entryLen = (int)(ptr - (_buffer + startPos));
        if (entryLen <= 127)
        {
            *ptr++ = (byte)entryLen;
        }
        else
        {
            *ptr++ = (byte)(entryLen & 0x7F);
        }
        
        // Write end marker
        *ptr = EndMarker;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ListpackEntry DecodeEntryFast(int pos)
    {
        byte* ptr = _buffer + pos;
        byte firstByte = *ptr;
        
        if ((firstByte & 0x80) == 0)
        {
            // 7-bit uint
            return new ListpackEntry { IntValue = firstByte, IsInteger = true };
        }
        else if ((firstByte & 0xC0) == 0x80)
        {
            // 6-bit string
            int len = firstByte & 0x3F;
            byte[] data = new byte[len];
            fixed (byte* dst = data)
            {
                SimdHelpers.CopyMemory(ptr + 1, dst, len);
            }
            return new ListpackEntry { StringValue = data, IsInteger = false };
        }
        else if ((firstByte & 0xE0) == 0xC0)
        {
            // 13-bit int
            int value = ((firstByte & 0x1F) << 8) | ptr[1];
            if ((firstByte & 0x10) != 0) value |= unchecked((int)0xFFFFE000); // Sign extend
            return new ListpackEntry { IntValue = value, IsInteger = true };
        }
        else if (firstByte == Enc16BitInt)
        {
            int value = BinaryPrimitives.ReadInt16LittleEndian(new ReadOnlySpan<byte>(ptr + 1, 2));
            return new ListpackEntry { IntValue = value, IsInteger = true };
        }
        else if (firstByte == Enc32BitInt)
        {
            int value = BinaryPrimitives.ReadInt32LittleEndian(new ReadOnlySpan<byte>(ptr + 1, 4));
            return new ListpackEntry { IntValue = value, IsInteger = true };
        }
        else if (firstByte == Enc64BitInt)
        {
            long value = BinaryPrimitives.ReadInt64LittleEndian(new ReadOnlySpan<byte>(ptr + 1, 8));
            return new ListpackEntry { IntValue = value, IsInteger = true };
        }
        
        // Default: treat as string
        return new ListpackEntry { StringValue = Array.Empty<byte>(), IsInteger = false };
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int SkipEntryFast(int pos)
    {
        byte* ptr = _buffer + pos;
        byte firstByte = *ptr;
        
        if ((firstByte & 0x80) == 0)
        {
            return pos + 2; // 1 byte value + 1 byte backlen
        }
        else if ((firstByte & 0xC0) == 0x80)
        {
            int len = firstByte & 0x3F;
            return pos + 1 + len + 1; // 1 byte header + data + backlen
        }
        else if ((firstByte & 0xE0) == 0xC0)
        {
            return pos + 3; // 2 bytes value + 1 byte backlen
        }
        
        // Read backlen to determine size
        int entryStart = pos;
        while (ptr[0] != EndMarker)
        {
            ptr++;
        }
        ptr--; // Backlen is before end marker
        int backlen = *ptr;
        return entryStart + backlen + 1; // +1 for end marker
    }
    
    private void EnsureCapacity(int required)
    {
        if (_capacity >= required) return;
        
        int newSize = Math.Max(required, _capacity * 2);
        byte* newBuffer = (byte*)UnsafeMemoryManager.AlignedAlloc((nuint)newSize, SimdHelpers.CacheLineSize);
        
        SimdHelpers.CopyMemory(_buffer, newBuffer, TotalBytes);
        
        UnsafeMemoryManager.AlignedFree(_buffer);
        _buffer = newBuffer;
        _capacity = newSize;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void UpdateHeader()
    {
        int totalBytes = TotalBytes;
        BinaryPrimitives.WriteInt32LittleEndian(new Span<byte>(_buffer, 4), totalBytes);
        BinaryPrimitives.WriteUInt16LittleEndian(new Span<byte>(_buffer + 4, 2), (ushort)_numElements);
    }
    
    public void Dispose()
    {
        if (_buffer != null)
        {
            UnsafeMemoryManager.AlignedFree(_buffer);
            _buffer = null;
            _numElements = 0;
            _capacity = 0;
        }
    }
}

/// <summary>
/// Represents a listpack entry.
/// </summary>
public struct ListpackEntry
{
    public bool IsInteger { get; set; }
    public long IntValue { get; set; }
    public byte[]? StringValue { get; set; }
    
    public ReadOnlySpan<byte> GetBytes()
    {
        if (IsInteger)
        {
            return Encoding.UTF8.GetBytes(IntValue.ToString());
        }
        return StringValue ?? Array.Empty<byte>();
    }
}
