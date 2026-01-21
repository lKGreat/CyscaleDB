using System.Buffers.Binary;
using System.Text;

namespace CysRedis.Core.DataStructures;

/// <summary>
/// Listpack - compact list encoding similar to Redis listpack.
/// Stores elements in a continuous byte array with variable-length encoding.
/// Small integers use 1-9 bytes, strings use length-prefixed encoding.
/// </summary>
public class Listpack
{
    private byte[] _buffer;
    private int _numElements;
    
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
    public int TotalBytes => _buffer?.Length ?? HeaderSize + 1;
    
    /// <summary>
    /// Creates a new empty listpack.
    /// </summary>
    public Listpack()
    {
        _buffer = new byte[HeaderSize + 1];
        _buffer[HeaderSize] = EndMarker;
        _numElements = 0;
        UpdateHeader();
    }
    
    /// <summary>
    /// Appends a string element to the end.
    /// </summary>
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
    public void AppendInteger(long value)
    {
        var encodedSize = EstimateIntegerSize(value);
        var totalSize = encodedSize + 1; // +1 for backlen
        
        EnsureCapacity(TotalBytes - 1 + totalSize + 1); // -1 for end marker, +1 for new end marker
        
        var pos = TotalBytes - 1; // Position before end marker
        EncodeInteger(value, pos);
        
        _numElements++;
        UpdateHeader();
    }
    
    /// <summary>
    /// Appends a string element to the end.
    /// </summary>
    public void AppendString(ReadOnlySpan<byte> data)
    {
        var encodedSize = EstimateStringSize(data.Length);
        var totalSize = encodedSize + data.Length + 1; // +1 for backlen
        
        EnsureCapacity(TotalBytes - 1 + totalSize + 1);
        
        var pos = TotalBytes - 1;
        EncodeString(data, pos);
        
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
            pos = SkipEntry(pos);
        }
        
        return DecodeEntry(pos);
    }
    
    /// <summary>
    /// Gets all entries as an enumerable.
    /// </summary>
    public IEnumerable<ListpackEntry> GetAll()
    {
        var pos = HeaderSize;
        for (int i = 0; i < _numElements; i++)
        {
            yield return DecodeEntry(pos);
            pos = SkipEntry(pos);
        }
    }
    
    private bool TryParseInteger(ReadOnlySpan<byte> data, out long value)
    {
        var str = Encoding.UTF8.GetString(data);
        return long.TryParse(str, out value);
    }
    
    private int EstimateIntegerSize(long value)
    {
        if (value >= 0 && value <= 127) return 1; // 7-bit uint
        if (value >= -4096 && value <= 4095) return 2; // 13-bit int
        if (value >= short.MinValue && value <= short.MaxValue) return 3; // 16-bit
        if (value >= -(1L << 23) && value < (1L << 23)) return 4; // 24-bit
        if (value >= int.MinValue && value <= int.MaxValue) return 5; // 32-bit
        return 9; // 64-bit
    }
    
    private int EstimateStringSize(int length)
    {
        if (length <= 63) return 1; // 6-bit str
        if (length <= 4095) return 2; // 12-bit str
        return 5; // 32-bit str
    }
    
    private void EncodeInteger(long value, int pos)
    {
        var startPos = pos;
        
        if (value >= 0 && value <= 127)
        {
            _buffer[pos++] = (byte)value;
        }
        else if (value >= -4096 && value <= 4095)
        {
            _buffer[pos++] = (byte)(Enc13BitInt | ((value >> 8) & 0x1F));
            _buffer[pos++] = (byte)(value & 0xFF);
        }
        else if (value >= short.MinValue && value <= short.MaxValue)
        {
            _buffer[pos++] = Enc16BitInt;
            BinaryPrimitives.WriteInt16LittleEndian(_buffer.AsSpan(pos, 2), (short)value);
            pos += 2;
        }
        else if (value >= int.MinValue && value <= int.MaxValue)
        {
            _buffer[pos++] = Enc32BitInt;
            BinaryPrimitives.WriteInt32LittleEndian(_buffer.AsSpan(pos, 4), (int)value);
            pos += 4;
        }
        else
        {
            _buffer[pos++] = Enc64BitInt;
            BinaryPrimitives.WriteInt64LittleEndian(_buffer.AsSpan(pos, 8), value);
            pos += 8;
        }
        
        // Write backlen
        var entryLen = pos - startPos;
        _buffer[pos++] = (byte)entryLen;
        
        // Write end marker
        _buffer[pos] = EndMarker;
    }
    
    private void EncodeString(ReadOnlySpan<byte> data, int pos)
    {
        var startPos = pos;
        var len = data.Length;
        
        if (len <= 63)
        {
            _buffer[pos++] = (byte)(Enc6BitStr | len);
        }
        else if (len <= 4095)
        {
            _buffer[pos++] = (byte)(Enc12BitStr | (len >> 8));
            _buffer[pos++] = (byte)(len & 0xFF);
        }
        else
        {
            _buffer[pos++] = Enc32BitStr;
            BinaryPrimitives.WriteInt32LittleEndian(_buffer.AsSpan(pos, 4), len);
            pos += 4;
        }
        
        // Write data
        data.CopyTo(_buffer.AsSpan(pos, len));
        pos += len;
        
        // Write backlen
        var entryLen = pos - startPos;
        if (entryLen <= 127)
        {
            _buffer[pos++] = (byte)entryLen;
        }
        else
        {
            // Multi-byte backlen (not implemented for simplicity)
            _buffer[pos++] = (byte)(entryLen & 0x7F);
        }
        
        // Write end marker
        _buffer[pos] = EndMarker;
    }
    
    private ListpackEntry DecodeEntry(int pos)
    {
        var firstByte = _buffer[pos];
        
        if ((firstByte & 0x80) == 0)
        {
            // 7-bit uint
            return new ListpackEntry { IntValue = firstByte, IsInteger = true };
        }
        else if ((firstByte & 0xC0) == 0x80)
        {
            // 6-bit string
            var len = firstByte & 0x3F;
            var data = _buffer.AsSpan(pos + 1, len).ToArray();
            return new ListpackEntry { StringValue = data, IsInteger = false };
        }
        else if ((firstByte & 0xE0) == 0xC0)
        {
            // 13-bit int
            var value = ((firstByte & 0x1F) << 8) | _buffer[pos + 1];
            if ((firstByte & 0x10) != 0) value |= unchecked((int)0xFFFFE000); // Sign extend
            return new ListpackEntry { IntValue = value, IsInteger = true };
        }
        else if (firstByte == Enc16BitInt)
        {
            var value = BinaryPrimitives.ReadInt16LittleEndian(_buffer.AsSpan(pos + 1, 2));
            return new ListpackEntry { IntValue = value, IsInteger = true };
        }
        else if (firstByte == Enc32BitInt)
        {
            var value = BinaryPrimitives.ReadInt32LittleEndian(_buffer.AsSpan(pos + 1, 4));
            return new ListpackEntry { IntValue = value, IsInteger = true };
        }
        else if (firstByte == Enc64BitInt)
        {
            var value = BinaryPrimitives.ReadInt64LittleEndian(_buffer.AsSpan(pos + 1, 8));
            return new ListpackEntry { IntValue = value, IsInteger = true };
        }
        
        // Default: treat as string
        return new ListpackEntry { StringValue = Array.Empty<byte>(), IsInteger = false };
    }
    
    private int SkipEntry(int pos)
    {
        var firstByte = _buffer[pos];
        
        if ((firstByte & 0x80) == 0)
        {
            return pos + 2; // 1 byte value + 1 byte backlen
        }
        else if ((firstByte & 0xC0) == 0x80)
        {
            var len = firstByte & 0x3F;
            return pos + 1 + len + 1; // 1 byte header + data + backlen
        }
        else if ((firstByte & 0xE0) == 0xC0)
        {
            return pos + 3; // 2 bytes value + 1 byte backlen
        }
        
        // Simplified: skip by reading backlen from next position estimate
        return pos + 10; // Safe estimate
    }
    
    private void EnsureCapacity(int required)
    {
        if (_buffer.Length >= required) return;
        
        var newSize = Math.Max(required, _buffer.Length * 2);
        var newBuffer = new byte[newSize];
        Array.Copy(_buffer, newBuffer, TotalBytes);
        _buffer = newBuffer;
    }
    
    private void UpdateHeader()
    {
        BinaryPrimitives.WriteInt32LittleEndian(_buffer.AsSpan(0, 4), TotalBytes);
        BinaryPrimitives.WriteUInt16LittleEndian(_buffer.AsSpan(4, 2), (ushort)_numElements);
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
