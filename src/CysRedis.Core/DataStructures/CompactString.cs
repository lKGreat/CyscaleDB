using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;

namespace CysRedis.Core.DataStructures;

/// <summary>
/// Compact string implementation similar to Redis SDS (Simple Dynamic String).
/// Uses variable-size headers based on string length to minimize memory overhead.
/// </summary>
public class CompactString
{
    private byte[] _data;
    
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
    
    private CompactString(byte[] data, int length, int capacity, byte type, int headerSize)
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
    public static CompactString Create(ReadOnlySpan<byte> data)
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
    public static CompactString Create(string str)
    {
        var bytes = Encoding.UTF8.GetBytes(str);
        return Create(bytes);
    }
    
    /// <summary>
    /// Gets the string content as a span.
    /// </summary>
    public ReadOnlySpan<byte> AsSpan()
    {
        return _data.AsSpan(HeaderSize, Length);
    }
    
    /// <summary>
    /// Gets the string content as UTF-8 text.
    /// </summary>
    public string GetString()
    {
        return Encoding.UTF8.GetString(_data, HeaderSize, Length);
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
            // Need to grow
            GrowCapacity(newLength);
        }
        
        data.CopyTo(_data.AsSpan(HeaderSize + Length));
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
    public int MemoryUsage => _data.Length;
    
    private static CompactString CreateType5(ReadOnlySpan<byte> data)
    {
        var len = data.Length;
        var headerSize = 1;
        var capacity = len; // Type5 doesn't have free space
        var buffer = new byte[headerSize + capacity];
        
        // Header: 3 bits type + 5 bits length
        buffer[0] = (byte)((SdsType5 << 5) | len);
        
        // Copy data
        data.CopyTo(buffer.AsSpan(headerSize));
        
        return new CompactString(buffer, len, capacity, SdsType5, headerSize);
    }
    
    private static CompactString CreateType8(ReadOnlySpan<byte> data)
    {
        var len = data.Length;
        var headerSize = 3;
        var capacity = CalculateCapacity(len);
        var buffer = new byte[headerSize + capacity];
        
        // Header: len (1 byte) + alloc (1 byte) + flags (1 byte)
        buffer[0] = (byte)len;
        buffer[1] = (byte)capacity;
        buffer[2] = SdsType8;
        
        // Copy data
        data.CopyTo(buffer.AsSpan(headerSize));
        
        return new CompactString(buffer, len, capacity, SdsType8, headerSize);
    }
    
    private static CompactString CreateType16(ReadOnlySpan<byte> data)
    {
        var len = data.Length;
        var headerSize = 5;
        var capacity = CalculateCapacity(len);
        var buffer = new byte[headerSize + capacity];
        
        // Header: len (2 bytes) + alloc (2 bytes) + flags (1 byte)
        BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(0, 2), (ushort)len);
        BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(2, 2), (ushort)capacity);
        buffer[4] = SdsType16;
        
        // Copy data
        data.CopyTo(buffer.AsSpan(headerSize));
        
        return new CompactString(buffer, len, capacity, SdsType16, headerSize);
    }
    
    private static CompactString CreateType32(ReadOnlySpan<byte> data)
    {
        var len = data.Length;
        var headerSize = 9;
        var capacity = CalculateCapacity(len);
        var buffer = new byte[headerSize + capacity];
        
        // Header: len (4 bytes) + alloc (4 bytes) + flags (1 byte)
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(0, 4), len);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(4, 4), capacity);
        buffer[8] = SdsType32;
        
        // Copy data
        data.CopyTo(buffer.AsSpan(headerSize));
        
        return new CompactString(buffer, len, capacity, SdsType32, headerSize);
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
            // Need to change type
            var newString = newType switch
            {
                SdsType8 => CreateType8(AsSpan()),
                SdsType16 => CreateType16(AsSpan()),
                SdsType32 => CreateType32(AsSpan()),
                _ => throw new InvalidOperationException("Invalid type")
            };
            
            _data = newString._data;
            Capacity = newString.Capacity;
            Type = newString.Type;
            HeaderSize = newString.HeaderSize;
        }
        else
        {
            // Same type, just extend buffer
            var newHeaderSize = GetHeaderSize(newType);
            var newBuffer = new byte[newHeaderSize + newCapacity];
            
            // Copy header and existing data
            Array.Copy(_data, 0, newBuffer, 0, HeaderSize + Length);
            
            _data = newBuffer;
            Capacity = newCapacity;
            UpdateHeader();
        }
    }
    
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
                BinaryPrimitives.WriteUInt16LittleEndian(_data.AsSpan(0, 2), (ushort)Length);
                BinaryPrimitives.WriteUInt16LittleEndian(_data.AsSpan(2, 2), (ushort)Capacity);
                break;
            case SdsType32:
                BinaryPrimitives.WriteInt32LittleEndian(_data.AsSpan(0, 4), Length);
                BinaryPrimitives.WriteInt32LittleEndian(_data.AsSpan(4, 4), Capacity);
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
}
