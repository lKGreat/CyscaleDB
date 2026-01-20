using System.Buffers.Binary;
using System.Text;

namespace CyscaleDB.Core.Protocol;

/// <summary>
/// Writes MySQL protocol packets to a stream.
/// MySQL packets use little-endian byte order.
/// Packet format: [3 bytes length][1 byte sequence][payload]
/// </summary>
public sealed class PacketWriter : IDisposable
{
    private readonly Stream _stream;
    private byte _sequenceNumber;
    private bool _disposed;
    private const int MaxPacketSize = 0xFFFFFF; // 16MB - 1

    /// <summary>
    /// Creates a new packet writer.
    /// </summary>
    public PacketWriter(Stream stream)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _sequenceNumber = 0;
    }

    /// <summary>
    /// Writes a packet to the stream.
    /// </summary>
    public async Task WritePacketAsync(byte[] payload, CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();

        if (payload.Length == 0)
        {
            await WritePacketHeaderAsync(0, cancellationToken);
            return;
        }

        // Split into multiple packets if payload exceeds max size
        var offset = 0;
        while (offset < payload.Length)
        {
            var chunkSize = Math.Min(payload.Length - offset, MaxPacketSize);
            var isLastChunk = offset + chunkSize >= payload.Length;

            var chunk = new byte[chunkSize];
            Buffer.BlockCopy(payload, offset, chunk, 0, chunkSize);

            var packetLength = isLastChunk ? chunkSize : MaxPacketSize;
            await WritePacketHeaderAsync(packetLength, cancellationToken);
            await _stream.WriteAsync(chunk, cancellationToken);
            await _stream.FlushAsync(cancellationToken);

            offset += chunkSize;
        }
    }

    /// <summary>
    /// Writes a packet header (3 bytes length + 1 byte sequence).
    /// </summary>
    private async Task WritePacketHeaderAsync(int length, CancellationToken cancellationToken)
    {
        var header = new byte[4];
        header[0] = (byte)(length & 0xFF);
        header[1] = (byte)((length >> 8) & 0xFF);
        header[2] = (byte)((length >> 16) & 0xFF);
        header[3] = _sequenceNumber;

        await _stream.WriteAsync(header, cancellationToken);
        _sequenceNumber = (byte)((_sequenceNumber + 1) % 256);
    }

    /// <summary>
    /// Writes a length-encoded integer.
    /// </summary>
    public static void WriteLengthEncodedInteger(MemoryStream buffer, ulong value)
    {
        if (value < 251)
        {
            buffer.WriteByte((byte)value);
        }
        else if (value < 65536)
        {
            buffer.WriteByte(0xFC);
            var bytes = new byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(bytes, (ushort)value);
            buffer.Write(bytes);
        }
        else if (value < 16777216)
        {
            buffer.WriteByte(0xFD);
            var bytes = new byte[3];
            bytes[0] = (byte)(value & 0xFF);
            bytes[1] = (byte)((value >> 8) & 0xFF);
            bytes[2] = (byte)((value >> 16) & 0xFF);
            buffer.Write(bytes);
        }
        else
        {
            buffer.WriteByte(0xFE);
            var bytes = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
            buffer.Write(bytes);
        }
    }

    /// <summary>
    /// Writes a null-terminated string.
    /// </summary>
    public static void WriteNullTerminatedString(MemoryStream buffer, string value, Encoding encoding)
    {
        if (!string.IsNullOrEmpty(value))
        {
            var bytes = encoding.GetBytes(value);
            buffer.Write(bytes);
        }
        buffer.WriteByte(0); // Null terminator
    }

    /// <summary>
    /// Writes a length-encoded string.
    /// </summary>
    public static void WriteLengthEncodedString(MemoryStream buffer, string value, Encoding encoding)
    {
        if (string.IsNullOrEmpty(value))
        {
            WriteLengthEncodedInteger(buffer, 0);
            return;
        }

        var bytes = encoding.GetBytes(value);
        WriteLengthEncodedInteger(buffer, (ulong)bytes.Length);
        buffer.Write(bytes);
    }

    /// <summary>
    /// Writes a fixed-length string (padded or truncated).
    /// </summary>
    public static void WriteFixedString(MemoryStream buffer, string value, int length, Encoding encoding)
    {
        var bytes = encoding.GetBytes(value ?? string.Empty);
        if (bytes.Length > length)
        {
            buffer.Write(bytes, 0, length);
        }
        else
        {
            buffer.Write(bytes);
            // Pad with zeros
            for (var i = bytes.Length; i < length; i++)
                buffer.WriteByte(0);
        }
    }

    /// <summary>
    /// Resets the sequence number (used after handshake).
    /// </summary>
    public void ResetSequence()
    {
        _sequenceNumber = 0;
    }

    private void EnsureNotDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(PacketWriter));
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
        }
    }
}
