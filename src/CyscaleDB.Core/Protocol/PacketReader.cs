using System.Buffers.Binary;
using System.Text;

namespace CyscaleDB.Core.Protocol;

/// <summary>
/// Reads MySQL protocol packets from a stream.
/// MySQL packets use little-endian byte order.
/// Packet format: [3 bytes length][1 byte sequence][payload]
/// </summary>
public sealed class PacketReader : IDisposable
{
    private readonly Stream _stream;
    private byte _sequenceNumber;
    private bool _disposed;

    /// <summary>
    /// Creates a new packet reader.
    /// </summary>
    public PacketReader(Stream stream)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _sequenceNumber = 0;
    }

    /// <summary>
    /// Reads a complete MySQL packet from the stream.
    /// </summary>
    public async Task<byte[]> ReadPacketAsync(CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();

        // Read packet header: 3 bytes length + 1 byte sequence
        var header = new byte[4];
        var bytesRead = await _stream.ReadAsync(header, cancellationToken);
        if (bytesRead == 0)
            throw new EndOfStreamException("Connection closed");

        if (bytesRead < 4)
            throw new InvalidOperationException("Incomplete packet header");

        // Parse packet length (3 bytes, little-endian)
        var packetLength = header[0] | (header[1] << 8) | (header[2] << 16);
        var receivedSequence = header[3];

        // Verify sequence number
        if (receivedSequence != _sequenceNumber)
            throw new InvalidOperationException($"Sequence number mismatch: expected {_sequenceNumber}, got {receivedSequence}");

        _sequenceNumber = (byte)((_sequenceNumber + 1) % 256);

        // Handle multi-packet payloads (if length is 0xFFFFFF)
        if (packetLength == 0xFFFFFF)
        {
            // Multi-packet payload - read until we get a packet with length < 0xFFFFFF
            var chunks = new List<byte[]>();
            while (packetLength == 0xFFFFFF)
            {
                var chunk = new byte[packetLength];
                bytesRead = await _stream.ReadAsync(chunk, cancellationToken);
                if (bytesRead < packetLength)
                    throw new InvalidOperationException("Incomplete packet payload");

                chunks.Add(chunk);

                // Read next packet header
                bytesRead = await _stream.ReadAsync(header, cancellationToken);
                if (bytesRead < 4)
                    throw new InvalidOperationException("Incomplete packet header");

                packetLength = header[0] | (header[1] << 8) | (header[2] << 16);
                receivedSequence = header[3];

                if (receivedSequence != _sequenceNumber)
                    throw new InvalidOperationException($"Sequence number mismatch: expected {_sequenceNumber}, got {receivedSequence}");

                _sequenceNumber = (byte)((_sequenceNumber + 1) % 256);
            }

            // Read final chunk
            if (packetLength > 0)
            {
                var finalChunk = new byte[packetLength];
                bytesRead = await _stream.ReadAsync(finalChunk, cancellationToken);
                if (bytesRead < packetLength)
                    throw new InvalidOperationException("Incomplete packet payload");
                chunks.Add(finalChunk);
            }

            // Combine all chunks
            var totalLength = chunks.Sum(c => c.Length);
            var result = new byte[totalLength];
            var offset = 0;
            foreach (var chunk in chunks)
            {
                Buffer.BlockCopy(chunk, 0, result, offset, chunk.Length);
                offset += chunk.Length;
            }
            return result;
        }

        // Single packet
        if (packetLength == 0)
            return Array.Empty<byte>();

        var payload = new byte[packetLength];
        bytesRead = await _stream.ReadAsync(payload, cancellationToken);
        if (bytesRead < packetLength)
            throw new InvalidOperationException("Incomplete packet payload");

        return payload;
    }

    /// <summary>
    /// Reads a length-encoded integer from the current position.
    /// </summary>
    public static ulong ReadLengthEncodedInteger(byte[] data, ref int offset)
    {
        if (offset >= data.Length)
            throw new ArgumentOutOfRangeException(nameof(offset));

        var firstByte = data[offset++];
        if (firstByte < 251)
            return firstByte;

        if (firstByte == 0xFC)
        {
            if (offset + 2 > data.Length)
                throw new InvalidOperationException("Incomplete length-encoded integer");
            var value = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(offset));
            offset += 2;
            return value;
        }

        if (firstByte == 0xFD)
        {
            if (offset + 3 > data.Length)
                throw new InvalidOperationException("Incomplete length-encoded integer");
            var value = (ulong)(data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16));
            offset += 3;
            return value;
        }

        if (firstByte == 0xFE)
        {
            if (offset + 8 > data.Length)
                throw new InvalidOperationException("Incomplete length-encoded integer");
            var value = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(offset));
            offset += 8;
            return value;
        }

        throw new InvalidOperationException($"Invalid length-encoded integer prefix: 0x{firstByte:X2}");
    }

    /// <summary>
    /// Reads a null-terminated string from the current position.
    /// </summary>
    public static string ReadNullTerminatedString(byte[] data, ref int offset, Encoding encoding)
    {
        var startOffset = offset;
        while (offset < data.Length && data[offset] != 0)
            offset++;

        if (offset >= data.Length)
            throw new InvalidOperationException("String not null-terminated");

        var length = offset - startOffset;
        offset++; // Skip null terminator

        if (length == 0)
            return string.Empty;

        return encoding.GetString(data, startOffset, length);
    }

    /// <summary>
    /// Reads a length-encoded string from the current position.
    /// </summary>
    public static string ReadLengthEncodedString(byte[] data, ref int offset, Encoding encoding)
    {
        var length = (int)ReadLengthEncodedInteger(data, ref offset);
        if (length == 0)
            return string.Empty;

        if (offset + length > data.Length)
            throw new InvalidOperationException("Incomplete length-encoded string");

        var result = encoding.GetString(data, offset, length);
        offset += length;
        return result;
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
            throw new ObjectDisposedException(nameof(PacketReader));
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
        }
    }
}
