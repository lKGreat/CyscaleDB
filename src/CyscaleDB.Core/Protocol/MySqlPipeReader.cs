using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Text;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Protocol;

/// <summary>
/// High-performance MySQL packet reader using System.IO.Pipelines.
/// Provides zero-copy parsing with proper sticky packet handling.
/// </summary>
public sealed class MySqlPipeReader : IDisposable
{
    private readonly PipeReader _pipeReader;
    private byte _sequenceNumber;
    private bool _disposed;

    private const int PacketHeaderSize = 4;
    private const int MaxPacketSize = 0xFFFFFF; // 16MB - 1

    /// <summary>
    /// Creates a MySqlPipeReader from a PipeReader.
    /// </summary>
    private MySqlPipeReader(PipeReader pipeReader)
    {
        _pipeReader = pipeReader;
        _sequenceNumber = 0;
    }

    /// <summary>
    /// Creates a MySqlPipeReader from a stream with optional configuration.
    /// </summary>
    public static MySqlPipeReader Create(Stream stream, MySqlServerOptions? options = null)
    {
        options ??= MySqlServerOptions.Default;

        var pipeOptions = new StreamPipeReaderOptions(
            pool: IoBufferPool.CreateMemoryPool(),
            bufferSize: options.MinimumSegmentSize,
            minimumReadSize: options.MinimumSegmentSize / 2,
            leaveOpen: true);

        var pipeReader = PipeReader.Create(stream, pipeOptions);
        return new MySqlPipeReader(pipeReader);
    }

    /// <summary>
    /// Reads a complete MySQL packet asynchronously.
    /// Handles sticky packets and multi-packet payloads correctly.
    /// </summary>
    public async ValueTask<byte[]?> ReadPacketAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(MySqlPipeReader));

        List<byte[]>? multiPacketChunks = null;

        while (true)
        {
            var result = await _pipeReader.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            if (result.IsCanceled)
                return null;

            if (TryParsePacket(ref buffer, out var packet, out var consumed, out var isMultiPacket))
            {
                _pipeReader.AdvanceTo(consumed);

                if (isMultiPacket)
                {
                    // Multi-packet payload - need to read more packets
                    multiPacketChunks ??= new List<byte[]>();
                    multiPacketChunks.Add(packet!);

                    // Continue reading if this packet was exactly MaxPacketSize
                    if (packet!.Length == MaxPacketSize)
                        continue;

                    // Combine all chunks
                    return CombineChunks(multiPacketChunks);
                }

                if (multiPacketChunks != null)
                {
                    // Final chunk of multi-packet payload
                    multiPacketChunks.Add(packet!);
                    return CombineChunks(multiPacketChunks);
                }

                return packet;
            }

            if (result.IsCompleted)
            {
                if (buffer.Length > 0)
                    throw new InvalidOperationException("Incomplete packet at end of stream");
                return null;
            }

            // Need more data - tell the pipe how much we've examined
            _pipeReader.AdvanceTo(buffer.Start, buffer.End);
        }
    }

    /// <summary>
    /// Tries to parse a MySQL packet from the buffer.
    /// </summary>
    private bool TryParsePacket(
        ref ReadOnlySequence<byte> buffer,
        out byte[]? packet,
        out SequencePosition consumed,
        out bool isMultiPacket)
    {
        packet = null;
        consumed = buffer.Start;
        isMultiPacket = false;

        if (buffer.Length < PacketHeaderSize)
            return false;

        // Read header
        Span<byte> header = stackalloc byte[PacketHeaderSize];
        buffer.Slice(0, PacketHeaderSize).CopyTo(header);

        // Parse packet length (3 bytes, little-endian)
        var packetLength = header[0] | (header[1] << 8) | (header[2] << 16);
        var receivedSequence = header[3];

        // Verify sequence number
        if (receivedSequence != _sequenceNumber)
        {
            throw new InvalidOperationException(
                $"Sequence number mismatch: expected {_sequenceNumber}, got {receivedSequence}");
        }

        // Check if we have the complete packet
        var totalLength = PacketHeaderSize + packetLength;
        if (buffer.Length < totalLength)
            return false;

        // Update sequence number
        _sequenceNumber = (byte)((_sequenceNumber + 1) % 256);

        // Check if this is a multi-packet payload
        isMultiPacket = packetLength == MaxPacketSize;

        // Extract payload
        if (packetLength == 0)
        {
            packet = Array.Empty<byte>();
        }
        else
        {
            packet = new byte[packetLength];
            buffer.Slice(PacketHeaderSize, packetLength).CopyTo(packet);
        }

        consumed = buffer.GetPosition(totalLength);
        return true;
    }

    /// <summary>
    /// Combines multiple packet chunks into a single array.
    /// </summary>
    private static byte[] CombineChunks(List<byte[]> chunks)
    {
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

    /// <summary>
    /// Resets the sequence number (used after handshake).
    /// </summary>
    public void ResetSequence()
    {
        _sequenceNumber = 0;
    }

    /// <summary>
    /// Sets the expected sequence number.
    /// Used to synchronize with writer during handshake.
    /// </summary>
    public void SetSequence(byte sequence)
    {
        _sequenceNumber = sequence;
    }

    /// <summary>
    /// Gets the current expected sequence number.
    /// </summary>
    public byte CurrentSequence => _sequenceNumber;

    #region Static Helper Methods

    /// <summary>
    /// Reads a length-encoded integer from the data.
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
    /// Reads a null-terminated string from the data.
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
    /// Reads a length-encoded string from the data.
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
    /// Reads a fixed-length string from the data.
    /// </summary>
    public static string ReadFixedString(byte[] data, ref int offset, int length, Encoding encoding)
    {
        if (offset + length > data.Length)
            throw new InvalidOperationException("Incomplete fixed-length string");

        var result = encoding.GetString(data, offset, length);
        offset += length;
        return result.TrimEnd('\0');
    }

    #endregion

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _pipeReader.Complete();
        }
    }
}
