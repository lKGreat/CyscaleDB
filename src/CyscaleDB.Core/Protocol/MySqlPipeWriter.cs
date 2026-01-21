using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Text;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Protocol;

/// <summary>
/// High-performance MySQL packet writer using System.IO.Pipelines.
/// Provides efficient buffering with automatic flushing.
/// </summary>
public sealed class MySqlPipeWriter : IDisposable
{
    private readonly PipeWriter _pipeWriter;
    private byte _sequenceNumber;
    private bool _disposed;

    private const int PacketHeaderSize = 4;
    private const int MaxPacketSize = 0xFFFFFF; // 16MB - 1

    /// <summary>
    /// Creates a MySqlPipeWriter from a PipeWriter.
    /// </summary>
    private MySqlPipeWriter(PipeWriter pipeWriter)
    {
        _pipeWriter = pipeWriter;
        _sequenceNumber = 0;
    }

    /// <summary>
    /// Creates a MySqlPipeWriter from a stream with optional configuration.
    /// </summary>
    public static MySqlPipeWriter Create(Stream stream, MySqlServerOptions? options = null)
    {
        options ??= MySqlServerOptions.Default;

        var pipeOptions = new StreamPipeWriterOptions(
            pool: IoBufferPool.CreateMemoryPool(),
            minimumBufferSize: options.MinimumSegmentSize,
            leaveOpen: true);

        var pipeWriter = PipeWriter.Create(stream, pipeOptions);
        return new MySqlPipeWriter(pipeWriter);
    }

    /// <summary>
    /// Writes a MySQL packet asynchronously.
    /// Automatically splits large payloads into multiple packets.
    /// </summary>
    public async ValueTask WritePacketAsync(byte[] payload, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(MySqlPipeWriter));

        await WritePacketAsync(payload.AsMemory(), cancellationToken);
    }

    /// <summary>
    /// Writes a MySQL packet from a memory span asynchronously.
    /// </summary>
    public async ValueTask WritePacketAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(MySqlPipeWriter));

        if (payload.Length == 0)
        {
            WritePacketHeader(0);
            await FlushAsync(cancellationToken);
            return;
        }

        var offset = 0;
        while (offset < payload.Length)
        {
            var chunkSize = Math.Min(payload.Length - offset, MaxPacketSize);
            var isLastChunk = offset + chunkSize >= payload.Length;

            // For multi-packet payloads, all packets except the last must be exactly MaxPacketSize
            var packetLength = isLastChunk ? chunkSize : MaxPacketSize;

            // Write header
            WritePacketHeader(packetLength);

            // Write payload chunk
            var chunk = payload.Slice(offset, chunkSize);
            WriteRaw(chunk.Span);

            offset += chunkSize;
        }

        await FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Writes a packet header (3 bytes length + 1 byte sequence).
    /// </summary>
    private void WritePacketHeader(int length)
    {
        var span = _pipeWriter.GetSpan(PacketHeaderSize);
        span[0] = (byte)(length & 0xFF);
        span[1] = (byte)((length >> 8) & 0xFF);
        span[2] = (byte)((length >> 16) & 0xFF);
        span[3] = _sequenceNumber;
        _pipeWriter.Advance(PacketHeaderSize);
        _sequenceNumber = (byte)((_sequenceNumber + 1) % 256);
    }

    /// <summary>
    /// Writes raw bytes to the pipe.
    /// </summary>
    private void WriteRaw(ReadOnlySpan<byte> data)
    {
        var span = _pipeWriter.GetSpan(data.Length);
        data.CopyTo(span);
        _pipeWriter.Advance(data.Length);
    }

    /// <summary>
    /// Flushes the pipe writer.
    /// </summary>
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        await _pipeWriter.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Resets the sequence number (used after handshake).
    /// </summary>
    public void ResetSequence()
    {
        _sequenceNumber = 0;
    }

    /// <summary>
    /// Gets the current sequence number (next sequence to be used).
    /// </summary>
    public byte CurrentSequence => _sequenceNumber;

    #region Static Helper Methods

    /// <summary>
    /// Writes a length-encoded integer to a buffer.
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
            Span<byte> bytes = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(bytes, (ushort)value);
            buffer.Write(bytes);
        }
        else if (value < 16777216)
        {
            buffer.WriteByte(0xFD);
            Span<byte> bytes = stackalloc byte[3];
            bytes[0] = (byte)(value & 0xFF);
            bytes[1] = (byte)((value >> 8) & 0xFF);
            bytes[2] = (byte)((value >> 16) & 0xFF);
            buffer.Write(bytes);
        }
        else
        {
            buffer.WriteByte(0xFE);
            Span<byte> bytes = stackalloc byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
            buffer.Write(bytes);
        }
    }

    /// <summary>
    /// Writes a null-terminated string to a buffer.
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
    /// Writes a length-encoded string to a buffer.
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
    /// Writes a fixed-length string to a buffer (padded or truncated).
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
    /// Writes a 16-bit integer in little-endian format.
    /// </summary>
    public static void WriteInt16(MemoryStream buffer, short value)
    {
        Span<byte> bytes = stackalloc byte[2];
        BinaryPrimitives.WriteInt16LittleEndian(bytes, value);
        buffer.Write(bytes);
    }

    /// <summary>
    /// Writes a 32-bit integer in little-endian format.
    /// </summary>
    public static void WriteInt32(MemoryStream buffer, int value)
    {
        Span<byte> bytes = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        buffer.Write(bytes);
    }

    /// <summary>
    /// Writes a 64-bit integer in little-endian format.
    /// </summary>
    public static void WriteInt64(MemoryStream buffer, long value)
    {
        Span<byte> bytes = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        buffer.Write(bytes);
    }

    #endregion

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _pipeWriter.Complete();
        }
    }
}
