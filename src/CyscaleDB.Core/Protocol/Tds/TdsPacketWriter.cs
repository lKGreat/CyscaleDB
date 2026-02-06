using System.Buffers.Binary;
using System.Text;

namespace CyscaleDB.Core.Protocol.Tds;

/// <summary>
/// Writes TDS protocol packets to a network stream.
/// Handles packet splitting when the payload exceeds the negotiated packet size.
/// </summary>
public sealed class TdsPacketWriter
{
    private readonly Stream _stream;
    private int _packetSize;
    private byte _packetId;

    public TdsPacketWriter(Stream stream, int packetSize = TdsConstants.DefaultPacketSize)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _packetSize = packetSize;
        _packetId = 1;
    }

    /// <summary>
    /// Sets the negotiated packet size.
    /// </summary>
    public void SetPacketSize(int size)
    {
        _packetSize = Math.Clamp(size, 512, TdsConstants.MaxPacketSize);
    }

    /// <summary>
    /// Writes a complete TDS message, splitting into multiple packets if needed.
    /// </summary>
    public async Task WriteMessageAsync(byte packetType, byte[] payload, CancellationToken ct = default)
    {
        var maxPayloadPerPacket = _packetSize - TdsConstants.PacketHeaderSize;
        var offset = 0;

        while (offset < payload.Length)
        {
            var remaining = payload.Length - offset;
            var chunkSize = Math.Min(remaining, maxPayloadPerPacket);
            var isLast = (offset + chunkSize) >= payload.Length;

            var header = new byte[TdsConstants.PacketHeaderSize];
            header[0] = packetType;
            header[1] = isLast ? TdsConstants.StatusEndOfMessage : TdsConstants.StatusNormal;
            BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(2, 2),
                (ushort)(TdsConstants.PacketHeaderSize + chunkSize));
            header[4] = 0; // SPID high
            header[5] = 0; // SPID low
            header[6] = _packetId++;
            header[7] = 0; // Window

            await _stream.WriteAsync(header, ct);
            await _stream.WriteAsync(payload.AsMemory(offset, chunkSize), ct);
            await _stream.FlushAsync(ct);

            offset += chunkSize;
        }
    }

    /// <summary>
    /// Writes a complete TDS message from a MemoryStream.
    /// </summary>
    public async Task WriteMessageAsync(byte packetType, MemoryStream ms, CancellationToken ct = default)
    {
        await WriteMessageAsync(packetType, ms.ToArray(), ct);
    }

    /// <summary>
    /// Writes a pre-built raw packet (header already included).
    /// Used for PreLogin responses where the header is built manually.
    /// </summary>
    public async Task WriteRawAsync(byte[] data, CancellationToken ct = default)
    {
        await _stream.WriteAsync(data, ct);
        await _stream.FlushAsync(ct);
    }
}
