using System.Buffers.Binary;
using System.Text;

namespace CyscaleDB.Core.Protocol.Tds;

/// <summary>
/// Reads TDS protocol packets from a network stream.
/// TDS packet format:
///   [Type:1][Status:1][Length:2 (big-endian)][SPID:2][PacketId:1][Window:1][Payload...]
/// </summary>
public sealed class TdsPacketReader
{
    private readonly Stream _stream;
    private readonly byte[] _headerBuffer = new byte[TdsConstants.PacketHeaderSize];

    public TdsPacketReader(Stream stream)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
    }

    /// <summary>
    /// Reads a complete TDS message (may span multiple packets).
    /// Returns the packet type and the reassembled payload.
    /// </summary>
    public async Task<(byte PacketType, byte[] Payload)?> ReadMessageAsync(CancellationToken ct = default)
    {
        using var payload = new MemoryStream();
        byte packetType = 0;

        while (true)
        {
            // Read 8-byte header
            var bytesRead = await ReadExactAsync(_headerBuffer, 0, TdsConstants.PacketHeaderSize, ct);
            if (bytesRead < TdsConstants.PacketHeaderSize)
                return null; // Connection closed

            packetType = _headerBuffer[0];
            var status = _headerBuffer[1];
            var length = BinaryPrimitives.ReadUInt16BigEndian(_headerBuffer.AsSpan(2, 2));
            // SPID at offset 4-5, PacketId at 6, Window at 7 (ignored for now)

            if (length < TdsConstants.PacketHeaderSize)
                throw new InvalidDataException($"TDS packet length {length} is too small");

            var payloadLength = length - TdsConstants.PacketHeaderSize;
            if (payloadLength > 0)
            {
                var buf = new byte[payloadLength];
                var read = await ReadExactAsync(buf, 0, payloadLength, ct);
                if (read < payloadLength)
                    return null;
                payload.Write(buf, 0, payloadLength);
            }

            // Check if this is the last packet in the message
            if ((status & TdsConstants.StatusEndOfMessage) != 0)
                break;
        }

        return (packetType, payload.ToArray());
    }

    private async Task<int> ReadExactAsync(byte[] buffer, int offset, int count, CancellationToken ct)
    {
        int totalRead = 0;
        while (totalRead < count)
        {
            var read = await _stream.ReadAsync(buffer.AsMemory(offset + totalRead, count - totalRead), ct);
            if (read == 0) return totalRead;
            totalRead += read;
        }
        return totalRead;
    }
}
