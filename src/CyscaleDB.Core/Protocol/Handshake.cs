using System.Text;

namespace CyscaleDB.Core.Protocol;

/// <summary>
/// Handles MySQL protocol handshake and authentication.
/// </summary>
public static class Handshake
{
    /// <summary>
    /// Protocol version for MySQL 8.0+.
    /// </summary>
    private const byte ProtocolVersion = 10;

    /// <summary>
    /// Creates a handshake packet (Initial Handshake Packet).
    /// </summary>
    public static byte[] CreateHandshakePacket(string serverVersion, byte[] salt, uint capabilities)
    {
        using var buffer = new MemoryStream();

        // Protocol version (1 byte)
        buffer.WriteByte(ProtocolVersion);

        // Server version (null-terminated string)
        var versionBytes = Encoding.UTF8.GetBytes(serverVersion);
        buffer.Write(versionBytes);
        buffer.WriteByte(0);

        // Connection ID (4 bytes, little-endian)
        var connectionId = Random.Shared.Next(1, int.MaxValue);
        buffer.WriteByte((byte)(connectionId & 0xFF));
        buffer.WriteByte((byte)((connectionId >> 8) & 0xFF));
        buffer.WriteByte((byte)((connectionId >> 16) & 0xFF));
        buffer.WriteByte((byte)((connectionId >> 24) & 0xFF));

        // Auth plugin data part 1 (8 bytes)
        if (salt.Length < 8)
            throw new ArgumentException("Salt must be at least 8 bytes", nameof(salt));
        buffer.Write(salt, 0, 8);

        // Filler (1 byte)
        buffer.WriteByte(0);

        // Capabilities lower 2 bytes (2 bytes, little-endian)
        buffer.WriteByte((byte)(capabilities & 0xFF));
        buffer.WriteByte((byte)((capabilities >> 8) & 0xFF));

        // Character set (1 byte) - UTF8MB4 = 255
        buffer.WriteByte(255);

        // Status flags (2 bytes, little-endian)
        buffer.WriteByte(0);
        buffer.WriteByte(0);

        // Capabilities upper 2 bytes (2 bytes, little-endian)
        buffer.WriteByte((byte)((capabilities >> 16) & 0xFF));
        buffer.WriteByte((byte)((capabilities >> 24) & 0xFF));

        // Auth plugin data length (1 byte)
        // For MySQL 8.0+, this is typically 21 (8 + 13)
        buffer.WriteByte(21);

        // Reserved (10 bytes, all zeros)
        for (var i = 0; i < 10; i++)
            buffer.WriteByte(0);

        // Auth plugin data part 2 (13 bytes)
        if (salt.Length >= 21)
        {
            buffer.Write(salt, 8, 13);
        }
        else
        {
            // Pad with zeros if salt is shorter
            var remaining = Math.Min(13, salt.Length - 8);
            if (remaining > 0)
                buffer.Write(salt, 8, remaining);
            for (var i = remaining; i < 13; i++)
                buffer.WriteByte(0);
        }

        // Auth plugin name (null-terminated string) - "mysql_native_password" or "caching_sha2_password"
        var authPluginName = Encoding.UTF8.GetBytes("mysql_native_password");
        buffer.Write(authPluginName);
        buffer.WriteByte(0);

        return buffer.ToArray();
    }

    /// <summary>
    /// Generates a random salt for authentication.
    /// </summary>
    public static byte[] GenerateSalt()
    {
        var salt = new byte[20];
        Random.Shared.NextBytes(salt);
        return salt;
    }

    /// <summary>
    /// Parses the client handshake response packet.
    /// </summary>
    public static ClientHandshakeResponse ParseHandshakeResponse(byte[] packet)
    {
        var offset = 0;

        // Capabilities (4 bytes, little-endian)
        var capabilities = 0;
        if (packet.Length >= 4)
        {
            capabilities = packet[offset] | (packet[offset + 1] << 8) | (packet[offset + 2] << 16) | (packet[offset + 3] << 24);
            offset += 4;
        }

        // Max packet size (4 bytes, little-endian) - skip
        offset += 4;

        // Character set (1 byte) - skip
        offset += 1;

        // Reserved (23 bytes) - skip
        offset += 23;

        // Username (null-terminated string)
        var username = PacketReader.ReadNullTerminatedString(packet, ref offset, Encoding.UTF8);

        // Auth response (length-encoded string)
        var authResponse = PacketReader.ReadLengthEncodedString(packet, ref offset, Encoding.UTF8);

        // Database name (null-terminated string, optional)
        string? database = null;
        if (offset < packet.Length && packet[offset] != 0)
        {
            database = PacketReader.ReadNullTerminatedString(packet, ref offset, Encoding.UTF8);
        }

        return new ClientHandshakeResponse
        {
            Capabilities = capabilities,
            Username = username,
            AuthResponse = authResponse,
            Database = database
        };
    }
}

/// <summary>
/// Represents a client handshake response.
/// </summary>
public class ClientHandshakeResponse
{
    public int Capabilities { get; set; }
    public string Username { get; set; } = string.Empty;
    public string AuthResponse { get; set; } = string.Empty;
    public string? Database { get; set; }
}
