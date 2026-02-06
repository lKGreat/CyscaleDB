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

        // Auth plugin data part 2 (12 bytes of random data + 1 null terminator = 13 bytes)
        if (salt.Length >= 20)
        {
            buffer.Write(salt, 8, 12);
        }
        else
        {
            // Pad with random data if salt is shorter
            var remaining = Math.Max(0, salt.Length - 8);
            if (remaining > 0)
                buffer.Write(salt, 8, remaining);
            for (var i = remaining; i < 12; i++)
                buffer.WriteByte((byte)Random.Shared.Next(1, 127));
        }
        // Null terminator for auth-plugin-data-part-2
        buffer.WriteByte(0);

        // Auth plugin name (null-terminated string) - "mysql_native_password" or "caching_sha2_password"
        var authPluginName = Encoding.UTF8.GetBytes("mysql_native_password");
        buffer.Write(authPluginName);
        buffer.WriteByte(0);

        return buffer.ToArray();
    }

    /// <summary>
    /// Generates a random salt for authentication.
    /// Salt must not contain null bytes or special characters that could cause parsing issues.
    /// </summary>
    public static byte[] GenerateSalt()
    {
        var salt = new byte[20];
        for (int i = 0; i < salt.Length; i++)
        {
            // Generate bytes in range 1-127 to avoid null bytes and high bytes
            salt[i] = (byte)(Random.Shared.Next(1, 127));
        }
        return salt;
    }

    /// <summary>
    /// Parses the client handshake response packet (HandshakeResponse41).
    /// </summary>
    public static ClientHandshakeResponse ParseHandshakeResponse(byte[] packet)
    {
        if (packet.Length < 32)
        {
            throw new InvalidOperationException($"Handshake response too short: {packet.Length} bytes");
        }

        var offset = 0;

        // Client capabilities (4 bytes, little-endian)
        var capabilities = packet[offset] | (packet[offset + 1] << 8) | (packet[offset + 2] << 16) | (packet[offset + 3] << 24);
        offset += 4;

        // Max packet size (4 bytes, little-endian) - skip
        offset += 4;

        // Character set (1 byte)
        int characterSet = 255; // utf8mb4
        if (offset < packet.Length)
        {
            characterSet = packet[offset];
        }
        offset += 1;

        // Reserved (23 bytes) - skip
        offset += 23;

        // Username (null-terminated string)
        var username = ReadNullTerminatedStringSafe(packet, ref offset);

        // Auth response - depends on capabilities
        string authResponse = "";
        const int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
        const int CLIENT_SECURE_CONNECTION = 0x00008000;

        if ((capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0)
        {
            // Length-encoded string
            authResponse = PacketReader.ReadLengthEncodedString(packet, ref offset, Encoding.UTF8);
        }
        else if ((capabilities & CLIENT_SECURE_CONNECTION) != 0)
        {
            // 1-byte length prefix + data
            if (offset < packet.Length)
            {
                var authLen = packet[offset++];
                if (authLen > 0 && offset + authLen <= packet.Length)
                {
                    authResponse = Encoding.UTF8.GetString(packet, offset, authLen);
                    offset += authLen;
                }
            }
        }
        else
        {
            // Null-terminated (old style)
            authResponse = ReadNullTerminatedStringSafe(packet, ref offset);
        }

        // Database name (null-terminated string, optional if CLIENT_CONNECT_WITH_DB is set)
        string? database = null;
        const int CLIENT_CONNECT_WITH_DB = 0x00000008;
        if ((capabilities & CLIENT_CONNECT_WITH_DB) != 0 && offset < packet.Length)
        {
            database = ReadNullTerminatedStringSafe(packet, ref offset);
            if (string.IsNullOrEmpty(database))
                database = null;
        }

        // Auth plugin name (null-terminated, optional if CLIENT_PLUGIN_AUTH is set)
        string? authPlugin = null;
        const int CLIENT_PLUGIN_AUTH = 0x00080000;
        if ((capabilities & CLIENT_PLUGIN_AUTH) != 0 && offset < packet.Length)
        {
            authPlugin = ReadNullTerminatedStringSafe(packet, ref offset);
        }
        // Client attributes (length-encoded, optional if CLIENT_CONNECT_ATTRS is set) - skip

        return new ClientHandshakeResponse
        {
            Capabilities = capabilities,
            Username = username,
            AuthResponse = authResponse,
            Database = database,
            AuthPlugin = authPlugin,
            CharacterSet = characterSet
        };
    }

    private static string ReadNullTerminatedStringSafe(byte[] packet, ref int offset)
    {
        if (offset >= packet.Length)
            return string.Empty;

        var startOffset = offset;
        while (offset < packet.Length && packet[offset] != 0)
            offset++;

        var length = offset - startOffset;
        if (offset < packet.Length)
            offset++; // Skip null terminator

        if (length == 0)
            return string.Empty;

        return Encoding.UTF8.GetString(packet, startOffset, length);
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
    public string? AuthPlugin { get; set; }
    public int CharacterSet { get; set; } = 255; // utf8mb4_general_ci
}
