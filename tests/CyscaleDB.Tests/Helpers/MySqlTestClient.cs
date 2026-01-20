using System.Net.Sockets;
using System.Text;
using CyscaleDB.Core.Protocol;

namespace CyscaleDB.Tests.Helpers;

/// <summary>
/// Simple MySQL protocol client for testing purposes.
/// </summary>
public sealed class MySqlTestClient : IDisposable
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    private readonly PacketReader _reader;
    private readonly PacketWriter _writer;
    private bool _authenticated;
    private bool _disposed;

    public MySqlTestClient(string host, int port)
    {
        _client = new TcpClient();
        _client.Connect(host, port);
        _stream = _client.GetStream();
        _reader = new PacketReader(_stream);
        _writer = new PacketWriter(_stream);
    }

    /// <summary>
    /// Performs handshake authentication.
    /// </summary>
    public async Task AuthenticateAsync(string username = "test", string? database = null, CancellationToken cancellationToken = default)
    {
        if (_authenticated)
            return;

        // Read handshake packet
        var handshakePacket = await _reader.ReadPacketAsync(cancellationToken);
        
        // Parse handshake (simplified - we just need to send a response)
        var salt = new byte[20];
        Array.Copy(handshakePacket, 5, salt, 0, Math.Min(8, salt.Length));
        if (handshakePacket.Length > 24)
        {
            Array.Copy(handshakePacket, 24, salt, 8, Math.Min(13, salt.Length - 8));
        }

        // Build handshake response
        using var response = new MemoryStream();
        
        // Capabilities (4 bytes)
        var capabilities = 512 | 8 | 0x00080000 | 0x00008000; // CLIENT_PROTOCOL_41 | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH | CLIENT_SECURE_CONNECTION
        response.WriteByte((byte)(capabilities & 0xFF));
        response.WriteByte((byte)((capabilities >> 8) & 0xFF));
        response.WriteByte((byte)((capabilities >> 16) & 0xFF));
        response.WriteByte((byte)((capabilities >> 24) & 0xFF));

        // Max packet size (4 bytes)
        var maxPacketSize = BitConverter.GetBytes(0xFFFFFF);
        if (!BitConverter.IsLittleEndian) Array.Reverse(maxPacketSize);
        response.Write(maxPacketSize);

        // Character set (1 byte) - UTF8MB4 = 255
        response.WriteByte(255);

        // Reserved (23 bytes)
        for (int i = 0; i < 23; i++)
            response.WriteByte(0);

        // Username (null-terminated)
        var usernameBytes = Encoding.UTF8.GetBytes(username);
        response.Write(usernameBytes);
        response.WriteByte(0);

        // Auth response (empty for testing - no password verification)
        response.WriteByte(0); // Length-encoded: 0 = empty

        // Database name (null-terminated, optional)
        if (!string.IsNullOrEmpty(database))
        {
            var dbBytes = Encoding.UTF8.GetBytes(database);
            response.Write(dbBytes);
        }
        response.WriteByte(0);

        // Send response
        await _writer.WritePacketAsync(response.ToArray(), cancellationToken);
        _writer.ResetSequence();
        _reader.ResetSequence();

        // Read OK packet
        var okPacket = await _reader.ReadPacketAsync(cancellationToken);
        if (okPacket.Length == 0 || okPacket[0] != 0x00)
        {
            throw new InvalidOperationException("Authentication failed");
        }

        _authenticated = true;
    }

    /// <summary>
    /// Executes a query and returns the result set.
    /// </summary>
    public async Task<QueryResult> QueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        if (!_authenticated)
            throw new InvalidOperationException("Not authenticated");

        // Send COM_QUERY
        var sqlBytes = Encoding.UTF8.GetBytes(sql);
        var commandPacket = new byte[sqlBytes.Length + 1];
        commandPacket[0] = 0x03; // COM_QUERY
        Array.Copy(sqlBytes, 0, commandPacket, 1, sqlBytes.Length);
        
        await _writer.WritePacketAsync(commandPacket, cancellationToken);

        // Read response
        var response = await _reader.ReadPacketAsync(cancellationToken);
        
        if (response[0] == 0xFF) // Error packet
        {
            var errorCode = response[1] | (response[2] << 8);
            var errorOffset = 4; // Skip header and error code
            errorOffset += 1; // Skip SQL state marker '#'
            errorOffset += 5; // Skip SQL state
            var errorMessage = PacketReader.ReadNullTerminatedString(response, ref errorOffset, Encoding.UTF8);
            throw new MySqlException(errorCode, errorMessage);
        }

        if (response[0] == 0x00) // OK packet
        {
            return new QueryResult { Type = QueryResultType.Ok };
        }

        // Result set
        var columnCount = ReadLengthEncodedInteger(response, 0, out var responseOffset);
        
        // Read column definitions
        var columns = new List<ColumnInfo>();
        for (int i = 0; i < (int)columnCount; i++)
        {
            var colPacket = await _reader.ReadPacketAsync(cancellationToken);
            var col = ParseColumnDefinition(colPacket);
            columns.Add(col);
        }

        // Read EOF packet
        var eof1 = await _reader.ReadPacketAsync(cancellationToken);
        if (eof1[0] != 0xFE)
            throw new InvalidOperationException("Expected EOF packet");

        // Read rows
        var rows = new List<object[]>();
        while (true)
        {
            var rowPacket = await _reader.ReadPacketAsync(cancellationToken);
            if (rowPacket[0] == 0xFE) // EOF packet
                break;

            var row = ParseRow(rowPacket, columns.Count);
            rows.Add(row);
        }

        return new QueryResult
        {
            Type = QueryResultType.ResultSet,
            Columns = columns,
            Rows = rows
        };
    }

    private ColumnInfo ParseColumnDefinition(byte[] packet)
    {
        var offset = 0;
        var catalog = PacketReader.ReadLengthEncodedString(packet, ref offset, Encoding.UTF8);
        var schema = PacketReader.ReadLengthEncodedString(packet, ref offset, Encoding.UTF8);
        var table = PacketReader.ReadLengthEncodedString(packet, ref offset, Encoding.UTF8);
        var origTable = PacketReader.ReadLengthEncodedString(packet, ref offset, Encoding.UTF8);
        var name = PacketReader.ReadLengthEncodedString(packet, ref offset, Encoding.UTF8);
        var origName = PacketReader.ReadLengthEncodedString(packet, ref offset, Encoding.UTF8);
        
        // Skip remaining fields (we don't need them for basic testing)
        return new ColumnInfo
        {
            Name = name,
            Table = table,
            Schema = schema
        };
    }

    private object[] ParseRow(byte[] packet, int columnCount)
    {
        var row = new object[columnCount];
        var offset = 0;

        for (int i = 0; i < columnCount; i++)
        {
            if (packet[offset] == 0xFB) // NULL
            {
                row[i] = null!;
                offset++;
            }
            else
            {
                var value = PacketReader.ReadLengthEncodedString(packet, ref offset, Encoding.UTF8);
                row[i] = value;
            }
        }

        return row;
    }

    private ulong ReadLengthEncodedInteger(byte[] data, int startOffset, out int newOffset)
    {
        newOffset = startOffset;
        return PacketReader.ReadLengthEncodedInteger(data, ref newOffset);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        // Send COM_QUIT
        if (_authenticated)
        {
            try
            {
                var quitPacket = new byte[] { 0x01 }; // COM_QUIT
                _writer.WritePacketAsync(quitPacket).Wait(TimeSpan.FromSeconds(1));
            }
            catch
            {
                // Ignore errors during cleanup
            }
        }

        _writer.Dispose();
        _reader.Dispose();
        _stream.Dispose();
        _client.Dispose();
        _disposed = true;
    }
}

public class QueryResult
{
    public QueryResultType Type { get; set; }
    public List<ColumnInfo> Columns { get; set; } = new();
    public List<object[]> Rows { get; set; } = new();
}

public enum QueryResultType
{
    Ok,
    ResultSet,
    Error
}

public class ColumnInfo
{
    public string Name { get; set; } = string.Empty;
    public string Table { get; set; } = string.Empty;
    public string Schema { get; set; } = string.Empty;
}

public class MySqlException : Exception
{
    public int ErrorCode { get; }

    public MySqlException(int errorCode, string message) : base(message)
    {
        ErrorCode = errorCode;
    }
}
