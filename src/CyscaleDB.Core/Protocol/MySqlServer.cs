using System.Net;
using System.Net.Sockets;
using System.Text;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Transactions;

namespace CyscaleDB.Core.Protocol;

/// <summary>
/// MySQL protocol server that handles client connections.
/// </summary>
public sealed class MySqlServer : IDisposable
{
    private readonly StorageEngine _storageEngine;
    private readonly TransactionManager _transactionManager;
    private readonly Executor _executor;
    private readonly TcpListener _listener;
    private readonly Logger _logger;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private bool _disposed;
    private Task? _acceptTask;

    /// <summary>
    /// Gets the port the server is listening on.
    /// </summary>
    public int Port { get; }

    /// <summary>
    /// Creates a new MySQL protocol server.
    /// </summary>
    public MySqlServer(StorageEngine storageEngine, TransactionManager transactionManager, int port = Constants.DefaultPort)
    {
        _storageEngine = storageEngine ?? throw new ArgumentNullException(nameof(storageEngine));
        _transactionManager = transactionManager ?? throw new ArgumentNullException(nameof(transactionManager));
        _executor = new Executor(storageEngine.Catalog);
        _listener = new TcpListener(IPAddress.Any, port);
        _logger = LogManager.Default.GetLogger<MySqlServer>();
        _cancellationTokenSource = new CancellationTokenSource();
        Port = port;
    }

    /// <summary>
    /// Starts the server and begins accepting connections.
    /// </summary>
    public void Start()
    {
        EnsureNotDisposed();
        _listener.Start();
        _logger.Info("MySQL protocol server started on port {0}", Port);

        _acceptTask = Task.Run(AcceptConnectionsAsync, _cancellationTokenSource.Token);
    }

    /// <summary>
    /// Stops the server and closes all connections.
    /// </summary>
    public void Stop()
    {
        if (_disposed)
            return;

        _cancellationTokenSource.Cancel();
        _listener.Stop();

        try
        {
            _acceptTask?.Wait(TimeSpan.FromSeconds(5));
        }
        catch (AggregateException)
        {
            // Expected when cancelling
        }

        _logger.Info("MySQL protocol server stopped");
    }

    private async Task AcceptConnectionsAsync()
    {
        while (!_cancellationTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                var client = await _listener.AcceptTcpClientAsync();
                _ = Task.Run(() => HandleClientAsync(client, _cancellationTokenSource.Token), _cancellationTokenSource.Token);
            }
            catch (ObjectDisposedException)
            {
                // Listener was closed
                break;
            }
            catch (Exception ex)
            {
                if (!_cancellationTokenSource.Token.IsCancellationRequested)
                    _logger.Error("Error accepting connection", ex);
            }
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        var clientEndPoint = client.Client.RemoteEndPoint?.ToString() ?? "unknown";
        _logger.Debug("Client connected from {0}", clientEndPoint);

        try
        {
            using (client)
            {
                var stream = client.GetStream();
                var reader = new PacketReader(stream);
                var writer = new PacketWriter(stream);

                // Send handshake packet
                var salt = Handshake.GenerateSalt();
                var capabilities = GetServerCapabilities();
                var handshakePacket = Handshake.CreateHandshakePacket(
                    Constants.ServerVersion,
                    salt,
                    capabilities);

                await writer.WritePacketAsync(handshakePacket, cancellationToken);
                writer.ResetSequence();
                reader.ResetSequence();

                // Read handshake response
                var responsePacket = await reader.ReadPacketAsync(cancellationToken);
                var response = Handshake.ParseHandshakeResponse(responsePacket);

                _logger.Debug("Client authenticated: username={0}, database={1}", response.Username, response.Database ?? "(none)");

                // Send OK packet (authentication success)
                await SendOkPacketAsync(writer, cancellationToken);

                // Set database if requested
                if (!string.IsNullOrEmpty(response.Database))
                {
                    try
                    {
                        _executor.CurrentDatabase = response.Database;
                    }
                    catch (DatabaseNotFoundException)
                    {
                        await SendErrorPacketAsync(writer, 1049, "42000", $"Unknown database '{response.Database}'", cancellationToken);
                        return;
                    }
                }

                // Command loop
                await ProcessCommandsAsync(reader, writer, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.Error("Error handling client {0}", ex, clientEndPoint);
        }
        finally
        {
            _logger.Debug("Client disconnected: {0}", clientEndPoint);
        }
    }

    private async Task ProcessCommandsAsync(PacketReader reader, PacketWriter writer, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var packet = await reader.ReadPacketAsync(cancellationToken);
                if (packet.Length == 0)
                    continue;

                var command = packet[0];
                var payload = packet.Length > 1 ? packet[1..] : Array.Empty<byte>();

                switch (command)
                {
                    case 0x01: // COM_QUIT
                        return; // Close connection

                    case 0x0E: // COM_PING
                        await SendOkPacketAsync(writer, cancellationToken);
                        break;

                    case 0x03: // COM_QUERY
                        var sql = Encoding.UTF8.GetString(payload);
                        _logger.Debug("Executing SQL: {0}", sql);
                        await ExecuteQueryAsync(writer, sql, cancellationToken);
                        break;

                    default:
                        _logger.Warning("Unsupported command: 0x{0:X2}", command);
                        await SendErrorPacketAsync(writer, 1047, "08S01", $"Unsupported command: 0x{command:X2}", cancellationToken);
                        break;
                }
            }
            catch (EndOfStreamException)
            {
                // Client disconnected
                return;
            }
            catch (Exception ex)
            {
                _logger.Error("Error processing command", ex);
                await SendErrorPacketAsync(writer, 1064, "42000", $"Error executing query: {ex.Message}", cancellationToken);
            }
        }
    }

    private async Task ExecuteQueryAsync(PacketWriter writer, string sql, CancellationToken cancellationToken)
    {
        try
        {
            var result = _executor.Execute(sql);

            switch (result.Type)
            {
                case ResultType.Query:
                    await SendResultSetAsync(writer, result.ResultSet!, cancellationToken);
                    break;

                case ResultType.Modification:
                    await SendOkPacketAsync(writer, cancellationToken, affectedRows: result.AffectedRows);
                    break;

                case ResultType.Ddl:
                    await SendOkPacketAsync(writer, cancellationToken, message: result.Message);
                    break;

                case ResultType.Empty:
                    await SendOkPacketAsync(writer, cancellationToken);
                    break;

                default:
                    await SendOkPacketAsync(writer, cancellationToken);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.Error("Query execution error", ex);
            await SendErrorPacketAsync(writer, 1064, "42000", ex.Message, cancellationToken);
        }
    }

    private async Task SendResultSetAsync(PacketWriter writer, ResultSet resultSet, CancellationToken cancellationToken)
    {
        // Send column count packet
        var columnCountPacket = new MemoryStream();
        PacketWriter.WriteLengthEncodedInteger(columnCountPacket, (ulong)resultSet.ColumnCount);
        await writer.WritePacketAsync(columnCountPacket.ToArray(), cancellationToken);

        // Send column definition packets
        foreach (var column in resultSet.Columns)
        {
            await SendColumnDefinitionAsync(writer, column, cancellationToken);
        }

        // Send EOF packet (before rows)
        await SendEofPacketAsync(writer, cancellationToken);

        // Send row data packets
        foreach (var row in resultSet.Rows)
        {
            await SendRowPacketAsync(writer, row, cancellationToken);
        }

        // Send EOF packet (after rows)
        await SendEofPacketAsync(writer, cancellationToken);
    }

    private async Task SendColumnDefinitionAsync(PacketWriter writer, ResultColumn column, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();

        // Catalog (always "def")
        PacketWriter.WriteLengthEncodedString(buffer, "def", Encoding.UTF8);

        // Schema (database name)
        PacketWriter.WriteLengthEncodedString(buffer, column.DatabaseName ?? _executor.CurrentDatabase, Encoding.UTF8);

        // Table (table name, empty for expressions)
        PacketWriter.WriteLengthEncodedString(buffer, column.TableName ?? "", Encoding.UTF8);

        // Original table (same as table)
        PacketWriter.WriteLengthEncodedString(buffer, column.TableName ?? "", Encoding.UTF8);

        // Column name
        PacketWriter.WriteLengthEncodedString(buffer, column.Name, Encoding.UTF8);

        // Original column name (same as column name)
        PacketWriter.WriteLengthEncodedString(buffer, column.Name, Encoding.UTF8);

        // Length of fixed-length fields (0x0C = 12 bytes)
        buffer.WriteByte(0x0C);

        // Character set (UTF8MB4 = 255)
        var charsetBytes = new byte[2];
        charsetBytes[0] = 255;
        charsetBytes[1] = 0;
        buffer.Write(charsetBytes);

        // Column length
        var columnLength = GetColumnLength(column.DataType);
        var lengthBytes = BitConverter.GetBytes(columnLength);
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(lengthBytes);
        buffer.Write(lengthBytes);

        // Column type
        buffer.WriteByte(GetMySqlColumnType(column.DataType));

        // Flags
        var flagsBytes = new byte[2];
        flagsBytes[0] = 0;
        flagsBytes[1] = 0;
        buffer.Write(flagsBytes);

        // Decimals
        buffer.WriteByte(0);

        // Filler (2 bytes)
        buffer.WriteByte(0);
        buffer.WriteByte(0);

        await writer.WritePacketAsync(buffer.ToArray(), cancellationToken);
    }

    private async Task SendRowPacketAsync(PacketWriter writer, DataValue[] row, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();

        foreach (var value in row)
        {
            if (value.IsNull)
            {
                buffer.WriteByte(0xFB); // NULL marker
            }
            else
            {
                WriteValue(buffer, value);
            }
        }

        await writer.WritePacketAsync(buffer.ToArray(), cancellationToken);
    }

    private void WriteValue(MemoryStream buffer, DataValue value)
    {
        switch (value.Type)
        {
            case DataType.Int:
                var intVal = value.AsInt();
                // Convert to string for proper encoding (handles negatives)
                var intStr = intVal.ToString();
                PacketWriter.WriteLengthEncodedString(buffer, intStr, Encoding.UTF8);
                break;

            case DataType.BigInt:
                var bigIntVal = value.AsBigInt();
                var bigIntStr = bigIntVal.ToString();
                PacketWriter.WriteLengthEncodedString(buffer, bigIntStr, Encoding.UTF8);
                break;

            case DataType.SmallInt:
            case DataType.TinyInt:
                var smallIntVal = value.Type == DataType.SmallInt ? (long)value.AsSmallInt() : value.AsTinyInt();
                var smallStr = smallIntVal.ToString();
                PacketWriter.WriteLengthEncodedString(buffer, smallStr, Encoding.UTF8);
                break;

            case DataType.Boolean:
                buffer.WriteByte((byte)(value.AsBoolean() ? 1 : 0));
                break;

            case DataType.VarChar:
            case DataType.Char:
            case DataType.Text:
                PacketWriter.WriteLengthEncodedString(buffer, value.AsString(), Encoding.UTF8);
                break;

            case DataType.DateTime:
                var dt = value.AsDateTime();
                PacketWriter.WriteLengthEncodedString(buffer, dt.ToString("yyyy-MM-dd HH:mm:ss"), Encoding.UTF8);
                break;

            case DataType.Date:
                var date = value.AsDate();
                PacketWriter.WriteLengthEncodedString(buffer, date.ToString("yyyy-MM-dd"), Encoding.UTF8);
                break;

            case DataType.Time:
                var time = value.AsTime();
                PacketWriter.WriteLengthEncodedString(buffer, time.ToString("HH:mm:ss"), Encoding.UTF8);
                break;

            case DataType.Float:
                var floatVal = value.AsFloat();
                PacketWriter.WriteLengthEncodedString(buffer, floatVal.ToString("G"), Encoding.UTF8);
                break;

            case DataType.Double:
                var doubleVal = value.AsDouble();
                PacketWriter.WriteLengthEncodedString(buffer, doubleVal.ToString("G"), Encoding.UTF8);
                break;

            case DataType.Decimal:
                var decimalVal = value.AsDecimal();
                PacketWriter.WriteLengthEncodedString(buffer, decimalVal.ToString("G"), Encoding.UTF8);
                break;

            default:
                PacketWriter.WriteLengthEncodedString(buffer, value.GetRawValue()?.ToString() ?? "", Encoding.UTF8);
                break;
        }
    }

    private async Task SendOkPacketAsync(PacketWriter writer, CancellationToken cancellationToken, long affectedRows = 0, string? message = null)
    {
        using var buffer = new MemoryStream();

        // OK packet header (0x00)
        buffer.WriteByte(0x00);

        // Affected rows (length-encoded integer)
        PacketWriter.WriteLengthEncodedInteger(buffer, (ulong)affectedRows);

        // Last insert ID (length-encoded integer, 0 for now)
        PacketWriter.WriteLengthEncodedInteger(buffer, 0);

        // Status flags (2 bytes, little-endian)
        buffer.WriteByte(0);
        buffer.WriteByte(0);

        // Warnings (2 bytes, little-endian)
        buffer.WriteByte(0);
        buffer.WriteByte(0);

        // Message (optional, length-encoded string)
        if (!string.IsNullOrEmpty(message))
        {
            PacketWriter.WriteLengthEncodedString(buffer, message, Encoding.UTF8);
        }

        await writer.WritePacketAsync(buffer.ToArray(), cancellationToken);
    }

    private async Task SendEofPacketAsync(PacketWriter writer, CancellationToken cancellationToken)
    {
        // EOF packet: 0xFE + warnings (2 bytes) + status flags (2 bytes)
        var eofPacket = new byte[] { 0xFE, 0x00, 0x00, 0x00, 0x00 };
        await writer.WritePacketAsync(eofPacket, cancellationToken);
    }

    private async Task SendErrorPacketAsync(PacketWriter writer, int errorCode, string sqlState, string message, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();

        // Error packet header (0xFF)
        buffer.WriteByte(0xFF);

        // Error code (2 bytes, little-endian)
        buffer.WriteByte((byte)(errorCode & 0xFF));
        buffer.WriteByte((byte)((errorCode >> 8) & 0xFF));

        // SQL state marker ('#')
        buffer.WriteByte(0x23);

        // SQL state (5 bytes)
        var sqlStateBytes = Encoding.UTF8.GetBytes(sqlState.PadRight(5).Substring(0, 5));
        buffer.Write(sqlStateBytes);

        // Error message (null-terminated string)
        PacketWriter.WriteNullTerminatedString(buffer, message, Encoding.UTF8);

        await writer.WritePacketAsync(buffer.ToArray(), cancellationToken);
    }

    private int GetServerCapabilities()
    {
        // Basic capabilities for MySQL 8.0+
        // CLIENT_PROTOCOL_41 = 512
        // CLIENT_CONNECT_WITH_DB = 8
        // CLIENT_PLUGIN_AUTH = 0x00080000
        // CLIENT_SECURE_CONNECTION = 0x00008000
        return 512 | 8 | 0x00080000 | 0x00008000;
    }

    private uint GetColumnLength(DataType dataType)
    {
        return dataType switch
        {
            DataType.TinyInt => 4,
            DataType.SmallInt => 6,
            DataType.Int => 11,
            DataType.BigInt => 20,
            DataType.Float => 12,
            DataType.Double => 22,
            DataType.Decimal => 20,
            DataType.VarChar => 65535,
            DataType.Char => 255,
            DataType.Text => 65535,
            DataType.DateTime => 19,
            DataType.Date => 10,
            DataType.Time => 8,
            DataType.Boolean => 1,
            _ => 255
        };
    }

    private byte GetMySqlColumnType(DataType dataType)
    {
        return dataType switch
        {
            DataType.TinyInt => 1,      // MYSQL_TYPE_TINY
            DataType.SmallInt => 2,     // MYSQL_TYPE_SHORT
            DataType.Int => 3,          // MYSQL_TYPE_LONG
            DataType.BigInt => 8,       // MYSQL_TYPE_LONGLONG
            DataType.Float => 4,        // MYSQL_TYPE_FLOAT
            DataType.Double => 5,       // MYSQL_TYPE_DOUBLE
            DataType.Decimal => 246,    // MYSQL_TYPE_NEWDECIMAL
            DataType.VarChar => 253,    // MYSQL_TYPE_VARCHAR
            DataType.Char => 254,       // MYSQL_TYPE_STRING
            DataType.Text => 252,       // MYSQL_TYPE_BLOB
            DataType.DateTime => 12,    // MYSQL_TYPE_DATETIME
            DataType.Date => 10,        // MYSQL_TYPE_DATE
            DataType.Time => 11,        // MYSQL_TYPE_TIME
            DataType.Boolean => 1,      // MYSQL_TYPE_TINY (as TINYINT)
            _ => 253                    // Default to VARCHAR
        };
    }

    private void EnsureNotDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(MySqlServer));
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            Stop();
            _cancellationTokenSource.Dispose();
            _disposed = true;
        }
    }
}
