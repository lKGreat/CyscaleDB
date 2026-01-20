using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using CyscaleDB.Core.Auth;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Transactions;

namespace CyscaleDB.Core.Protocol;

/// <summary>
/// MySQL protocol capability flags.
/// </summary>
[Flags]
public enum MySqlCapabilities : uint
{
    None = 0,
    LongPassword = 1,
    FoundRows = 2,
    LongFlag = 4,
    ConnectWithDb = 8,
    NoSchema = 16,
    Compress = 32,
    Odbc = 64,
    LocalFiles = 128,
    IgnoreSpace = 256,
    Protocol41 = 512,
    Interactive = 1024,
    Ssl = 2048,
    IgnoreSigpipe = 4096,
    Transactions = 8192,
    Reserved = 16384,
    SecureConnection = 32768,
    MultiStatements = 65536,
    MultiResults = 131072,
    PsMultiResults = 262144,
    PluginAuth = 524288,
    ConnectAttrs = 1048576,
    PluginAuthLenencClientData = 2097152,
    CanHandleExpiredPasswords = 4194304,
    SessionTrack = 8388608,
    DeprecateEof = 16777216,
    OptionalResultsetMetadata = 33554432,
    ZstdCompressionAlgorithm = 67108864,
    QueryAttributes = 134217728,
    MultiFactor = 268435456,
    CapabilityExtension = 536870912,
}

/// <summary>
/// MySQL server status flags.
/// </summary>
[Flags]
public enum MySqlServerStatus : ushort
{
    InTransaction = 1,
    AutoCommit = 2,
    MoreResultsExist = 8,
    NoGoodIndexUsed = 16,
    NoIndexUsed = 32,
    CursorExists = 64,
    LastRowSent = 128,
    DbDropped = 256,
    NoBackslashEscapes = 512,
    MetadataChanged = 1024,
    QueryWasSlow = 2048,
    PsOutParams = 4096,
    InTransactionReadonly = 8192,
    SessionStateChanged = 16384,
}

/// <summary>
/// MySQL protocol server that handles client connections.
/// Implements MySQL 8.0+ protocol with CLIENT_DEPRECATE_EOF support.
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
    /// Server capabilities advertised to clients.
    /// </summary>
    public static MySqlCapabilities ServerCapabilities =>
        MySqlCapabilities.Protocol41 |
        MySqlCapabilities.ConnectWithDb |
        MySqlCapabilities.SecureConnection |
        MySqlCapabilities.PluginAuth |
        MySqlCapabilities.PluginAuthLenencClientData |
        MySqlCapabilities.Transactions |
        MySqlCapabilities.MultiStatements |
        MySqlCapabilities.MultiResults |
        MySqlCapabilities.DeprecateEof |
        MySqlCapabilities.SessionTrack |
        MySqlCapabilities.FoundRows |
        MySqlCapabilities.IgnoreSpace |
        MySqlCapabilities.Interactive;

    /// <summary>
    /// Creates a new MySQL protocol server.
    /// </summary>
    public MySqlServer(StorageEngine storageEngine, TransactionManager transactionManager, int port = Constants.DefaultPort)
    {
        _storageEngine = storageEngine ?? throw new ArgumentNullException(nameof(storageEngine));
        _transactionManager = transactionManager ?? throw new ArgumentNullException(nameof(transactionManager));
        _executor = new Executor(storageEngine.Catalog, transactionManager);
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
                var handshakePacket = Handshake.CreateHandshakePacket(
                    Constants.ServerVersion,
                    salt,
                    (uint)ServerCapabilities);

                await writer.WritePacketAsync(handshakePacket, cancellationToken);
                // MySQL handshake sequence: Server(0) → Client(1) → Server(2)
                // Synchronize reader's expected sequence with writer's current sequence
                // After sending seq 0, writer is now at 1, so reader should expect 1
                reader.SetSequence(writer.CurrentSequence);

                // Read handshake response (client sends with sequence 1)
                var responsePacket = await reader.ReadPacketAsync(cancellationToken);
                var response = Handshake.ParseHandshakeResponse(responsePacket);

                _logger.Debug("Client connecting: username={0}, database={1}, capabilities=0x{2:X8}",
                    response.Username, response.Database ?? "(none)", response.Capabilities);

                // Validate authentication
                var clientHost = GetClientHost(clientEndPoint);
                var authBytes = string.IsNullOrEmpty(response.AuthResponse)
                    ? Array.Empty<byte>()
                    : Encoding.Latin1.GetBytes(response.AuthResponse);

                if (!UserManager.Instance.ValidatePassword(response.Username, authBytes, salt, clientHost))
                {
                    _logger.Warning("Authentication failed for user '{0}'@'{1}'", response.Username, clientHost);
                    await SendErrorPacketAsync(writer, 1045, "28000",
                        $"Access denied for user '{response.Username}'@'{clientHost}' (using password: {(authBytes.Length > 0 ? "YES" : "NO")})",
                        cancellationToken);
                    return;
                }

                _logger.Info("Client authenticated: username={0}, database={1}",
                    response.Username, response.Database ?? "(none)");

                // Create client session with negotiated capabilities
                var clientCapabilities = (MySqlCapabilities)(response.Capabilities & (int)ServerCapabilities);
                var session = new ClientSession(clientCapabilities, _executor);
                session.Username = response.Username;

                // Send OK packet (authentication success)
                await SendOkPacketAsync(writer, session, cancellationToken);

                // Reset sequence numbers for command phase
                // Each command starts a new conversation with sequence 0
                writer.ResetSequence();
                reader.ResetSequence();

                // Set database if requested
                if (!string.IsNullOrEmpty(response.Database))
                {
                    try
                    {
                        session.CurrentDatabase = response.Database;
                    }
                    catch (DatabaseNotFoundException)
                    {
                        await SendErrorPacketAsync(writer, 1049, "42000", $"Unknown database '{response.Database}'", cancellationToken);
                        return;
                    }
                }

                // Command loop
                await ProcessCommandsAsync(reader, writer, session, cancellationToken);
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

    private async Task ProcessCommandsAsync(PacketReader reader, PacketWriter writer, ClientSession session, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Reset sequence numbers at the start of each command
                // MySQL protocol: each command starts with sequence 0
                reader.ResetSequence();
                writer.ResetSequence();

                var packet = await reader.ReadPacketAsync(cancellationToken);
                if (packet.Length == 0)
                    continue;

                var command = packet[0];
                var payload = packet.Length > 1 ? packet[1..] : Array.Empty<byte>();

                switch (command)
                {
                    case 0x01: // COM_QUIT
                        return; // Close connection

                    case 0x02: // COM_INIT_DB
                        await HandleInitDbAsync(writer, session, payload, cancellationToken);
                        break;

                    case 0x03: // COM_QUERY
                        var sql = Encoding.UTF8.GetString(payload);
                        _logger.Debug("Executing SQL: {0}", sql);
                        await ExecuteQueryAsync(writer, session, sql, cancellationToken);
                        break;

                    case 0x04: // COM_FIELD_LIST
                        await HandleFieldListAsync(writer, session, payload, cancellationToken);
                        break;

                    case 0x09: // COM_STATISTICS
                        await HandleStatisticsAsync(writer, cancellationToken);
                        break;

                    case 0x0E: // COM_PING
                        await SendOkPacketAsync(writer, session, cancellationToken);
                        break;

                    case 0x11: // COM_CHANGE_USER
                        // For now, just accept the change
                        await SendOkPacketAsync(writer, session, cancellationToken);
                        break;

                    case 0x19: // COM_RESET_CONNECTION
                        session.Reset();
                        await SendOkPacketAsync(writer, session, cancellationToken);
                        break;

                    case 0x1B: // COM_SET_OPTION
                        await HandleSetOptionAsync(writer, session, payload, cancellationToken);
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
                try
                {
                    await SendErrorPacketAsync(writer, 1064, "42000", $"Error executing query: {ex.Message}", cancellationToken);
                }
                catch
                {
                    // Ignore errors when sending error response
                }
            }
        }
    }

    private async Task HandleInitDbAsync(PacketWriter writer, ClientSession session, byte[] payload, CancellationToken cancellationToken)
    {
        var dbName = Encoding.UTF8.GetString(payload);
        try
        {
            session.CurrentDatabase = dbName;
            await SendOkPacketAsync(writer, session, cancellationToken);
        }
        catch (DatabaseNotFoundException)
        {
            await SendErrorPacketAsync(writer, 1049, "42000", $"Unknown database '{dbName}'", cancellationToken);
        }
    }

    private async Task HandleFieldListAsync(PacketWriter writer, ClientSession session, byte[] payload, CancellationToken cancellationToken)
    {
        // Parse table name (null-terminated)
        var offset = 0;
        var tableName = PacketReader.ReadNullTerminatedString(payload, ref offset, Encoding.UTF8);

        try
        {
            var schema = _storageEngine.Catalog.GetTableSchema(session.CurrentDatabase, tableName);
            if (schema == null)
            {
                await SendErrorPacketAsync(writer, 1146, "42S02", $"Table '{tableName}' doesn't exist", cancellationToken);
                return;
            }

            // Send column definitions
            foreach (var column in schema.Columns)
            {
                var resultColumn = new ResultColumn
                {
                    Name = column.Name,
                    DataType = column.DataType,
                    TableName = tableName,
                    DatabaseName = session.CurrentDatabase
                };
                await SendColumnDefinitionAsync(writer, session, resultColumn, cancellationToken);
            }

            // Send EOF or OK based on capabilities
            await SendEofOrOkPacketAsync(writer, session, cancellationToken);
        }
        catch (Exception ex)
        {
            await SendErrorPacketAsync(writer, 1064, "42000", ex.Message, cancellationToken);
        }
    }

    private async Task HandleStatisticsAsync(PacketWriter writer, CancellationToken cancellationToken)
    {
        var stats = _storageEngine.GetBufferPoolStats();
        var uptime = (long)(DateTime.UtcNow - Process.GetCurrentProcess().StartTime.ToUniversalTime()).TotalSeconds;
        var message = $"Uptime: {uptime}  Threads: 1  Questions: 0  Slow queries: 0  " +
                      $"Opens: 0  Flush tables: 0  Open tables: {stats.cachedPages}  " +
                      $"Queries per second avg: 0.000";

        var messageBytes = Encoding.UTF8.GetBytes(message);
        await writer.WritePacketAsync(messageBytes, cancellationToken);
    }

    private async Task HandleSetOptionAsync(PacketWriter writer, ClientSession session, byte[] payload, CancellationToken cancellationToken)
    {
        if (payload.Length >= 2)
        {
            var option = payload[0] | (payload[1] << 8);
            // 0 = MYSQL_OPTION_MULTI_STATEMENTS_ON
            // 1 = MYSQL_OPTION_MULTI_STATEMENTS_OFF
            session.MultiStatements = option == 0;
        }
        await SendEofOrOkPacketAsync(writer, session, cancellationToken);
    }

    private async Task ExecuteQueryAsync(PacketWriter writer, ClientSession session, string sql, CancellationToken cancellationToken)
    {
        try
        {
            var result = session.Executor.Execute(sql);

            switch (result.Type)
            {
                case ResultType.Query:
                    await SendResultSetAsync(writer, session, result.ResultSet!, cancellationToken);
                    break;

                case ResultType.Modification:
                    await SendOkPacketAsync(writer, session, cancellationToken, 
                        affectedRows: result.AffectedRows, 
                        lastInsertId: result.LastInsertId);
                    break;

                case ResultType.Ddl:
                    await SendOkPacketAsync(writer, session, cancellationToken, message: result.Message);
                    break;

                case ResultType.Empty:
                    await SendOkPacketAsync(writer, session, cancellationToken);
                    break;

                default:
                    await SendOkPacketAsync(writer, session, cancellationToken);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.Error("Query execution error", ex);
            await SendErrorPacketAsync(writer, 1064, "42000", ex.Message, cancellationToken);
        }
    }

    private async Task SendResultSetAsync(PacketWriter writer, ClientSession session, ResultSet resultSet, CancellationToken cancellationToken)
    {
        // Send column count packet
        var columnCountPacket = new MemoryStream();
        PacketWriter.WriteLengthEncodedInteger(columnCountPacket, (ulong)resultSet.ColumnCount);
        await writer.WritePacketAsync(columnCountPacket.ToArray(), cancellationToken);

        // Send column definition packets
        foreach (var column in resultSet.Columns)
        {
            await SendColumnDefinitionAsync(writer, session, column, cancellationToken);
        }

        // Send EOF packet (before rows) - only if not using DEPRECATE_EOF
        if (!session.UseDeprecateEof)
        {
            await SendEofPacketAsync(writer, session, cancellationToken);
        }

        // Send row data packets
        foreach (var row in resultSet.Rows)
        {
            await SendRowPacketAsync(writer, row, cancellationToken);
        }

        // Send EOF or OK packet (after rows)
        await SendEofOrOkPacketAsync(writer, session, cancellationToken);
    }

    private async Task SendColumnDefinitionAsync(PacketWriter writer, ClientSession session, ResultColumn column, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();

        // Catalog (always "def")
        PacketWriter.WriteLengthEncodedString(buffer, "def", Encoding.UTF8);

        // Schema (database name)
        PacketWriter.WriteLengthEncodedString(buffer, column.DatabaseName ?? session.CurrentDatabase, Encoding.UTF8);

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
        buffer.WriteByte(255);
        buffer.WriteByte(0);

        // Column length (4 bytes, little-endian)
        var columnLength = GetColumnLength(column.DataType);
        buffer.WriteByte((byte)(columnLength & 0xFF));
        buffer.WriteByte((byte)((columnLength >> 8) & 0xFF));
        buffer.WriteByte((byte)((columnLength >> 16) & 0xFF));
        buffer.WriteByte((byte)((columnLength >> 24) & 0xFF));

        // Column type
        buffer.WriteByte(GetMySqlColumnType(column.DataType));

        // Flags (2 bytes)
        buffer.WriteByte(0);
        buffer.WriteByte(0);

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

    private static void WriteValue(MemoryStream buffer, DataValue value)
    {
        // MySQL text protocol: all values are sent as length-encoded strings
        string strValue = value.Type switch
        {
            DataType.Int => value.AsInt().ToString(),
            DataType.BigInt => value.AsBigInt().ToString(),
            DataType.SmallInt => value.AsSmallInt().ToString(),
            DataType.TinyInt => value.AsTinyInt().ToString(),
            DataType.Boolean => value.AsBoolean() ? "1" : "0",  // Fixed: Boolean as string
            DataType.VarChar or DataType.Char or DataType.Text => value.AsString(),
            DataType.DateTime => value.AsDateTime().ToString("yyyy-MM-dd HH:mm:ss"),
            DataType.Date => value.AsDate().ToString("yyyy-MM-dd"),
            DataType.Time => value.AsTime().ToString(@"hh\:mm\:ss"),
            DataType.Float => value.AsFloat().ToString("G9"),
            DataType.Double => value.AsDouble().ToString("G17"),
            DataType.Decimal => value.AsDecimal().ToString("G"),
            _ => value.GetRawValue()?.ToString() ?? ""
        };

        PacketWriter.WriteLengthEncodedString(buffer, strValue, Encoding.UTF8);
    }

    private async Task SendOkPacketAsync(PacketWriter writer, ClientSession session, CancellationToken cancellationToken,
        long affectedRows = 0, long lastInsertId = 0, string? message = null)
    {
        using var buffer = new MemoryStream();

        // OK packet header (0x00)
        buffer.WriteByte(0x00);

        // Affected rows (length-encoded integer)
        PacketWriter.WriteLengthEncodedInteger(buffer, (ulong)affectedRows);

        // Last insert ID (length-encoded integer)
        PacketWriter.WriteLengthEncodedInteger(buffer, (ulong)lastInsertId);

        // Status flags (2 bytes, little-endian)
        var status = session.GetServerStatus();
        buffer.WriteByte((byte)(status & 0xFF));
        buffer.WriteByte((byte)((status >> 8) & 0xFF));

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

    private async Task SendEofPacketAsync(PacketWriter writer, ClientSession session, CancellationToken cancellationToken)
    {
        // Legacy EOF packet: 0xFE + warnings (2 bytes) + status flags (2 bytes)
        var status = session.GetServerStatus();
        var eofPacket = new byte[]
        {
            0xFE,
            0x00, 0x00, // warnings
            (byte)(status & 0xFF), (byte)((status >> 8) & 0xFF) // status flags
        };
        await writer.WritePacketAsync(eofPacket, cancellationToken);
    }

    private async Task SendEofOrOkPacketAsync(PacketWriter writer, ClientSession session, CancellationToken cancellationToken)
    {
        if (session.UseDeprecateEof)
        {
            // Send OK packet with 0xFE header (EOF replacement)
            using var buffer = new MemoryStream();

            // OK packet header (0xFE when replacing EOF)
            buffer.WriteByte(0xFE);

            // Affected rows = 0
            PacketWriter.WriteLengthEncodedInteger(buffer, 0);

            // Last insert ID = 0
            PacketWriter.WriteLengthEncodedInteger(buffer, 0);

            // Status flags (2 bytes, little-endian)
            var status = session.GetServerStatus();
            buffer.WriteByte((byte)(status & 0xFF));
            buffer.WriteByte((byte)((status >> 8) & 0xFF));

            // Warnings (2 bytes, little-endian)
            buffer.WriteByte(0);
            buffer.WriteByte(0);

            await writer.WritePacketAsync(buffer.ToArray(), cancellationToken);
        }
        else
        {
            await SendEofPacketAsync(writer, session, cancellationToken);
        }
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
        var sqlStateBytes = Encoding.UTF8.GetBytes(sqlState.PadRight(5)[..5]);
        buffer.Write(sqlStateBytes);

        // Error message (rest of packet)
        var messageBytes = Encoding.UTF8.GetBytes(message);
        buffer.Write(messageBytes);

        await writer.WritePacketAsync(buffer.ToArray(), cancellationToken);
    }

    private static uint GetColumnLength(DataType dataType)
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

    private static byte GetMySqlColumnType(DataType dataType)
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
            DataType.VarChar => 253,    // MYSQL_TYPE_VAR_STRING
            DataType.Char => 254,       // MYSQL_TYPE_STRING
            DataType.Text => 252,       // MYSQL_TYPE_BLOB
            DataType.DateTime => 12,    // MYSQL_TYPE_DATETIME
            DataType.Date => 10,        // MYSQL_TYPE_DATE
            DataType.Time => 11,        // MYSQL_TYPE_TIME
            DataType.Boolean => 1,      // MYSQL_TYPE_TINY (as TINYINT)
            _ => 253                    // Default to VAR_STRING
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

    /// <summary>
    /// Extracts the host address from a client endpoint string.
    /// </summary>
    private static string GetClientHost(string clientEndPoint)
    {
        if (string.IsNullOrEmpty(clientEndPoint))
            return "localhost";

        // Parse IPv4:port or [IPv6]:port format
        var colonIndex = clientEndPoint.LastIndexOf(':');
        if (colonIndex > 0)
        {
            var host = clientEndPoint[..colonIndex];
            // Remove brackets for IPv6
            if (host.StartsWith('[') && host.EndsWith(']'))
                host = host[1..^1];
            return host;
        }

        return clientEndPoint;
    }
}

/// <summary>
/// Represents a client session with negotiated capabilities.
/// </summary>
internal sealed class ClientSession
{
    private readonly Executor _executor;

    public MySqlCapabilities Capabilities { get; }
    public Executor Executor => _executor;
    public bool UseDeprecateEof => (Capabilities & MySqlCapabilities.DeprecateEof) != 0;
    public bool MultiStatements { get; set; } = true;
    public bool InTransaction { get; set; }
    public bool AutoCommit { get; set; } = true;
    public string Username { get; set; } = "root";

    public string CurrentDatabase
    {
        get => _executor.CurrentDatabase;
        set => _executor.CurrentDatabase = value;
    }

    public ClientSession(MySqlCapabilities capabilities, Executor executor)
    {
        Capabilities = capabilities;
        _executor = executor;
    }

    public ushort GetServerStatus()
    {
        ushort status = 0;
        if (AutoCommit)
            status |= (ushort)MySqlServerStatus.AutoCommit;
        if (InTransaction)
            status |= (ushort)MySqlServerStatus.InTransaction;
        return status;
    }

    public void Reset()
    {
        InTransaction = false;
        AutoCommit = true;
        MultiStatements = true;
    }
}
