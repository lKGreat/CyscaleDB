using System.Collections.Concurrent;
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
/// MySQL protocol server with optimized networking and connection management.
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
    private readonly MySqlServerOptions _options;
    private readonly ConcurrentDictionary<long, ClientSession> _sessions;
    private bool _disposed;
    private Task? _acceptTask;
    private Task? _healthCheckTask;

    // Statistics
    private long _totalConnectionsReceived;
    private long _rejectedConnections;
    private long _totalQueriesExecuted;

    /// <summary>
    /// Gets the port the server is listening on.
    /// </summary>
    public int Port => _options.Port;

    /// <summary>
    /// Gets the current number of active connections.
    /// </summary>
    public int ActiveConnections => _sessions.Count;

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
    /// Creates a new MySQL protocol server with default options.
    /// </summary>
    public MySqlServer(StorageEngine storageEngine, TransactionManager transactionManager, int port = Constants.DefaultPort)
        : this(storageEngine, transactionManager, new MySqlServerOptions { Port = port })
    {
    }

    /// <summary>
    /// Creates a new MySQL protocol server with custom options.
    /// </summary>
    public MySqlServer(StorageEngine storageEngine, TransactionManager transactionManager, MySqlServerOptions options)
    {
        _storageEngine = storageEngine ?? throw new ArgumentNullException(nameof(storageEngine));
        _transactionManager = transactionManager ?? throw new ArgumentNullException(nameof(transactionManager));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _executor = new Executor(storageEngine.Catalog, transactionManager);
        _sessions = new ConcurrentDictionary<long, ClientSession>();
        _logger = LogManager.Default.GetLogger<MySqlServer>();
        _cancellationTokenSource = new CancellationTokenSource();

        // Create listener with configured endpoint
        var endpoint = new IPEndPoint(
            IPAddress.Parse(_options.BindAddress),
            _options.Port);
        _listener = new TcpListener(endpoint);

        // Configure listener socket
        ConfigureListenerSocket(_listener.Server);
    }

    /// <summary>
    /// Configures the listener socket with system-level optimizations.
    /// </summary>
    private void ConfigureListenerSocket(Socket socket)
    {
        if (_options.ReuseAddress)
        {
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        }

        socket.ReceiveBufferSize = _options.ReceiveBufferSize;
        socket.SendBufferSize = _options.SendBufferSize;
    }

    /// <summary>
    /// Configures a client socket with system-level optimizations.
    /// </summary>
    private void ConfigureClientSocket(Socket socket)
    {
        // Disable Nagle's algorithm for lower latency
        socket.NoDelay = _options.TcpNoDelay;

        // Configure TCP Keep-Alive
        if (_options.TcpKeepAlive)
        {
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

            // Set keep-alive timing (platform-specific)
            try
            {
                socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, _options.TcpKeepAliveTime);
                socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, _options.TcpKeepAliveInterval);
                socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, _options.TcpKeepAliveRetryCount);
            }
            catch (SocketException)
            {
                // Some platforms may not support all keep-alive options
                _logger.Trace("Some TCP keep-alive options not supported on this platform");
            }
        }

        // Set buffer sizes
        socket.ReceiveBufferSize = _options.ReceiveBufferSize;
        socket.SendBufferSize = _options.SendBufferSize;
    }

    /// <summary>
    /// Starts the server and begins accepting connections.
    /// </summary>
    public void Start()
    {
        EnsureNotDisposed();
        _listener.Start(_options.Backlog);
        _logger.Info("MySQL protocol server started on {0}:{1}", _options.BindAddress, _options.Port);

        _acceptTask = Task.Run(AcceptConnectionsAsync, _cancellationTokenSource.Token);

        // Start health check task if configured
        if (_options.HealthCheckInterval > TimeSpan.Zero)
        {
            _healthCheckTask = Task.Run(RunHealthCheckAsync, _cancellationTokenSource.Token);
            _logger.Debug("Health check task started with interval {0}s", _options.HealthCheckInterval.TotalSeconds);
        }
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

        // Close all active sessions
        foreach (var session in _sessions.Values)
        {
            try
            {
                session.Dispose();
            }
            catch (Exception ex)
            {
                _logger.Trace("Error disposing session: {0}", ex.Message);
            }
        }
        _sessions.Clear();

        try
        {
            _acceptTask?.Wait(TimeSpan.FromSeconds(5));
            _healthCheckTask?.Wait(TimeSpan.FromSeconds(2));
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
                var client = await _listener.AcceptTcpClientAsync(_cancellationTokenSource.Token);
                Interlocked.Increment(ref _totalConnectionsReceived);

                // Check max connections limit
                if (_sessions.Count >= _options.MaxConnections)
                {
                    Interlocked.Increment(ref _rejectedConnections);
                    _logger.Warning("Connection rejected: max connections ({0}) reached", _options.MaxConnections);
                    await RejectConnectionAsync(client, "Too many connections");
                    continue;
                }

                // Configure client socket
                ConfigureClientSocket(client.Client);

                _ = Task.Run(() => HandleClientAsync(client, _cancellationTokenSource.Token), _cancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (Exception ex)
            {
                if (!_cancellationTokenSource.Token.IsCancellationRequested)
                    _logger.Error("Error accepting connection", ex);
            }
        }
    }

    /// <summary>
    /// Rejects a connection with an error message.
    /// </summary>
    private static async Task RejectConnectionAsync(TcpClient tcpClient, string errorMessage)
    {
        try
        {
            using (tcpClient)
            {
                var stream = tcpClient.GetStream();
                var writer = new PacketWriter(stream);

                // Send error packet
                using var buffer = new MemoryStream();
                buffer.WriteByte(0xFF); // Error packet header
                buffer.WriteByte(0x15); // Error code low byte (1040)
                buffer.WriteByte(0x04); // Error code high byte
                buffer.WriteByte(0x23); // SQL state marker '#'
                var sqlStateBytes = Encoding.UTF8.GetBytes("08004");
                buffer.Write(sqlStateBytes);
                var messageBytes = Encoding.UTF8.GetBytes(errorMessage);
                buffer.Write(messageBytes);

                await writer.WritePacketAsync(buffer.ToArray());
            }
        }
        catch
        {
            // Ignore errors when rejecting
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        var clientEndPoint = client.Client.RemoteEndPoint?.ToString() ?? "unknown";
        _logger.Debug("Client connected from {0}", clientEndPoint);
        ClientSession? session = null;

        try
        {
            using (client)
            {
                var stream = client.GetStream();

                // Create pipe-based reader/writer
                var pipeReader = MySqlPipeReader.Create(stream, _options);
                var pipeWriter = MySqlPipeWriter.Create(stream, _options);

                // Also keep legacy writers for compatibility during handshake
                var legacyWriter = new PacketWriter(stream);

                // Send handshake packet
                var salt = Handshake.GenerateSalt();
                var handshakePacket = Handshake.CreateHandshakePacket(
                    Constants.ServerVersion,
                    salt,
                    (uint)ServerCapabilities);

                await legacyWriter.WritePacketAsync(handshakePacket, cancellationToken);
                pipeReader.SetSequence(legacyWriter.CurrentSequence);

                // Read handshake response using pipe reader
                var responsePacket = await pipeReader.ReadPacketAsync(cancellationToken);
                if (responsePacket == null)
                {
                    _logger.Debug("Client disconnected during handshake");
                    return;
                }

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

                    // Synchronize pipe writer with legacy writer sequence
                    pipeWriter = MySqlPipeWriter.Create(stream, _options);
                    
                    await SendErrorPacketAsync(pipeWriter, 1045, "28000",
                        $"Access denied for user '{response.Username}'@'{clientHost}' (using password: {(authBytes.Length > 0 ? "YES" : "NO")})",
                        cancellationToken);
                    return;
                }

                _logger.Info("Client authenticated: username={0}, database={1}",
                    response.Username, response.Database ?? "(none)");

                // Create client session with negotiated capabilities
                var clientCapabilities = (MySqlCapabilities)(response.Capabilities & (int)ServerCapabilities);
                
                // Reset pipe writer for command phase
                pipeWriter = MySqlPipeWriter.Create(stream, _options);
                
                session = new ClientSession(clientCapabilities, new Executor(_storageEngine.Catalog, _transactionManager), pipeReader, pipeWriter)
                {
                    Username = response.Username,
                    RemoteAddress = clientEndPoint
                };

                // Register session
                _sessions[session.Id] = session;

                // Send OK packet (authentication success)
                await SendOkPacketAsync(pipeWriter, session, cancellationToken);

                // Reset sequence numbers for command phase
                pipeWriter.ResetSequence();
                pipeReader.ResetSequence();

                // Set database if requested
                if (!string.IsNullOrEmpty(response.Database))
                {
                    try
                    {
                        session.CurrentDatabase = response.Database;
                    }
                    catch (DatabaseNotFoundException)
                    {
                        await SendErrorPacketAsync(pipeWriter, 1049, "42000", $"Unknown database '{response.Database}'", cancellationToken);
                        return;
                    }
                }

                // Command loop
                await ProcessCommandsAsync(pipeReader, pipeWriter, session, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
        catch (Exception ex)
        {
            _logger.Error("Error handling client {0}", ex, clientEndPoint);
        }
        finally
        {
            if (session != null)
            {
                _sessions.TryRemove(session.Id, out _);
                session.Dispose();
            }
            _logger.Debug("Client disconnected: {0}", clientEndPoint);
        }
    }

    private async Task ProcessCommandsAsync(MySqlPipeReader reader, MySqlPipeWriter writer, ClientSession session, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Reset sequence numbers at the start of each command
                reader.ResetSequence();
                writer.ResetSequence();

                var packet = await reader.ReadPacketAsync(cancellationToken);
                if (packet == null || packet.Length == 0)
                    break;

                // Update activity
                session.UpdateActivity();

                var command = packet[0];
                var payload = packet.Length > 1 ? packet[1..] : Array.Empty<byte>();

                switch (command)
                {
                    case 0x01: // COM_QUIT
                        return;

                    case 0x02: // COM_INIT_DB
                        await HandleInitDbAsync(writer, session, payload, cancellationToken);
                        break;

                    case 0x03: // COM_QUERY
                        var sql = Encoding.UTF8.GetString(payload);
                        _logger.Debug("Executing SQL: {0}", sql);
                        session.RecordQuery();
                        Interlocked.Increment(ref _totalQueriesExecuted);
                        await ExecuteQueryAsync(writer, session, sql, cancellationToken);
                        break;

                    case 0x04: // COM_FIELD_LIST
                        await HandleFieldListAsync(writer, session, payload, cancellationToken);
                        break;

                    case 0x09: // COM_STATISTICS
                        await HandleStatisticsAsync(writer, cancellationToken);
                        break;

                    case 0x0E: // COM_PING
                        session.UpdateActivity();
                        await SendOkPacketAsync(writer, session, cancellationToken);
                        break;

                    case 0x11: // COM_CHANGE_USER
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

    /// <summary>
    /// Health check task that closes idle connections.
    /// </summary>
    private async Task RunHealthCheckAsync()
    {
        while (!_cancellationTokenSource.Token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.HealthCheckInterval, _cancellationTokenSource.Token);

                var now = DateTime.UtcNow;
                var idleTimeout = _options.ConnectionIdleTimeout;
                var sessionsToClose = new List<long>();

                foreach (var (sessionId, session) in _sessions)
                {
                    if (session.IdleTime > idleTimeout)
                    {
                        sessionsToClose.Add(sessionId);
                        _logger.Debug("Session {0} idle for {1}s, marking for closure",
                            sessionId, session.IdleTime.TotalSeconds);
                    }
                }

                foreach (var sessionId in sessionsToClose)
                {
                    if (_sessions.TryRemove(sessionId, out var session))
                    {
                        try
                        {
                            // Try to send error packet before closing
                            if (session.PipeWriter != null)
                            {
                                await SendErrorPacketAsync(session.PipeWriter, 1205, "HY000",
                                    "Connection idle timeout exceeded", CancellationToken.None);
                            }
                        }
                        catch
                        {
                            // Ignore errors when sending timeout notification
                        }
                        finally
                        {
                            session.Dispose();
                            _logger.Info("Closed idle session {0}", sessionId);
                        }
                    }
                }

                if (sessionsToClose.Count > 0)
                {
                    _logger.Debug("Health check: closed {0} idle sessions, {1} active",
                        sessionsToClose.Count, _sessions.Count);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.Error("Error in health check task", ex);
            }
        }
    }

    private async Task HandleInitDbAsync(MySqlPipeWriter writer, ClientSession session, byte[] payload, CancellationToken cancellationToken)
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

    private async Task HandleFieldListAsync(MySqlPipeWriter writer, ClientSession session, byte[] payload, CancellationToken cancellationToken)
    {
        var offset = 0;
        var tableName = MySqlPipeReader.ReadNullTerminatedString(payload, ref offset, Encoding.UTF8);

        try
        {
            var schema = _storageEngine.Catalog.GetTableSchema(session.CurrentDatabase, tableName);
            if (schema == null)
            {
                await SendErrorPacketAsync(writer, 1146, "42S02", $"Table '{tableName}' doesn't exist", cancellationToken);
                return;
            }

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

            await SendEofOrOkPacketAsync(writer, session, cancellationToken);
        }
        catch (Exception ex)
        {
            await SendErrorPacketAsync(writer, 1064, "42000", ex.Message, cancellationToken);
        }
    }

    private async Task HandleStatisticsAsync(MySqlPipeWriter writer, CancellationToken cancellationToken)
    {
        var stats = _storageEngine.GetBufferPoolStats();
        var uptime = (long)(DateTime.UtcNow - Process.GetCurrentProcess().StartTime.ToUniversalTime()).TotalSeconds;
        var message = $"Uptime: {uptime}  Threads: {_sessions.Count}  Questions: {_totalQueriesExecuted}  Slow queries: 0  " +
                      $"Opens: 0  Flush tables: 0  Open tables: {stats.cachedPages}  " +
                      $"Queries per second avg: {(uptime > 0 ? (double)_totalQueriesExecuted / uptime : 0):F3}";

        var messageBytes = Encoding.UTF8.GetBytes(message);
        await writer.WritePacketAsync(messageBytes, cancellationToken);
    }

    private async Task HandleSetOptionAsync(MySqlPipeWriter writer, ClientSession session, byte[] payload, CancellationToken cancellationToken)
    {
        if (payload.Length >= 2)
        {
            var option = payload[0] | (payload[1] << 8);
            session.MultiStatements = option == 0;
        }
        await SendEofOrOkPacketAsync(writer, session, cancellationToken);
    }

    private async Task ExecuteQueryAsync(MySqlPipeWriter writer, ClientSession session, string sql, CancellationToken cancellationToken)
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
            _logger.Error("Query execution error for SQL: {0}", sql);
            _logger.Error("Query execution error", ex);
            await SendErrorPacketAsync(writer, 1064, "42000", ex.Message, cancellationToken);
        }
    }

    private async Task SendResultSetAsync(MySqlPipeWriter writer, ClientSession session, ResultSet resultSet, CancellationToken cancellationToken)
    {
        using var columnCountPacket = new MemoryStream();
        MySqlPipeWriter.WriteLengthEncodedInteger(columnCountPacket, (ulong)resultSet.ColumnCount);
        await writer.WritePacketAsync(columnCountPacket.ToArray(), cancellationToken);

        foreach (var column in resultSet.Columns)
        {
            await SendColumnDefinitionAsync(writer, session, column, cancellationToken);
        }

        if (!session.UseDeprecateEof)
        {
            await SendEofPacketAsync(writer, session, cancellationToken);
        }

        foreach (var row in resultSet.Rows)
        {
            await SendRowPacketAsync(writer, row, cancellationToken);
        }

        await SendEofOrOkPacketAsync(writer, session, cancellationToken);
    }

    private async Task SendColumnDefinitionAsync(MySqlPipeWriter writer, ClientSession session, ResultColumn column, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();

        MySqlPipeWriter.WriteLengthEncodedString(buffer, "def", Encoding.UTF8);
        MySqlPipeWriter.WriteLengthEncodedString(buffer, column.DatabaseName ?? session.CurrentDatabase, Encoding.UTF8);
        MySqlPipeWriter.WriteLengthEncodedString(buffer, column.TableName ?? "", Encoding.UTF8);
        MySqlPipeWriter.WriteLengthEncodedString(buffer, column.TableName ?? "", Encoding.UTF8);
        MySqlPipeWriter.WriteLengthEncodedString(buffer, column.Name, Encoding.UTF8);
        MySqlPipeWriter.WriteLengthEncodedString(buffer, column.Name, Encoding.UTF8);

        buffer.WriteByte(0x0C);

        buffer.WriteByte(255);
        buffer.WriteByte(0);

        var columnLength = GetColumnLength(column.DataType);
        buffer.WriteByte((byte)(columnLength & 0xFF));
        buffer.WriteByte((byte)((columnLength >> 8) & 0xFF));
        buffer.WriteByte((byte)((columnLength >> 16) & 0xFF));
        buffer.WriteByte((byte)((columnLength >> 24) & 0xFF));

        buffer.WriteByte(GetMySqlColumnType(column.DataType));

        buffer.WriteByte(0);
        buffer.WriteByte(0);

        buffer.WriteByte(0);

        buffer.WriteByte(0);
        buffer.WriteByte(0);

        await writer.WritePacketAsync(buffer.ToArray(), cancellationToken);
    }

    private async Task SendRowPacketAsync(MySqlPipeWriter writer, DataValue[] row, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();

        foreach (var value in row)
        {
            if (value.IsNull)
            {
                buffer.WriteByte(0xFB);
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
        string strValue = value.Type switch
        {
            DataType.Int => value.AsInt().ToString(),
            DataType.BigInt => value.AsBigInt().ToString(),
            DataType.SmallInt => value.AsSmallInt().ToString(),
            DataType.TinyInt => value.AsTinyInt().ToString(),
            DataType.Boolean => value.AsBoolean() ? "1" : "0",
            DataType.VarChar or DataType.Char or DataType.Text => value.AsString(),
            DataType.DateTime => value.AsDateTime().ToString("yyyy-MM-dd HH:mm:ss"),
            DataType.Date => value.AsDate().ToString("yyyy-MM-dd"),
            DataType.Time => value.AsTime().ToString(@"hh\:mm\:ss"),
            DataType.Float => value.AsFloat().ToString("G9"),
            DataType.Double => value.AsDouble().ToString("G17"),
            DataType.Decimal => value.AsDecimal().ToString("G"),
            _ => value.GetRawValue()?.ToString() ?? ""
        };

        MySqlPipeWriter.WriteLengthEncodedString(buffer, strValue, Encoding.UTF8);
    }

    private async Task SendOkPacketAsync(MySqlPipeWriter writer, ClientSession session, CancellationToken cancellationToken,
        long affectedRows = 0, long lastInsertId = 0, string? message = null)
    {
        using var buffer = new MemoryStream();

        buffer.WriteByte(0x00);

        MySqlPipeWriter.WriteLengthEncodedInteger(buffer, (ulong)affectedRows);
        MySqlPipeWriter.WriteLengthEncodedInteger(buffer, (ulong)lastInsertId);

        var status = session.GetServerStatus();
        buffer.WriteByte((byte)(status & 0xFF));
        buffer.WriteByte((byte)((status >> 8) & 0xFF));

        buffer.WriteByte(0);
        buffer.WriteByte(0);

        if (!string.IsNullOrEmpty(message))
        {
            MySqlPipeWriter.WriteLengthEncodedString(buffer, message, Encoding.UTF8);
        }

        await writer.WritePacketAsync(buffer.ToArray(), cancellationToken);
    }

    private async Task SendEofPacketAsync(MySqlPipeWriter writer, ClientSession session, CancellationToken cancellationToken)
    {
        var status = session.GetServerStatus();
        var eofPacket = new byte[]
        {
            0xFE,
            0x00, 0x00,
            (byte)(status & 0xFF), (byte)((status >> 8) & 0xFF)
        };
        await writer.WritePacketAsync(eofPacket, cancellationToken);
    }

    private async Task SendEofOrOkPacketAsync(MySqlPipeWriter writer, ClientSession session, CancellationToken cancellationToken)
    {
        if (session.UseDeprecateEof)
        {
            using var buffer = new MemoryStream();

            buffer.WriteByte(0xFE);

            MySqlPipeWriter.WriteLengthEncodedInteger(buffer, 0);
            MySqlPipeWriter.WriteLengthEncodedInteger(buffer, 0);

            var status = session.GetServerStatus();
            buffer.WriteByte((byte)(status & 0xFF));
            buffer.WriteByte((byte)((status >> 8) & 0xFF));

            buffer.WriteByte(0);
            buffer.WriteByte(0);

            await writer.WritePacketAsync(buffer.ToArray(), cancellationToken);
        }
        else
        {
            await SendEofPacketAsync(writer, session, cancellationToken);
        }
    }

    private async Task SendErrorPacketAsync(MySqlPipeWriter writer, int errorCode, string sqlState, string message, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();

        buffer.WriteByte(0xFF);

        buffer.WriteByte((byte)(errorCode & 0xFF));
        buffer.WriteByte((byte)((errorCode >> 8) & 0xFF));

        buffer.WriteByte(0x23);

        var sqlStateBytes = Encoding.UTF8.GetBytes(sqlState.PadRight(5)[..5]);
        buffer.Write(sqlStateBytes);

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
            DataType.TinyInt => 1,
            DataType.SmallInt => 2,
            DataType.Int => 3,
            DataType.BigInt => 8,
            DataType.Float => 4,
            DataType.Double => 5,
            DataType.Decimal => 246,
            DataType.VarChar => 253,
            DataType.Char => 254,
            DataType.Text => 252,
            DataType.DateTime => 12,
            DataType.Date => 10,
            DataType.Time => 11,
            DataType.Boolean => 1,
            _ => 253
        };
    }

    /// <summary>
    /// Gets server statistics.
    /// </summary>
    public ServerStats GetStats()
    {
        return new ServerStats
        {
            ActiveConnections = _sessions.Count,
            TotalConnectionsReceived = _totalConnectionsReceived,
            RejectedConnections = _rejectedConnections,
            TotalQueriesExecuted = _totalQueriesExecuted,
            MaxConnections = _options.MaxConnections
        };
    }

    /// <summary>
    /// Gets all active session statistics.
    /// </summary>
    public IEnumerable<SessionStats> GetSessionStats()
    {
        return _sessions.Values.Select(s => s.GetStats());
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

    private static string GetClientHost(string clientEndPoint)
    {
        if (string.IsNullOrEmpty(clientEndPoint))
            return "localhost";

        var colonIndex = clientEndPoint.LastIndexOf(':');
        if (colonIndex > 0)
        {
            var host = clientEndPoint[..colonIndex];
            if (host.StartsWith('[') && host.EndsWith(']'))
                host = host[1..^1];
            return host;
        }

        return clientEndPoint;
    }
}

/// <summary>
/// Server statistics snapshot.
/// </summary>
public sealed class ServerStats
{
    public int ActiveConnections { get; init; }
    public long TotalConnectionsReceived { get; init; }
    public long RejectedConnections { get; init; }
    public long TotalQueriesExecuted { get; init; }
    public int MaxConnections { get; init; }
}
