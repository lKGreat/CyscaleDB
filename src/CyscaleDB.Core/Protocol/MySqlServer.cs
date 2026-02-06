using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using CyscaleDB.Core.Auth;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;
using CyscaleDB.Core.Protocol.Monitoring;
using CyscaleDB.Core.Protocol.Security;
using CyscaleDB.Core.Protocol.Transport;
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

    // Enterprise components
    private readonly SslHandler? _sslHandler;
    private readonly IpFilter _ipFilter;
    private readonly ConnectionRateLimiter _rateLimiter;
    private readonly SocketAsyncEventArgsPool _socketPool;
    private readonly NetworkMetrics _metrics;
    private readonly LatencyHistogram _queryLatency;
    private readonly CompressionHandler _compressionHandler;

    // Shutdown state
    private volatile bool _isShuttingDown;

    // Statistics (legacy, now also in _metrics)
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
    /// Gets the network metrics collector.
    /// </summary>
    public NetworkMetrics Metrics => _metrics;

    /// <summary>
    /// Gets the query latency histogram.
    /// </summary>
    public LatencyHistogram QueryLatency => _queryLatency;

    /// <summary>
    /// Base server capabilities advertised to clients.
    /// </summary>
    private static MySqlCapabilities BaseServerCapabilities =>
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
    /// Gets the server capabilities based on configuration.
    /// </summary>
    public MySqlCapabilities GetServerCapabilities()
    {
        var caps = BaseServerCapabilities;

        // Add SSL capability if enabled
        if (_options.Ssl.Enabled)
        {
            caps |= MySqlCapabilities.Ssl;
        }

        // Add compression capability if enabled
        if (_options.EnableCompression)
        {
            caps |= MySqlCapabilities.Compress;
            if (_options.PreferredCompression == CompressionAlgorithm.Zstd)
            {
                caps |= MySqlCapabilities.ZstdCompressionAlgorithm;
            }
        }

        return caps;
    }

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

        // Initialize enterprise components
        _ipFilter = new IpFilter(options.IpFilter);
        _rateLimiter = new ConnectionRateLimiter(options.RateLimit);
        _socketPool = new SocketAsyncEventArgsPool(
            options.SocketPoolSize,
            options.SocketPoolMaxSize,
            options.SocketBufferSize);
        _metrics = new NetworkMetrics();
        _queryLatency = new LatencyHistogram();
        _compressionHandler = new CompressionHandler(new CompressionOptions
        {
            Enabled = options.EnableCompression,
            PreferredAlgorithm = options.PreferredCompression,
            CompressionThreshold = options.CompressionThreshold,
            CompressionLevel = options.CompressionLevel
        });

        // Initialize SSL handler if enabled
        if (options.Ssl.Enabled)
        {
            _sslHandler = new SslHandler(options.Ssl);
        }

        // Create listener with configured endpoint
        var endpoint = new IPEndPoint(
            IPAddress.Parse(_options.BindAddress),
            _options.Port);
        _listener = new TcpListener(endpoint);

        // Configure listener socket
        ConfigureListenerSocket(_listener.Server);

        _logger.Info("MySQL server initialized with enterprise features: SSL={0}, Compression={1}, RateLimit={2}, IpFilter={3}",
            options.Ssl.Enabled, options.EnableCompression, options.RateLimit.Enabled, options.IpFilter.Enabled);
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
        StopAsync(_options.GracefulShutdownTimeout).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Stops the server gracefully, waiting for active connections to complete.
    /// </summary>
    /// <param name="timeout">Maximum time to wait for graceful shutdown.</param>
    public async Task StopAsync(TimeSpan timeout)
    {
        if (_disposed)
            return;

        _isShuttingDown = true;
        _logger.Info("Initiating graceful shutdown with timeout {0}s...", timeout.TotalSeconds);

        // 1. Stop accepting new connections
        _listener.Stop();

        // 2. Notify all sessions about shutdown
        foreach (var session in _sessions.Values)
        {
            try
            {
                await NotifyShutdownAsync(session);
            }
            catch (Exception ex)
            {
                _logger.Trace("Error notifying session of shutdown: {0}", ex.Message);
            }
        }

        // 3. Wait for sessions to complete (with timeout)
        var deadline = DateTime.UtcNow + timeout;
        while (_sessions.Count > 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100);
        }

        if (_sessions.Count > 0)
        {
            _logger.Warning("Forcefully closing {0} remaining sessions", _sessions.Count);
        }

        // 4. Force close remaining sessions
        _cancellationTokenSource.Cancel();
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

        // Wait for background tasks
        try
        {
            if (_acceptTask != null)
                await _acceptTask.WaitAsync(TimeSpan.FromSeconds(5));
            if (_healthCheckTask != null)
                await _healthCheckTask.WaitAsync(TimeSpan.FromSeconds(2));
        }
        catch (Exception)
        {
            // Expected when cancelling
        }

        _logger.Info("MySQL protocol server stopped");
    }

    /// <summary>
    /// Notifies a session that the server is shutting down.
    /// </summary>
    private async Task NotifyShutdownAsync(ClientSession session)
    {
        if (session.PipeWriter == null)
            return;

        try
        {
            // Send MySQL error packet indicating server shutdown
            using var buffer = new MemoryStream();
            buffer.WriteByte(0xFF); // Error packet
            buffer.WriteByte(0xED); // Error code 1053 (Server shutdown in progress) low
            buffer.WriteByte(0x04); // Error code high
            buffer.WriteByte(0x23); // SQL state marker '#'
            var sqlStateBytes = Encoding.UTF8.GetBytes("08S01");
            buffer.Write(sqlStateBytes);
            var messageBytes = Encoding.UTF8.GetBytes("Server shutdown in progress");
            buffer.Write(messageBytes);

            await session.PipeWriter.WritePacketAsync(buffer.ToArray());
        }
        catch
        {
            // Ignore errors when notifying
        }
    }

    private async Task AcceptConnectionsAsync()
    {
        while (!_cancellationTokenSource.Token.IsCancellationRequested && !_isShuttingDown)
        {
            try
            {
                var client = await _listener.AcceptTcpClientAsync(_cancellationTokenSource.Token);
                Interlocked.Increment(ref _totalConnectionsReceived);

                var remoteEndPoint = client.Client.RemoteEndPoint as IPEndPoint;
                var clientIp = remoteEndPoint?.Address.ToString() ?? "unknown";

                // Check IP filter
                if (remoteEndPoint != null && !_ipFilter.IsAllowed(remoteEndPoint.Address))
                {
                    Interlocked.Increment(ref _rejectedConnections);
                    _metrics.RecordRejectedConnection();
                    _logger.Warning("Connection rejected by IP filter: {0}", clientIp);
                    client.Close();
                    continue;
                }

                // Check rate limit
                if (!_rateLimiter.TryAcquire(clientIp))
                {
                    Interlocked.Increment(ref _rejectedConnections);
                    _metrics.RecordRejectedConnection();
                    _logger.Warning("Connection rejected by rate limiter: {0}", clientIp);
                    await RejectConnectionAsync(client, "Too many connections from your address");
                    continue;
                }

                // Check max connections limit
                if (_sessions.Count >= _options.MaxConnections)
                {
                    Interlocked.Increment(ref _rejectedConnections);
                    _metrics.RecordRejectedConnection();
                    _rateLimiter.Release(clientIp);
                    _logger.Warning("Connection rejected: max connections ({0}) reached", _options.MaxConnections);
                    await RejectConnectionAsync(client, "Too many connections");
                    continue;
                }

                // Configure client socket
                ConfigureClientSocket(client.Client);

                // Record connection
                _metrics.RecordConnection();

                _ = Task.Run(() => HandleClientAsync(client, clientIp, _cancellationTokenSource.Token), _cancellationTokenSource.Token);
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

    private async Task HandleClientAsync(TcpClient client, string clientIp, CancellationToken cancellationToken)
    {
        var clientEndPoint = client.Client.RemoteEndPoint?.ToString() ?? "unknown";
        _logger.Debug("Client connected from {0}", clientEndPoint);
        ClientSession? session = null;
        Stream? activeStream = null;

        try
        {
            using (client)
            {
                activeStream = client.GetStream();

                // Create pipe-based reader/writer
                var pipeReader = MySqlPipeReader.Create(activeStream, _options);
                var pipeWriter = MySqlPipeWriter.Create(activeStream, _options);

                // Also keep legacy writers for compatibility during handshake
                var legacyWriter = new PacketWriter(activeStream);

                // Send handshake packet with dynamic capabilities
                var salt = Handshake.GenerateSalt();
                var handshakePacket = Handshake.CreateHandshakePacket(
                    Constants.ServerVersion,
                    salt,
                    (uint)GetServerCapabilities());

                await legacyWriter.WritePacketAsync(handshakePacket, cancellationToken);
                pipeReader.SetSequence(legacyWriter.CurrentSequence);

                // Read handshake response using pipe reader
                var responsePacket = await pipeReader.ReadPacketAsync(cancellationToken);
                if (responsePacket == null)
                {
                    _logger.Debug("Client disconnected during handshake");
                    return;
                }

                // Check if this is an SSL Request (short packet with CLIENT_SSL flag)
                if (responsePacket.Length == 32 && _sslHandler != null && _options.Ssl?.Enabled == true)
                {
                    // First 4 bytes are capabilities
                    var sslCaps = (uint)(responsePacket[0] | (responsePacket[1] << 8) |
                                         (responsePacket[2] << 16) | (responsePacket[3] << 24));
                    if ((sslCaps & (uint)MySqlCapabilities.Ssl) != 0)
                    {
                        _logger.Debug("SSL Request received, upgrading connection");
                        try
                        {
                            activeStream = await _sslHandler.AuthenticateAsServerAsync(activeStream, cancellationToken);
                            pipeReader = MySqlPipeReader.Create(activeStream, _options);
                            pipeWriter = MySqlPipeWriter.Create(activeStream, _options);
                            _logger.Info("SSL connection established for {0}", clientEndPoint);
                        }
                        catch (Exception sslEx)
                        {
                            _logger.Error("SSL upgrade failed for {0}: {1}", clientEndPoint, sslEx.Message);
                            return;
                        }

                        // Read the actual handshake response after SSL upgrade
                        responsePacket = await pipeReader.ReadPacketAsync(cancellationToken);
                        if (responsePacket == null)
                        {
                            _logger.Debug("Client disconnected after SSL upgrade");
                            return;
                        }
                    }
                }

                var response = Handshake.ParseHandshakeResponse(responsePacket);

                _logger.Debug("Client connecting: username={0}, database={1}, capabilities=0x{2:X8}",
                    response.Username, response.Database ?? "(none)", response.Capabilities);

                // Validate authentication
                var clientHost = GetClientHost(clientEndPoint);
                var authBytes = string.IsNullOrEmpty(response.AuthResponse)
                    ? Array.Empty<byte>()
                    : Encoding.Latin1.GetBytes(response.AuthResponse);

                // Handle auth plugin switch if client uses caching_sha2_password
                bool authenticated = false;
                if (response.AuthPlugin == "caching_sha2_password")
                {
                    _logger.Debug("Client using caching_sha2_password, sending auth switch to mysql_native_password");
                    // Send Auth Switch Request to downgrade to mysql_native_password
                    pipeWriter = MySqlPipeWriter.Create(activeStream, _options);
                    using var switchBuffer = new MemoryStream();
                    switchBuffer.WriteByte(0xFE); // Auth Switch Request marker
                    var pluginName = Encoding.UTF8.GetBytes("mysql_native_password");
                    switchBuffer.Write(pluginName);
                    switchBuffer.WriteByte(0x00); // null terminator
                    switchBuffer.Write(salt); // auth data
                    switchBuffer.WriteByte(0x00); // null terminator
                    await pipeWriter.WritePacketAsync(switchBuffer.ToArray(), cancellationToken);

                    // Read the new auth response
                    var authSwitchResponse = await pipeReader.ReadPacketAsync(cancellationToken);
                    if (authSwitchResponse != null && authSwitchResponse.Length > 0)
                    {
                        authenticated = UserManager.Instance.ValidatePassword(response.Username, authSwitchResponse, salt, clientHost);
                    }
                }
                else
                {
                    authenticated = UserManager.Instance.ValidatePassword(response.Username, authBytes, salt, clientHost);
                }

                if (!authenticated)
                {
                    _logger.Warning("Authentication failed for user '{0}'@'{1}'", response.Username, clientHost);

                    // Synchronize pipe writer with legacy writer sequence
                    pipeWriter = MySqlPipeWriter.Create(activeStream, _options);
                    _metrics.RecordProtocolError();
                    
                    await SendErrorPacketAsync(pipeWriter, 1045, "28000",
                        $"Access denied for user '{response.Username}'@'{clientHost}' (using password: {(authBytes.Length > 0 ? "YES" : "NO")})",
                        cancellationToken);
                    return;
                }

                _logger.Info("Client authenticated: username={0}, database={1}",
                    response.Username, response.Database ?? "(none)");

                // Create client session with negotiated capabilities
                var clientCapabilities = (MySqlCapabilities)(response.Capabilities & (int)GetServerCapabilities());
                
                // Reset pipe writer for command phase
                pipeWriter = MySqlPipeWriter.Create(activeStream, _options);
                
                var executor = new Executor(_storageEngine.Catalog, _transactionManager);
                session = new ClientSession(clientCapabilities, executor, pipeReader, pipeWriter)
                {
                    Username = response.Username,
                    RemoteAddress = clientEndPoint
                };

                // Wire up session context to executor (connection ID, user, warnings)
                executor.SetSessionContext(
                    session.Id,
                    response.Username,
                    clientHost,
                    addWarning: (level, code, msg) => session.AddWarning(level, code, msg),
                    getWarnings: () => session.GetWarnings()
                        .Select(w => (w.Level, w.Code, w.Message)).ToList()
                );

                // Apply character set negotiation from handshake response
                var charsetName = MapCharacterSetIdToName(response.CharacterSet);
                if (!string.IsNullOrEmpty(charsetName))
                {
                    try
                    {
                        executor.Execute($"SET NAMES {charsetName}");
                    }
                    catch
                    {
                        // If charset set fails, continue with default
                    }
                }

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
            // Cleanup
            if (session != null)
            {
                _sessions.TryRemove(session.Id, out _);
                session.Dispose();
            }

            // Release rate limiter slot
            _rateLimiter.Release(clientIp);

            // Record disconnection
            _metrics.RecordDisconnection();

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

                    case 0x1F: // COM_RESET_CONNECTION
                        session.Reset();
                        await SendOkPacketAsync(writer, session, cancellationToken);
                        break;

                    case 0x16: // COM_STMT_PREPARE
                        await HandleStmtPrepareAsync(writer, session, payload, cancellationToken);
                        break;

                    case 0x17: // COM_STMT_EXECUTE
                        session.RecordQuery();
                        Interlocked.Increment(ref _totalQueriesExecuted);
                        await HandleStmtExecuteAsync(writer, session, payload, cancellationToken);
                        break;

                    case 0x18: // COM_STMT_SEND_LONG_DATA
                        // Acknowledge but no response needed
                        break;

                    case 0x19: // COM_STMT_CLOSE (no response)
                        HandleStmtClose(session, payload);
                        break;

                    case 0x1A: // COM_STMT_RESET
                        if (payload.Length >= 4)
                        {
                            var resetStmtId = BitConverter.ToInt32(payload, 0);
                            if (session.PreparedStatements.ContainsKey(resetStmtId))
                            {
                                await SendOkPacketAsync(writer, session, cancellationToken);
                            }
                            else
                            {
                                await SendErrorPacketAsync(writer, 1243, "HY000", "Unknown prepared statement handler", cancellationToken);
                            }
                        }
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
                    var (mysqlCode, sqlState) = MySqlErrorMapper.Map(ex);
                    await SendErrorPacketAsync(writer, mysqlCode, sqlState, ex.Message, cancellationToken);
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

    /// <summary>
    /// Handles COM_STMT_PREPARE: parse SQL, count params, return statement metadata.
    /// </summary>
    private async Task HandleStmtPrepareAsync(MySqlPipeWriter writer, ClientSession session, byte[] payload, CancellationToken cancellationToken)
    {
        var sql = Encoding.UTF8.GetString(payload);
        _logger.Debug("COM_STMT_PREPARE: {0}", sql);

        try
        {
            // Count parameter placeholders
            int numParams = 0;
            for (int i = 0; i < sql.Length; i++)
            {
                if (sql[i] == '?') numParams++;
            }

            // Try to parse to detect syntax errors early and get column info
            var stmtId = session.AllocateStatementId();
            var columns = new List<ResultColumn>();

            // For SELECT, try to get column definitions
            var sqlUpper = sql.TrimStart().ToUpperInvariant();
            if (sqlUpper.StartsWith("SELECT") || sqlUpper.StartsWith("SHOW") || sqlUpper.StartsWith("DESC"))
            {
                try
                {
                    // Replace ? with NULL for parsing column schema
                    var testSql = sql.Replace("?", "NULL");
                    var testResult = session.Executor.Execute(testSql);
                    if (testResult.ResultSet != null)
                    {
                        columns.AddRange(testResult.ResultSet.Columns);
                    }
                }
                catch
                {
                    // If schema detection fails, continue with empty columns
                }
            }

            var info = new PreparedStatementInfo
            {
                StatementId = stmtId,
                Sql = sql,
                NumParams = numParams,
                Columns = columns
            };
            session.PreparedStatements[stmtId] = info;

            // Send COM_STMT_PREPARE_OK
            using var buffer = new MemoryStream();
            buffer.WriteByte(0x00); // status OK

            // statement_id (4 bytes LE)
            buffer.WriteByte((byte)(stmtId & 0xFF));
            buffer.WriteByte((byte)((stmtId >> 8) & 0xFF));
            buffer.WriteByte((byte)((stmtId >> 16) & 0xFF));
            buffer.WriteByte((byte)((stmtId >> 24) & 0xFF));

            // num_columns (2 bytes LE)
            var numCols = (ushort)columns.Count;
            buffer.WriteByte((byte)(numCols & 0xFF));
            buffer.WriteByte((byte)((numCols >> 8) & 0xFF));

            // num_params (2 bytes LE)
            buffer.WriteByte((byte)(numParams & 0xFF));
            buffer.WriteByte((byte)((numParams >> 8) & 0xFF));

            // reserved
            buffer.WriteByte(0x00);

            // warning_count (2 bytes)
            buffer.WriteByte(0x00);
            buffer.WriteByte(0x00);

            await writer.WritePacketAsync(buffer.ToArray(), cancellationToken);

            // Send parameter definitions if any
            if (numParams > 0)
            {
                for (int i = 0; i < numParams; i++)
                {
                    var paramCol = new ResultColumn
                    {
                        Name = $"?",
                        DataType = DataType.VarChar,
                        TableName = "",
                        DatabaseName = ""
                    };
                    await SendColumnDefinitionAsync(writer, session, paramCol, cancellationToken);
                }
                if (!session.UseDeprecateEof)
                {
                    await SendEofPacketAsync(writer, session, cancellationToken);
                }
            }

            // Send column definitions if any
            if (numCols > 0)
            {
                foreach (var col in columns)
                {
                    await SendColumnDefinitionAsync(writer, session, col, cancellationToken);
                }
                if (!session.UseDeprecateEof)
                {
                    await SendEofPacketAsync(writer, session, cancellationToken);
                }
            }
        }
        catch (Exception ex)
        {
            var (mysqlCode, sqlState) = MySqlErrorMapper.Map(ex);
            await SendErrorPacketAsync(writer, mysqlCode, sqlState, ex.Message, cancellationToken);
        }
    }

    /// <summary>
    /// Handles COM_STMT_EXECUTE: bind params and execute.
    /// </summary>
    private async Task HandleStmtExecuteAsync(MySqlPipeWriter writer, ClientSession session, byte[] payload, CancellationToken cancellationToken)
    {
        try
        {
            if (payload.Length < 9)
            {
                await SendErrorPacketAsync(writer, 1105, "HY000", "Malformed COM_STMT_EXECUTE packet", cancellationToken);
                return;
            }

            var stmtId = BitConverter.ToInt32(payload, 0);
            // flags = payload[4], iteration_count = payload[5..8] (always 1)

            if (!session.PreparedStatements.TryGetValue(stmtId, out var info))
            {
                await SendErrorPacketAsync(writer, 1243, "HY000", "Unknown prepared statement handler", cancellationToken);
                return;
            }

            // Build the actual SQL by replacing ? with bound parameter values
            var sql = info.Sql;
            if (info.NumParams > 0)
            {
                var paramValues = ExtractBinaryParams(payload, 9, info.NumParams);
                sql = BindParams(info.Sql, paramValues);
            }

            _logger.Debug("COM_STMT_EXECUTE (stmt={0}): {1}", stmtId, sql);

            // Execute as text query
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
                default:
                    await SendOkPacketAsync(writer, session, cancellationToken);
                    break;
            }
        }
        catch (Exception ex)
        {
            var (mysqlCode, sqlState) = MySqlErrorMapper.Map(ex);
            await SendErrorPacketAsync(writer, mysqlCode, sqlState, ex.Message, cancellationToken);
        }
    }

    /// <summary>
    /// Handles COM_STMT_CLOSE: remove prepared statement from session.
    /// </summary>
    private static void HandleStmtClose(ClientSession session, byte[] payload)
    {
        if (payload.Length >= 4)
        {
            var stmtId = BitConverter.ToInt32(payload, 0);
            session.PreparedStatements.Remove(stmtId);
        }
        // COM_STMT_CLOSE doesn't send a response
    }

    /// <summary>
    /// Extracts parameter values from binary protocol COM_STMT_EXECUTE payload.
    /// </summary>
    private static string[] ExtractBinaryParams(byte[] payload, int offset, int numParams)
    {
        var values = new string[numParams];
        try
        {
            if (offset >= payload.Length)
            {
                for (int i = 0; i < numParams; i++) values[i] = "NULL";
                return values;
            }

            // null_bitmap: (numParams + 7) / 8 bytes
            int nullBitmapLen = (numParams + 7) / 8;
            var nullBitmap = new byte[nullBitmapLen];
            if (offset + nullBitmapLen <= payload.Length)
            {
                Array.Copy(payload, offset, nullBitmap, 0, nullBitmapLen);
            }
            offset += nullBitmapLen;

            // new_params_bound_flag (1 byte)
            bool newParamsBound = offset < payload.Length && payload[offset] == 1;
            offset++;

            // type info (2 bytes per param) if new_params_bound_flag = 1
            var paramTypes = new byte[numParams];
            if (newParamsBound)
            {
                for (int i = 0; i < numParams; i++)
                {
                    if (offset + 1 < payload.Length)
                    {
                        paramTypes[i] = payload[offset]; // type
                        offset += 2; // type + flags
                    }
                }
            }

            // Read values
            for (int i = 0; i < numParams; i++)
            {
                // Check null bitmap
                if ((nullBitmap[i / 8] & (1 << (i % 8))) != 0)
                {
                    values[i] = "NULL";
                    continue;
                }

                var type = paramTypes[i];
                switch (type)
                {
                    case 1: // MYSQL_TYPE_TINY
                        if (offset < payload.Length)
                            values[i] = payload[offset++].ToString();
                        break;
                    case 2: // MYSQL_TYPE_SHORT
                        if (offset + 1 < payload.Length)
                        {
                            values[i] = BitConverter.ToInt16(payload, offset).ToString();
                            offset += 2;
                        }
                        break;
                    case 3: // MYSQL_TYPE_LONG
                        if (offset + 3 < payload.Length)
                        {
                            values[i] = BitConverter.ToInt32(payload, offset).ToString();
                            offset += 4;
                        }
                        break;
                    case 8: // MYSQL_TYPE_LONGLONG
                        if (offset + 7 < payload.Length)
                        {
                            values[i] = BitConverter.ToInt64(payload, offset).ToString();
                            offset += 8;
                        }
                        break;
                    case 4: // MYSQL_TYPE_FLOAT
                        if (offset + 3 < payload.Length)
                        {
                            values[i] = BitConverter.ToSingle(payload, offset).ToString("G9");
                            offset += 4;
                        }
                        break;
                    case 5: // MYSQL_TYPE_DOUBLE
                        if (offset + 7 < payload.Length)
                        {
                            values[i] = BitConverter.ToDouble(payload, offset).ToString("G17");
                            offset += 8;
                        }
                        break;
                    default: // String types (VAR_STRING, STRING, BLOB, etc.)
                    {
                        // length-encoded string
                        if (offset < payload.Length)
                        {
                            var (strLen, bytesRead) = ReadLengthEncodedInt(payload, offset);
                            offset += bytesRead;
                            if (offset + (int)strLen <= payload.Length)
                            {
                                var strVal = Encoding.UTF8.GetString(payload, offset, (int)strLen);
                                // Escape single quotes for SQL embedding
                                values[i] = "'" + strVal.Replace("\\", "\\\\").Replace("'", "\\'") + "'";
                                offset += (int)strLen;
                            }
                            else
                            {
                                values[i] = "NULL";
                            }
                        }
                        continue; // Skip the default assignment below
                    }
                }

                values[i] ??= "NULL";
            }
        }
        catch
        {
            // On any parsing error, fill remaining with NULL
            for (int i = 0; i < numParams; i++)
                values[i] ??= "NULL";
        }
        return values;
    }

    /// <summary>
    /// Reads a length-encoded integer from a byte array.
    /// </summary>
    private static (ulong value, int bytesRead) ReadLengthEncodedInt(byte[] data, int offset)
    {
        if (offset >= data.Length) return (0, 1);
        var first = data[offset];
        if (first < 0xFB) return (first, 1);
        if (first == 0xFC && offset + 2 < data.Length)
            return (BitConverter.ToUInt16(data, offset + 1), 3);
        if (first == 0xFD && offset + 3 < data.Length)
            return ((ulong)(data[offset + 1] | (data[offset + 2] << 8) | (data[offset + 3] << 16)), 4);
        if (first == 0xFE && offset + 8 < data.Length)
            return (BitConverter.ToUInt64(data, offset + 1), 9);
        return (0, 1);
    }

    /// <summary>
    /// Replaces ? placeholders with actual param values in SQL.
    /// </summary>
    private static string BindParams(string sql, string[] values)
    {
        var sb = new StringBuilder(sql.Length + values.Length * 10);
        int paramIdx = 0;
        bool inString = false;
        char stringChar = '\0';

        for (int i = 0; i < sql.Length; i++)
        {
            var c = sql[i];

            if (inString)
            {
                sb.Append(c);
                if (c == '\\' && i + 1 < sql.Length)
                {
                    sb.Append(sql[++i]); // skip escaped char
                }
                else if (c == stringChar)
                {
                    inString = false;
                }
            }
            else if (c == '\'' || c == '"')
            {
                inString = true;
                stringChar = c;
                sb.Append(c);
            }
            else if (c == '?' && paramIdx < values.Length)
            {
                sb.Append(values[paramIdx++]);
            }
            else
            {
                sb.Append(c);
            }
        }

        return sb.ToString();
    }

    private async Task ExecuteQueryAsync(MySqlPipeWriter writer, ClientSession session, string sql, CancellationToken cancellationToken)
    {
        try
        {
            // Clear warnings before each new query
            session.ClearWarnings();

            // Use multi-result execution for multi-statement support
            var results = session.Executor.ExecuteMultiple(sql);

            for (int i = 0; i < results.Count; i++)
            {
                var result = results[i];
                bool moreResults = i < results.Count - 1;
                ushort? statusOverride = moreResults
                    ? (ushort)(session.GetServerStatus() | (ushort)MySqlServerStatus.MoreResultsExist)
                    : null;

                switch (result.Type)
                {
                    case ResultType.Query:
                        await SendResultSetAsync(writer, session, result.ResultSet!, cancellationToken, moreResults);
                        break;

                    case ResultType.Modification:
                        await SendOkPacketAsync(writer, session, cancellationToken,
                            affectedRows: result.AffectedRows,
                            lastInsertId: result.LastInsertId,
                            statusOverride: statusOverride);
                        break;

                    case ResultType.Ddl:
                        await SendOkPacketAsync(writer, session, cancellationToken,
                            message: result.Message, statusOverride: statusOverride);
                        break;

                    default:
                        await SendOkPacketAsync(writer, session, cancellationToken,
                            statusOverride: statusOverride);
                        break;
                }

                // Reset sequence number between result sets for proper framing
                if (moreResults)
                {
                    writer.ResetSequence();
                }
            }
        }
        catch (Exception ex)
        {
            _logger.Error("Query execution error for SQL: {0}", sql);
            _logger.Error("Query execution error", ex);
            var (mysqlCode, sqlState) = MySqlErrorMapper.Map(ex);
            await SendErrorPacketAsync(writer, mysqlCode, sqlState, ex.Message, cancellationToken);
        }
    }

    private async Task SendResultSetAsync(MySqlPipeWriter writer, ClientSession session, ResultSet resultSet, CancellationToken cancellationToken, bool moreResults = false)
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

        if (moreResults)
        {
            // Send EOF/OK with SERVER_MORE_RESULTS_EXISTS flag
            var status = (ushort)(session.GetServerStatus() | (ushort)MySqlServerStatus.MoreResultsExist);
            await SendEofOrOkPacketAsync(writer, session, cancellationToken, statusOverride: status);
        }
        else
        {
            await SendEofOrOkPacketAsync(writer, session, cancellationToken);
        }
    }

    private async Task SendColumnDefinitionAsync(MySqlPipeWriter writer, ClientSession session, ResultColumn column, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();

        // catalog
        MySqlPipeWriter.WriteLengthEncodedString(buffer, "def", Encoding.UTF8);
        // schema
        MySqlPipeWriter.WriteLengthEncodedString(buffer, column.DatabaseName ?? session.CurrentDatabase, Encoding.UTF8);
        // table (virtual name / alias)
        MySqlPipeWriter.WriteLengthEncodedString(buffer, column.TableName ?? "", Encoding.UTF8);
        // org_table (physical table name)
        MySqlPipeWriter.WriteLengthEncodedString(buffer, column.OriginalTableName ?? column.TableName ?? "", Encoding.UTF8);
        // name (column alias)
        MySqlPipeWriter.WriteLengthEncodedString(buffer, column.Name, Encoding.UTF8);
        // org_name (physical column name)
        MySqlPipeWriter.WriteLengthEncodedString(buffer, column.OriginalName ?? column.Name, Encoding.UTF8);

        // length of fixed-length fields [0c]
        buffer.WriteByte(0x0C);

        // character_set: utf8mb4 = 255
        buffer.WriteByte(255);
        buffer.WriteByte(0);

        // column_length
        var columnLength = GetColumnLength(column.DataType);
        buffer.WriteByte((byte)(columnLength & 0xFF));
        buffer.WriteByte((byte)((columnLength >> 8) & 0xFF));
        buffer.WriteByte((byte)((columnLength >> 16) & 0xFF));
        buffer.WriteByte((byte)((columnLength >> 24) & 0xFF));

        // column_type
        buffer.WriteByte(GetMySqlColumnType(column.DataType));

        // flags (2 bytes little-endian)
        var flags = column.Flags;
        buffer.WriteByte((byte)(flags & 0xFF));
        buffer.WriteByte((byte)((flags >> 8) & 0xFF));

        // decimals
        buffer.WriteByte(column.Decimals);

        // filler
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
        long affectedRows = 0, long lastInsertId = 0, string? message = null, ushort? statusOverride = null)
    {
        using var buffer = new MemoryStream();

        buffer.WriteByte(0x00);

        MySqlPipeWriter.WriteLengthEncodedInteger(buffer, (ulong)affectedRows);
        MySqlPipeWriter.WriteLengthEncodedInteger(buffer, (ulong)lastInsertId);

        var status = statusOverride ?? session.GetServerStatus();
        buffer.WriteByte((byte)(status & 0xFF));
        buffer.WriteByte((byte)((status >> 8) & 0xFF));

        // warning count from session
        var warningCount = (ushort)session.WarningCount;
        buffer.WriteByte((byte)(warningCount & 0xFF));
        buffer.WriteByte((byte)((warningCount >> 8) & 0xFF));

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

    private async Task SendEofOrOkPacketAsync(MySqlPipeWriter writer, ClientSession session, CancellationToken cancellationToken, ushort? statusOverride = null)
    {
        if (session.UseDeprecateEof)
        {
            using var buffer = new MemoryStream();

            buffer.WriteByte(0xFE);

            MySqlPipeWriter.WriteLengthEncodedInteger(buffer, 0);
            MySqlPipeWriter.WriteLengthEncodedInteger(buffer, 0);

            var status = statusOverride ?? session.GetServerStatus();
            buffer.WriteByte((byte)(status & 0xFF));
            buffer.WriteByte((byte)((status >> 8) & 0xFF));

            var warningCount = (ushort)session.WarningCount;
            buffer.WriteByte((byte)(warningCount & 0xFF));
            buffer.WriteByte((byte)((warningCount >> 8) & 0xFF));

            await writer.WritePacketAsync(buffer.ToArray(), cancellationToken);
        }
        else
        {
            if (statusOverride.HasValue)
            {
                var eofPacket = new byte[]
                {
                    0xFE, 0x00, 0x00,
                    (byte)(statusOverride.Value & 0xFF), (byte)((statusOverride.Value >> 8) & 0xFF)
                };
                await writer.WritePacketAsync(eofPacket, cancellationToken);
            }
            else
            {
                await SendEofPacketAsync(writer, session, cancellationToken);
            }
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
            _sslHandler?.Dispose();
            _rateLimiter.Dispose();
            _socketPool.Dispose();
            _disposed = true;
        }
    }

    /// <summary>
    /// Gets comprehensive server statistics including all enterprise components.
    /// </summary>
    public EnterpriseServerStats GetEnterpriseStats()
    {
        return new EnterpriseServerStats
        {
            BasicStats = GetStats(),
            NetworkMetrics = _metrics.GetSnapshot(),
            QueryLatency = _queryLatency.GetPercentiles(),
            SslStats = _sslHandler?.GetStats(),
            RateLimiterStats = _rateLimiter.GetStats(),
            IpFilterStats = _ipFilter.GetStats(),
            SocketPoolStats = _socketPool.GetStats(),
            CompressionStats = _compressionHandler.GetStats()
        };
    }

    /// <summary>
    /// Maps MySQL character set ID to character set name.
    /// </summary>
    private static string MapCharacterSetIdToName(int charsetId)
    {
        return charsetId switch
        {
            8 => "latin1",
            28 => "gbk",
            33 => "utf8",
            45 => "utf8mb4",
            63 => "binary",
            192 => "utf8",
            224 => "utf8mb4",
            255 => "utf8mb4",
            _ => "utf8mb4" // default
        };
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

/// <summary>
/// Comprehensive enterprise server statistics.
/// </summary>
public sealed class EnterpriseServerStats
{
    public ServerStats? BasicStats { get; init; }
    public NetworkMetricsSnapshot? NetworkMetrics { get; init; }
    public LatencyPercentiles? QueryLatency { get; init; }
    public SslHandlerStats? SslStats { get; init; }
    public RateLimiterStats? RateLimiterStats { get; init; }
    public IpFilterStats? IpFilterStats { get; init; }
    public SocketPoolStats? SocketPoolStats { get; init; }
    public CompressionStats? CompressionStats { get; init; }

    public override string ToString()
    {
        return $"EnterpriseStats: {BasicStats?.ActiveConnections} connections, " +
               $"{NetworkMetrics?.TotalQueries} queries, " +
               $"p99 latency={QueryLatency?.P99:F2}ms";
    }
}
