using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using CysRedis.Core.Auth;
using CysRedis.Core.Blocking;
using CysRedis.Core.Cluster;
using CysRedis.Core.Commands;
using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;
using CysRedis.Core.Monitoring;
using CysRedis.Core.Notifications;
using CysRedis.Core.PubSub;
using CysRedis.Core.Replication;
using CysRedis.Core.Scripting;
using CysRedis.Core.Security;
using CysRedis.Core.Storage;
using CysRedis.Core.Threading;

namespace CysRedis.Core.Protocol;

/// <summary>
/// High-performance Redis protocol server with optimized networking.
/// </summary>
public class RedisServer : IDisposable
{
    private readonly TcpListener _listener;
    private readonly TcpListener? _tlsListener;
    private readonly CancellationTokenSource _cts;
    private readonly ConcurrentDictionary<long, RedisClient> _clients;
    private readonly RedisServerOptions _options;
    private Task? _acceptTask;
    private Task? _acceptTlsTask;
    private Task? _healthCheckTask;
    private bool _disposed;
    private long _totalCommandsProcessed;
    private long _totalConnectionsReceived;
    private long _rejectedConnections;

    /// <summary>
    /// Server port.
    /// </summary>
    public int Port => _options.Port;

    /// <summary>
    /// Server options.
    /// </summary>
    public RedisServerOptions Options => _options;

    /// <summary>
    /// The Redis database store.
    /// </summary>
    public RedisStore Store { get; }

    /// <summary>
    /// Command dispatcher.
    /// </summary>
    public CommandDispatcher Dispatcher { get; }

    /// <summary>
    /// RDB persistence.
    /// </summary>
    public RdbPersistence? Persistence { get; private set; }

    /// <summary>
    /// AOF persistence.
    /// </summary>
    public AofPersistence? Aof { get; private set; }

    /// <summary>
    /// Pub/Sub manager.
    /// </summary>
    public PubSubManager PubSub { get; }

    /// <summary>
    /// Script manager.
    /// </summary>
    public ScriptManager ScriptManager { get; }

    /// <summary>
    /// Replication manager.
    /// </summary>
    public ReplicationManager Replication { get; }

    /// <summary>
    /// ACL manager.
    /// </summary>
    public AclManager Acl { get; }

    /// <summary>
    /// Blocking manager for blocking commands.
    /// </summary>
    public BlockingManager Blocking { get; }

    /// <summary>
    /// Keyspace event notifier.
    /// </summary>
    public KeyspaceNotifier KeyspaceNotifier { get; }

    /// <summary>
    /// Cluster manager.
    /// </summary>
    public ClusterManager Cluster { get; }

    /// <summary>
    /// Slow log manager.
    /// </summary>
    public SlowLog SlowLog { get; }

    /// <summary>
    /// Latency monitor for tracking latency spikes.
    /// </summary>
    public LatencyMonitor LatencyMonitor { get; }

    /// <summary>
    /// Command latency histogram for percentile statistics.
    /// </summary>
    public LatencyHistogram CommandLatency { get; }

    /// <summary>
    /// Network metrics collector.
    /// </summary>
    public NetworkMetrics NetworkMetrics { get; }

    /// <summary>
    /// IP filter for connection access control.
    /// </summary>
    public IpFilter IpFilter { get; }

    /// <summary>
    /// Connection rate limiter.
    /// </summary>
    public RateLimiter RateLimiter { get; }

    /// <summary>
    /// TLS handler for encrypted connections.
    /// </summary>
    public TlsHandler TlsHandler { get; }

    /// <summary>
    /// Client dispatcher for multi-threaded I/O.
    /// </summary>
    public ClientDispatcher IoDispatcher { get; }

    /// <summary>
    /// Number of connected clients.
    /// </summary>
    public int ClientCount => _clients.Count;

    /// <summary>
    /// Server start time.
    /// </summary>
    public DateTime StartTime { get; private set; }

    /// <summary>
    /// Last save time.
    /// </summary>
    public DateTime LastSaveTime { get; internal set; }

    /// <summary>
    /// Total commands processed.
    /// </summary>
    public long TotalCommandsProcessed => Interlocked.Read(ref _totalCommandsProcessed);

    /// <summary>
    /// Total connections received.
    /// </summary>
    public long TotalConnectionsReceived => Interlocked.Read(ref _totalConnectionsReceived);

    /// <summary>
    /// Rejected connections due to max clients limit.
    /// </summary>
    public long RejectedConnections => Interlocked.Read(ref _rejectedConnections);

    /// <summary>
    /// Creates a new Redis server with default options.
    /// </summary>
    public RedisServer(int port = Constants.DefaultPort, string? dataDir = null)
        : this(new RedisServerOptions { Port = port, DataDir = dataDir })
    {
    }

    /// <summary>
    /// Creates a new Redis server with specified options.
    /// </summary>
    public RedisServer(RedisServerOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        
        var bindAddress = IPAddress.Parse(_options.BindAddress);
        _listener = new TcpListener(bindAddress, _options.Port);
        
        // Configure server socket options
        ConfigureListenerSocket(_listener.Server);
        
        _cts = new CancellationTokenSource();
        _clients = new ConcurrentDictionary<long, RedisClient>();
        Store = new RedisStore(
            databaseCount: Constants.DefaultDatabaseCount,
            evictionPolicy: _options.EvictionPolicy,
            maxMemory: _options.MaxMemory);
        Dispatcher = new CommandDispatcher(this);
        PubSub = new PubSubManager();
        ScriptManager = new ScriptManager();
        Replication = new ReplicationManager(this);
        Acl = new AclManager();
        Blocking = new BlockingManager();
        KeyspaceNotifier = new KeyspaceNotifier(PubSub);
        Cluster = new ClusterManager();
        SlowLog = new SlowLog(_options.SlowLogThreshold, _options.SlowLogMaxLen);
        LatencyMonitor = new LatencyMonitor(_options.LatencyMonitorThreshold);
        CommandLatency = new LatencyHistogram();
        NetworkMetrics = new NetworkMetrics();
        IpFilter = new IpFilter(_options.IpFilter);
        RateLimiter = new RateLimiter(_options.RateLimit);
        TlsHandler = new TlsHandler(_options.Tls);
        IoDispatcher = new ClientDispatcher(_options.IoThreading, HandleCommandAsync);
        LastSaveTime = DateTime.UtcNow;

        // Initialize TLS listener if enabled
        if (_options.Tls.Enabled)
        {
            _tlsListener = new TcpListener(bindAddress, _options.Tls.Port);
            ConfigureListenerSocket(_tlsListener.Server);
        }

        // Initialize persistence if data directory is provided
        if (!string.IsNullOrEmpty(_options.DataDir))
        {
            Persistence = new RdbPersistence(Store, _options.DataDir);
            Aof = new AofPersistence(_options.DataDir);
        }
    }

    /// <summary>
    /// Configures the listener socket with optimized options.
    /// </summary>
    private void ConfigureListenerSocket(Socket socket)
    {
        // Allow address reuse for quick restart
        if (_options.ReuseAddress)
        {
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        }
    }

    /// <summary>
    /// Configures a client socket with optimized options.
    /// </summary>
    private void ConfigureClientSocket(Socket socket)
    {
        // Disable Nagle's algorithm for low latency
        socket.NoDelay = _options.TcpNoDelay;
        
        // Configure TCP Keep-Alive for connection health monitoring
        if (_options.TcpKeepAlive)
        {
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            
            // Platform-specific keep-alive settings
            try
            {
                socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, _options.TcpKeepAliveTime);
                socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, _options.TcpKeepAliveInterval);
                socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, _options.TcpKeepAliveRetryCount);
            }
            catch (SocketException)
            {
                // Some platforms may not support these options
                Logger.Debug("TCP keep-alive fine-tuning not supported on this platform");
            }
        }
        
        // Configure buffer sizes
        socket.ReceiveBufferSize = _options.ReceiveBufferSize;
        socket.SendBufferSize = _options.SendBufferSize;
    }

    /// <summary>
    /// Starts the server.
    /// </summary>
    public void Start()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RedisServer));

        _listener.Start(_options.Backlog);
        StartTime = DateTime.UtcNow;
        Logger.Info("Redis server started on port {0}", Port);
        Logger.Info("  TCP_NODELAY: {0}", _options.TcpNoDelay);
        Logger.Info("  TCP Keep-Alive: {0}", _options.TcpKeepAlive);
        Logger.Info("  Max clients: {0}", _options.MaxClients == 0 ? "unlimited" : _options.MaxClients.ToString());
        Logger.Info("  Client idle timeout: {0}", _options.ClientIdleTimeout == TimeSpan.Zero ? "disabled" : _options.ClientIdleTimeout.ToString());

        _acceptTask = AcceptConnectionsAsync();

        // Start TLS listener if enabled
        if (_tlsListener != null)
        {
            _tlsListener.Start(_options.Backlog);
            Logger.Info("TLS listener started on port {0}", _options.Tls.Port);
            _acceptTlsTask = AcceptTlsConnectionsAsync();
        }

        // Start I/O thread pool if enabled
        if (IoDispatcher.IsEnabled)
        {
            IoDispatcher.Start();
            Logger.Info("I/O threading enabled with {0} threads", IoDispatcher.ThreadPool.ThreadCount);
        }
        
        // Start health check task if idle timeout is configured
        if (_options.ClientIdleTimeout > TimeSpan.Zero)
        {
            _healthCheckTask = RunHealthCheckAsync();
        }
        
        // Start LRU clock updater for memory eviction
        _ = Memory.LruClock.StartClockUpdater(_cts.Token);
        Logger.Info("LRU clock updater started");
    }

    /// <summary>
    /// Stops the server.
    /// </summary>
    public void Stop()
    {
        StopAsync().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Stops the server gracefully.
    /// </summary>
    public async Task StopAsync()
    {
        if (_disposed) return;

        Logger.Info("Initiating graceful shutdown...");
        
        var shutdownTimeout = _options.GracefulShutdownTimeout;
        var shutdownDeadline = DateTime.UtcNow + shutdownTimeout;

        // Phase 1: Stop accepting new connections
        Logger.Info("Phase 1: Stopping listener...");
        _listener.Stop();
        _tlsListener?.Stop();

        // Phase 2: Notify clients about shutdown
        if (_options.WaitForClientsOnShutdown)
        {
            Logger.Info("Phase 2: Waiting for clients to finish ({0} active)...", _clients.Count);
            
            // Send shutdown notification to non-transaction clients
            var notificationTasks = new List<Task>();
            foreach (var client in _clients.Values.Where(c => !c.InTransaction && !c.InPubSubMode))
            {
                notificationTasks.Add(TryNotifyShutdownAsync(client));
            }

            try
            {
                await Task.WhenAll(notificationTasks).ConfigureAwait(false);
            }
            catch
            {
                // Ignore notification errors
            }

            // Wait for in-progress transactions to complete
            var waitStart = DateTime.UtcNow;
            while (_clients.Values.Any(c => c.InTransaction) && DateTime.UtcNow < shutdownDeadline)
            {
                Logger.Debug("Waiting for {0} transactions to complete...",
                    _clients.Values.Count(c => c.InTransaction));
                await Task.Delay(100).ConfigureAwait(false);
            }

            var waitDuration = DateTime.UtcNow - waitStart;
            Logger.Info("Waited {0:F1}s for clients", waitDuration.TotalSeconds);
        }

        // Phase 3: Save data if configured
        if (_options.SaveOnShutdown && Persistence != null)
        {
            Logger.Info("Phase 3: Saving RDB...");
            try
            {
                await Task.Run(() => Persistence.Save()).ConfigureAwait(false);
                Logger.Info("RDB saved successfully");
            }
            catch (Exception ex)
            {
                Logger.Error("Failed to save RDB on shutdown", ex);
            }
        }

        // Phase 4: Cancel operations and close connections
        Logger.Info("Phase 4: Closing connections...");
        _cts.Cancel();

        // Close all client connections
        foreach (var client in _clients.Values)
        {
            try { client.Close(); }
            catch { /* ignore */ }
        }
        _clients.Clear();

        // Wait for accept tasks to complete
        try
        {
            var remainingTime = shutdownDeadline - DateTime.UtcNow;
            if (remainingTime > TimeSpan.Zero)
            {
                var acceptTimeout = (int)Math.Min(remainingTime.TotalMilliseconds, 5000);
                if (_acceptTask != null)
                    await _acceptTask.WaitAsync(TimeSpan.FromMilliseconds(acceptTimeout)).ConfigureAwait(false);
                if (_acceptTlsTask != null)
                    await _acceptTlsTask.WaitAsync(TimeSpan.FromMilliseconds(acceptTimeout)).ConfigureAwait(false);
                if (_healthCheckTask != null)
                    await _healthCheckTask.WaitAsync(TimeSpan.FromMilliseconds(2000)).ConfigureAwait(false);
            }
        }
        catch (TimeoutException)
        {
            Logger.Warning("Shutdown tasks did not complete within timeout");
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        catch (AggregateException)
        {
            // Expected
        }

        Logger.Info("Server shutdown complete");
    }

    /// <summary>
    /// Tries to send shutdown notification to a client.
    /// </summary>
    private async Task TryNotifyShutdownAsync(RedisClient client)
    {
        try
        {
            // Send a push message to notify about shutdown
            await client.WriteErrorAsync(
                "ERR Server is shutting down",
                CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // Ignore errors - client may already be disconnected
        }
    }

    /// <summary>
    /// Handles a command for a client (used by I/O threads).
    /// </summary>
    private async Task HandleCommandAsync(RedisClient client, string[] args, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _totalCommandsProcessed);
        await Dispatcher.ExecuteAsync(client, args, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Background task to accept new connections.
    /// </summary>
    private async Task AcceptConnectionsAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                var tcpClient = await _listener.AcceptTcpClientAsync(_cts.Token).ConfigureAwait(false);
                Interlocked.Increment(ref _totalConnectionsReceived);

                // Check IP filter
                var remoteEndPoint = tcpClient.Client.RemoteEndPoint as IPEndPoint;
                var clientIp = remoteEndPoint?.Address.ToString() ?? "unknown";

                if (remoteEndPoint != null && !IpFilter.IsAllowed(remoteEndPoint.Address))
                {
                    Interlocked.Increment(ref _rejectedConnections);
                    NetworkMetrics.RecordRejectedConnection();
                    tcpClient.Close();
                    continue;
                }

                // Check rate limit
                if (!RateLimiter.TryAcquire(clientIp))
                {
                    Interlocked.Increment(ref _rejectedConnections);
                    NetworkMetrics.RecordRejectedConnection();
                    await RejectConnectionAsync(tcpClient, "ERR too many connections from your address").ConfigureAwait(false);
                    continue;
                }
                
                // Check max clients limit
                if (_options.MaxClients > 0 && _clients.Count >= _options.MaxClients)
                {
                    Interlocked.Increment(ref _rejectedConnections);
                    NetworkMetrics.RecordRejectedConnection();
                    RateLimiter.Release(clientIp);
                    Logger.Warning("Max clients ({0}) reached, rejecting connection from {1}", 
                        _options.MaxClients, tcpClient.Client.RemoteEndPoint);
                    
                    await RejectConnectionAsync(tcpClient, "ERR max number of clients reached").ConfigureAwait(false);
                    continue;
                }
                
                // Configure socket options for the accepted connection
                ConfigureClientSocket(tcpClient.Client);
                
                var client = new RedisClient(tcpClient, _options);
                
                if (_clients.TryAdd(client.Id, client))
                {
                    NetworkMetrics.RecordConnection();

                    // Assign to I/O thread if enabled
                    var threadId = IoDispatcher.AssignClient(client);
                    if (threadId >= 0)
                    {
                        Logger.Debug("Client {0} connected, assigned to I/O thread {1}", client.Id, threadId);
                    }
                    else
                    {
                        Logger.Debug("Client connected: {0}", client);
                    }

                    _ = HandleClientAsync(client);
                }
                else
                {
                    client.Dispose();
                }
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
                if (!_cts.Token.IsCancellationRequested)
                {
                    NetworkMetrics.RecordConnectionError();
                    Logger.Error("Error accepting connection", ex);
                }
            }
        }
    }

    /// <summary>
    /// Accepts TLS connections.
    /// </summary>
    private async Task AcceptTlsConnectionsAsync()
    {
        while (!_cts.Token.IsCancellationRequested && _tlsListener != null)
        {
            try
            {
                var tcpClient = await _tlsListener.AcceptTcpClientAsync(_cts.Token).ConfigureAwait(false);
                Interlocked.Increment(ref _totalConnectionsReceived);

                // Check IP filter
                var remoteEndPoint = tcpClient.Client.RemoteEndPoint as IPEndPoint;
                var clientIp = remoteEndPoint?.Address.ToString() ?? "unknown";

                if (remoteEndPoint != null && !IpFilter.IsAllowed(remoteEndPoint.Address))
                {
                    Interlocked.Increment(ref _rejectedConnections);
                    NetworkMetrics.RecordRejectedConnection();
                    tcpClient.Close();
                    continue;
                }

                // Check rate limit
                if (!RateLimiter.TryAcquire(clientIp))
                {
                    Interlocked.Increment(ref _rejectedConnections);
                    NetworkMetrics.RecordRejectedConnection();
                    tcpClient.Close();
                    continue;
                }

                // Check max clients limit
                if (_options.MaxClients > 0 && _clients.Count >= _options.MaxClients)
                {
                    Interlocked.Increment(ref _rejectedConnections);
                    NetworkMetrics.RecordRejectedConnection();
                    RateLimiter.Release(clientIp);
                    tcpClient.Close();
                    continue;
                }

                // Configure socket options
                ConfigureClientSocket(tcpClient.Client);

                // Perform TLS handshake
                try
                {
                    var networkStream = tcpClient.GetStream();
                    var sslStream = await TlsHandler.AuthenticateAsServerAsync(networkStream, _cts.Token)
                        .ConfigureAwait(false);

                    var client = new RedisClient(tcpClient, sslStream, _options, isTls: true);

                    if (_clients.TryAdd(client.Id, client))
                    {
                        NetworkMetrics.RecordConnection();

                        // Assign to I/O thread if enabled
                        var threadId = IoDispatcher.AssignClient(client);
                        if (threadId >= 0)
                        {
                            Logger.Debug("TLS client {0} connected, assigned to I/O thread {1}", client.Id, threadId);
                        }
                        else
                        {
                            Logger.Debug("TLS client connected: {0}", client);
                        }

                        _ = HandleClientAsync(client);
                    }
                    else
                    {
                        client.Dispose();
                    }
                }
                catch (Exception ex)
                {
                    Logger.Warning("TLS handshake failed for {0}: {1}", clientIp, ex.Message);
                    RateLimiter.Release(clientIp);
                    tcpClient.Close();
                }
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
                if (!_cts.Token.IsCancellationRequested)
                {
                    NetworkMetrics.RecordConnectionError();
                    Logger.Error("Error accepting TLS connection", ex);
                }
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
            var stream = tcpClient.GetStream();
            var errorResponse = Encoding.UTF8.GetBytes($"-{errorMessage}\r\n");
            await stream.WriteAsync(errorResponse).ConfigureAwait(false);
            await stream.FlushAsync().ConfigureAwait(false);
        }
        catch
        {
            // Ignore errors during rejection
        }
        finally
        {
            tcpClient.Close();
        }
    }

    /// <summary>
    /// Background task to check client health and close idle connections.
    /// </summary>
    private async Task RunHealthCheckAsync()
    {
        Logger.Debug("Health check task started (interval: {0}s, timeout: {1}s)", 
            _options.HealthCheckInterval.TotalSeconds,
            _options.ClientIdleTimeout.TotalSeconds);
        
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.HealthCheckInterval, _cts.Token).ConfigureAwait(false);
                
                var now = DateTime.UtcNow;
                var closedCount = 0;
                
                foreach (var kvp in _clients)
                {
                    var client = kvp.Value;
                    
                    // Skip clients in special modes (PubSub, blocked, etc.)
                    if (client.InPubSubMode || client.Flags.HasFlag(ClientFlags.Blocked))
                        continue;
                    
                    if (client.IdleTime > _options.ClientIdleTimeout)
                    {
                        Logger.Debug("Closing idle client: {0} (idle: {1}s)", 
                            client, client.IdleTime.TotalSeconds);
                        
                        // Try to send timeout error before closing
                        try
                        {
                            await client.WriteErrorAsync("ERR connection timeout", _cts.Token).ConfigureAwait(false);
                        }
                        catch
                        {
                            // Ignore write errors
                        }
                        
                        if (_clients.TryRemove(client.Id, out _))
                        {
                            client.Close();
                            closedCount++;
                        }
                    }
                }
                
                if (closedCount > 0)
                {
                    Logger.Info("Health check: closed {0} idle connection(s), {1} active", closedCount, _clients.Count);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.Error("Error in health check task", ex);
            }
        }
        
        Logger.Debug("Health check task stopped");
    }

    /// <summary>
    /// Handles a client connection.
    /// </summary>
    private async Task HandleClientAsync(RedisClient client)
    {
        try
        {
            while (!_cts.Token.IsCancellationRequested && client.IsConnected)
            {
                try
                {
                    var args = await client.ReadCommandAsync(_cts.Token).ConfigureAwait(false);
                    if (args == null || args.Length == 0)
                    {
                        // Client disconnected or sent empty command
                        break;
                    }

                    Interlocked.Increment(ref _totalCommandsProcessed);
                    
                    // Dispatch command
                    await Dispatcher.ExecuteAsync(client, args, _cts.Token).ConfigureAwait(false);
                }
                catch (RedisException ex)
                {
                    await client.WriteErrorAsync(ex.GetRespError(), _cts.Token).ConfigureAwait(false);
                }
                catch (IOException)
                {
                    // Client disconnected
                    break;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Logger.Error("Error handling command for {0}", client, ex);
                    try
                    {
                        await client.WriteErrorAsync("ERR internal error", _cts.Token).ConfigureAwait(false);
                    }
                    catch
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            Logger.Debug("Client disconnected: {0}", client);
            _clients.TryRemove(client.Id, out _);
            NetworkMetrics.RecordDisconnection();

            // Release rate limiter slot
            var clientIp = ExtractIpFromAddress(client.Address);
            RateLimiter.Release(clientIp);

            // Remove from I/O thread
            IoDispatcher.RemoveClient(client.Id);

            client.Dispose();
        }
    }

    /// <summary>
    /// Extracts IP address from client address string (IP:port format).
    /// </summary>
    private static string ExtractIpFromAddress(string address)
    {
        if (string.IsNullOrEmpty(address))
            return "unknown";

        // Handle IPv6 addresses: [::1]:port
        if (address.StartsWith('['))
        {
            var endBracket = address.IndexOf(']');
            if (endBracket > 0)
                return address[1..endBracket];
        }

        // Handle IPv4 addresses: 127.0.0.1:port
        var colonIndex = address.LastIndexOf(':');
        if (colonIndex > 0)
            return address[..colonIndex];

        return address;
    }

    /// <summary>
    /// Gets all connected clients.
    /// </summary>
    public IEnumerable<RedisClient> GetClients() => _clients.Values;

    /// <summary>
    /// Gets a client by ID.
    /// </summary>
    public RedisClient? GetClient(long id)
    {
        _clients.TryGetValue(id, out var client);
        return client;
    }

    /// <summary>
    /// Kills a client by ID.
    /// </summary>
    public bool KillClient(long id)
    {
        if (_clients.TryRemove(id, out var client))
        {
            client.Close();
            return true;
        }
        return false;
    }

    /// <summary>
    /// Gets server statistics.
    /// </summary>
    public ServerStats GetStats()
    {
        return new ServerStats
        {
            ConnectedClients = _clients.Count,
            TotalConnectionsReceived = TotalConnectionsReceived,
            TotalCommandsProcessed = TotalCommandsProcessed,
            RejectedConnections = RejectedConnections,
            UptimeSeconds = (long)(DateTime.UtcNow - StartTime).TotalSeconds
        };
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        Stop();
        _cts.Dispose();
        RateLimiter.Dispose();
        TlsHandler.Dispose();
        IoDispatcher.Dispose();
        Store.Dispose();

        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Server statistics.
/// </summary>
public class ServerStats
{
    public int ConnectedClients { get; init; }
    public long TotalConnectionsReceived { get; init; }
    public long TotalCommandsProcessed { get; init; }
    public long RejectedConnections { get; init; }
    public long UptimeSeconds { get; init; }
}
