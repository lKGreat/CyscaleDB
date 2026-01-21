using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using CysRedis.Core.Auth;
using CysRedis.Core.Commands;
using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;
using CysRedis.Core.PubSub;
using CysRedis.Core.Replication;
using CysRedis.Core.Scripting;
using CysRedis.Core.Storage;

namespace CysRedis.Core.Protocol;

/// <summary>
/// High-performance Redis protocol server with optimized networking.
/// </summary>
public class RedisServer : IDisposable
{
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts;
    private readonly ConcurrentDictionary<long, RedisClient> _clients;
    private readonly RedisServerOptions _options;
    private Task? _acceptTask;
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
        Store = new RedisStore();
        Dispatcher = new CommandDispatcher(this);
        PubSub = new PubSubManager();
        ScriptManager = new ScriptManager();
        Replication = new ReplicationManager(this);
        Acl = new AclManager();
        LastSaveTime = DateTime.UtcNow;

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
        
        // Start health check task if idle timeout is configured
        if (_options.ClientIdleTimeout > TimeSpan.Zero)
        {
            _healthCheckTask = RunHealthCheckAsync();
        }
    }

    /// <summary>
    /// Stops the server.
    /// </summary>
    public void Stop()
    {
        if (_disposed) return;

        Logger.Info("Stopping Redis server...");
        _cts.Cancel();
        _listener.Stop();

        // Close all client connections
        foreach (var client in _clients.Values)
        {
            try { client.Close(); }
            catch { /* ignore */ }
        }
        _clients.Clear();

        try
        {
            _acceptTask?.Wait(TimeSpan.FromSeconds(5));
            _healthCheckTask?.Wait(TimeSpan.FromSeconds(2));
        }
        catch (AggregateException)
        {
            // Expected
        }

        Logger.Info("Redis server stopped");
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
                
                // Check max clients limit
                if (_options.MaxClients > 0 && _clients.Count >= _options.MaxClients)
                {
                    Interlocked.Increment(ref _rejectedConnections);
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
                    Logger.Debug("Client connected: {0}", client);
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
                    Logger.Error("Error accepting connection", ex);
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
            client.Dispose();
        }
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
