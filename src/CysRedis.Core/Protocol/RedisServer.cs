using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
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
/// Redis protocol server.
/// </summary>
public class RedisServer : IDisposable
{
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts;
    private readonly ConcurrentDictionary<long, RedisClient> _clients;
    private Task? _acceptTask;
    private bool _disposed;
    private long _totalCommandsProcessed;

    /// <summary>
    /// Server port.
    /// </summary>
    public int Port { get; }

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
    /// Creates a new Redis server.
    /// </summary>
    public RedisServer(int port = Constants.DefaultPort, string? dataDir = null)
    {
        Port = port;
        _listener = new TcpListener(IPAddress.Any, port);
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
        if (!string.IsNullOrEmpty(dataDir))
        {
            Persistence = new RdbPersistence(Store, dataDir);
            Aof = new AofPersistence(dataDir);
        }
    }

    /// <summary>
    /// Starts the server.
    /// </summary>
    public void Start()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RedisServer));

        _listener.Start();
        StartTime = DateTime.UtcNow;
        Logger.Info("Redis server started on port {0}", Port);

        _acceptTask = AcceptConnectionsAsync();
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
        }
        catch (AggregateException)
        {
            // Expected
        }

        Logger.Info("Redis server stopped");
    }

    private async Task AcceptConnectionsAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                var tcpClient = await _listener.AcceptTcpClientAsync(_cts.Token);
                var client = new RedisClient(tcpClient);
                
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

    private async Task HandleClientAsync(RedisClient client)
    {
        try
        {
            while (!_cts.Token.IsCancellationRequested && client.IsConnected)
            {
                try
                {
                    var args = await client.ReadCommandAsync(_cts.Token);
                    if (args == null || args.Length == 0)
                    {
                        // Client disconnected or sent empty command
                        break;
                    }

                    Interlocked.Increment(ref _totalCommandsProcessed);
                    
                    // Dispatch command
                    await Dispatcher.ExecuteAsync(client, args, _cts.Token);
                }
                catch (RedisException ex)
                {
                    await client.WriteErrorAsync(ex.GetRespError(), _cts.Token);
                }
                catch (IOException)
                {
                    // Client disconnected
                    break;
                }
                catch (Exception ex)
                {
                    Logger.Error("Error handling command for {0}", client, ex);
                    try
                    {
                        await client.WriteErrorAsync("ERR internal error", _cts.Token);
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
