using System.Diagnostics;

namespace CysRedis.Core.Monitoring;

/// <summary>
/// Network metrics collector for Redis server.
/// Provides real-time statistics on traffic, connections, and errors.
/// </summary>
public sealed class NetworkMetrics
{
    // Traffic statistics
    private long _bytesReceived;
    private long _bytesSent;
    private long _packetsReceived;
    private long _packetsSent;

    // Connection statistics
    private int _activeConnections;
    private long _totalConnections;
    private long _rejectedConnections;
    private long _timedOutConnections;

    // Error statistics
    private long _protocolErrors;
    private long _connectionErrors;

    // Timing
    private readonly DateTime _startTime;
    private readonly Stopwatch _uptime;

    /// <summary>
    /// Gets the total bytes received.
    /// </summary>
    public long BytesReceived => Interlocked.Read(ref _bytesReceived);

    /// <summary>
    /// Gets the total bytes sent.
    /// </summary>
    public long BytesSent => Interlocked.Read(ref _bytesSent);

    /// <summary>
    /// Gets the total packets received.
    /// </summary>
    public long PacketsReceived => Interlocked.Read(ref _packetsReceived);

    /// <summary>
    /// Gets the total packets sent.
    /// </summary>
    public long PacketsSent => Interlocked.Read(ref _packetsSent);

    /// <summary>
    /// Gets the current number of active connections.
    /// </summary>
    public int ActiveConnections => _activeConnections;

    /// <summary>
    /// Gets the total number of connections established.
    /// </summary>
    public long TotalConnections => Interlocked.Read(ref _totalConnections);

    /// <summary>
    /// Gets the number of rejected connections.
    /// </summary>
    public long RejectedConnections => Interlocked.Read(ref _rejectedConnections);

    /// <summary>
    /// Gets the number of timed out connections.
    /// </summary>
    public long TimedOutConnections => Interlocked.Read(ref _timedOutConnections);

    /// <summary>
    /// Gets the number of protocol errors.
    /// </summary>
    public long ProtocolErrors => Interlocked.Read(ref _protocolErrors);

    /// <summary>
    /// Gets the number of connection errors.
    /// </summary>
    public long ConnectionErrors => Interlocked.Read(ref _connectionErrors);

    /// <summary>
    /// Gets the server uptime.
    /// </summary>
    public TimeSpan Uptime => _uptime.Elapsed;

    /// <summary>
    /// Gets the server start time.
    /// </summary>
    public DateTime StartTime => _startTime;

    /// <summary>
    /// Creates a new network metrics collector.
    /// </summary>
    public NetworkMetrics()
    {
        _startTime = DateTime.UtcNow;
        _uptime = Stopwatch.StartNew();
    }

    #region Recording Methods

    /// <summary>
    /// Records bytes received.
    /// </summary>
    public void RecordReceive(int bytes)
    {
        Interlocked.Add(ref _bytesReceived, bytes);
        Interlocked.Increment(ref _packetsReceived);
    }

    /// <summary>
    /// Records bytes sent.
    /// </summary>
    public void RecordSend(int bytes)
    {
        Interlocked.Add(ref _bytesSent, bytes);
        Interlocked.Increment(ref _packetsSent);
    }

    /// <summary>
    /// Records a new connection.
    /// </summary>
    public void RecordConnection()
    {
        Interlocked.Increment(ref _activeConnections);
        Interlocked.Increment(ref _totalConnections);
    }

    /// <summary>
    /// Records a connection closed.
    /// </summary>
    public void RecordDisconnection()
    {
        Interlocked.Decrement(ref _activeConnections);
    }

    /// <summary>
    /// Records a rejected connection.
    /// </summary>
    public void RecordRejectedConnection()
    {
        Interlocked.Increment(ref _rejectedConnections);
    }

    /// <summary>
    /// Records a connection timeout.
    /// </summary>
    public void RecordTimeout()
    {
        Interlocked.Increment(ref _timedOutConnections);
    }

    /// <summary>
    /// Records a protocol error.
    /// </summary>
    public void RecordProtocolError()
    {
        Interlocked.Increment(ref _protocolErrors);
    }

    /// <summary>
    /// Records a connection error.
    /// </summary>
    public void RecordConnectionError()
    {
        Interlocked.Increment(ref _connectionErrors);
    }

    #endregion

    /// <summary>
    /// Gets a snapshot of all metrics.
    /// </summary>
    public NetworkMetricsSnapshot GetSnapshot()
    {
        var uptime = _uptime.Elapsed;
        var uptimeSeconds = uptime.TotalSeconds;

        return new NetworkMetricsSnapshot
        {
            // Traffic
            BytesReceived = BytesReceived,
            BytesSent = BytesSent,
            PacketsReceived = PacketsReceived,
            PacketsSent = PacketsSent,
            BytesReceivedPerSecond = uptimeSeconds > 0 ? BytesReceived / uptimeSeconds : 0,
            BytesSentPerSecond = uptimeSeconds > 0 ? BytesSent / uptimeSeconds : 0,

            // Connections
            ActiveConnections = ActiveConnections,
            TotalConnections = TotalConnections,
            RejectedConnections = RejectedConnections,
            TimedOutConnections = TimedOutConnections,
            ConnectionsPerSecond = uptimeSeconds > 0 ? TotalConnections / uptimeSeconds : 0,

            // Errors
            ProtocolErrors = ProtocolErrors,
            ConnectionErrors = ConnectionErrors,
            TotalErrors = ProtocolErrors + ConnectionErrors,

            // Timing
            Uptime = uptime,
            StartTime = _startTime
        };
    }

    /// <summary>
    /// Resets all metrics.
    /// </summary>
    public void Reset()
    {
        _bytesReceived = 0;
        _bytesSent = 0;
        _packetsReceived = 0;
        _packetsSent = 0;
        _totalConnections = 0;
        _rejectedConnections = 0;
        _timedOutConnections = 0;
        _protocolErrors = 0;
        _connectionErrors = 0;
        _uptime.Restart();
    }
}

/// <summary>
/// Snapshot of network metrics at a point in time.
/// </summary>
public sealed class NetworkMetricsSnapshot
{
    // Traffic
    public long BytesReceived { get; init; }
    public long BytesSent { get; init; }
    public long PacketsReceived { get; init; }
    public long PacketsSent { get; init; }
    public double BytesReceivedPerSecond { get; init; }
    public double BytesSentPerSecond { get; init; }

    // Connections
    public int ActiveConnections { get; init; }
    public long TotalConnections { get; init; }
    public long RejectedConnections { get; init; }
    public long TimedOutConnections { get; init; }
    public double ConnectionsPerSecond { get; init; }

    // Errors
    public long ProtocolErrors { get; init; }
    public long ConnectionErrors { get; init; }
    public long TotalErrors { get; init; }

    // Timing
    public TimeSpan Uptime { get; init; }
    public DateTime StartTime { get; init; }

    public override string ToString()
    {
        return $"Metrics: {ActiveConnections} active, {TotalConnections} total, " +
               $"rx={FormatBytes(BytesReceived)}, tx={FormatBytes(BytesSent)}";
    }

    private static string FormatBytes(long bytes)
    {
        string[] sizes = { "B", "KB", "MB", "GB", "TB" };
        double len = bytes;
        int order = 0;
        while (len >= 1024 && order < sizes.Length - 1)
        {
            order++;
            len /= 1024;
        }
        return $"{len:0.##}{sizes[order]}";
    }
}
