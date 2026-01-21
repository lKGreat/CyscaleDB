using System.Diagnostics;

namespace CyscaleDB.Core.Protocol.Monitoring;

/// <summary>
/// Comprehensive network metrics collector for MySQL server.
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
    private long _timeoutErrors;
    private long _sslErrors;
    private long _compressionErrors;

    // Query statistics
    private long _totalQueries;
    private long _slowQueries;

    // Timing
    private readonly DateTime _startTime;
    private readonly Stopwatch _uptime;

    /// <summary>
    /// Gets the total bytes received.
    /// </summary>
    public long BytesReceived => _bytesReceived;

    /// <summary>
    /// Gets the total bytes sent.
    /// </summary>
    public long BytesSent => _bytesSent;

    /// <summary>
    /// Gets the total packets received.
    /// </summary>
    public long PacketsReceived => _packetsReceived;

    /// <summary>
    /// Gets the total packets sent.
    /// </summary>
    public long PacketsSent => _packetsSent;

    /// <summary>
    /// Gets the current number of active connections.
    /// </summary>
    public int ActiveConnections => _activeConnections;

    /// <summary>
    /// Gets the total number of connections established.
    /// </summary>
    public long TotalConnections => _totalConnections;

    /// <summary>
    /// Gets the number of rejected connections.
    /// </summary>
    public long RejectedConnections => _rejectedConnections;

    /// <summary>
    /// Gets the number of protocol errors.
    /// </summary>
    public long ProtocolErrors => _protocolErrors;

    /// <summary>
    /// Gets the number of timeout errors.
    /// </summary>
    public long TimeoutErrors => _timeoutErrors;

    /// <summary>
    /// Gets the number of SSL errors.
    /// </summary>
    public long SslErrors => _sslErrors;

    /// <summary>
    /// Gets the total number of queries executed.
    /// </summary>
    public long TotalQueries => _totalQueries;

    /// <summary>
    /// Gets the number of slow queries.
    /// </summary>
    public long SlowQueries => _slowQueries;

    /// <summary>
    /// Gets the server uptime.
    /// </summary>
    public TimeSpan Uptime => _uptime.Elapsed;

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
        Interlocked.Increment(ref _timeoutErrors);
    }

    /// <summary>
    /// Records a protocol error.
    /// </summary>
    public void RecordProtocolError()
    {
        Interlocked.Increment(ref _protocolErrors);
    }

    /// <summary>
    /// Records an SSL error.
    /// </summary>
    public void RecordSslError()
    {
        Interlocked.Increment(ref _sslErrors);
    }

    /// <summary>
    /// Records a compression error.
    /// </summary>
    public void RecordCompressionError()
    {
        Interlocked.Increment(ref _compressionErrors);
    }

    /// <summary>
    /// Records a query execution.
    /// </summary>
    /// <param name="isSlow">Whether the query was slow.</param>
    public void RecordQuery(bool isSlow = false)
    {
        Interlocked.Increment(ref _totalQueries);
        if (isSlow)
        {
            Interlocked.Increment(ref _slowQueries);
        }
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
            BytesReceived = _bytesReceived,
            BytesSent = _bytesSent,
            PacketsReceived = _packetsReceived,
            PacketsSent = _packetsSent,
            BytesReceivedPerSecond = uptimeSeconds > 0 ? _bytesReceived / uptimeSeconds : 0,
            BytesSentPerSecond = uptimeSeconds > 0 ? _bytesSent / uptimeSeconds : 0,

            // Connections
            ActiveConnections = _activeConnections,
            TotalConnections = _totalConnections,
            RejectedConnections = _rejectedConnections,
            TimedOutConnections = _timedOutConnections,
            ConnectionsPerSecond = uptimeSeconds > 0 ? _totalConnections / uptimeSeconds : 0,

            // Errors
            ProtocolErrors = _protocolErrors,
            TimeoutErrors = _timeoutErrors,
            SslErrors = _sslErrors,
            CompressionErrors = _compressionErrors,
            TotalErrors = _protocolErrors + _timeoutErrors + _sslErrors + _compressionErrors,

            // Queries
            TotalQueries = _totalQueries,
            SlowQueries = _slowQueries,
            QueriesPerSecond = uptimeSeconds > 0 ? _totalQueries / uptimeSeconds : 0,

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
        _timeoutErrors = 0;
        _sslErrors = 0;
        _compressionErrors = 0;
        _totalQueries = 0;
        _slowQueries = 0;
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
    public long TimeoutErrors { get; init; }
    public long SslErrors { get; init; }
    public long CompressionErrors { get; init; }
    public long TotalErrors { get; init; }

    // Queries
    public long TotalQueries { get; init; }
    public long SlowQueries { get; init; }
    public double QueriesPerSecond { get; init; }

    // Timing
    public TimeSpan Uptime { get; init; }
    public DateTime StartTime { get; init; }

    public override string ToString()
    {
        return $"Metrics: {ActiveConnections} active, {TotalConnections} total, " +
               $"{TotalQueries} queries, {TotalErrors} errors, uptime={Uptime}";
    }
}
