using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Transactions;

namespace CyscaleDB.Core.Protocol.Tds;

/// <summary>
/// TDS (Tabular Data Stream) protocol server implementing [MS-TDS] specification.
/// Enables connectivity from SSMS, Azure Data Studio, DBeaver (SQL Server mode),
/// and other SQL Server client tools.
///
/// Connection flow:
///   1. Client connects to port 1433
///   2. Client sends PreLogin → Server responds with PreLogin
///   3. Client sends LOGIN7 → Server validates and sends LoginAck + EnvChange + Done
///   4. Client sends SQL Batch / RPC → Server executes and returns results
/// </summary>
public sealed class TdsServer : IDisposable
{
    private readonly StorageEngine _storageEngine;
    private readonly TransactionManager _transactionManager;
    private readonly TcpListener _listener;
    private readonly Logger _logger;
    private readonly CancellationTokenSource _cts;
    private readonly ConcurrentDictionary<int, TdsSession> _sessions;
    private readonly int _port;
    private bool _disposed;
    private Task? _acceptTask;
    private int _nextSessionId = 1;

    /// <summary>
    /// Gets the port the TDS server is listening on.
    /// </summary>
    public int Port => _port;

    /// <summary>
    /// Gets the number of active sessions.
    /// </summary>
    public int ActiveSessionCount => _sessions.Count;

    public TdsServer(StorageEngine storageEngine, TransactionManager transactionManager, int port = TdsConstants.DefaultPort)
    {
        _storageEngine = storageEngine ?? throw new ArgumentNullException(nameof(storageEngine));
        _transactionManager = transactionManager ?? throw new ArgumentNullException(nameof(transactionManager));
        _port = port;
        _listener = new TcpListener(IPAddress.Any, port);
        _logger = LogManager.Default.GetLogger<TdsServer>();
        _cts = new CancellationTokenSource();
        _sessions = new ConcurrentDictionary<int, TdsSession>();
    }

    /// <summary>
    /// Starts the TDS server.
    /// </summary>
    public void Start()
    {
        _listener.Start();
        _logger.Info("TDS server started on port {0}", _port);
        _acceptTask = AcceptClientsAsync(_cts.Token);
    }

    /// <summary>
    /// Stops the TDS server.
    /// </summary>
    public void Stop()
    {
        _cts.Cancel();
        _listener.Stop();

        foreach (var session in _sessions.Values)
            session.Dispose();
        _sessions.Clear();

        _logger.Info("TDS server stopped");
    }

    private async Task AcceptClientsAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var client = await _listener.AcceptTcpClientAsync(ct);
                _ = HandleClientAsync(client, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.Error("Error accepting TDS client: {0}", ex.Message);
            }
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
    {
        var sessionId = Interlocked.Increment(ref _nextSessionId);
        TdsSession? session = null;

        try
        {
            client.NoDelay = true;
            var stream = client.GetStream();
            var reader = new TdsPacketReader(stream);
            var writer = new TdsPacketWriter(stream);

            var remoteEp = client.Client.RemoteEndPoint?.ToString() ?? "unknown";
            _logger.Info("TDS client connected from {0}, SPID={1}", remoteEp, sessionId);

            // Step 1: Handle PreLogin
            var preLoginResult = await HandlePreLoginAsync(reader, writer, ct);
            if (!preLoginResult)
            {
                _logger.Warning("TDS PreLogin failed for SPID={0}", sessionId);
                return;
            }

            // Step 2: Handle LOGIN7
            var executor = new Executor(_storageEngine.Catalog, _transactionManager);
            session = new TdsSession(sessionId, executor)
            {
                RemoteAddress = remoteEp
            };

            var loginResult = await HandleLogin7Async(reader, writer, session, ct);
            if (!loginResult)
            {
                _logger.Warning("TDS LOGIN7 failed for SPID={0}", sessionId);
                return;
            }

            _sessions.TryAdd(sessionId, session);
            _logger.Info("TDS login successful: user={0}, db={1}, SPID={2}",
                session.Username, session.CurrentDatabase, sessionId);

            // Step 3: Command loop
            await ProcessCommandsAsync(reader, writer, session, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.Error("TDS session error SPID={0}: {1}", sessionId, ex.Message);
        }
        finally
        {
            if (session != null)
            {
                _sessions.TryRemove(sessionId, out _);
                session.Dispose();
            }
            client.Close();
            _logger.Info("TDS client disconnected SPID={0}", sessionId);
        }
    }

    #region PreLogin

    private async Task<bool> HandlePreLoginAsync(TdsPacketReader reader, TdsPacketWriter writer, CancellationToken ct)
    {
        var msg = await reader.ReadMessageAsync(ct);
        if (msg == null || msg.Value.PacketType != TdsConstants.PacketTypePreLogin)
            return false;

        // Build PreLogin response
        using var response = new MemoryStream();
        using var bw = new BinaryWriter(response);

        // Option tokens: VERSION, ENCRYPTION, THREADID, MARS, TERMINATOR
        // Each option: [Token:1][Offset:2][Length:2]
        // Data starts after option tokens

        var optionHeaderSize = 5 * 4 + 1; // 4 options * 5 bytes + terminator byte = 21
        var versionOffset = (ushort)optionHeaderSize;
        var encryptionOffset = (ushort)(versionOffset + 6); // version is 6 bytes
        var threadIdOffset = (ushort)(encryptionOffset + 1); // encryption is 1 byte
        var marsOffset = (ushort)(threadIdOffset + 4); // thread ID is 4 bytes

        // VERSION option
        bw.Write(TdsConstants.PreLoginVersion);
        BinaryPrimitives.WriteUInt16BigEndian(response.GetBuffer().AsSpan((int)response.Position, 2), versionOffset);
        response.Position += 2;
        BinaryPrimitives.WriteUInt16BigEndian(response.GetBuffer().AsSpan((int)response.Position, 2), 6);
        response.Position += 2;

        // ENCRYPTION option
        bw.Write(TdsConstants.PreLoginEncryption);
        BinaryPrimitives.WriteUInt16BigEndian(response.GetBuffer().AsSpan((int)response.Position, 2), encryptionOffset);
        response.Position += 2;
        BinaryPrimitives.WriteUInt16BigEndian(response.GetBuffer().AsSpan((int)response.Position, 2), 1);
        response.Position += 2;

        // THREADID option
        bw.Write(TdsConstants.PreLoginThreadId);
        BinaryPrimitives.WriteUInt16BigEndian(response.GetBuffer().AsSpan((int)response.Position, 2), threadIdOffset);
        response.Position += 2;
        BinaryPrimitives.WriteUInt16BigEndian(response.GetBuffer().AsSpan((int)response.Position, 2), 4);
        response.Position += 2;

        // MARS option
        bw.Write(TdsConstants.PreLoginMars);
        BinaryPrimitives.WriteUInt16BigEndian(response.GetBuffer().AsSpan((int)response.Position, 2), marsOffset);
        response.Position += 2;
        BinaryPrimitives.WriteUInt16BigEndian(response.GetBuffer().AsSpan((int)response.Position, 2), 1);
        response.Position += 2;

        // Terminator
        bw.Write(TdsConstants.PreLoginTerminator);

        // Data section
        // Version: Major(1) Minor(1) Build(2) SubBuild(2)
        bw.Write((byte)TdsConstants.ServerVersionMajor);
        bw.Write((byte)TdsConstants.ServerVersionMinor);
        bw.Write(TdsConstants.ServerVersionBuild);
        bw.Write((ushort)0); // sub-build

        // Encryption: NOT_SUP (we don't support TLS yet)
        bw.Write(TdsConstants.EncryptNotSup);

        // Thread ID
        bw.Write((uint)Environment.ProcessId);

        // MARS: disabled
        bw.Write((byte)0);

        await writer.WriteMessageAsync(TdsConstants.PacketTypeResponse, response, ct);
        return true;
    }

    #endregion

    #region LOGIN7

    private async Task<bool> HandleLogin7Async(TdsPacketReader reader, TdsPacketWriter writer, TdsSession session, CancellationToken ct)
    {
        var msg = await reader.ReadMessageAsync(ct);
        if (msg == null || msg.Value.PacketType != TdsConstants.PacketTypeLogin7)
            return false;

        var payload = msg.Value.Payload;
        if (payload.Length < 36)
            return false;

        // Parse LOGIN7 fixed fields
        var tdsVersion = BitConverter.ToUInt32(payload, 4);
        var packetSize = BitConverter.ToInt32(payload, 8);
        // ClientProgVer at 12, ClientPID at 16, ConnectionID at 20

        session.TdsVersion = tdsVersion;
        session.PacketSize = Math.Clamp(packetSize, 512, TdsConstants.MaxPacketSize);

        // Parse variable-length fields using offset/length pairs starting at offset 36
        // Each pair is 4 bytes: offset(2) + length(2)
        if (payload.Length >= 94)
        {
            session.ClientHostname = ReadLogin7String(payload, 36);
            session.Username = ReadLogin7String(payload, 40);
            // Password at offset 44 (encrypted, skip for now)
            session.ApplicationName = ReadLogin7String(payload, 48);
            // ServerName at 52, ExtensionData at 56, CtlIntName at 60
            // Language at 64, Database at 68
            var dbName = ReadLogin7String(payload, 68);
            if (!string.IsNullOrEmpty(dbName))
                session.CurrentDatabase = dbName;
        }

        // Send login response
        var tokenWriter = new TdsTokenWriter();

        // LOGINACK
        tokenWriter.WriteLoginAck("CyscaleDB", session.TdsVersion);

        // ENVCHANGE: database
        tokenWriter.WriteEnvChangeDatabase(session.CurrentDatabase, "");

        // ENVCHANGE: packet size
        tokenWriter.WriteEnvChangePacketSize(session.PacketSize, TdsConstants.DefaultPacketSize);

        // ENVCHANGE: collation
        tokenWriter.WriteEnvChangeCollation();

        // INFO: connection info
        tokenWriter.WriteInfo(5701,
            $"Changed database context to '{session.CurrentDatabase}'.");
        tokenWriter.WriteInfo(5703,
            "Changed language setting to 'us_english'.");

        // DONE
        tokenWriter.WriteDone();

        writer.SetPacketSize(session.PacketSize);
        await writer.WriteMessageAsync(TdsConstants.PacketTypeResponse, tokenWriter.ToArray(), ct);
        return true;
    }

    private static string ReadLogin7String(byte[] payload, int fieldOffset)
    {
        if (fieldOffset + 4 > payload.Length) return "";
        var offset = BitConverter.ToUInt16(payload, fieldOffset);
        var length = BitConverter.ToUInt16(payload, fieldOffset + 2);
        if (length == 0 || offset + length * 2 > payload.Length) return "";
        return Encoding.Unicode.GetString(payload, offset, length * 2);
    }

    #endregion

    #region Command Processing

    private async Task ProcessCommandsAsync(TdsPacketReader reader, TdsPacketWriter writer, TdsSession session, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var msg = await reader.ReadMessageAsync(ct);
            if (msg == null) break; // Client disconnected

            var (packetType, payload) = msg.Value;

            switch (packetType)
            {
                case TdsConstants.PacketTypeSqlBatch:
                    await HandleSqlBatchAsync(payload, writer, session, ct);
                    break;

                case TdsConstants.PacketTypeRpc:
                    await HandleRpcAsync(payload, writer, session, ct);
                    break;

                case TdsConstants.PacketTypeAttention:
                    // Cancel current query - send DONE with ATTN
                    var doneToken = new TdsTokenWriter();
                    doneToken.WriteDone(0x0020); // DONE_ATTN
                    await writer.WriteMessageAsync(TdsConstants.PacketTypeResponse, doneToken.ToArray(), ct);
                    break;

                default:
                    _logger.Warning("Unknown TDS packet type: 0x{0:X2}", packetType);
                    break;
            }
        }
    }

    private async Task HandleSqlBatchAsync(byte[] payload, TdsPacketWriter writer, TdsSession session, CancellationToken ct)
    {
        // SQL Batch payload is UTF-16LE encoded SQL text
        // Skip the ALL_HEADERS section if present (first 4 bytes = total length of headers)
        var offset = 0;

        if (payload.Length >= 4)
        {
            var totalHeadersLength = BitConverter.ToInt32(payload, 0);
            if (totalHeadersLength > 4 && totalHeadersLength <= payload.Length)
            {
                offset = totalHeadersLength;
            }
        }

        var sql = Encoding.Unicode.GetString(payload, offset, payload.Length - offset).TrimEnd('\0');

        if (string.IsNullOrWhiteSpace(sql))
        {
            var doneToken = new TdsTokenWriter();
            doneToken.WriteDone();
            await writer.WriteMessageAsync(TdsConstants.PacketTypeResponse, doneToken.ToArray(), ct);
            return;
        }

        _logger.Debug("TDS SQL Batch SPID={0}: {1}", session.SessionId,
            sql.Length > 200 ? sql[..200] + "..." : sql);

        // Translate T-SQL to MySQL-compatible SQL
        var translatedSql = TsqlTranslator.Translate(sql);

        try
        {
            // Use the session database
            if (!string.IsNullOrEmpty(session.CurrentDatabase))
            {
                try { session.Executor.Execute($"USE `{session.CurrentDatabase}`"); }
                catch { /* Ignore if db doesn't exist yet */ }
            }

            var result = session.Executor.Execute(translatedSql);
            var tokenWriter = new TdsTokenWriter();

            if (result.ResultSet != null && result.ResultSet.RowCount > 0)
            {
                // Build a TableSchema from ResultSet columns for TDS wire format
                var columns = new List<ColumnDefinition>();
                for (int ci = 0; ci < result.ResultSet.Columns.Count; ci++)
                {
                    var rc = result.ResultSet.Columns[ci];
                    columns.Add(new ColumnDefinition(rc.Name, rc.DataType, 255, 0, 0, true)
                    { OrdinalPosition = ci });
                }
                var tdsSchema = new TableSchema(0, session.CurrentDatabase, "result", columns);

                // Result set response
                tokenWriter.WriteColumnMetadata(tdsSchema);
                foreach (var rowValues in result.ResultSet.Rows)
                {
                    var row = new Row(tdsSchema, rowValues);
                    tokenWriter.WriteRow(row, tdsSchema);
                }
                tokenWriter.WriteDone(TdsConstants.DoneStatusCount, 0, result.ResultSet.RowCount);
            }
            else if (result.ResultSet != null && result.ResultSet.ColumnCount > 0)
            {
                // Empty result set with column metadata
                var columns = new List<ColumnDefinition>();
                for (int ci = 0; ci < result.ResultSet.Columns.Count; ci++)
                {
                    var rc = result.ResultSet.Columns[ci];
                    columns.Add(new ColumnDefinition(rc.Name, rc.DataType, 255, 0, 0, true)
                    { OrdinalPosition = ci });
                }
                var tdsSchema = new TableSchema(0, session.CurrentDatabase, "result", columns);
                tokenWriter.WriteColumnMetadata(tdsSchema);
                tokenWriter.WriteDone(TdsConstants.DoneStatusCount, 0, 0);
            }
            else
            {
                // Non-query response (DDL/DML)
                if (result.AffectedRows > 0)
                {
                    tokenWriter.WriteDone(TdsConstants.DoneStatusCount, 0, result.AffectedRows);
                }
                else
                {
                    tokenWriter.WriteDone();
                }
            }

            await writer.WriteMessageAsync(TdsConstants.PacketTypeResponse, tokenWriter.ToArray(), ct);
        }
        catch (Exception ex)
        {
            var tokenWriter = new TdsTokenWriter();
            tokenWriter.WriteError(50000, ex.Message);
            tokenWriter.WriteDone(TdsConstants.DoneStatusError);
            await writer.WriteMessageAsync(TdsConstants.PacketTypeResponse, tokenWriter.ToArray(), ct);
        }
    }

    private async Task HandleRpcAsync(byte[] payload, TdsPacketWriter writer, TdsSession session, CancellationToken ct)
    {
        // Simplified RPC handling - primarily for sp_executesql
        var tokenWriter = new TdsTokenWriter();
        tokenWriter.WriteError(50000, "RPC calls are not yet fully supported. Use SQL batch mode.");
        tokenWriter.WriteDone(TdsConstants.DoneStatusError);
        await writer.WriteMessageAsync(TdsConstants.PacketTypeResponse, tokenWriter.ToArray(), ct);
    }

    #endregion

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Stop();
        _cts.Dispose();
    }
}
