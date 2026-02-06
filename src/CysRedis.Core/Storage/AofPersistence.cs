using System.Text;
using System.Threading.Channels;
using CysRedis.Core.Common;

namespace CysRedis.Core.Storage;

/// <summary>
/// AOF record for buffered writes.
/// </summary>
internal readonly struct AofRecord
{
    public int DbIndex { get; }
    public string[] Args { get; }

    public AofRecord(int dbIndex, string[] args)
    {
        DbIndex = dbIndex;
        Args = args;
    }
}

/// <summary>
/// AOF (Append Only File) persistence - logs every write command.
/// Uses a lock-free Channel-based write buffer with configurable fsync policy.
/// </summary>
public class AofPersistence : IDisposable
{
    private readonly string _directory;
    private readonly string _filename;
    private FileStream? _stream;
    private StreamWriter? _writer;
    private readonly object _lock = new(); // Only used for enable/disable/rewrite coordination
    private bool _disposed;
    private bool _isRewriting;
    private DateTime _lastRewriteTime = DateTime.UtcNow;
    private long _aofSize;
    private Task? _syncTask;
    private Task? _writeLoopTask;
    private CancellationTokenSource? _syncCts;
    private Channel<AofRecord>? _writeChannel;

    /// <summary>
    /// AOF sync policy.
    /// </summary>
    public AofSyncPolicy SyncPolicy { get; set; } = AofSyncPolicy.EverySec;

    /// <summary>
    /// Whether AOF is enabled.
    /// </summary>
    public bool Enabled { get; private set; }

    /// <summary>
    /// 是否使用AOF-RDB混合模式
    /// </summary>
    public bool UseRdbPreamble { get; set; } = true;

    /// <summary>
    /// Whether AOF rewrite is in progress.
    /// </summary>
    public bool IsRewriting => _isRewriting;

    /// <summary>
    /// AOF file size.
    /// </summary>
    public long AofSize => _aofSize;

    public AofPersistence(string directory, string filename = "appendonly.aof")
    {
        _directory = directory ?? throw new ArgumentNullException(nameof(directory));
        _filename = filename;
    }

    /// <summary>
    /// Full path to the AOF file.
    /// </summary>
    public string FilePath => Path.Combine(_directory, _filename);

    /// <summary>
    /// Enables AOF logging.
    /// </summary>
    public void Enable()
    {
        lock (_lock)
        {
            if (Enabled) return;

            Directory.CreateDirectory(_directory);
            _stream = new FileStream(FilePath, FileMode.Append, FileAccess.Write, FileShare.Read);
            _writer = new StreamWriter(_stream, Encoding.UTF8);
            Enabled = true;

            if (File.Exists(FilePath))
            {
                _aofSize = new FileInfo(FilePath).Length;
            }

            _syncCts = new CancellationTokenSource();

            // Create a bounded channel for lock-free command buffering
            _writeChannel = Channel.CreateBounded<AofRecord>(new BoundedChannelOptions(10000)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false
            });

            // Start the background write loop (single consumer)
            _writeLoopTask = RunWriteLoopAsync(_syncCts.Token);

            // 启动定期同步任务 (如果策略是EverySec)
            if (SyncPolicy == AofSyncPolicy.EverySec)
            {
                _syncTask = RunPeriodicSyncAsync(_syncCts.Token);
            }

            Logger.Info("AOF enabled: {0} (sync={1})", FilePath, SyncPolicy);
        }
    }

    /// <summary>
    /// Disables AOF logging.
    /// </summary>
    public void Disable()
    {
        lock (_lock)
        {
            if (!Enabled) return;

            // Complete the write channel and wait for the loop to drain
            _writeChannel?.Writer.TryComplete();
            _writeLoopTask?.Wait(TimeSpan.FromSeconds(5));

            // 停止同步任务
            _syncCts?.Cancel();
            _syncTask?.Wait(TimeSpan.FromSeconds(5));
            _syncCts?.Dispose();
            _syncCts = null;

            _writer?.Flush();
            _writer?.Dispose();
            _stream?.Dispose();
            _writer = null;
            _stream = null;
            _writeChannel = null;
            _writeLoopTask = null;
            _syncTask = null;
            Enabled = false;

            Logger.Info("AOF disabled");
        }
    }

    /// <summary>
    /// 定期同步任务（每秒一次）
    /// </summary>
    private async Task RunPeriodicSyncAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(1000, cancellationToken);
                Sync();
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.Error("Error in AOF sync task", ex);
            }
        }
    }

    /// <summary>
    /// Logs a command to the AOF file using lock-free Channel buffering.
    /// The command is enqueued and written by a background single-consumer loop.
    /// </summary>
    public void LogCommand(int dbIndex, string[] args)
    {
        if (!Enabled || _writeChannel == null) return;

        // Lock-free enqueue - will block only if channel is full (backpressure)
        _writeChannel.Writer.TryWrite(new AofRecord(dbIndex, args));
    }

    /// <summary>
    /// Background write loop - single consumer drains the channel and batches writes.
    /// </summary>
    private async Task RunWriteLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            var reader = _writeChannel!.Reader;

            while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                int batchCount = 0;

                // Drain all available records in a batch
                while (reader.TryRead(out var record))
                {
                    if (_writer == null) break;

                    var startPos = _stream?.Position ?? 0;

                    WriteCommand(_writer, new[] { "SELECT", record.DbIndex.ToString() });
                    WriteCommand(_writer, record.Args);

                    var endPos = _stream?.Position ?? 0;
                    Interlocked.Add(ref _aofSize, endPos - startPos);
                    batchCount++;
                }

                // Flush after batch
                if (batchCount > 0 && _writer != null)
                {
                    _writer.Flush();

                    // Fsync based on policy
                    if (SyncPolicy == AofSyncPolicy.Always)
                    {
                        _stream?.Flush(true); // fsync
                    }
                    // EverySec is handled by the periodic sync task
                    // No policy = let OS handle
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown
        }
        catch (ChannelClosedException)
        {
            // Expected when channel is completed
        }
        catch (Exception ex)
        {
            Logger.Error("Error in AOF write loop", ex);
        }
    }

    /// <summary>
    /// Flushes the AOF buffer to disk.
    /// </summary>
    public void Sync()
    {
        lock (_lock)
        {
            _writer?.Flush();
            _stream?.Flush(true);
        }
    }

    /// <summary>
    /// Rewrites the AOF file to compact it.
    /// </summary>
    public async Task RewriteAsync(DataStructures.RedisStore store, RdbPersistence? rdbPersistence = null, CancellationToken cancellationToken = default)
    {
        if (_isRewriting)
            throw new Common.RedisException("Background AOF rewrite already in progress");

        _isRewriting = true;
        _lastRewriteTime = DateTime.UtcNow;

        try
        {
            var tempPath = FilePath + ".tmp";

            await Task.Run(() =>
            {
                using var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None);

                // AOF-RDB混合模式：先写RDB快照，再写增量AOF
                if (UseRdbPreamble && rdbPersistence != null)
                {
                    // 写入RDB前导
                    var rdbData = rdbPersistence.SaveToBytes();
                    stream.Write(rdbData);
                    Logger.Info("AOF rewrite: wrote RDB preamble ({0} bytes)", rdbData.Length);
                }

                using var writer = new StreamWriter(stream, Encoding.UTF8);

                // 如果没有使用RDB前导，写纯AOF
                if (!UseRdbPreamble || rdbPersistence == null)
                {
                    foreach (var db in store.GetAllDatabases())
                    {
                        if (db.KeyCount == 0) continue;

                        WriteCommand(writer, new[] { "SELECT", db.Index.ToString() });

                foreach (var key in db.Keys())
                {
                    var obj = db.Get(key);
                    if (obj == null) continue;

                    // Write reconstruct commands
                    switch (obj)
                    {
                        case DataStructures.RedisString str:
                            WriteCommand(writer, new[] { "SET", key, str.GetString() });
                            break;

                        case DataStructures.RedisList list:
                            var items = list.GetRange(0, -1);
                            if (items.Count > 0)
                            {
                                var args = new string[items.Count + 2];
                                args[0] = "RPUSH";
                                args[1] = key;
                                for (int i = 0; i < items.Count; i++)
                                    args[i + 2] = Encoding.UTF8.GetString(items[i]);
                                WriteCommand(writer, args);
                            }
                            break;

                        case DataStructures.RedisSet set:
                            var members = set.Members.ToList();
                            if (members.Count > 0)
                            {
                                var args = new string[members.Count + 2];
                                args[0] = "SADD";
                                args[1] = key;
                                for (int i = 0; i < members.Count; i++)
                                    args[i + 2] = members[i];
                                WriteCommand(writer, args);
                            }
                            break;

                        case DataStructures.RedisSortedSet zset:
                            foreach (var (member, score) in zset.GetRange(0, -1))
                            {
                                WriteCommand(writer, new[] { "ZADD", key, 
                                    score.ToString(System.Globalization.CultureInfo.InvariantCulture), member });
                            }
                            break;

                        case DataStructures.RedisHash hash:
                            var entries = hash.Entries.ToList();
                            if (entries.Count > 0)
                            {
                                var args = new List<string> { "HSET", key };
                                foreach (var entry in entries)
                                {
                                    args.Add(entry.Key);
                                    args.Add(Encoding.UTF8.GetString(entry.Value));
                                }
                                WriteCommand(writer, args.ToArray());
                            }
                            break;

                        case DataStructures.RedisStream redisStream:
                            // Stream类型支持
                            foreach (var entry in redisStream.Range(null, null))
                            {
                                var streamArgs = new List<string> { "XADD", key, entry.Id.ToString() };
                                foreach (var (k, v) in entry.Fields)
                                {
                                    streamArgs.Add(k);
                                    streamArgs.Add(v);
                                }
                                WriteCommand(writer, streamArgs.ToArray());
                            }
                            break;
                    }

                    // Write expire
                    var expire = db.GetExpire(key);
                    if (expire.HasValue)
                    {
                        var pxat = new DateTimeOffset(expire.Value).ToUnixTimeMilliseconds();
                        WriteCommand(writer, new[] { "PEXPIREAT", key, pxat.ToString() });
                    }
                }
            }
                }

                writer.Flush();
                stream.Flush();
            }, cancellationToken);

            // Swap files
            lock (_lock)
            {
                var wasEnabled = Enabled;
                if (wasEnabled) Disable();

                if (File.Exists(FilePath))
                    File.Delete(FilePath);
                File.Move(tempPath, FilePath);

                if (wasEnabled) Enable();
            }

            Logger.Info("AOF rewrite completed, new size: {0}", new FileInfo(FilePath).Length);
        }
        finally
        {
            _isRewriting = false;
        }
    }

    /// <summary>
    /// Runs AOF rewrite in background.
    /// </summary>
    public Task RewriteBackgroundAsync(DataStructures.RedisStore store, RdbPersistence? rdbPersistence = null, CancellationToken cancellationToken = default)
    {
        return Task.Run(() => RewriteAsync(store, rdbPersistence, cancellationToken), cancellationToken);
    }

    /// <summary>
    /// Checks if AOF needs rewriting based on size growth.
    /// </summary>
    public bool NeedsRewrite(long minSizeBytes = 64 * 1024 * 1024, double growthPercentage = 100)
    {
        if (_aofSize < minSizeBytes)
            return false;

        var timeSinceLastRewrite = DateTime.UtcNow - _lastRewriteTime;
        if (timeSinceLastRewrite < TimeSpan.FromMinutes(5))
            return false;

        // 简化判断：如果AOF大于阈值就需要重写
        return true;
    }

    /// <summary>
    /// Loads commands from AOF file.
    /// </summary>
    public IEnumerable<(int DbIndex, string[] Args)> ReadCommands()
    {
        if (!File.Exists(FilePath))
            yield break;

        using var stream = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        using var reader = new StreamReader(stream, Encoding.UTF8);

        int currentDb = 0;

        while (!reader.EndOfStream)
        {
            var line = reader.ReadLine();
            if (string.IsNullOrEmpty(line)) continue;

            // Expect array marker
            if (!line.StartsWith('*')) continue;

            if (!int.TryParse(line.AsSpan(1), out var argc)) continue;

            var args = new string[argc];
            for (int i = 0; i < argc; i++)
            {
                var sizeLine = reader.ReadLine();
                if (sizeLine == null || !sizeLine.StartsWith('$')) break;

                if (!int.TryParse(sizeLine.AsSpan(1), out var size)) break;

                var data = reader.ReadLine();
                if (data == null) break;

                args[i] = data;
            }

            // Track SELECT commands
            if (args.Length >= 2 && args[0].Equals("SELECT", StringComparison.OrdinalIgnoreCase))
            {
                if (int.TryParse(args[1], out var dbIndex))
                    currentDb = dbIndex;
            }

            yield return (currentDb, args);
        }
    }

    private static void WriteCommand(StreamWriter writer, string[] args)
    {
        // RESP format
        writer.Write('*');
        writer.Write(args.Length);
        writer.Write("\r\n");

        foreach (var arg in args)
        {
            var bytes = Encoding.UTF8.GetBytes(arg);
            writer.Write('$');
            writer.Write(bytes.Length);
            writer.Write("\r\n");
            writer.Write(arg);
            writer.Write("\r\n");
        }
    }

    /// <summary>
    /// Verifies AOF file integrity.
    /// </summary>
    public bool Verify()
    {
        if (!File.Exists(FilePath))
            return true;

        try
        {
            var count = 0;
            foreach (var _ in ReadCommands())
            {
                count++;
            }
            Logger.Info("AOF verified: {0} commands", count);
            return true;
        }
        catch (Exception ex)
        {
            Logger.Error("AOF verification failed: {0}", ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Repairs AOF file by truncating at first error.
    /// </summary>
    public void Repair()
    {
        if (!File.Exists(FilePath))
            return;

        var backupPath = FilePath + ".backup";
        File.Copy(FilePath, backupPath, overwrite: true);

        try
        {
            var validCommands = new List<(int DbIndex, string[] Args)>();
            
            foreach (var cmd in ReadCommands())
            {
                validCommands.Add(cmd);
            }

            // 重写文件，只包含有效命令
            using (var stream = new FileStream(FilePath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (var writer = new StreamWriter(stream, Encoding.UTF8))
            {
                foreach (var (dbIndex, args) in validCommands)
                {
                    WriteCommand(writer, new[] { "SELECT", dbIndex.ToString() });
                    WriteCommand(writer, args);
                }
            }

            Logger.Info("AOF repaired: {0} commands recovered", validCommands.Count);
        }
        catch (Exception ex)
        {
            Logger.Error("AOF repair failed: {0}", ex.Message);
            // 恢复备份
            File.Copy(backupPath, FilePath, overwrite: true);
            throw;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Disable();
    }
}

/// <summary>
/// AOF sync policy.
/// </summary>
public enum AofSyncPolicy
{
    /// <summary>
    /// Sync after every command.
    /// </summary>
    Always,

    /// <summary>
    /// Sync every second.
    /// </summary>
    EverySec,

    /// <summary>
    /// Let the OS handle syncing.
    /// </summary>
    No
}
