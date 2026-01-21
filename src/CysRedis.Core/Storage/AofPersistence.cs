using System.Text;
using CysRedis.Core.Common;

namespace CysRedis.Core.Storage;

/// <summary>
/// AOF (Append Only File) persistence - logs every write command.
/// </summary>
public class AofPersistence : IDisposable
{
    private readonly string _directory;
    private readonly string _filename;
    private FileStream? _stream;
    private StreamWriter? _writer;
    private readonly object _lock = new();
    private bool _disposed;

    /// <summary>
    /// AOF sync policy.
    /// </summary>
    public AofSyncPolicy SyncPolicy { get; set; } = AofSyncPolicy.EverySec;

    /// <summary>
    /// Whether AOF is enabled.
    /// </summary>
    public bool Enabled { get; private set; }

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

            Logger.Info("AOF enabled: {0}", FilePath);
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

            _writer?.Flush();
            _writer?.Dispose();
            _stream?.Dispose();
            _writer = null;
            _stream = null;
            Enabled = false;

            Logger.Info("AOF disabled");
        }
    }

    /// <summary>
    /// Logs a command to the AOF file.
    /// </summary>
    public void LogCommand(int dbIndex, string[] args)
    {
        if (!Enabled || _writer == null) return;

        lock (_lock)
        {
            // Write SELECT if needed (simplified - always write SELECT)
            WriteCommand(_writer, new[] { "SELECT", dbIndex.ToString() });
            WriteCommand(_writer, args);

            if (SyncPolicy == AofSyncPolicy.Always)
            {
                _writer.Flush();
                _stream?.Flush(true);
            }
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
    public async Task RewriteAsync(DataStructures.RedisStore store, CancellationToken cancellationToken = default)
    {
        var tempPath = FilePath + ".tmp";

        await Task.Run(() =>
        {
            using var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None);
            using var writer = new StreamWriter(stream, Encoding.UTF8);

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

        Logger.Info("AOF rewrite completed");
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
