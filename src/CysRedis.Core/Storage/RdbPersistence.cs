using System.IO.Compression;
using System.Text;
using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;

namespace CysRedis.Core.Storage;

/// <summary>
/// RDB persistence - save and load Redis database snapshots.
/// Simplified implementation compatible with Redis RDB format concepts.
/// </summary>
public class RdbPersistence
{
    private const byte RDB_VERSION = 10;
    private const string RDB_MAGIC = "REDIS";
    
    // RDB type codes
    private const byte RDB_TYPE_STRING = 0;
    private const byte RDB_TYPE_LIST = 1;
    private const byte RDB_TYPE_SET = 2;
    private const byte RDB_TYPE_ZSET = 3;
    private const byte RDB_TYPE_HASH = 4;
    private const byte RDB_TYPE_STREAM_LISTPACKS = 15;
    
    // RDB opcodes
    private const byte RDB_OPCODE_EXPIRETIME_MS = 252;
    private const byte RDB_OPCODE_EXPIRETIME = 253;
    private const byte RDB_OPCODE_SELECTDB = 254;
    private const byte RDB_OPCODE_EOF = 255;
    private const byte RDB_OPCODE_RESIZEDB = 251;

    private readonly RedisStore _store;
    private readonly string _directory;
    private readonly string _filename;
    private bool _isSaving;

    public RdbPersistence(RedisStore store, string directory, string filename = "dump.rdb")
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _directory = directory ?? throw new ArgumentNullException(nameof(directory));
        _filename = filename;
    }

    /// <summary>
    /// Full path to the RDB file.
    /// </summary>
    public string FilePath => Path.Combine(_directory, _filename);

    /// <summary>
    /// Whether a save operation is in progress.
    /// </summary>
    public bool IsSaving => _isSaving;

    /// <summary>
    /// Saves the database to RDB file (blocking).
    /// </summary>
    public void Save()
    {
        if (_isSaving)
            throw new RedisException("Background save already in progress");

        _isSaving = true;
        try
        {
            Directory.CreateDirectory(_directory);
            var tempPath = FilePath + ".tmp";

            using (var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (var writer = new BinaryWriter(stream))
            {
                WriteRdb(writer);
            }

            // Atomic replace
            if (File.Exists(FilePath))
                File.Delete(FilePath);
            File.Move(tempPath, FilePath);

            Logger.Info("RDB saved to {0}", FilePath);
        }
        finally
        {
            _isSaving = false;
        }
    }

    /// <summary>
    /// Saves the database in background.
    /// </summary>
    public Task SaveBackgroundAsync(CancellationToken cancellationToken = default)
    {
        return Task.Run(() => Save(), cancellationToken);
    }

    /// <summary>
    /// Saves the database to a byte array (for replication).
    /// </summary>
    public byte[] SaveToBytes()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        WriteRdb(writer);
        return stream.ToArray();
    }

    /// <summary>
    /// Loads the database from RDB file.
    /// </summary>
    public bool Load()
    {
        if (!File.Exists(FilePath))
            return false;

        try
        {
            using var stream = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new BinaryReader(stream);
            ReadRdb(reader);
            Logger.Info("RDB loaded from {0}", FilePath);
            return true;
        }
        catch (Exception ex)
        {
            Logger.Error("Failed to load RDB: {0}", ex.Message);
            return false;
        }
    }

    private void WriteRdb(BinaryWriter writer)
    {
        // Magic string
        writer.Write(Encoding.ASCII.GetBytes(RDB_MAGIC));
        
        // Version (4 bytes as string)
        writer.Write(Encoding.ASCII.GetBytes(RDB_VERSION.ToString("D4")));

        // Write each database
        foreach (var db in _store.GetAllDatabases())
        {
            if (db.KeyCount == 0) continue;

            // Select DB
            writer.Write(RDB_OPCODE_SELECTDB);
            WriteLength(writer, db.Index);

            // Resize DB info
            writer.Write(RDB_OPCODE_RESIZEDB);
            WriteLength(writer, db.KeyCount);
            WriteLength(writer, 0); // expires count (simplified)

            // Write each key
            foreach (var key in db.Keys())
            {
                var obj = db.Get(key);
                if (obj == null) continue;

                // Write expire if present
                var expire = db.GetExpire(key);
                if (expire.HasValue)
                {
                    writer.Write(RDB_OPCODE_EXPIRETIME_MS);
                    writer.Write(new DateTimeOffset(expire.Value).ToUnixTimeMilliseconds());
                }

                // Write type
                byte typeCode = obj switch
                {
                    RedisString => RDB_TYPE_STRING,
                    RedisList => RDB_TYPE_LIST,
                    RedisSet => RDB_TYPE_SET,
                    RedisSortedSet => RDB_TYPE_ZSET,
                    RedisHash => RDB_TYPE_HASH,
                    RedisStream => RDB_TYPE_STREAM_LISTPACKS,
                    _ => throw new NotSupportedException($"Unknown type: {obj.GetType()}")
                };
                writer.Write(typeCode);

                // Write key
                WriteString(writer, key);

                // Write value
                switch (obj)
                {
                    case RedisString str:
                        WriteString(writer, str.Value);
                        break;
                    case RedisList list:
                        WriteList(writer, list);
                        break;
                    case RedisSet set:
                        WriteSet(writer, set);
                        break;
                    case RedisSortedSet zset:
                        WriteZSet(writer, zset);
                        break;
                    case RedisHash hash:
                        WriteHash(writer, hash);
                        break;
                    case RedisStream stream:
                        WriteStream(writer, stream);
                        break;
                }
            }
        }

        // EOF
        writer.Write(RDB_OPCODE_EOF);
        
        // Calculate and write CRC64 checksum
        var position = writer.BaseStream.Position;
        writer.BaseStream.Position = 0;
        var allData = new byte[position];
        writer.BaseStream.Read(allData, 0, (int)position);
        writer.BaseStream.Position = position;
        
        var checksum = Crc64.Compute(allData);
        writer.Write(checksum);
    }

    private void ReadRdb(BinaryReader reader)
    {
        // Magic string
        var magic = Encoding.ASCII.GetString(reader.ReadBytes(5));
        if (magic != RDB_MAGIC)
            throw new RedisException("Invalid RDB file format");

        // Version
        var versionStr = Encoding.ASCII.GetString(reader.ReadBytes(4));
        if (!int.TryParse(versionStr, out var version) || version > RDB_VERSION)
            throw new RedisException($"Unsupported RDB version: {versionStr}");

        int currentDb = 0;
        DateTime? nextExpire = null;

        while (true)
        {
            var opcode = reader.ReadByte();

            switch (opcode)
            {
                case RDB_OPCODE_EOF:
                    // Skip checksum
                    return;

                case RDB_OPCODE_SELECTDB:
                    currentDb = (int)ReadLength(reader);
                    break;

                case RDB_OPCODE_RESIZEDB:
                    ReadLength(reader); // db size
                    ReadLength(reader); // expires size
                    break;

                case RDB_OPCODE_EXPIRETIME:
                    var expireSec = reader.ReadInt32();
                    nextExpire = DateTimeOffset.FromUnixTimeSeconds(expireSec).UtcDateTime;
                    break;

                case RDB_OPCODE_EXPIRETIME_MS:
                    var expireMs = reader.ReadInt64();
                    nextExpire = DateTimeOffset.FromUnixTimeMilliseconds(expireMs).UtcDateTime;
                    break;

                default:
                    // Type code
                    var key = ReadString(reader);
                    var db = _store.GetDatabase(currentDb);
                    RedisObject? obj = opcode switch
                    {
                        RDB_TYPE_STRING => new RedisString(ReadBytes(reader)),
                        RDB_TYPE_LIST => ReadList(reader),
                        RDB_TYPE_SET => ReadSet(reader),
                        RDB_TYPE_ZSET => ReadZSet(reader),
                        RDB_TYPE_HASH => ReadHash(reader),
                        RDB_TYPE_STREAM_LISTPACKS => ReadStream(reader),
                        _ => null
                    };

                    if (obj != null)
                    {
                        db.Set(key, obj);
                        if (nextExpire.HasValue)
                        {
                            db.SetExpire(key, nextExpire.Value);
                            nextExpire = null;
                        }
                    }
                    break;
            }
        }
    }

    private static void WriteLength(BinaryWriter writer, int length)
    {
        if (length < 64)
        {
            writer.Write((byte)length);
        }
        else if (length < 16384)
        {
            writer.Write((byte)(0x40 | (length >> 8)));
            writer.Write((byte)(length & 0xFF));
        }
        else
        {
            writer.Write((byte)0x80);
            writer.Write(length);
        }
    }

    private static int ReadLength(BinaryReader reader)
    {
        var first = reader.ReadByte();
        var type = (first & 0xC0) >> 6;

        return type switch
        {
            0 => first & 0x3F,
            1 => ((first & 0x3F) << 8) | reader.ReadByte(),
            2 => reader.ReadInt32(),
            _ => throw new RedisException("Invalid length encoding")
        };
    }

    private static void WriteString(BinaryWriter writer, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteString(writer, bytes);
    }

    private static void WriteString(BinaryWriter writer, byte[] bytes)
    {
        WriteLength(writer, bytes.Length);
        writer.Write(bytes);
    }

    private static string ReadString(BinaryReader reader)
    {
        var bytes = ReadBytes(reader);
        return Encoding.UTF8.GetString(bytes);
    }

    private static byte[] ReadBytes(BinaryReader reader)
    {
        var length = ReadLength(reader);
        return reader.ReadBytes(length);
    }

    private static void WriteList(BinaryWriter writer, RedisList list)
    {
        var items = list.GetRange(0, -1);
        WriteLength(writer, items.Count);
        foreach (var item in items)
        {
            WriteString(writer, item);
        }
    }

    private static RedisList ReadList(BinaryReader reader)
    {
        var list = new RedisList();
        var count = ReadLength(reader);
        for (int i = 0; i < count; i++)
        {
            list.PushRight(ReadBytes(reader));
        }
        return list;
    }

    private static void WriteSet(BinaryWriter writer, RedisSet set)
    {
        WriteLength(writer, set.Count);
        foreach (var member in set.Members)
        {
            WriteString(writer, member);
        }
    }

    private static RedisSet ReadSet(BinaryReader reader)
    {
        var set = new RedisSet();
        var count = ReadLength(reader);
        for (int i = 0; i < count; i++)
        {
            set.Add(ReadString(reader));
        }
        return set;
    }

    private static void WriteZSet(BinaryWriter writer, RedisSortedSet zset)
    {
        WriteLength(writer, zset.Count);
        foreach (var (member, score) in zset.GetRange(0, -1))
        {
            WriteString(writer, member);
            writer.Write(score);
        }
    }

    private static RedisSortedSet ReadZSet(BinaryReader reader)
    {
        var zset = new RedisSortedSet();
        var count = ReadLength(reader);
        for (int i = 0; i < count; i++)
        {
            var member = ReadString(reader);
            var score = reader.ReadDouble();
            zset.Add(member, score);
        }
        return zset;
    }

    private static void WriteHash(BinaryWriter writer, RedisHash hash)
    {
        WriteLength(writer, hash.Count);
        foreach (var entry in hash.Entries)
        {
            WriteString(writer, entry.Key);
            WriteString(writer, entry.Value);
        }
    }

    private static RedisHash ReadHash(BinaryReader reader)
    {
        var hash = new RedisHash();
        var count = ReadLength(reader);
        for (int i = 0; i < count; i++)
        {
            var field = ReadString(reader);
            var value = ReadBytes(reader);
            hash.Set(field, value);
        }
        return hash;
    }

    private static void WriteStream(BinaryWriter writer, RedisStream stream)
    {
        // Write stream length
        WriteLength(writer, (int)stream.Length);
        
        // Write last ID
        writer.Write(stream.LastId?.Timestamp ?? 0);
        writer.Write(stream.LastId?.Sequence ?? 0);

        // Write entries
        foreach (var entry in stream.Range(null, null))
        {
            writer.Write(entry.Id.Timestamp);
            writer.Write(entry.Id.Sequence);
            
            WriteLength(writer, entry.Fields.Count);
            foreach (var (key, value) in entry.Fields)
            {
                WriteString(writer, key);
                WriteString(writer, value);
            }
        }

        // Write consumer groups
        var groups = stream.GetGroups().ToList();
        WriteLength(writer, groups.Count);
        
        foreach (var group in groups)
        {
            WriteString(writer, group.Name);
            writer.Write(group.LastDeliveredId.Timestamp);
            writer.Write(group.LastDeliveredId.Sequence);
            
            // Write consumers
            var consumers = group.GetConsumers().ToList();
            WriteLength(writer, consumers.Count);
            foreach (var consumer in consumers)
            {
                WriteString(writer, consumer.Name);
                writer.Write(new DateTimeOffset(consumer.LastSeenTime).ToUnixTimeMilliseconds());
                WriteLength(writer, consumer.PendingCount);
            }
        }
    }

    private static RedisStream ReadStream(BinaryReader reader)
    {
        var stream = new RedisStream();
        var length = ReadLength(reader);
        
        // Read last ID (but don't use it for now)
        reader.ReadInt64(); // timestamp
        reader.ReadInt64(); // sequence

        // Read entries
        for (int i = 0; i < length; i++)
        {
            var timestamp = reader.ReadInt64();
            var sequence = reader.ReadInt64();
            var id = new StreamEntryId(timestamp, sequence);
            
            var fieldCount = ReadLength(reader);
            var fields = new Dictionary<string, string>();
            for (int j = 0; j < fieldCount; j++)
            {
                var key = ReadString(reader);
                var value = ReadString(reader);
                fields[key] = value;
            }
            
            // Add entry directly (skip validation for loading)
            stream.Add(id.ToString(), fields);
        }

        // Read consumer groups
        var groupCount = ReadLength(reader);
        for (int i = 0; i < groupCount; i++)
        {
            var groupName = ReadString(reader);
            var lastTs = reader.ReadInt64();
            var lastSeq = reader.ReadInt64();
            var lastId = new StreamEntryId(lastTs, lastSeq);
            
            stream.CreateGroup(groupName, lastId);
            var group = stream.GetGroup(groupName);
            
            if (group != null)
            {
                // Read consumers
                var consumerCount = ReadLength(reader);
                for (int j = 0; j < consumerCount; j++)
                {
                    var consumerName = ReadString(reader);
                    var lastSeenMs = reader.ReadInt64();
                    var pendingCount = ReadLength(reader);
                    
                    var consumer = group.GetOrCreateConsumer(consumerName);
                    consumer.LastSeenTime = DateTimeOffset.FromUnixTimeMilliseconds(lastSeenMs).UtcDateTime;
                    consumer.PendingCount = pendingCount;
                }
            }
        }

        return stream;
    }
}
