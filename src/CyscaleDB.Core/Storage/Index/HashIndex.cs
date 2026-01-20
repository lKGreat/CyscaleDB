using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage.Index;

/// <summary>
/// Extendible Hash index implementation for equality lookups.
/// Uses a directory of bucket pointers that can grow dynamically.
/// </summary>
public sealed class HashIndex : IDisposable
{
    private readonly IndexInfo _info;
    private readonly PageManager _pageManager;
    private readonly int _bucketCapacity;
    private readonly object _lock = new();
    private readonly Logger _logger;
    private int _globalDepth;
    private int[] _directory; // Maps hash prefix to bucket page ID
    private bool _disposed;

    private const int DirectoryPageId = 0;
    private const int MetadataOffset = 0;

    /// <summary>
    /// Gets the index metadata.
    /// </summary>
    public IndexInfo Info => _info;

    /// <summary>
    /// Gets whether this index is unique.
    /// </summary>
    public bool IsUnique => _info.IsUnique;

    /// <summary>
    /// Creates a new Hash index.
    /// </summary>
    public HashIndex(IndexInfo info, string filePath, int bucketCapacity = Constants.HashBucketCapacity)
    {
        _info = info ?? throw new ArgumentNullException(nameof(info));
        _bucketCapacity = bucketCapacity;
        _pageManager = new PageManager(filePath);
        _logger = LogManager.Default.GetLogger<HashIndex>();
        _directory = [];
        _globalDepth = 0;
    }

    /// <summary>
    /// Opens the index file.
    /// </summary>
    public void Open(bool createIfNotExists = true)
    {
        _pageManager.Open(createIfNotExists);

        if (_pageManager.PageCount == 0)
        {
            // Initialize new hash index
            InitializeIndex();
            _logger.Debug("Created new Hash index with global depth {0}", _globalDepth);
        }
        else
        {
            // Load existing directory
            LoadDirectory();
            _logger.Debug("Opened Hash index with global depth {0}", _globalDepth);
        }
    }

    /// <summary>
    /// Inserts a key-RowId pair into the index.
    /// </summary>
    public bool Insert(DataValue[] keyValues, RowId rowId)
    {
        var key = IndexInfo.CreateCompositeKey(keyValues);
        var hashValue = ComputeHash(key);

        lock (_lock)
        {
            // Check for duplicates if unique index
            if (IsUnique && Lookup(keyValues).Any())
            {
                throw new ConstraintViolationException($"Duplicate key violation on index '{_info.IndexName}'", _info.IndexName);
            }

            var bucketPageId = GetBucketPageId(hashValue);
            var bucket = ReadBucket(bucketPageId);

            if (bucket.CanInsert())
            {
                bucket.Insert(key, rowId);
                WriteBucket(bucket);
                return true;
            }
            else
            {
                // Bucket is full, need to split
                SplitBucket(bucketPageId, bucket);

                // Retry insert after split
                return Insert(keyValues, rowId);
            }
        }
    }

    /// <summary>
    /// Deletes a key-RowId pair from the index.
    /// </summary>
    public bool Delete(DataValue[] keyValues, RowId rowId)
    {
        var key = IndexInfo.CreateCompositeKey(keyValues);
        var hashValue = ComputeHash(key);

        lock (_lock)
        {
            var bucketPageId = GetBucketPageId(hashValue);
            var bucket = ReadBucket(bucketPageId);

            if (bucket.Delete(key, rowId))
            {
                WriteBucket(bucket);
                return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Updates an index entry (delete old, insert new).
    /// </summary>
    public bool Update(DataValue[] oldKeyValues, DataValue[] newKeyValues, RowId rowId)
    {
        lock (_lock)
        {
            if (!Delete(oldKeyValues, rowId))
                return false;
            return Insert(newKeyValues, rowId);
        }
    }

    /// <summary>
    /// Looks up RowIds for the given key values.
    /// </summary>
    public IEnumerable<RowId> Lookup(DataValue[] keyValues)
    {
        var key = IndexInfo.CreateCompositeKey(keyValues);
        var hashValue = ComputeHash(key);

        lock (_lock)
        {
            var bucketPageId = GetBucketPageId(hashValue);
            var bucket = ReadBucket(bucketPageId);

            foreach (var entry in bucket.GetEntries())
            {
                if (entry.Key == key)
                {
                    yield return entry.RowId;
                }
            }
        }
    }

    /// <summary>
    /// Scans all entries in the index.
    /// </summary>
    public IEnumerable<RowId> ScanAll()
    {
        lock (_lock)
        {
            var visitedBuckets = new HashSet<int>();

            for (int i = 0; i < _directory.Length; i++)
            {
                var bucketPageId = _directory[i];
                if (visitedBuckets.Contains(bucketPageId))
                    continue;

                visitedBuckets.Add(bucketPageId);
                var bucket = ReadBucket(bucketPageId);

                foreach (var entry in bucket.GetEntries())
                {
                    yield return entry.RowId;
                }
            }
        }
    }

    /// <summary>
    /// Flushes all data to disk.
    /// </summary>
    public void Flush()
    {
        lock (_lock)
        {
            SaveDirectory();
            _pageManager.Flush();
        }
    }

    #region Private Methods

    private void InitializeIndex()
    {
        _globalDepth = 2; // Start with 4 buckets
        var directorySize = 1 << _globalDepth;
        _directory = new int[directorySize];

        // Create initial buckets
        for (int i = 0; i < directorySize; i++)
        {
            var bucket = CreateBucket(_globalDepth);
            _directory[i] = bucket.PageId;
        }

        SaveDirectory();
    }

    private void LoadDirectory()
    {
        var dirPage = _pageManager.ReadPage(DirectoryPageId);
        var data = dirPage.GetData();

        // Read global depth
        _globalDepth = BitConverter.ToInt32(data, MetadataOffset);
        var directorySize = 1 << _globalDepth;
        _directory = new int[directorySize];

        // Read directory entries
        for (int i = 0; i < directorySize; i++)
        {
            _directory[i] = BitConverter.ToInt32(data, MetadataOffset + 4 + (i * 4));
        }
    }

    private void SaveDirectory()
    {
        var dirPage = _pageManager.PageCount > 0 
            ? _pageManager.ReadPage(DirectoryPageId) 
            : _pageManager.AllocatePage();
        var data = dirPage.GetData();

        // Write global depth
        BitConverter.TryWriteBytes(data.AsSpan(MetadataOffset, 4), _globalDepth);

        // Write directory entries
        for (int i = 0; i < _directory.Length; i++)
        {
            BitConverter.TryWriteBytes(data.AsSpan(MetadataOffset + 4 + (i * 4), 4), _directory[i]);
        }

        _pageManager.WritePage(dirPage);
    }

    private int GetBucketPageId(uint hashValue)
    {
        var directoryIndex = (int)(hashValue & ((1u << _globalDepth) - 1));
        return _directory[directoryIndex];
    }

    private HashBucket CreateBucket(int localDepth)
    {
        var page = _pageManager.AllocatePage();
        return new HashBucket(page.PageId, localDepth, _bucketCapacity);
    }

    private HashBucket ReadBucket(int pageId)
    {
        var page = _pageManager.ReadPage(pageId);
        return new HashBucket(pageId, page.GetData(), _bucketCapacity);
    }

    private void WriteBucket(HashBucket bucket)
    {
        var page = new Page(bucket.PageId, bucket.GetData());
        _pageManager.WritePage(page);
    }

    private void SplitBucket(int bucketPageId, HashBucket bucket)
    {
        var localDepth = bucket.LocalDepth;

        if (localDepth == _globalDepth)
        {
            // Need to double directory size
            DoubleDirectory();
        }

        // Create new bucket
        var newBucket = CreateBucket(localDepth + 1);
        bucket.LocalDepth = localDepth + 1;

        // Redistribute entries
        var entries = bucket.GetEntries().ToList();
        bucket.Clear();

        foreach (var entry in entries)
        {
            var hashValue = ComputeHash(entry.Key);
            var newBit = (hashValue >> localDepth) & 1;

            if (newBit == 0)
            {
                bucket.Insert(entry.Key, entry.RowId);
            }
            else
            {
                newBucket.Insert(entry.Key, entry.RowId);
            }
        }

        // Update directory pointers
        UpdateDirectoryAfterSplit(bucketPageId, bucket.PageId, newBucket.PageId, localDepth);

        WriteBucket(bucket);
        WriteBucket(newBucket);
        SaveDirectory();
    }

    private void DoubleDirectory()
    {
        var oldSize = _directory.Length;
        var newSize = oldSize * 2;
        var newDirectory = new int[newSize];

        // Copy and duplicate entries
        for (int i = 0; i < oldSize; i++)
        {
            newDirectory[i] = _directory[i];
            newDirectory[i + oldSize] = _directory[i];
        }

        _directory = newDirectory;
        _globalDepth++;
    }

    private void UpdateDirectoryAfterSplit(int oldBucketPageId, int bucket1PageId, int bucket2PageId, int splitDepth)
    {
        var mask = (1 << (splitDepth + 1)) - 1;
        var highBit = 1 << splitDepth;

        for (int i = 0; i < _directory.Length; i++)
        {
            if (_directory[i] == oldBucketPageId)
            {
                if ((i & highBit) == 0)
                {
                    _directory[i] = bucket1PageId;
                }
                else
                {
                    _directory[i] = bucket2PageId;
                }
            }
        }
    }

    private static uint ComputeHash(CompositeKey key)
    {
        // Simple hash combining all key values
        uint hash = 2166136261; // FNV-1a offset basis
        for (int i = 0; i < key.Length; i++)
        {
            var valueHash = (uint)key.Values[i].GetHashCode();
            hash ^= valueHash;
            hash *= 16777619; // FNV-1a prime
        }
        return hash;
    }

    #endregion

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        Flush();
        _pageManager.Dispose();
    }
}

/// <summary>
/// Represents a hash bucket storing key-RowId pairs.
/// </summary>
internal sealed class HashBucket
{
    private const int HeaderSize = 8; // PageId (4) + LocalDepth (2) + EntryCount (2)
    private const int EntryHeaderSize = 8; // KeyLength (4) + RowId (6) rounded up

    private readonly byte[] _data;
    private readonly int _capacity;

    public int PageId { get; }

    public int LocalDepth
    {
        get => BitConverter.ToInt16(_data, 4);
        set => BitConverter.TryWriteBytes(_data.AsSpan(4, 2), (short)value);
    }

    public int EntryCount
    {
        get => BitConverter.ToInt16(_data, 6);
        private set => BitConverter.TryWriteBytes(_data.AsSpan(6, 2), (short)value);
    }

    public HashBucket(int pageId, int localDepth, int capacity)
    {
        PageId = pageId;
        _capacity = capacity;
        _data = new byte[Constants.PageSize];

        // Initialize header
        BitConverter.TryWriteBytes(_data.AsSpan(0, 4), pageId);
        LocalDepth = localDepth;
        EntryCount = 0;
    }

    public HashBucket(int pageId, byte[] data, int capacity)
    {
        PageId = pageId;
        _capacity = capacity;
        _data = data;
    }

    public byte[] GetData() => _data;

    public bool CanInsert() => EntryCount < _capacity;

    public bool Insert(CompositeKey key, RowId rowId)
    {
        if (!CanInsert())
            return false;

        var keyData = SerializeKey(key);
        var entryOffset = HeaderSize + (EntryCount * GetEntrySize());

        // Write key length and data
        BitConverter.TryWriteBytes(_data.AsSpan(entryOffset, 4), keyData.Length);
        Buffer.BlockCopy(keyData, 0, _data, entryOffset + 4, keyData.Length);

        // Write RowId
        var rowIdOffset = entryOffset + 4 + 256; // Fixed max key size
        BitConverter.TryWriteBytes(_data.AsSpan(rowIdOffset, 4), rowId.PageId);
        BitConverter.TryWriteBytes(_data.AsSpan(rowIdOffset + 4, 2), rowId.SlotNumber);

        EntryCount++;
        return true;
    }

    public bool Delete(CompositeKey key, RowId rowId)
    {
        for (int i = 0; i < EntryCount; i++)
        {
            var entry = ReadEntry(i);
            if (entry.Key == key && entry.RowId == rowId)
            {
                // Shift remaining entries
                for (int j = i; j < EntryCount - 1; j++)
                {
                    var nextEntry = ReadEntry(j + 1);
                    WriteEntry(j, nextEntry.Key, nextEntry.RowId);
                }
                EntryCount--;
                return true;
            }
        }
        return false;
    }

    public void Clear()
    {
        EntryCount = 0;
    }

    public IEnumerable<(CompositeKey Key, RowId RowId)> GetEntries()
    {
        for (int i = 0; i < EntryCount; i++)
        {
            yield return ReadEntry(i);
        }
    }

    private int GetEntrySize() => 4 + 256 + 6; // KeyLength + MaxKey + RowId

    private (CompositeKey Key, RowId RowId) ReadEntry(int index)
    {
        var entryOffset = HeaderSize + (index * GetEntrySize());

        // Read key
        var keyLength = BitConverter.ToInt32(_data, entryOffset);
        var keyData = new byte[keyLength];
        Buffer.BlockCopy(_data, entryOffset + 4, keyData, 0, keyLength);
        var key = DeserializeKey(keyData);

        // Read RowId
        var rowIdOffset = entryOffset + 4 + 256;
        var pageId = BitConverter.ToInt32(_data, rowIdOffset);
        var slotNumber = BitConverter.ToInt16(_data, rowIdOffset + 4);
        var rowId = new RowId(pageId, slotNumber);

        return (key, rowId);
    }

    private void WriteEntry(int index, CompositeKey key, RowId rowId)
    {
        var entryOffset = HeaderSize + (index * GetEntrySize());
        var keyData = SerializeKey(key);

        BitConverter.TryWriteBytes(_data.AsSpan(entryOffset, 4), keyData.Length);
        Buffer.BlockCopy(keyData, 0, _data, entryOffset + 4, keyData.Length);

        var rowIdOffset = entryOffset + 4 + 256;
        BitConverter.TryWriteBytes(_data.AsSpan(rowIdOffset, 4), rowId.PageId);
        BitConverter.TryWriteBytes(_data.AsSpan(rowIdOffset + 4, 2), rowId.SlotNumber);
    }

    private static byte[] SerializeKey(CompositeKey key)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(key.Length);
        for (int i = 0; i < key.Length; i++)
        {
            var valueBytes = key.Values[i].Serialize();
            writer.Write(valueBytes.Length);
            writer.Write(valueBytes);
        }

        return stream.ToArray();
    }

    private static CompositeKey DeserializeKey(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var count = reader.ReadInt32();
        var values = new DataValue[count];
        for (int i = 0; i < count; i++)
        {
            var valueLength = reader.ReadInt32();
            var valueBytes = reader.ReadBytes(valueLength);
            values[i] = DataValue.Deserialize(valueBytes);
        }

        return new CompositeKey(values);
    }
}
