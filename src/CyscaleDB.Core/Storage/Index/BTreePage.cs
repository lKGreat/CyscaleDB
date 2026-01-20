using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage.Index;

/// <summary>
/// Types of B-Tree pages.
/// </summary>
public enum BTreePageType : byte
{
    /// <summary>
    /// Leaf node containing key-RowId pairs.
    /// </summary>
    Leaf = 0,

    /// <summary>
    /// Internal node containing key-childPageId pairs.
    /// </summary>
    Internal = 1
}

/// <summary>
/// Represents a B-Tree page for index storage.
/// 
/// Page format:
/// - Header (32 bytes):
///   - PageId (4 bytes)
///   - PageType (1 byte)
///   - IsLeaf (1 byte)
///   - KeyCount (2 bytes)
///   - ParentPageId (4 bytes)
///   - NextLeafPageId (4 bytes) - for leaf pages, forms a linked list
///   - PrevLeafPageId (4 bytes) - for leaf pages, doubly linked
///   - Reserved (12 bytes)
/// - Keys and pointers area
/// </summary>
public sealed class BTreePage
{
    private const int HeaderSize = 32;
    private const int KeyValueHeaderSize = 8; // 4 bytes length + 4 bytes for RowId/ChildPageId

    private readonly byte[] _data;

    /// <summary>
    /// The page ID.
    /// </summary>
    public int PageId { get; }

    /// <summary>
    /// Whether this is a leaf page.
    /// </summary>
    public bool IsLeaf
    {
        get => _data[5] == 1;
        set => _data[5] = value ? (byte)1 : (byte)0;
    }

    /// <summary>
    /// Number of keys in this page.
    /// </summary>
    public int KeyCount
    {
        get => BitConverter.ToInt16(_data, 6);
        private set => BitConverter.TryWriteBytes(_data.AsSpan(6, 2), (short)value);
    }

    /// <summary>
    /// Parent page ID (-1 if root).
    /// </summary>
    public int ParentPageId
    {
        get => BitConverter.ToInt32(_data, 8);
        set => BitConverter.TryWriteBytes(_data.AsSpan(8, 4), value);
    }

    /// <summary>
    /// Next leaf page ID (-1 if last leaf).
    /// </summary>
    public int NextLeafPageId
    {
        get => BitConverter.ToInt32(_data, 12);
        set => BitConverter.TryWriteBytes(_data.AsSpan(12, 4), value);
    }

    /// <summary>
    /// Previous leaf page ID (-1 if first leaf).
    /// </summary>
    public int PrevLeafPageId
    {
        get => BitConverter.ToInt32(_data, 16);
        set => BitConverter.TryWriteBytes(_data.AsSpan(16, 4), value);
    }

    /// <summary>
    /// The maximum number of keys this page can hold.
    /// </summary>
    public int Capacity { get; }

    /// <summary>
    /// Whether this page is full.
    /// </summary>
    public bool IsFull => KeyCount >= Capacity;

    /// <summary>
    /// Whether this page has less than half capacity (needs merge consideration).
    /// </summary>
    public bool IsUnderflow => KeyCount < Capacity / 2;

    /// <summary>
    /// Whether this page is dirty.
    /// </summary>
    public bool IsDirty { get; set; }

    /// <summary>
    /// Creates a new B-Tree page.
    /// </summary>
    public BTreePage(int pageId, bool isLeaf, int capacity)
    {
        PageId = pageId;
        Capacity = capacity;
        _data = new byte[Constants.PageSize];

        // Initialize header
        BitConverter.TryWriteBytes(_data.AsSpan(0, 4), pageId);
        _data[4] = (byte)PageType.Index;
        IsLeaf = isLeaf;
        KeyCount = 0;
        ParentPageId = -1;
        NextLeafPageId = -1;
        PrevLeafPageId = -1;
    }

    /// <summary>
    /// Creates a B-Tree page from existing data.
    /// </summary>
    public BTreePage(int pageId, byte[] data, int capacity)
    {
        if (data.Length != Constants.PageSize)
            throw new ArgumentException($"Page data must be exactly {Constants.PageSize} bytes");

        PageId = pageId;
        Capacity = capacity;
        _data = data;
    }

    /// <summary>
    /// Gets the raw page data.
    /// </summary>
    public byte[] GetData() => _data;

    #region Key Operations

    /// <summary>
    /// Inserts a key-value pair at the appropriate position.
    /// For leaf pages: key-RowId pair.
    /// For internal pages: key-childPageId pair.
    /// </summary>
    public bool InsertKey(CompositeKey key, RowId rowId)
    {
        if (IsFull)
            return false;

        var keyData = SerializeKey(key);
        var position = FindInsertPosition(key);

        // Shift existing entries to make room
        ShiftEntriesRight(position);

        // Write the new entry
        WriteEntry(position, keyData, rowId);
        KeyCount++;
        IsDirty = true;

        return true;
    }

    /// <summary>
    /// Inserts a key-childPageId pair (for internal nodes).
    /// </summary>
    public bool InsertKeyChild(CompositeKey key, int childPageId)
    {
        if (IsFull)
            return false;

        var keyData = SerializeKey(key);
        var position = FindInsertPosition(key);

        ShiftEntriesRight(position);
        WriteEntryInternal(position, keyData, childPageId);
        KeyCount++;
        IsDirty = true;

        return true;
    }

    /// <summary>
    /// Deletes a key from this page.
    /// </summary>
    public bool DeleteKey(CompositeKey key)
    {
        var position = FindKeyPosition(key);
        if (position < 0)
            return false;

        ShiftEntriesLeft(position);
        KeyCount--;
        IsDirty = true;

        return true;
    }

    /// <summary>
    /// Searches for a key and returns its RowId (for leaf pages).
    /// </summary>
    public RowId? SearchKey(CompositeKey key)
    {
        var position = FindKeyPosition(key);
        if (position < 0)
            return null;

        return ReadRowId(position);
    }

    /// <summary>
    /// Finds the child page ID for the given key (for internal pages).
    /// </summary>
    public int FindChildPageId(CompositeKey key)
    {
        for (int i = 0; i < KeyCount; i++)
        {
            var entryKey = ReadKey(i);
            if (key < entryKey)
            {
                // Go to left child
                return ReadChildPageId(i);
            }
        }
        // Go to rightmost child
        return ReadChildPageId(KeyCount);
    }

    /// <summary>
    /// Gets all keys and RowIds in this page (for leaf pages).
    /// </summary>
    public IEnumerable<(CompositeKey Key, RowId RowId)> GetAllEntries()
    {
        for (int i = 0; i < KeyCount; i++)
        {
            var key = ReadKey(i);
            var rowId = ReadRowId(i);
            yield return (key, rowId);
        }
    }

    /// <summary>
    /// Gets all keys and child page IDs (for internal pages).
    /// </summary>
    public IEnumerable<(CompositeKey Key, int ChildPageId)> GetAllInternalEntries()
    {
        for (int i = 0; i < KeyCount; i++)
        {
            var key = ReadKey(i);
            var childPageId = ReadChildPageId(i);
            yield return (key, childPageId);
        }
    }

    /// <summary>
    /// Gets the key at the specified index.
    /// </summary>
    public CompositeKey GetKey(int index)
    {
        if (index < 0 || index >= KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));
        return ReadKey(index);
    }

    /// <summary>
    /// Gets the RowId at the specified index (leaf pages only).
    /// </summary>
    public RowId GetRowId(int index)
    {
        if (index < 0 || index >= KeyCount)
            throw new ArgumentOutOfRangeException(nameof(index));
        return ReadRowId(index);
    }

    /// <summary>
    /// Gets the child page ID at the specified index (internal pages only).
    /// </summary>
    public int GetChildPageId(int index)
    {
        if (index < 0 || index > KeyCount) // Note: can have KeyCount+1 children
            throw new ArgumentOutOfRangeException(nameof(index));
        return ReadChildPageId(index);
    }

    /// <summary>
    /// Sets the child page ID at the specified index.
    /// </summary>
    public void SetChildPageId(int index, int pageId)
    {
        WriteChildPageId(index, pageId);
        IsDirty = true;
    }

    /// <summary>
    /// Performs range scan from startKey to endKey.
    /// </summary>
    public IEnumerable<(CompositeKey Key, RowId RowId)> RangeScan(CompositeKey? startKey, CompositeKey? endKey)
    {
        for (int i = 0; i < KeyCount; i++)
        {
            var key = ReadKey(i);
            
            if (startKey != null && key < startKey.Value)
                continue;
            
            if (endKey != null && key > endKey.Value)
                break;

            var rowId = ReadRowId(i);
            yield return (key, rowId);
        }
    }

    #endregion

    #region Split and Merge

    /// <summary>
    /// Splits this page into two pages, returning the median key and new page.
    /// </summary>
    public (CompositeKey MedianKey, BTreePage NewPage) Split(int newPageId)
    {
        var midPoint = KeyCount / 2;
        var newPage = new BTreePage(newPageId, IsLeaf, Capacity);

        // Copy upper half to new page
        for (int i = midPoint; i < KeyCount; i++)
        {
            var key = ReadKey(i);
            if (IsLeaf)
            {
                var rowId = ReadRowId(i);
                newPage.InsertKey(key, rowId);
            }
            else
            {
                var childId = ReadChildPageId(i);
                newPage.InsertKeyChild(key, childId);
            }
        }

        // If internal node, copy rightmost child pointer
        if (!IsLeaf)
        {
            newPage.SetChildPageId(newPage.KeyCount, ReadChildPageId(KeyCount));
        }

        var medianKey = ReadKey(midPoint);
        
        // Truncate this page
        KeyCount = midPoint;

        // Update leaf pointers
        if (IsLeaf)
        {
            newPage.NextLeafPageId = NextLeafPageId;
            newPage.PrevLeafPageId = PageId;
            NextLeafPageId = newPageId;
        }

        IsDirty = true;
        newPage.IsDirty = true;

        return (medianKey, newPage);
    }

    /// <summary>
    /// Merges this page with the sibling page.
    /// </summary>
    public void MergeWith(BTreePage sibling)
    {
        for (int i = 0; i < sibling.KeyCount; i++)
        {
            var key = sibling.ReadKey(i);
            if (IsLeaf)
            {
                var rowId = sibling.ReadRowId(i);
                InsertKey(key, rowId);
            }
            else
            {
                var childId = sibling.ReadChildPageId(i);
                InsertKeyChild(key, childId);
            }
        }

        if (!IsLeaf)
        {
            SetChildPageId(KeyCount, sibling.ReadChildPageId(sibling.KeyCount));
        }

        if (IsLeaf)
        {
            NextLeafPageId = sibling.NextLeafPageId;
        }

        IsDirty = true;
    }

    #endregion

    #region Private Methods

    private int FindInsertPosition(CompositeKey key)
    {
        for (int i = 0; i < KeyCount; i++)
        {
            var existingKey = ReadKey(i);
            if (key < existingKey)
                return i;
        }
        return KeyCount;
    }

    private int FindKeyPosition(CompositeKey key)
    {
        for (int i = 0; i < KeyCount; i++)
        {
            var existingKey = ReadKey(i);
            if (key == existingKey)
                return i;
        }
        return -1;
    }

    private int GetEntryOffset(int index)
    {
        // Each entry has variable key size + fixed pointer size
        // We use a simple fixed-size entry model for simplicity
        // Entry size = max key size + 6 bytes for RowId or 4 bytes for child pointer
        var entrySize = GetMaxEntrySize();
        return HeaderSize + (index * entrySize);
    }

    private int GetMaxEntrySize()
    {
        // Estimate: 256 bytes for key + 6 bytes for RowId
        // This limits keys but simplifies implementation
        return 262;
    }

    private void ShiftEntriesRight(int fromPosition)
    {
        var entrySize = GetMaxEntrySize();
        for (int i = KeyCount; i > fromPosition; i--)
        {
            var srcOffset = GetEntryOffset(i - 1);
            var dstOffset = GetEntryOffset(i);
            Buffer.BlockCopy(_data, srcOffset, _data, dstOffset, entrySize);
        }
    }

    private void ShiftEntriesLeft(int fromPosition)
    {
        var entrySize = GetMaxEntrySize();
        for (int i = fromPosition; i < KeyCount - 1; i++)
        {
            var srcOffset = GetEntryOffset(i + 1);
            var dstOffset = GetEntryOffset(i);
            Buffer.BlockCopy(_data, srcOffset, _data, dstOffset, entrySize);
        }
    }

    private void WriteEntry(int index, byte[] keyData, RowId rowId)
    {
        var offset = GetEntryOffset(index);
        
        // Write key length and data
        BitConverter.TryWriteBytes(_data.AsSpan(offset, 4), keyData.Length);
        Buffer.BlockCopy(keyData, 0, _data, offset + 4, keyData.Length);
        
        // Write RowId at fixed position after max key size
        var rowIdOffset = offset + 4 + 256; // 4 for length + 256 for max key
        BitConverter.TryWriteBytes(_data.AsSpan(rowIdOffset, 4), rowId.PageId);
        BitConverter.TryWriteBytes(_data.AsSpan(rowIdOffset + 4, 2), rowId.SlotNumber);
    }

    private void WriteEntryInternal(int index, byte[] keyData, int childPageId)
    {
        var offset = GetEntryOffset(index);
        
        // Write key length and data
        BitConverter.TryWriteBytes(_data.AsSpan(offset, 4), keyData.Length);
        Buffer.BlockCopy(keyData, 0, _data, offset + 4, keyData.Length);
        
        // Write child page ID
        var childOffset = offset + 4 + 256;
        BitConverter.TryWriteBytes(_data.AsSpan(childOffset, 4), childPageId);
    }

    private CompositeKey ReadKey(int index)
    {
        var offset = GetEntryOffset(index);
        var keyLength = BitConverter.ToInt32(_data, offset);
        var keyData = new byte[keyLength];
        Buffer.BlockCopy(_data, offset + 4, keyData, 0, keyLength);
        return DeserializeKey(keyData);
    }

    private RowId ReadRowId(int index)
    {
        var offset = GetEntryOffset(index);
        var rowIdOffset = offset + 4 + 256;
        var pageId = BitConverter.ToInt32(_data, rowIdOffset);
        var slotNumber = BitConverter.ToInt16(_data, rowIdOffset + 4);
        return new RowId(pageId, slotNumber);
    }

    private int ReadChildPageId(int index)
    {
        var offset = GetEntryOffset(index);
        var childOffset = offset + 4 + 256;
        return BitConverter.ToInt32(_data, childOffset);
    }

    private void WriteChildPageId(int index, int pageId)
    {
        var offset = GetEntryOffset(index);
        var childOffset = offset + 4 + 256;
        BitConverter.TryWriteBytes(_data.AsSpan(childOffset, 4), pageId);
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

    #endregion
}
