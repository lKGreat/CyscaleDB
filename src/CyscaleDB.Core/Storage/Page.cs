using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Represents a fixed-size page in the database.
/// Page format:
/// - Page Header (16 bytes):
///   - PageId (4 bytes)
///   - PageType (1 byte)
///   - Flags (1 byte)
///   - SlotCount (2 bytes)
///   - FreeSpaceOffset (2 bytes) - where free space starts (after records)
///   - FreeSpaceEnd (2 bytes) - where free space ends (before slot directory)
///   - Checksum (4 bytes)
/// - Record Area: grows forward from PageHeaderSize
/// - Free Space: between records and slot directory
/// - Slot Directory: grows backward from end of page
///   Each slot is 4 bytes: offset (2 bytes) + length (2 bytes)
/// </summary>
public sealed class Page
{
    /// <summary>
    /// The raw page data buffer.
    /// </summary>
    private readonly byte[] _data;

    /// <summary>
    /// The unique identifier of this page.
    /// </summary>
    public int PageId { get; }

    /// <summary>
    /// The type of this page.
    /// </summary>
    public PageType PageType
    {
        get => (PageType)_data[4];
        set => _data[4] = (byte)value;
    }

    /// <summary>
    /// Page flags.
    /// </summary>
    public PageFlags Flags
    {
        get => (PageFlags)_data[5];
        set => _data[5] = (byte)value;
    }

    /// <summary>
    /// Number of slots in this page.
    /// </summary>
    public int SlotCount
    {
        get => BitConverter.ToInt16(_data, 6);
        private set => BitConverter.TryWriteBytes(_data.AsSpan(6, 2), (short)value);
    }

    /// <summary>
    /// Offset where free space starts (end of last record).
    /// </summary>
    public int FreeSpaceOffset
    {
        get => BitConverter.ToInt16(_data, 8);
        private set => BitConverter.TryWriteBytes(_data.AsSpan(8, 2), (short)value);
    }

    /// <summary>
    /// Offset where free space ends (start of slot directory).
    /// </summary>
    public int FreeSpaceEnd
    {
        get => BitConverter.ToInt16(_data, 10);
        private set => BitConverter.TryWriteBytes(_data.AsSpan(10, 2), (short)value);
    }

    /// <summary>
    /// Page checksum for integrity verification.
    /// </summary>
    public uint Checksum
    {
        get => BitConverter.ToUInt32(_data, 12);
        private set => BitConverter.TryWriteBytes(_data.AsSpan(12, 4), value);
    }

    /// <summary>
    /// Available free space in bytes.
    /// </summary>
    public int FreeSpace => FreeSpaceEnd - FreeSpaceOffset;

    /// <summary>
    /// Whether this page has been modified since last flush.
    /// </summary>
    public bool IsDirty { get; set; }

    /// <summary>
    /// Pin count for buffer pool management.
    /// </summary>
    public int PinCount { get; set; }

    /// <summary>
    /// Creates a new empty page.
    /// </summary>
    public Page(int pageId, PageType pageType = PageType.Data)
    {
        PageId = pageId;
        _data = new byte[Constants.PageSize];
        
        // Initialize header
        BitConverter.TryWriteBytes(_data.AsSpan(0, 4), pageId);
        PageType = pageType;
        Flags = PageFlags.None;
        SlotCount = 0;
        FreeSpaceOffset = Constants.PageHeaderSize;
        FreeSpaceEnd = Constants.PageSize;
        
        UpdateChecksum();
    }

    /// <summary>
    /// Creates a page from existing data.
    /// </summary>
    public Page(int pageId, byte[] data)
    {
        if (data.Length != Constants.PageSize)
            throw new ArgumentException($"Page data must be exactly {Constants.PageSize} bytes");

        PageId = pageId;
        _data = data;
    }

    /// <summary>
    /// Gets the raw page data.
    /// </summary>
    public byte[] GetData() => _data;

    /// <summary>
    /// Gets a span of the raw page data.
    /// </summary>
    public Span<byte> GetDataSpan() => _data.AsSpan();

    /// <summary>
    /// Checks if a record of the given size can fit in this page.
    /// </summary>
    public bool CanFit(int recordSize)
    {
        // Need space for the record plus a slot entry
        return FreeSpace >= recordSize + Constants.SlotSize;
    }

    /// <summary>
    /// Inserts a record into this page.
    /// </summary>
    /// <returns>The slot number of the inserted record, or -1 if page is full.</returns>
    public int InsertRecord(byte[] record)
    {
        if (!CanFit(record.Length))
            return -1;

        // Write record at current free space offset
        var recordOffset = FreeSpaceOffset;
        Array.Copy(record, 0, _data, recordOffset, record.Length);
        FreeSpaceOffset = recordOffset + record.Length;

        // Allocate slot (grows backward from end)
        var slotNumber = SlotCount;
        SlotCount++;
        FreeSpaceEnd -= Constants.SlotSize;

        // Write slot entry
        WriteSlot(slotNumber, (ushort)recordOffset, (ushort)record.Length);

        IsDirty = true;
        return slotNumber;
    }

    /// <summary>
    /// Gets a record by slot number.
    /// </summary>
    public byte[]? GetRecord(int slotNumber)
    {
        if (slotNumber < 0 || slotNumber >= SlotCount)
            return null;

        var (offset, length) = ReadSlot(slotNumber);
        
        // Check for deleted record (offset = 0 indicates deleted)
        if (offset == 0)
            return null;

        var record = new byte[length];
        Array.Copy(_data, offset, record, 0, length);
        return record;
    }

    /// <summary>
    /// Deletes a record by slot number (marks slot as deleted).
    /// </summary>
    public bool DeleteRecord(int slotNumber)
    {
        if (slotNumber < 0 || slotNumber >= SlotCount)
            return false;

        // Mark slot as deleted by setting offset to 0
        WriteSlot(slotNumber, 0, 0);
        IsDirty = true;
        return true;
    }

    /// <summary>
    /// Updates a record in place if it fits, otherwise returns false.
    /// </summary>
    public bool UpdateRecord(int slotNumber, byte[] newRecord)
    {
        if (slotNumber < 0 || slotNumber >= SlotCount)
            return false;

        var (offset, length) = ReadSlot(slotNumber);
        if (offset == 0)
            return false;

        // If new record is same size or smaller, update in place
        if (newRecord.Length <= length)
        {
            Array.Copy(newRecord, 0, _data, offset, newRecord.Length);
            WriteSlot(slotNumber, (ushort)offset, (ushort)newRecord.Length);
            IsDirty = true;
            return true;
        }

        // New record is larger - need to delete and insert
        // First check if there's enough space
        if (!CanFit(newRecord.Length))
            return false;

        // Mark old slot as deleted
        WriteSlot(slotNumber, 0, 0);

        // Write new record at end
        var newOffset = FreeSpaceOffset;
        Array.Copy(newRecord, 0, _data, newOffset, newRecord.Length);
        FreeSpaceOffset = newOffset + newRecord.Length;

        // Update slot to point to new location
        WriteSlot(slotNumber, (ushort)newOffset, (ushort)newRecord.Length);
        IsDirty = true;
        return true;
    }

    /// <summary>
    /// Gets the offset of a slot entry in the page.
    /// </summary>
    private int GetSlotOffset(int slotNumber)
    {
        return Constants.PageSize - ((slotNumber + 1) * Constants.SlotSize);
    }

    /// <summary>
    /// Reads a slot entry.
    /// </summary>
    private (ushort Offset, ushort Length) ReadSlot(int slotNumber)
    {
        var slotOffset = GetSlotOffset(slotNumber);
        var offset = BitConverter.ToUInt16(_data, slotOffset);
        var length = BitConverter.ToUInt16(_data, slotOffset + 2);
        return (offset, length);
    }

    /// <summary>
    /// Writes a slot entry.
    /// </summary>
    private void WriteSlot(int slotNumber, ushort offset, ushort length)
    {
        var slotOffset = GetSlotOffset(slotNumber);
        BitConverter.TryWriteBytes(_data.AsSpan(slotOffset, 2), offset);
        BitConverter.TryWriteBytes(_data.AsSpan(slotOffset + 2, 2), length);
    }

    /// <summary>
    /// Calculates and updates the page checksum.
    /// </summary>
    public void UpdateChecksum()
    {
        // Simple checksum: XOR of all 4-byte words (excluding checksum itself)
        uint checksum = 0;
        for (int i = 0; i < Constants.PageSize; i += 4)
        {
            if (i != 12) // Skip checksum field
            {
                checksum ^= BitConverter.ToUInt32(_data, i);
            }
        }
        Checksum = checksum;
    }

    /// <summary>
    /// Verifies the page checksum.
    /// </summary>
    public bool VerifyChecksum()
    {
        var storedChecksum = Checksum;
        UpdateChecksum();
        var valid = Checksum == storedChecksum;
        Checksum = storedChecksum; // Restore
        return valid;
    }

    /// <summary>
    /// Iterates over all valid records in this page.
    /// </summary>
    public IEnumerable<(int SlotNumber, byte[] Data)> EnumerateRecords()
    {
        for (int i = 0; i < SlotCount; i++)
        {
            var record = GetRecord(i);
            if (record != null)
            {
                yield return (i, record);
            }
        }
    }

    /// <summary>
    /// Compacts the page by removing gaps from deleted records.
    /// </summary>
    public void Compact()
    {
        var records = new List<(int Slot, byte[] Data)>();
        
        // Collect all valid records
        for (int i = 0; i < SlotCount; i++)
        {
            var record = GetRecord(i);
            if (record != null)
            {
                records.Add((i, record));
            }
        }

        // Reset free space pointers
        FreeSpaceOffset = Constants.PageHeaderSize;
        FreeSpaceEnd = Constants.PageSize - (SlotCount * Constants.SlotSize);

        // Rewrite all records
        foreach (var (slot, data) in records)
        {
            var offset = FreeSpaceOffset;
            Array.Copy(data, 0, _data, offset, data.Length);
            FreeSpaceOffset = offset + data.Length;
            WriteSlot(slot, (ushort)offset, (ushort)data.Length);
        }

        IsDirty = true;
    }
}

/// <summary>
/// Types of pages in the database.
/// </summary>
public enum PageType : byte
{
    /// <summary>
    /// Contains row data.
    /// </summary>
    Data = 0,

    /// <summary>
    /// Contains index data.
    /// </summary>
    Index = 1,

    /// <summary>
    /// Contains overflow data for large records.
    /// </summary>
    Overflow = 2,

    /// <summary>
    /// Free page (not currently allocated).
    /// </summary>
    Free = 3,

    /// <summary>
    /// System/metadata page.
    /// </summary>
    System = 4
}

/// <summary>
/// Page flags.
/// </summary>
[Flags]
public enum PageFlags : byte
{
    None = 0,
    Dirty = 1 << 0,
    Pinned = 1 << 1,
    Leaf = 1 << 2,    // For index pages
    Internal = 1 << 3  // For index pages
}
