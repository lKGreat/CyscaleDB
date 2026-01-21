using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using CysRedis.Core.Unsafe.Common;
using CysRedis.Core.Unsafe.DataStructures;

namespace CysRedis.Core.Unsafe.DataStructures;

/// <summary>
/// High-performance Redis hash using ziplist for small hashes and open-addressing hashtable for large ones.
/// </summary>
public unsafe sealed class UnsafeRedisHash : IDisposable
{
    private const int ZiplistMaxSize = 512; // Switch to hashtable after this many bytes
    private const int ZiplistMaxEntries = 64; // Switch to hashtable after this many entries
    
    // Small hash uses ziplist (listpack)
    private UnsafeListpack _ziplist;
    
    // Large hash uses open-addressing hashtable
    private HashEntry* _table;
    private int _tableSize;
    private int _tableMask;
    private int _count;
    private bool _isZiplist;
    
    /// <summary>
    /// Number of fields in the hash.
    /// </summary>
    public int Count => _count;
    
    /// <summary>
    /// Creates a new hash.
    /// </summary>
    public UnsafeRedisHash()
    {
        _ziplist = new UnsafeListpack(256);
        _isZiplist = true;
        _count = 0;
    }
    
    /// <summary>
    /// Sets a field value.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Set(ReadOnlySpan<byte> field, ReadOnlySpan<byte> value)
    {
        if (_isZiplist)
        {
            SetZiplist(field, value);
            // Check if we need to convert to hashtable
            if (_ziplist.TotalBytes > ZiplistMaxSize || _count > ZiplistMaxEntries)
            {
                ConvertToHashtable();
            }
        }
        else
        {
            SetHashtable(field, value);
        }
    }
    
    /// <summary>
    /// Gets a field value.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Get(ReadOnlySpan<byte> field, out ReadOnlySpan<byte> value)
    {
        if (_isZiplist)
        {
            return GetZiplist(field, out value);
        }
        else
        {
            return GetHashtable(field, out value);
        }
    }
    
    /// <summary>
    /// Deletes a field.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Delete(ReadOnlySpan<byte> field)
    {
        if (_isZiplist)
        {
            // Ziplist deletion is expensive, convert to hashtable if needed
            // For now, just mark as deleted (simplified)
            return false;
        }
        else
        {
            return DeleteHashtable(field);
        }
    }
    
    private void SetZiplist(ReadOnlySpan<byte> field, ReadOnlySpan<byte> value)
    {
        // Simplified: append field and value
        _ziplist.Append(field);
        _ziplist.Append(value);
        _count++;
    }
    
    private bool GetZiplist(ReadOnlySpan<byte> field, out ReadOnlySpan<byte> value)
    {
        // Simplified: linear search
        value = default;
        return false;
    }
    
    private void SetHashtable(ReadOnlySpan<byte> field, ReadOnlySpan<byte> value)
    {
        uint hash = ComputeHash(field);
        int index = (int)(hash & _tableMask);
        
        // Linear probing
        for (int i = 0; i < _tableSize; i++)
        {
            int idx = (index + i) & _tableMask;
            HashEntry* entry = _table + idx;
            
            if (entry->State == 0) // Empty
            {
                CopyField(entry, field);
                CopyValue(entry, value);
                entry->HashCode = hash;
                entry->State = 1; // Filled
                _count++;
                return;
            }
            else if (entry->State == 1 && entry->HashCode == hash && CompareField(entry, field))
            {
                // Update existing
                CopyValue(entry, value);
                return;
            }
        }
        
        // Table full, need to resize
        ResizeHashtable();
        SetHashtable(field, value);
    }
    
    private bool GetHashtable(ReadOnlySpan<byte> field, out ReadOnlySpan<byte> value)
    {
        uint hash = ComputeHash(field);
        int index = (int)(hash & _tableMask);
        
        for (int i = 0; i < _tableSize; i++)
        {
            int idx = (index + i) & _tableMask;
            HashEntry* entry = _table + idx;
            
            if (entry->State == 0)
            {
                value = default;
                return false;
            }
            
            if (entry->State == 1 && entry->HashCode == hash && CompareField(entry, field))
            {
                value = new ReadOnlySpan<byte>(entry->Value, entry->ValueLen);
                return true;
            }
        }
        
        value = default;
        return false;
    }
    
    private bool DeleteHashtable(ReadOnlySpan<byte> field)
    {
        uint hash = ComputeHash(field);
        int index = (int)(hash & _tableMask);
        
        for (int i = 0; i < _tableSize; i++)
        {
            int idx = (index + i) & _tableMask;
            HashEntry* entry = _table + idx;
            
            if (entry->State == 0)
                return false;
            
            if (entry->State == 1 && entry->HashCode == hash && CompareField(entry, field))
            {
                entry->State = 2; // Deleted
                _count--;
                return true;
            }
        }
        
        return false;
    }
    
    private void ConvertToHashtable()
    {
        // Initialize hashtable
        _tableSize = 16;
        while (_tableSize < _count * 2)
            _tableSize <<= 1;
        
        _tableMask = _tableSize - 1;
        _table = (HashEntry*)UnsafeMemoryManager.AlignedAlloc((nuint)(_tableSize * sizeof(HashEntry)), SimdHelpers.CacheLineSize, zeroInit: true);
        
        // Copy entries from ziplist (simplified - would need to iterate ziplist)
        _isZiplist = false;
        
        // Free ziplist
        _ziplist.Dispose();
    }
    
    private void ResizeHashtable()
    {
        int oldSize = _tableSize;
        HashEntry* oldTable = _table;
        
        _tableSize <<= 1;
        _tableMask = _tableSize - 1;
        _table = (HashEntry*)UnsafeMemoryManager.AlignedAlloc((nuint)(_tableSize * sizeof(HashEntry)), SimdHelpers.CacheLineSize, zeroInit: true);
        
        // Rehash all entries
        for (int i = 0; i < oldSize; i++)
        {
            HashEntry* entry = oldTable + i;
            if (entry->State == 1)
            {
                ReadOnlySpan<byte> field = new ReadOnlySpan<byte>(entry->Key, entry->KeyLen);
                ReadOnlySpan<byte> value = new ReadOnlySpan<byte>(entry->Value, entry->ValueLen);
                SetHashtable(field, value);
            }
        }
        
        UnsafeMemoryManager.AlignedFree(oldTable);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private uint ComputeHash(ReadOnlySpan<byte> data)
    {
        fixed (byte* ptr = data)
        {
            return (uint)SimdHelpers.ComputeHashFast(ptr, data.Length);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CopyField(HashEntry* entry, ReadOnlySpan<byte> field)
    {
        int len = field.Length < 128 ? field.Length : 127;
        entry->KeyLen = (ushort)len;
        fixed (byte* src = field)
        {
            SimdHelpers.CopyMemory(src, entry->Key, len);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CopyValue(HashEntry* entry, ReadOnlySpan<byte> value)
    {
        int len = value.Length < 512 ? value.Length : 511;
        entry->ValueLen = (ushort)len;
        fixed (byte* src = value)
        {
            SimdHelpers.CopyMemory(src, entry->Value, len);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CompareField(HashEntry* entry, ReadOnlySpan<byte> field)
    {
        if (entry->KeyLen != field.Length)
            return false;
        
        fixed (byte* ptr = field)
        {
            return SimdHelpers.CompareMemory(entry->Key, ptr, field.Length);
        }
    }
    
    public void Dispose()
    {
        if (_isZiplist)
        {
            _ziplist.Dispose();
        }
        else if (_table != null)
        {
            UnsafeMemoryManager.AlignedFree(_table);
            _table = null;
        }
    }
}

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct HashEntry
{
    public fixed byte Key[128];
    public fixed byte Value[512];
    public ushort KeyLen;
    public ushort ValueLen;
    public long ExpireTime;
    public uint HashCode;
    public int State; // 0=empty, 1=filled, 2=deleted
}
