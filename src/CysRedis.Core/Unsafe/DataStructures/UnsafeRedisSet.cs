using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.Marshalling;
using CysRedis.Core.Unsafe.Common;
using CysRedis.Core.Unsafe.DataStructures;

namespace CysRedis.Core.Unsafe.DataStructures;

/// <summary>
/// High-performance Redis set using intset for small integer sets and hashtable for large sets.
/// </summary>
public unsafe sealed class UnsafeRedisSet : IDisposable
{
    private const int IntSetMaxSize = 512; // Switch to hashtable after this many bytes
    
    // Small set uses intset
    private UnsafeIntSet _intset;
    
    // Large set uses open-addressing hashtable
    private SetEntry* _table;
    private int _tableSize;
    private int _tableMask;
    private int _count;
    private bool _isIntset;
    
    /// <summary>
    /// Number of members in the set.
    /// </summary>
    public int Count => _count;
    
    /// <summary>
    /// Creates a new set.
    /// </summary>
    public UnsafeRedisSet()
    {
        _intset = new UnsafeIntSet(8);
        _isIntset = true;
        _count = 0;
    }
    
    /// <summary>
    /// Adds a member to the set.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Add(ReadOnlySpan<byte> member)
    {
        if (_isIntset)
        {
            // Try to parse as integer
            if (TryParseInteger(member, out long intValue))
            {
                if (_intset.Add(intValue))
                {
                    _count++;
                    // Check if we need to convert to hashtable
                    if (_intset.MemoryUsage > IntSetMaxSize)
                    {
                        ConvertToHashtable();
                    }
                    return true;
                }
                return false;
            }
            else
            {
                // Not an integer, convert to hashtable
                ConvertToHashtable();
            }
        }
        
        // Use hashtable
        return AddHashtable(member);
    }
    
    /// <summary>
    /// Removes a member from the set.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Remove(ReadOnlySpan<byte> member)
    {
        if (_isIntset)
        {
            if (TryParseInteger(member, out long intValue))
            {
                if (_intset.Remove(intValue))
                {
                    _count--;
                    return true;
                }
                return false;
            }
            return false;
        }
        
        return RemoveHashtable(member);
    }
    
    /// <summary>
    /// Checks if a member exists in the set.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(ReadOnlySpan<byte> member)
    {
        if (_isIntset)
        {
            if (TryParseInteger(member, out long intValue))
            {
                return _intset.Contains(intValue);
            }
            return false;
        }
        
        return ContainsHashtable(member);
    }
    
    private bool AddHashtable(ReadOnlySpan<byte> member)
    {
        uint hash = ComputeHash(member);
        int index = (int)(hash & _tableMask);
        
        // Linear probing
        for (int i = 0; i < _tableSize; i++)
        {
            int idx = (index + i) & _tableMask;
            SetEntry* entry = _table + idx;
            
            if (entry->State == 0) // Empty
            {
                CopyMember(entry, member);
                entry->HashCode = hash;
                entry->State = 1; // Filled
                _count++;
                return true;
            }
            else if (entry->State == 1 && entry->HashCode == hash && CompareMember(entry, member))
            {
                // Already exists
                return false;
            }
        }
        
        // Table full, need to resize
        ResizeHashtable();
        return AddHashtable(member);
    }
    
    private bool RemoveHashtable(ReadOnlySpan<byte> member)
    {
        uint hash = ComputeHash(member);
        int index = (int)(hash & _tableMask);
        
        for (int i = 0; i < _tableSize; i++)
        {
            int idx = (index + i) & _tableMask;
            SetEntry* entry = _table + idx;
            
            if (entry->State == 0)
                return false;
            
            if (entry->State == 1 && entry->HashCode == hash && CompareMember(entry, member))
            {
                entry->State = 2; // Deleted
                _count--;
                return true;
            }
        }
        
        return false;
    }
    
    private bool ContainsHashtable(ReadOnlySpan<byte> member)
    {
        uint hash = ComputeHash(member);
        int index = (int)(hash & _tableMask);
        
        for (int i = 0; i < _tableSize; i++)
        {
            int idx = (index + i) & _tableMask;
            SetEntry* entry = _table + idx;
            
            if (entry->State == 0)
                return false;
            
            if (entry->State == 1 && entry->HashCode == hash && CompareMember(entry, member))
                return true;
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
        _table = (SetEntry*)UnsafeMemoryManager.AlignedAlloc((nuint)(_tableSize * sizeof(SetEntry)), SimdHelpers.CacheLineSize, zeroInit: true);
        
        // Copy entries from intset
        foreach (long value in _intset.GetAll())
        {
            var member = System.Text.Encoding.UTF8.GetBytes(value.ToString());
            fixed (byte* ptr = member)
            {
                AddHashtable(new ReadOnlySpan<byte>(ptr, member.Length));
            }
        }
        
        _isIntset = false;
        
        // Free intset
        _intset.Dispose();
    }
    
    private void ResizeHashtable()
    {
        int oldSize = _tableSize;
        SetEntry* oldTable = _table;
        
        _tableSize <<= 1;
        _tableMask = _tableSize - 1;
        _table = (SetEntry*)UnsafeMemoryManager.AlignedAlloc((nuint)(_tableSize * sizeof(SetEntry)), SimdHelpers.CacheLineSize, zeroInit: true);
        
        int oldCount = _count;
        _count = 0;
        
        // Rehash all entries
        for (int i = 0; i < oldSize; i++)
        {
            SetEntry* entry = oldTable + i;
            if (entry->State == 1)
            {
                ReadOnlySpan<byte> member = new ReadOnlySpan<byte>(entry->Member, entry->Length);
                AddHashtable(member);
            }
        }
        
        UnsafeMemoryManager.AlignedFree(oldTable);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryParseInteger(ReadOnlySpan<byte> data, out long value)
    {
        var str = System.Text.Encoding.UTF8.GetString(data);
        return long.TryParse(str, out value);
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
    private void CopyMember(SetEntry* entry, ReadOnlySpan<byte> member)
    {
        int len = member.Length < 256 ? member.Length : 255;
        entry->Length = (ushort)len;
        fixed (byte* src = member)
        {
            SimdHelpers.CopyMemory(src, entry->Member, len);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CompareMember(SetEntry* entry, ReadOnlySpan<byte> member)
    {
        if (entry->Length != member.Length)
            return false;
        
        fixed (byte* ptr = member)
        {
            return SimdHelpers.CompareMemory(entry->Member, ptr, member.Length);
        }
    }
    
    public void Dispose()
    {
        if (_isIntset)
        {
            _intset.Dispose();
        }
        else if (_table != null)
        {
            UnsafeMemoryManager.AlignedFree(_table);
            _table = null;
        }
    }
}

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct SetEntry
{
    public fixed byte Member[256];
    public ushort Length;
    public uint HashCode;
    public int State; // 0=empty, 1=filled, 2=deleted
}
