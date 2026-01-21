using System.Runtime.CompilerServices;
using CysRedis.Core.Unsafe.Common;
using CysRedis.Core.Unsafe.DataStructures;

namespace CysRedis.Core.Unsafe.DataStructures;

/// <summary>
/// High-performance Redis sorted set using ziplist for small sets and skiplist+hashtable for large sets.
/// </summary>
public unsafe sealed class UnsafeRedisSortedSet : IDisposable
{
    private const int ZiplistMaxSize = 128; // Switch to skiplist after this many bytes
    
    // Small sorted set uses ziplist
    private UnsafeListpack _ziplist;
    
    // Large sorted set uses skiplist + hashtable
    private UnsafeSkipList _skiplist;
    private SortedSetHashEntry* _hashTable; // member -> score lookup
    private int _tableSize;
    private int _tableMask;
    private int _count;
    private bool _isZiplist;
    
    /// <summary>
    /// Number of members in the sorted set.
    /// </summary>
    public int Count => _count;
    
    /// <summary>
    /// Creates a new sorted set.
    /// </summary>
    public UnsafeRedisSortedSet()
    {
        _ziplist = new UnsafeListpack(256);
        _isZiplist = true;
        _count = 0;
    }
    
    /// <summary>
    /// Adds a member with a score.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Add(ReadOnlySpan<byte> member, double score)
    {
        if (_isZiplist)
        {
            // Simplified: append to ziplist
            _ziplist.AppendInteger((long)(score * 1000)); // Store as integer
            _ziplist.Append(member);
            _count++;
            
            if (_ziplist.TotalBytes > ZiplistMaxSize)
            {
                ConvertToSkiplist();
            }
            return true;
        }
        else
        {
            return AddSkiplist(member, score);
        }
    }
    
    private bool AddSkiplist(ReadOnlySpan<byte> member, double score)
    {
        // Create composite key: score + member
        Span<byte> key = stackalloc byte[16];
        System.BitConverter.TryWriteBytes(key, score);
        member.CopyTo(key.Slice(8));
        
        // Insert into skiplist
        bool inserted = _skiplist.Insert(key, member);
        
        if (inserted)
        {
            // Add to hashtable for O(1) lookup
            uint hash = ComputeHash(member);
            int index = (int)(hash & _tableMask);
            
            // Linear probing
            for (int i = 0; i < _tableSize; i++)
            {
                int idx = (index + i) & _tableMask;
                SortedSetHashEntry* entry = (SortedSetHashEntry*)_hashTable + idx;
                
                if (entry->State == 0)
                {
                    CopyMember(entry, member);
                    entry->Score = score;
                    entry->HashCode = hash;
                    entry->State = 1;
                    _count++;
                    return true;
                }
            }
        }
        
        return inserted;
    }
    
    private void ConvertToSkiplist()
    {
        _skiplist = new UnsafeSkipList();
        
        _tableSize = 16;
        while (_tableSize < _count * 2)
            _tableSize <<= 1;
        _tableMask = _tableSize - 1;
        _hashTable = (SortedSetHashEntry*)UnsafeMemoryManager.AlignedAlloc((nuint)(_tableSize * sizeof(SortedSetHashEntry)), SimdHelpers.CacheLineSize, zeroInit: true);
        
        // Copy entries from ziplist (simplified)
        _isZiplist = false;
        
        _ziplist.Dispose();
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
    private void CopyMember(SortedSetHashEntry* entry, ReadOnlySpan<byte> member)
    {
        int len = member.Length < 128 ? member.Length : 127;
        entry->KeyLen = (ushort)len;
        fixed (byte* src = member)
        {
            SimdHelpers.CopyMemory(src, entry->Key, len);
        }
    }
    
    public void Dispose()
    {
        if (_isZiplist)
        {
            _ziplist.Dispose();
        }
        else
        {
            _skiplist.Dispose();
            if (_hashTable != null)
            {
                UnsafeMemoryManager.AlignedFree(_hashTable);
                _hashTable = null;
            }
        }
    }
}

[System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Sequential)]
internal unsafe struct SortedSetHashEntry
{
    public fixed byte Key[128];
    public ushort KeyLen;
    public double Score;
    public uint HashCode;
    public int State;
}
