using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using CysRedis.Core.Unsafe.Common;

namespace CysRedis.Core.Unsafe.DataStructures;

/// <summary>
/// High-performance key-value store with lock-free hash tables per slot.
/// Optimized for concurrent access with minimal contention.
/// </summary>
public unsafe sealed class UnsafeKvStore : IDisposable
{
    private const int SlotBits = 14; // 2^14 = 16384
    private const int NumSlots = 1 << SlotBits;
    
    private SlotHashTable* _slots;
    
    /// <summary>
    /// Number of slots in the store.
    /// </summary>
    public int SlotCount => NumSlots;
    
    /// <summary>
    /// Creates a new key-value store.
    /// </summary>
    public UnsafeKvStore()
    {
        _slots = (SlotHashTable*)UnsafeMemoryManager.AlignedAlloc((nuint)(NumSlots * sizeof(SlotHashTable)), SimdHelpers.CacheLineSize, zeroInit: true);
        
        // Initialize each slot
        for (int i = 0; i < NumSlots; i++)
        {
            InitializeSlot(_slots + i);
        }
    }
    
    /// <summary>
    /// Gets a value by key.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Get(ReadOnlySpan<byte> key, out void* value, out long expireTime)
    {
        int slot = GetSlotForKey(key);
        SlotHashTable* table = _slots + slot;
        
        uint hash = ComputeHash(key);
        int index = (int)(hash & table->Mask);
        
        // Linear probing
        for (int i = 0; i < table->Size; i++)
        {
            int idx = (index + i) & table->Mask;
            HashBucket* bucket = table->Buckets + idx;
            
            int state = Volatile.Read(ref bucket->State);
            if (state == 0) // Empty
            {
                value = null;
                expireTime = 0;
                return false;
            }
            
            if (state == 1 && bucket->HashCode == hash && CompareKey(bucket, key))
            {
                // Check expiration
                long exp = Volatile.Read(ref bucket->ExpireTime);
                if (exp > 0 && DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() >= exp)
                {
                    value = null;
                    expireTime = 0;
                    return false;
                }
                
                value = bucket->Value;
                expireTime = exp;
                return true;
            }
        }
        
        value = null;
        expireTime = 0;
        return false;
    }
    
    /// <summary>
    /// Sets a value.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Set(ReadOnlySpan<byte> key, void* value, long expireTime = 0)
    {
        int slot = GetSlotForKey(key);
        SlotHashTable* table = _slots + slot;
        
        uint hash = ComputeHash(key);
        int index = (int)(hash & table->Mask);
        
        // Try to update existing first
        for (int i = 0; i < table->Size; i++)
        {
            int idx = (index + i) & table->Mask;
            HashBucket* bucket = table->Buckets + idx;
            
            int state = Volatile.Read(ref bucket->State);
            if (state == 1 && bucket->HashCode == hash && CompareKey(bucket, key))
            {
                // Update existing
                CopyKey(bucket, key);
                bucket->Value = value;
                Volatile.Write(ref bucket->ExpireTime, expireTime);
                return true;
            }
        }
        
        // Insert new
        for (int i = 0; i < table->Size; i++)
        {
            int idx = (index + i) & table->Mask;
            HashBucket* bucket = table->Buckets + idx;
            
            int expected = 0;
            if (Interlocked.CompareExchange(ref bucket->State, 1, expected) == expected)
            {
                // Successfully acquired bucket
                CopyKey(bucket, key);
                bucket->HashCode = hash;
                bucket->Value = value;
                Volatile.Write(ref bucket->ExpireTime, expireTime);
                Interlocked.Increment(ref table->Count);
                return true;
            }
        }
        
        // Table full, need to resize
        ResizeSlot(table);
        return Set(key, value, expireTime);
    }
    
    /// <summary>
    /// Deletes a key.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Delete(ReadOnlySpan<byte> key)
    {
        int slot = GetSlotForKey(key);
        SlotHashTable* table = _slots + slot;
        
        uint hash = ComputeHash(key);
        int index = (int)(hash & table->Mask);
        
        for (int i = 0; i < table->Size; i++)
        {
            int idx = (index + i) & table->Mask;
            HashBucket* bucket = table->Buckets + idx;
            
            int state = Volatile.Read(ref bucket->State);
            if (state == 0)
                return false;
            
            if (state == 1 && bucket->HashCode == hash && CompareKey(bucket, key))
            {
                Interlocked.Exchange(ref bucket->State, 2); // Mark as deleted
                Interlocked.Decrement(ref table->Count);
                return true;
            }
        }
        
        return false;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetSlotForKey(ReadOnlySpan<byte> key)
    {
        // Simple hash-based slot assignment
        fixed (byte* ptr = key)
        {
            uint hash = (uint)SimdHelpers.ComputeHashFast(ptr, key.Length);
            return (int)(hash & (NumSlots - 1));
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ComputeHash(ReadOnlySpan<byte> data)
    {
        fixed (byte* ptr = data)
        {
            return (uint)SimdHelpers.ComputeHashFast(ptr, data.Length);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool CompareKey(HashBucket* bucket, ReadOnlySpan<byte> key)
    {
        if (bucket->KeyLen != key.Length)
            return false;
        
        fixed (byte* ptr = key)
        {
            return SimdHelpers.CompareMemory(bucket->Key, ptr, key.Length);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CopyKey(HashBucket* bucket, ReadOnlySpan<byte> key)
    {
        int len = key.Length < 256 ? key.Length : 255;
        bucket->KeyLen = (ushort)len;
        fixed (byte* src = key)
        {
            SimdHelpers.CopyMemory(src, bucket->Key, len);
        }
    }
    
    private static void InitializeSlot(SlotHashTable* table)
    {
        table->Size = 16;
        table->Mask = table->Size - 1;
        table->Count = 0;
        table->Version = 0;
        table->Buckets = (HashBucket*)UnsafeMemoryManager.AlignedAlloc((nuint)(table->Size * sizeof(HashBucket)), SimdHelpers.CacheLineSize, zeroInit: true);
    }
    
    private static void ResizeSlot(SlotHashTable* table)
    {
        int oldSize = table->Size;
        HashBucket* oldBuckets = table->Buckets;
        
        table->Size <<= 1;
        table->Mask = table->Size - 1;
        table->Buckets = (HashBucket*)UnsafeMemoryManager.AlignedAlloc((nuint)(table->Size * sizeof(HashBucket)), SimdHelpers.CacheLineSize, zeroInit: true);
        
        // Rehash all entries
        for (int i = 0; i < oldSize; i++)
        {
            HashBucket* oldBucket = oldBuckets + i;
            if (Volatile.Read(ref oldBucket->State) == 1)
            {
                ReadOnlySpan<byte> key = new ReadOnlySpan<byte>(oldBucket->Key, oldBucket->KeyLen);
                void* value = oldBucket->Value;
                long expireTime = Volatile.Read(ref oldBucket->ExpireTime);
                
                // Reinsert
                uint hash = oldBucket->HashCode;
                int index = (int)(hash & table->Mask);
                
                for (int j = 0; j < table->Size; j++)
                {
                    int idx = (index + j) & table->Mask;
                    HashBucket* bucket = table->Buckets + idx;
                    
                    if (Volatile.Read(ref bucket->State) == 0)
                    {
                        *bucket = *oldBucket;
                        Volatile.Write(ref bucket->State, 1);
                        break;
                    }
                }
            }
        }
        
        UnsafeMemoryManager.AlignedFree(oldBuckets);
        Interlocked.Increment(ref table->Version);
    }
    
    public void Dispose()
    {
        if (_slots != null)
        {
            for (int i = 0; i < NumSlots; i++)
            {
                SlotHashTable* table = _slots + i;
                if (table->Buckets != null)
                {
                    UnsafeMemoryManager.AlignedFree(table->Buckets);
                }
            }
            UnsafeMemoryManager.AlignedFree(_slots);
            _slots = null;
        }
    }
}

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct SlotHashTable
{
    public HashBucket* Buckets;
    public int Size;
    public int Mask;
    public int Count;
    public long Version;
}

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct HashBucket
{
    public fixed byte Key[256];
    public void* Value;
    public long ExpireTime;
    public ushort KeyLen;
    public uint HashCode;
    public volatile int State; // 0=empty, 1=filled, 2=deleted
}
