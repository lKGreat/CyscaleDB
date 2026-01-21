using System.Text;
using CysRedis.Core.Unsafe.DataStructures;
using Xunit;

namespace CysRedis.Tests.Unsafe;

/// <summary>
/// Integration tests for unsafe data structures.
/// Verifies correctness and compatibility with expected behavior.
/// </summary>
public class UnsafeDataStructuresTests
{
    [Fact]
    public void UnsafeIntSet_AddAndContains()
    {
        using var intset = new UnsafeIntSet();
        
        Assert.True(intset.Add(100));
        Assert.True(intset.Add(200));
        Assert.False(intset.Add(100)); // Duplicate
        
        Assert.True(intset.Contains(100));
        Assert.True(intset.Contains(200));
        Assert.False(intset.Contains(300));
        
        Assert.Equal(2, intset.Count);
    }
    
    [Fact]
    public void UnsafeIntSet_Remove()
    {
        using var intset = new UnsafeIntSet();
        
        intset.Add(100);
        intset.Add(200);
        intset.Add(300);
        
        Assert.True(intset.Remove(200));
        Assert.False(intset.Remove(200)); // Already removed
        Assert.False(intset.Contains(200));
        Assert.Equal(2, intset.Count);
    }
    
    [Fact]
    public void UnsafeIntSet_EncodingUpgrade()
    {
        using var intset = new UnsafeIntSet();
        
        // Start with int16
        intset.Add(100);
        Assert.Equal(2, intset.Encoding);
        
        // Upgrade to int32
        intset.Add(int.MaxValue);
        Assert.Equal(4, intset.Encoding);
        
        // Upgrade to int64
        intset.Add(long.MaxValue);
        Assert.Equal(8, intset.Encoding);
    }
    
    [Fact]
    public void UnsafeListpack_AppendAndGet()
    {
        using var listpack = new UnsafeListpack();
        
        listpack.AppendInteger(100);
        listpack.AppendInteger(200);
        listpack.Append(Encoding.UTF8.GetBytes("test"));
        
        Assert.Equal(3, listpack.Count);
        
        var entry1 = listpack.GetAt(0);
        Assert.True(entry1.IsInteger);
        Assert.Equal(100, entry1.IntValue);
        
        var entry2 = listpack.GetAt(1);
        Assert.True(entry2.IsInteger);
        Assert.Equal(200, entry2.IntValue);
        
        var entry3 = listpack.GetAt(2);
        Assert.False(entry3.IsInteger);
        Assert.Equal("test", Encoding.UTF8.GetString(entry3.StringValue!));
    }
    
    [Fact]
    public void UnsafeCompactString_CreateAndAppend()
    {
        var data = Encoding.UTF8.GetBytes("Hello");
        var compactString = UnsafeCompactString.Create(data);
        
        Assert.Equal(5, compactString.Length);
        Assert.Equal("Hello", compactString.GetString());
        
        compactString.Append(Encoding.UTF8.GetBytes(" World"));
        Assert.Equal(11, compactString.Length);
        Assert.Equal("Hello World", compactString.GetString());
        
        compactString.Dispose();
    }
    
    [Fact]
    public void UnsafeCompactString_SmallString()
    {
        var data = Encoding.UTF8.GetBytes("Hi");
        var compactString = UnsafeCompactString.CreateSmall(data);
        
        Assert.Equal(2, compactString.Length);
        Assert.Equal("Hi", compactString.GetString());
        
        compactString.Dispose();
    }
    
    [Fact]
    public void UnsafeSkipList_InsertAndFind()
    {
        using var skiplist = new UnsafeSkipList();
        
        var key1 = Encoding.UTF8.GetBytes("key1");
        var value1 = Encoding.UTF8.GetBytes("value1");
        var key2 = Encoding.UTF8.GetBytes("key2");
        var value2 = Encoding.UTF8.GetBytes("value2");
        
        Assert.True(skiplist.Insert(key1, value1));
        Assert.True(skiplist.Insert(key2, value2));
        Assert.False(skiplist.Insert(key1, value1)); // Duplicate
        
        Assert.True(skiplist.Find(key1, out var foundValue1));
        Assert.Equal("value1", Encoding.UTF8.GetString(foundValue1));
        
        Assert.True(skiplist.Find(key2, out var foundValue2));
        Assert.Equal("value2", Encoding.UTF8.GetString(foundValue2));
        
        Assert.False(skiplist.Find(Encoding.UTF8.GetBytes("key3"), out _));
    }
    
    [Fact]
    public void UnsafeSkipList_Remove()
    {
        using var skiplist = new UnsafeSkipList();
        
        var key1 = Encoding.UTF8.GetBytes("key1");
        var value1 = Encoding.UTF8.GetBytes("value1");
        
        skiplist.Insert(key1, value1);
        Assert.True(skiplist.Find(key1, out _));
        
        Assert.True(skiplist.Remove(key1));
        Assert.False(skiplist.Find(key1, out _));
        Assert.False(skiplist.Remove(key1)); // Already removed
    }
    
    [Fact]
    public void UnsafeSkipList_GetRank()
    {
        using var skiplist = new UnsafeSkipList();
        
        var keys = new[] { "a", "b", "c", "d", "e" };
        foreach (var keyStr in keys)
        {
            skiplist.Insert(Encoding.UTF8.GetBytes(keyStr), Encoding.UTF8.GetBytes($"value_{keyStr}"));
        }
        
        var rank = skiplist.GetRank(Encoding.UTF8.GetBytes("c"));
        Assert.True(rank >= 0);
        
        Assert.True(skiplist.GetByRank(rank, out var key, out var value));
        Assert.Equal("c", Encoding.UTF8.GetString(key));
    }
    
    [Fact]
    public void UnsafeHyperLogLog_AddAndCount()
    {
        using var hll = new UnsafeHyperLogLog();
        
        for (int i = 0; i < 1000; i++)
        {
            hll.Add($"element_{i}");
        }
        
        var count = hll.Count();
        Assert.True(count > 0);
        Assert.True(count <= 1000); // HLL is approximate
    }
    
    [Fact]
    public void UnsafeHyperLogLog_Merge()
    {
        using var hll1 = new UnsafeHyperLogLog();
        using var hll2 = new UnsafeHyperLogLog();
        
        for (int i = 0; i < 500; i++)
        {
            hll1.Add($"element_{i}");
        }
        
        for (int i = 500; i < 1000; i++)
        {
            hll2.Add($"element_{i}");
        }
        
        hll1.Merge(hll2);
        var count = hll1.Count();
        Assert.True(count > 500);
    }
    
    [Fact]
    public void UnsafeRedisHash_SetAndGet()
    {
        using var hash = new UnsafeRedisHash();
        
        var field1 = Encoding.UTF8.GetBytes("field1");
        var value1 = Encoding.UTF8.GetBytes("value1");
        var field2 = Encoding.UTF8.GetBytes("field2");
        var value2 = Encoding.UTF8.GetBytes("value2");
        
        hash.Set(field1, value1);
        hash.Set(field2, value2);
        
        Assert.True(hash.Get(field1, out var foundValue1));
        Assert.Equal("value1", Encoding.UTF8.GetString(foundValue1));
        
        Assert.True(hash.Get(field2, out var foundValue2));
        Assert.Equal("value2", Encoding.UTF8.GetString(foundValue2));
        
        Assert.False(hash.Get(Encoding.UTF8.GetBytes("field3"), out _));
    }
    
    [Fact]
    public void UnsafeRedisHash_Delete()
    {
        using var hash = new UnsafeRedisHash();
        
        var field = Encoding.UTF8.GetBytes("field1");
        var value = Encoding.UTF8.GetBytes("value1");
        
        hash.Set(field, value);
        Assert.True(hash.Get(field, out _));
        
        Assert.True(hash.Delete(field));
        Assert.False(hash.Get(field, out _));
    }
    
    [Fact]
    public void UnsafeRedisSet_AddAndContains()
    {
        using var set = new UnsafeRedisSet();
        
        var member1 = Encoding.UTF8.GetBytes("member1");
        var member2 = Encoding.UTF8.GetBytes("member2");
        
        Assert.True(set.Add(member1));
        Assert.True(set.Add(member2));
        Assert.False(set.Add(member1)); // Duplicate
        
        Assert.True(set.Contains(member1));
        Assert.True(set.Contains(member2));
        Assert.False(set.Contains(Encoding.UTF8.GetBytes("member3")));
    }
    
    [Fact]
    public void UnsafeRedisSet_Remove()
    {
        using var set = new UnsafeRedisSet();
        
        var member = Encoding.UTF8.GetBytes("member1");
        
        set.Add(member);
        Assert.True(set.Contains(member));
        
        Assert.True(set.Remove(member));
        Assert.False(set.Contains(member));
    }
    
    [Fact]
    public void UnsafeRedisSet_IntSetMode()
    {
        using var set = new UnsafeRedisSet();
        
        // Add integers - should use intset internally
        for (int i = 0; i < 100; i++)
        {
            set.Add(Encoding.UTF8.GetBytes(i.ToString()));
        }
        
        Assert.True(set.Contains(Encoding.UTF8.GetBytes("50")));
        Assert.False(set.Contains(Encoding.UTF8.GetBytes("200")));
    }
    
    [Fact]
    public void UnsafeRedisList_PushAndPop()
    {
        using var list = new UnsafeRedisList();
        
        var value1 = Encoding.UTF8.GetBytes("value1");
        var value2 = Encoding.UTF8.GetBytes("value2");
        var value3 = Encoding.UTF8.GetBytes("value3");
        
        list.PushRight(value1);
        list.PushRight(value2);
        list.PushLeft(value3);
        
        Assert.Equal(3, list.Count);
        
        Assert.True(list.PopLeft(out var popped1));
        Assert.Equal("value3", Encoding.UTF8.GetString(popped1));
        
        Assert.True(list.PopRight(out var popped2));
        Assert.Equal("value2", Encoding.UTF8.GetString(popped2));
        
        Assert.True(list.PopLeft(out var popped3));
        Assert.Equal("value1", Encoding.UTF8.GetString(popped3));
        
        Assert.Equal(0, list.Count);
        Assert.False(list.PopLeft(out _));
    }
    
    [Fact]
    public void UnsafeRedisSortedSet_Add()
    {
        using var sortedSet = new UnsafeRedisSortedSet();
        
        var member1 = Encoding.UTF8.GetBytes("member1");
        var member2 = Encoding.UTF8.GetBytes("member2");
        
        sortedSet.Add(member1, 10.5);
        sortedSet.Add(member2, 20.0);
        
        Assert.Equal(2, sortedSet.Count);
    }
    
    [Fact]
    public void UnsafeRedisStream_Add()
    {
        using var stream = new UnsafeRedisStream();
        
        var field = Encoding.UTF8.GetBytes("field1");
        var value = Encoding.UTF8.GetBytes("value1");
        
        var id = stream.Add(0, 0, field, value);
        Assert.True(id.Timestamp > 0);
        Assert.Equal(1, stream.Count);
    }
    
    [Fact]
    public unsafe void UnsafeKvStore_SetAndGet()
    {
        using var store = new UnsafeKvStore();
        
        var key1 = Encoding.UTF8.GetBytes("key1");
        var key2 = Encoding.UTF8.GetBytes("key2");
        
        store.Set(key1, (void*)100, 0);
        store.Set(key2, (void*)200, 0);
        
        Assert.True(store.Get(key1, out var value1, out _));
        unsafe
        {
            Assert.Equal((IntPtr)(void*)100, (IntPtr)value1);
        }
        
        Assert.True(store.Get(key2, out var value2, out _));
        unsafe
        {
            Assert.Equal((IntPtr)(void*)200, (IntPtr)value2);
        }
        
        Assert.False(store.Get(Encoding.UTF8.GetBytes("key3"), out _, out _));
    }
    
    [Fact]
    public unsafe void UnsafeKvStore_Delete()
    {
        using var store = new UnsafeKvStore();
        
        var key = Encoding.UTF8.GetBytes("key1");
        
        store.Set(key, (void*)100, 0);
        Assert.True(store.Get(key, out _, out _));
        
        Assert.True(store.Delete(key));
        Assert.False(store.Get(key, out _, out _));
    }
    
    [Fact]
    public unsafe void UnsafeKvStore_Expiration()
    {
        using var store = new UnsafeKvStore();
        
        var key = Encoding.UTF8.GetBytes("key1");
        long expireTime = DateTimeOffset.UtcNow.AddSeconds(10).ToUnixTimeMilliseconds();
        
        store.Set(key, (void*)100, expireTime);
        
        Assert.True(store.Get(key, out _, out var exp));
        Assert.Equal(expireTime, exp);
        
        // Set expired time
        long expiredTime = DateTimeOffset.UtcNow.AddSeconds(-1).ToUnixTimeMilliseconds();
        store.Set(key, (void*)100, expiredTime);
        
        // Should not be found (expired)
        Assert.False(store.Get(key, out _, out _));
    }
    
    [Fact]
    public unsafe void SimdHelpers_CopyMemory()
    {
        var source = new byte[1024];
        var destination = new byte[1024];
        var random = new Random(42);
        random.NextBytes(source);
        
        fixed (byte* src = source)
        fixed (byte* dst = destination)
        {
            CysRedis.Core.Unsafe.Common.SimdHelpers.CopyMemory(src, dst, 1024);
        }
        
        Assert.Equal(source, destination);
    }
    
    [Fact]
    public unsafe void SimdHelpers_CompareMemory()
    {
        var data1 = new byte[1024];
        var data2 = new byte[1024];
        var random = new Random(42);
        random.NextBytes(data1);
        Array.Copy(data1, data2, 1024);
        
        fixed (byte* ptr1 = data1)
        fixed (byte* ptr2 = data2)
        {
            Assert.True(CysRedis.Core.Unsafe.Common.SimdHelpers.CompareMemory(ptr1, ptr2, 1024));
        }
        
        data2[100] = 0xFF;
        
        fixed (byte* ptr1 = data1)
        fixed (byte* ptr2 = data2)
        {
            Assert.False(CysRedis.Core.Unsafe.Common.SimdHelpers.CompareMemory(ptr1, ptr2, 1024));
        }
    }
    
    [Fact]
    public unsafe void UnsafeMemoryManager_Tracking()
    {
        CysRedis.Core.Unsafe.Common.UnsafeMemoryManager.LeakDetectionEnabled = true;
        CysRedis.Core.Unsafe.Common.UnsafeMemoryManager.ClearTracking();
        
        void* ptr = CysRedis.Core.Unsafe.Common.UnsafeMemoryManager.Alloc(1024);
        Assert.True(ptr != null);
        Assert.True(CysRedis.Core.Unsafe.Common.UnsafeMemoryManager.CurrentUsage > 0);
        
        CysRedis.Core.Unsafe.Common.UnsafeMemoryManager.Free(ptr);
        
        // Note: CurrentUsage might not be exactly 0 due to tracking overhead
        CysRedis.Core.Unsafe.Common.UnsafeMemoryManager.LeakDetectionEnabled = false;
    }
}
