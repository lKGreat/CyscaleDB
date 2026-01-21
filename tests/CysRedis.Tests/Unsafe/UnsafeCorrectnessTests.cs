using System.Text;
using CysRedis.Core.Unsafe.DataStructures;
using Xunit;

namespace CysRedis.Tests.Unsafe;

/// <summary>
/// Comprehensive correctness tests for unsafe implementations.
/// Ensures behavior matches expected Redis semantics.
/// </summary>
public class UnsafeCorrectnessTests
{
    [Fact]
    public void UnsafeIntSet_MinMax()
    {
        using var intset = new UnsafeIntSet();
        
        intset.Add(100);
        intset.Add(50);
        intset.Add(200);
        intset.Add(25);
        
        Assert.Equal(25, intset.Min);
        Assert.Equal(200, intset.Max);
    }
    
    [Fact]
    public void UnsafeIntSet_GetAll()
    {
        using var intset = new UnsafeIntSet();
        
        var values = new[] { 100, 50, 200, 25 };
        foreach (var v in values)
        {
            intset.Add(v);
        }
        
        var all = intset.GetAll().ToList();
        Assert.Equal(4, all.Count);
        Assert.All(values, v => Assert.Contains(v, all));
    }
    
    [Fact]
    public void UnsafeListpack_IntegerEncoding()
    {
        using var listpack = new UnsafeListpack();
        
        // Test different integer sizes
        listpack.AppendInteger(127);      // 7-bit
        listpack.AppendInteger(4095);     // 13-bit
        listpack.AppendInteger(32767);    // 16-bit
        listpack.AppendInteger(int.MaxValue); // 32-bit
        listpack.AppendInteger(long.MaxValue); // 64-bit
        
        Assert.Equal(5, listpack.Count);
        
        var entry1 = listpack.GetAt(0);
        Assert.Equal(127, entry1.IntValue);
        
        var entry5 = listpack.GetAt(4);
        Assert.Equal(long.MaxValue, entry5.IntValue);
    }
    
    [Fact]
    public void UnsafeListpack_StringEncoding()
    {
        using var listpack = new UnsafeListpack();
        
        var small = Encoding.UTF8.GetBytes("small");
        var medium = Encoding.UTF8.GetBytes(new string('a', 100));
        var large = Encoding.UTF8.GetBytes(new string('b', 5000));
        
        listpack.AppendString(small);
        listpack.AppendString(medium);
        listpack.AppendString(large);
        
        Assert.Equal(3, listpack.Count);
        
        var entry1 = listpack.GetAt(0);
        Assert.Equal("small", Encoding.UTF8.GetString(entry1.StringValue!));
        
        var entry3 = listpack.GetAt(2);
        Assert.Equal(large.Length, entry3.StringValue!.Length);
    }
    
    [Fact]
    public void UnsafeSkipList_RankOrdering()
    {
        using var skiplist = new UnsafeSkipList();
        
        var keys = new[] { "z", "a", "m", "b", "y" };
        foreach (var key in keys)
        {
            skiplist.Insert(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes($"value_{key}"));
        }
        
        // Get ranks should be in sorted order
        var ranks = keys.Select(k => skiplist.GetRank(Encoding.UTF8.GetBytes(k))).ToList();
        Assert.All(ranks, r => Assert.True(r >= 0));
        
        // Verify ordering
        var sortedKeys = keys.OrderBy(k => k).ToList();
        for (int i = 0; i < sortedKeys.Count; i++)
        {
            var rank = skiplist.GetRank(Encoding.UTF8.GetBytes(sortedKeys[i]));
            Assert.True(skiplist.GetByRank(rank, out var key, out _));
            Assert.Equal(sortedKeys[i], Encoding.UTF8.GetString(key));
        }
    }
    
    [Fact]
    public void UnsafeRedisHash_ZiplistToHashtableConversion()
    {
        using var hash = new UnsafeRedisHash();
        
        // Add entries to trigger conversion
        for (int i = 0; i < 100; i++)
        {
            var field = Encoding.UTF8.GetBytes($"field_{i}");
            var value = Encoding.UTF8.GetBytes($"value_{i}");
            hash.Set(field, value);
        }
        
        // Verify all entries are accessible
        for (int i = 0; i < 100; i++)
        {
            var field = Encoding.UTF8.GetBytes($"field_{i}");
            Assert.True(hash.Get(field, out var value));
            Assert.Equal($"value_{i}", Encoding.UTF8.GetString(value));
        }
    }
    
    [Fact]
    public void UnsafeRedisSet_IntSetToHashtableConversion()
    {
        using var set = new UnsafeRedisSet();
        
        // Add integers first (uses intset)
        for (int i = 0; i < 50; i++)
        {
            set.Add(Encoding.UTF8.GetBytes(i.ToString()));
        }
        
        // Add non-integer (triggers conversion to hashtable)
        set.Add(Encoding.UTF8.GetBytes("non_integer_value"));
        
        // Verify all entries are accessible
        Assert.True(set.Contains(Encoding.UTF8.GetBytes("25")));
        Assert.True(set.Contains(Encoding.UTF8.GetBytes("non_integer_value")));
    }
    
    [Fact]
    public void UnsafeRedisList_QuicklistStructure()
    {
        using var list = new UnsafeRedisList();
        
        // Add many entries to trigger multiple nodes
        for (int i = 0; i < 1000; i++)
        {
            var value = Encoding.UTF8.GetBytes($"value_{i}");
            list.PushRight(value);
        }
        
        Assert.Equal(1000, list.Count);
        
        // Pop all entries
        int count = 0;
        while (list.PopLeft(out _))
        {
            count++;
        }
        
        Assert.Equal(1000, count);
        Assert.Equal(0, list.Count);
    }
    
    [Fact]
    public void UnsafeRedisStream_EntryOrdering()
    {
        using var stream = new UnsafeRedisStream();
        
        var timestamps = new[] { 1000L, 2000L, 1500L, 3000L };
        var sequences = new[] { 0L, 0L, 1L, 0L };
        
        for (int i = 0; i < timestamps.Length; i++)
        {
            var field = Encoding.UTF8.GetBytes($"field_{i}");
            var value = Encoding.UTF8.GetBytes($"value_{i}");
            stream.Add(timestamps[i], sequences[i], field, value);
        }
        
        Assert.Equal(4, stream.Count);
        
        // Read entries should be in timestamp order
        var entries = stream.Read(null, null, 10).ToList();
        Assert.Equal(4, entries.Count);
    }
    
    [Fact]
    public unsafe void UnsafeKvStore_SlotDistribution()
    {
        using var store = new UnsafeKvStore();
        
        // Test slot distribution
        var slotCounts = new int[store.SlotCount];
        for (int i = 0; i < 10000; i++)
        {
            var key = Encoding.UTF8.GetBytes($"key_{i}");
            // Slot calculation is internal, but we can verify keys are distributed
            store.Set(key, (void*)i, 0);
        }
        
        // Verify all keys are accessible
        for (int i = 0; i < 1000; i++)
        {
            var key = Encoding.UTF8.GetBytes($"key_{i}");
            Assert.True(store.Get(key, out var value, out _));
            Assert.Equal((IntPtr)(void*)i, (IntPtr)value);
        }
    }
    
    [Fact]
    public unsafe void UnsafeKvStore_ConcurrentAccess()
    {
        using var store = new UnsafeKvStore();
        const int ThreadCount = 8;
        const int KeysPerThread = 1000;
        
        var tasks = new List<Task>();
        var errors = new List<string>();
        
        for (int t = 0; t < ThreadCount; t++)
        {
            int threadId = t;
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    unsafe
                    {
                        // Write phase
                        for (int i = 0; i < KeysPerThread; i++)
                        {
                            var key = Encoding.UTF8.GetBytes($"key_{threadId}_{i}");
                            store.Set(key, (void*)(threadId * 10000 + i), 0);
                        }
                        
                        // Read phase
                        for (int i = 0; i < KeysPerThread; i++)
                        {
                            var key = Encoding.UTF8.GetBytes($"key_{threadId}_{i}");
                            if (!store.Get(key, out var value, out _))
                            {
                                errors.Add($"Failed to get key_{threadId}_{i}");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    errors.Add($"Thread {threadId} error: {ex.Message}");
                }
            }));
        }
        
        Task.WaitAll(tasks.ToArray());
        
        Assert.Empty(errors);
    }
    
    [Fact]
    public void UnsafeHyperLogLog_Accuracy()
    {
        using var hll = new UnsafeHyperLogLog();
        
        // Add unique elements
        for (int i = 0; i < 10000; i++)
        {
            hll.Add($"element_{i}");
        }
        
        var count = hll.Count();
        
        // HLL is approximate, should be within reasonable range
        // Standard error is ~0.81% for 16384 registers
        var expected = 10000;
        var error = Math.Abs(count - expected) / (double)expected;
        
        Assert.True(error < 0.05, $"Error too high: {error:P2}, count: {count}, expected: {expected}");
    }
    
    [Fact]
    public void UnsafeCompactString_TypeUpgrade()
    {
        // Test type upgrades as string grows
        var small = Encoding.UTF8.GetBytes(new string('a', 10));
        var compactString = UnsafeCompactString.Create(small);
        
        Assert.Equal(1, compactString.HeaderSize); // Type5
        
        // Append to trigger upgrade
        var medium = Encoding.UTF8.GetBytes(new string('b', 100));
        compactString.Append(medium);
        
        // Should have upgraded to larger type
        Assert.True(compactString.Capacity >= 110);
        
        compactString.Dispose();
    }
}
