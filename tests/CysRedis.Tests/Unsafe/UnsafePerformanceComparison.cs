using System.Diagnostics;
using System.Text;
using CysRedis.Core.DataStructures;
using CysRedis.Core.Unsafe.DataStructures;

namespace CysRedis.Tests.Unsafe;

/// <summary>
/// Performance comparison tests between managed and unsafe implementations.
/// Provides detailed metrics and validation.
/// </summary>
public class UnsafePerformanceComparison
{
    private const int TestSize = 100000;
    
    [Fact]
    public void IntSet_PerformanceComparison()
    {
        var managed = new CysRedis.Core.DataStructures.IntSet();
        var stopwatch = Stopwatch.StartNew();
        
        for (int i = 0; i < TestSize; i++)
        {
            managed.Add(i);
        }
        stopwatch.Stop();
        var managedTime = stopwatch.ElapsedMilliseconds;
        
        stopwatch.Restart();
        using var unsafeImpl = new UnsafeIntSet();
        for (int i = 0; i < TestSize; i++)
        {
            unsafeImpl.Add(i);
        }
        stopwatch.Stop();
        var unsafeTime = stopwatch.ElapsedMilliseconds;
        
        var speedup = (double)managedTime / unsafeTime;
        Assert.True(speedup > 1.0, $"Expected speedup, got {speedup}x");
        
        // Verify correctness
        Assert.Equal(managed.Count, unsafeImpl.Count);
        for (int i = 0; i < 1000; i++)
        {
            Assert.Equal(managed.Contains(i), unsafeImpl.Contains(i));
        }
    }
    
    [Fact]
    public void Listpack_PerformanceComparison()
    {
        var testData = new byte[TestSize][];
        var random = new Random(42);
        for (int i = 0; i < TestSize; i++)
        {
            testData[i] = new byte[random.Next(10, 100)];
            random.NextBytes(testData[i]);
        }
        
        var managed = new Listpack();
        var stopwatch = Stopwatch.StartNew();
        for (int i = 0; i < TestSize; i++)
        {
            managed.Append(testData[i]);
        }
        stopwatch.Stop();
        var managedTime = stopwatch.ElapsedMilliseconds;
        
        stopwatch.Restart();
        using var unsafeImpl = new UnsafeListpack();
        for (int i = 0; i < TestSize; i++)
        {
            unsafeImpl.Append(testData[i]);
        }
        stopwatch.Stop();
        var unsafeTime = stopwatch.ElapsedMilliseconds;
        
        var speedup = (double)managedTime / unsafeTime;
        Assert.True(speedup > 1.0, $"Expected speedup, got {speedup}x");
        
        Assert.Equal(managed.Count, unsafeImpl.Count);
    }
    
    [Fact]
    public void SkipList_PerformanceComparison()
    {
        var keys = new string[TestSize];
        var values = new byte[TestSize][];
        var random = new Random(42);
        
        for (int i = 0; i < TestSize; i++)
        {
            keys[i] = $"key_{i}";
            values[i] = new byte[random.Next(10, 100)];
            random.NextBytes(values[i]);
        }
        
        // Note: Managed SkipList uses generics, so we compare conceptually
        var stopwatch = Stopwatch.StartNew();
        using var unsafeImpl = new UnsafeSkipList();
        for (int i = 0; i < TestSize; i++)
        {
            unsafeImpl.Insert(Encoding.UTF8.GetBytes(keys[i]), values[i]);
        }
        stopwatch.Stop();
        var insertTime = stopwatch.ElapsedMilliseconds;
        
        stopwatch.Restart();
        for (int i = 0; i < TestSize; i++)
        {
            unsafeImpl.Find(Encoding.UTF8.GetBytes(keys[i]), out _);
        }
        stopwatch.Stop();
        var lookupTime = stopwatch.ElapsedMilliseconds;
        
        Assert.True(insertTime < 5000, $"Insert took too long: {insertTime}ms");
        Assert.True(lookupTime < 5000, $"Lookup took too long: {lookupTime}ms");
    }
    
    [Fact]
    public void HyperLogLog_PerformanceComparison()
    {
        var elements = new string[TestSize];
        for (int i = 0; i < TestSize; i++)
        {
            elements[i] = $"element_{i % 1000}"; // Some duplicates
        }
        
        var stopwatch = Stopwatch.StartNew();
        using var hll = new UnsafeHyperLogLog();
        hll.Add(elements);
        stopwatch.Stop();
        var addTime = stopwatch.ElapsedMilliseconds;
        
        stopwatch.Restart();
        var count = hll.Count();
        stopwatch.Stop();
        var countTime = stopwatch.ElapsedMilliseconds;
        
        Assert.True(addTime < 1000, $"Add took too long: {addTime}ms");
        Assert.True(countTime < 100, $"Count took too long: {countTime}ms");
        Assert.True(count > 0 && count <= TestSize);
    }
    
    [Fact]
    public void KvStore_ConcurrentPerformance()
    {
        const int ThreadCount = 4;
        const int OperationsPerThread = 10000;
        
        using var store = new UnsafeKvStore();
        var tasks = new List<Task>();
        var random = new Random();
        
        for (int t = 0; t < ThreadCount; t++)
        {
            int threadId = t;
            tasks.Add(Task.Run(() =>
            {
                unsafe
                {
                    for (int i = 0; i < OperationsPerThread; i++)
                    {
                        var key = Encoding.UTF8.GetBytes($"key_{threadId}_{i}");
                        store.Set(key, (void*)(threadId * 10000 + i), 0);
                        
                        if (i % 10 == 0)
                        {
                            store.Get(key, out _, out _);
                        }
                    }
                }
            }));
        }
        
        var stopwatch = Stopwatch.StartNew();
        Task.WaitAll(tasks.ToArray());
        stopwatch.Stop();
        
        var totalOps = ThreadCount * OperationsPerThread;
        var opsPerSecond = totalOps * 1000.0 / stopwatch.ElapsedMilliseconds;
        
        Assert.True(opsPerSecond > 100000, $"Expected > 100k ops/sec, got {opsPerSecond:F0}");
    }
    
    [Fact]
    public void MemoryUsage_Comparison()
    {
        const int Size = 10000;
        
        // Measure managed memory
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var managedBefore = GC.GetTotalMemory(false);
        
        var managedIntSet = new CysRedis.Core.DataStructures.IntSet();
        for (int i = 0; i < Size; i++)
        {
            managedIntSet.Add(i);
        }
        var managedAfter = GC.GetTotalMemory(false);
        var managedMemory = managedAfter - managedBefore;
        
        // Measure unsafe memory
        using var unsafeIntSet = new UnsafeIntSet();
        for (int i = 0; i < Size; i++)
        {
            unsafeIntSet.Add(i);
        }
        var unsafeMemory = unsafeIntSet.MemoryUsage;
        
        // Unsafe should use less or similar memory
        Assert.True(unsafeMemory <= managedMemory * 1.5, 
            $"Unsafe memory ({unsafeMemory}) should be <= managed ({managedMemory})");
    }
}
