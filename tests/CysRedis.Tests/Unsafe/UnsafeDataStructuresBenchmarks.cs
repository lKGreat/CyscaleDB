using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using CysRedis.Core.Unsafe.DataStructures;

namespace CysRedis.Tests.Unsafe;

/// <summary>
/// Performance benchmarks for unsafe data structures.
/// Compare with managed implementations to measure performance gains.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class UnsafeDataStructuresBenchmarks
{
    private const int TestSize = 10000;
    private byte[][] _testData;
    
    [GlobalSetup]
    public void Setup()
    {
        _testData = new byte[TestSize][];
        var random = new Random(42);
        for (int i = 0; i < TestSize; i++)
        {
            _testData[i] = new byte[random.Next(10, 100)];
            random.NextBytes(_testData[i]);
        }
    }
    
    [Benchmark(Baseline = true)]
    public void IntSet_Managed()
    {
        var intset = new CysRedis.Core.DataStructures.IntSet();
        for (int i = 0; i < TestSize; i++)
        {
            intset.Add(i);
        }
        for (int i = 0; i < TestSize; i++)
        {
            intset.Contains(i);
        }
    }
    
    [Benchmark]
    public void IntSet_Unsafe()
    {
        using var intset = new UnsafeIntSet();
        for (int i = 0; i < TestSize; i++)
        {
            intset.Add(i);
        }
        for (int i = 0; i < TestSize; i++)
        {
            intset.Contains(i);
        }
    }
    
    [Benchmark(Baseline = true)]
    public void Listpack_Managed()
    {
        var listpack = new CysRedis.Core.DataStructures.Listpack();
        for (int i = 0; i < TestSize; i++)
        {
            listpack.Append(_testData[i]);
        }
        var count = listpack.Count;
    }
    
    [Benchmark]
    public void Listpack_Unsafe()
    {
        using var listpack = new UnsafeListpack();
        for (int i = 0; i < TestSize; i++)
        {
            listpack.Append(_testData[i]);
        }
        var count = listpack.Count;
    }
    
    [Benchmark(Baseline = true)]
    public void CompactString_Managed()
    {
        var strings = new CysRedis.Core.DataStructures.CompactString[TestSize];
        for (int i = 0; i < TestSize; i++)
        {
            strings[i] = CysRedis.Core.DataStructures.CompactString.Create(_testData[i]);
        }
        for (int i = 0; i < TestSize; i++)
        {
            var span = strings[i].AsSpan();
        }
    }
    
    [Benchmark]
    public void CompactString_Unsafe()
    {
        var strings = new UnsafeCompactString[TestSize];
        for (int i = 0; i < TestSize; i++)
        {
            strings[i] = UnsafeCompactString.Create(_testData[i]);
        }
        for (int i = 0; i < TestSize; i++)
        {
            var span = strings[i].AsSpan();
        }
        for (int i = 0; i < TestSize; i++)
        {
            strings[i].Dispose();
        }
    }
    
    [Benchmark]
    public void HyperLogLog_Unsafe()
    {
        using var hll = new UnsafeHyperLogLog();
        var random = new Random(42);
        for (int i = 0; i < TestSize; i++)
        {
            hll.Add($"element_{random.Next(1000)}");
        }
        var count = hll.Count();
    }
    
    [Benchmark]
    public void SkipList_Unsafe()
    {
        using var skiplist = new UnsafeSkipList();
        for (int i = 0; i < TestSize; i++)
        {
            var key = System.Text.Encoding.UTF8.GetBytes($"key_{i}");
            var value = _testData[i];
            skiplist.Insert(key, value);
        }
        
        for (int i = 0; i < TestSize; i++)
        {
            var key = System.Text.Encoding.UTF8.GetBytes($"key_{i}");
            skiplist.Find(key, out _);
        }
    }
    
    [Benchmark]
    public void RedisHash_Unsafe()
    {
        using var hash = new UnsafeRedisHash();
        for (int i = 0; i < TestSize; i++)
        {
            var field = System.Text.Encoding.UTF8.GetBytes($"field_{i}");
            hash.Set(field, _testData[i]);
        }
        
        for (int i = 0; i < TestSize; i++)
        {
            var field = System.Text.Encoding.UTF8.GetBytes($"field_{i}");
            hash.Get(field, out _);
        }
    }
    
    [Benchmark]
    public void RedisSet_Unsafe()
    {
        using var set = new UnsafeRedisSet();
        for (int i = 0; i < TestSize; i++)
        {
            var member = System.Text.Encoding.UTF8.GetBytes($"member_{i}");
            set.Add(member);
        }
        
        for (int i = 0; i < TestSize; i++)
        {
            var member = System.Text.Encoding.UTF8.GetBytes($"member_{i}");
            set.Contains(member);
        }
    }
    
    [Benchmark]
    public void RedisList_Unsafe()
    {
        using var list = new UnsafeRedisList();
        for (int i = 0; i < TestSize; i++)
        {
            list.PushRight(_testData[i]);
        }
        
        for (int i = 0; i < TestSize; i++)
        {
            list.PopLeft(out _);
        }
    }
    
    [Benchmark]
    public unsafe void KvStore_Unsafe()
    {
        using var store = new UnsafeKvStore();
        for (int i = 0; i < TestSize; i++)
        {
            var key = System.Text.Encoding.UTF8.GetBytes($"key_{i}");
            store.Set(key, (void*)i, 0);
        }
        
        for (int i = 0; i < TestSize; i++)
        {
            var key = System.Text.Encoding.UTF8.GetBytes($"key_{i}");
            store.Get(key, out _, out _);
        }
    }
}

/// <summary>
/// SIMD performance benchmarks.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class SimdBenchmarks
{
    private const int BufferSize = 1024 * 1024; // 1MB
    private byte[] _source;
    private byte[] _destination;
    
    [GlobalSetup]
    public void Setup()
    {
        _source = new byte[BufferSize];
        _destination = new byte[BufferSize];
        var random = new Random(42);
        random.NextBytes(_source);
    }
    
    [Benchmark(Baseline = true)]
    public void MemoryCopy_Standard()
    {
        Array.Copy(_source, _destination, BufferSize);
    }
    
    [Benchmark]
    public void MemoryCopy_Simd()
    {
        unsafe
        {
            fixed (byte* src = _source)
            fixed (byte* dst = _destination)
            {
                CysRedis.Core.Unsafe.Common.SimdHelpers.CopyMemory(src, dst, BufferSize);
            }
        }
    }
    
    [Benchmark]
    public void MemoryCompare_Standard()
    {
        bool result = _source.SequenceEqual(_destination);
    }
    
    [Benchmark]
    public void MemoryCompare_Simd()
    {
        unsafe
        {
            fixed (byte* src = _source)
            fixed (byte* dst = _destination)
            {
                bool result = CysRedis.Core.Unsafe.Common.SimdHelpers.CompareMemory(src, dst, BufferSize);
            }
        }
    }
}
