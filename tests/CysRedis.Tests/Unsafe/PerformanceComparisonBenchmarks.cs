using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using CysRedis.Core.DataStructures;
using CysRedis.Core.DataStructures.Adapters;
using CysRedis.Core.Unsafe.DataStructures;

namespace CysRedis.Tests.Unsafe;

/// <summary>
/// Comprehensive performance comparison benchmarks between managed and unsafe implementations.
/// Generates detailed reports comparing both implementations.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
[MarkdownExporter]
[HtmlExporter]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class PerformanceComparisonBenchmarks
{
    private const int SmallSize = 100;
    private const int MediumSize = 1000;
    private const int LargeSize = 10000;
    
    private byte[][] _testData;
    private string[] _testStrings;
    
    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(42);
        
        // Prepare test data
        _testData = new byte[LargeSize][];
        _testStrings = new string[LargeSize];
        
        for (int i = 0; i < LargeSize; i++)
        {
            _testData[i] = new byte[random.Next(10, 100)];
            random.NextBytes(_testData[i]);
            _testStrings[i] = $"value_{i}_{Guid.NewGuid()}";
        }
    }

    #region RedisHash Benchmarks

    [BenchmarkCategory("Hash"), Benchmark]
    public void Hash_Set_Small_Managed()
    {
        var hash = new RedisHash();
        for (int i = 0; i < SmallSize; i++)
        {
            hash.Set($"field_{i}", _testData[i]);
        }
    }

    [BenchmarkCategory("Hash"), Benchmark]
    public void Hash_Set_Small_Unsafe()
    {
        var hash = new UnsafeRedisHashAdapter();
        for (int i = 0; i < SmallSize; i++)
        {
            hash.Set($"field_{i}", _testData[i]);
        }
        hash.Dispose();
    }

    [BenchmarkCategory("Hash"), Benchmark]
    public void Hash_Get_Medium_Managed()
    {
        var hash = new RedisHash();
        for (int i = 0; i < MediumSize; i++)
        {
            hash.Set($"field_{i}", _testData[i]);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            hash.Get($"field_{i}");
        }
    }

    [BenchmarkCategory("Hash"), Benchmark]
    public void Hash_Get_Medium_Unsafe()
    {
        var hash = new UnsafeRedisHashAdapter();
        for (int i = 0; i < MediumSize; i++)
        {
            hash.Set($"field_{i}", _testData[i]);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            hash.Get($"field_{i}");
        }
        hash.Dispose();
    }

    [BenchmarkCategory("Hash"), Benchmark]
    public void Hash_Mixed_Large_Managed()
    {
        var hash = new RedisHash();
        for (int i = 0; i < LargeSize; i++)
        {
            hash.Set($"field_{i}", _testData[i]);
            if (i % 10 == 0)
            {
                hash.Get($"field_{i}");
            }
            if (i % 100 == 0)
            {
                hash.Delete($"field_{i - 50}");
            }
        }
    }

    [BenchmarkCategory("Hash"), Benchmark]
    public void Hash_Mixed_Large_Unsafe()
    {
        var hash = new UnsafeRedisHashAdapter();
        for (int i = 0; i < LargeSize; i++)
        {
            hash.Set($"field_{i}", _testData[i]);
            if (i % 10 == 0)
            {
                hash.Get($"field_{i}");
            }
            if (i % 100 == 0)
            {
                hash.Delete($"field_{i - 50}");
            }
        }
        hash.Dispose();
    }

    #endregion

    #region RedisSet Benchmarks

    [BenchmarkCategory("Set"), Benchmark]
    public void Set_Add_Small_Managed()
    {
        var set = new RedisSet();
        for (int i = 0; i < SmallSize; i++)
        {
            set.Add(_testStrings[i]);
        }
    }

    [BenchmarkCategory("Set"), Benchmark]
    public void Set_Add_Small_Unsafe()
    {
        var set = new UnsafeRedisSetAdapter();
        for (int i = 0; i < SmallSize; i++)
        {
            set.Add(_testStrings[i]);
        }
        set.Dispose();
    }

    [BenchmarkCategory("Set"), Benchmark]
    public void Set_Contains_Medium_Managed()
    {
        var set = new RedisSet();
        for (int i = 0; i < MediumSize; i++)
        {
            set.Add(_testStrings[i]);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            set.Contains(_testStrings[i]);
        }
    }

    [BenchmarkCategory("Set"), Benchmark]
    public void Set_Contains_Medium_Unsafe()
    {
        var set = new UnsafeRedisSetAdapter();
        for (int i = 0; i < MediumSize; i++)
        {
            set.Add(_testStrings[i]);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            set.Contains(_testStrings[i]);
        }
        set.Dispose();
    }

    [BenchmarkCategory("Set"), Benchmark]
    public void Set_Mixed_Large_Managed()
    {
        var set = new RedisSet();
        for (int i = 0; i < LargeSize; i++)
        {
            set.Add(_testStrings[i]);
            if (i % 10 == 0)
            {
                set.Contains(_testStrings[i]);
            }
            if (i % 100 == 0)
            {
                set.Remove(_testStrings[i - 50]);
            }
        }
    }

    [BenchmarkCategory("Set"), Benchmark]
    public void Set_Mixed_Large_Unsafe()
    {
        var set = new UnsafeRedisSetAdapter();
        for (int i = 0; i < LargeSize; i++)
        {
            set.Add(_testStrings[i]);
            if (i % 10 == 0)
            {
                set.Contains(_testStrings[i]);
            }
            if (i % 100 == 0)
            {
                set.Remove(_testStrings[i - 50]);
            }
        }
        set.Dispose();
    }

    #endregion

    #region RedisList Benchmarks

    [BenchmarkCategory("List"), Benchmark]
    public void List_Push_Small_Managed()
    {
        var list = new RedisList();
        for (int i = 0; i < SmallSize; i++)
        {
            list.PushRight(_testData[i]);
        }
    }

    [BenchmarkCategory("List"), Benchmark]
    public void List_Push_Small_Unsafe()
    {
        var list = new UnsafeRedisListAdapter();
        for (int i = 0; i < SmallSize; i++)
        {
            list.PushRight(_testData[i]);
        }
        list.Dispose();
    }

    [BenchmarkCategory("List"), Benchmark]
    public void List_Pop_Medium_Managed()
    {
        var list = new RedisList();
        for (int i = 0; i < MediumSize; i++)
        {
            list.PushRight(_testData[i]);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            list.PopLeft();
        }
    }

    [BenchmarkCategory("List"), Benchmark]
    public void List_Pop_Medium_Unsafe()
    {
        var list = new UnsafeRedisListAdapter();
        for (int i = 0; i < MediumSize; i++)
        {
            list.PushRight(_testData[i]);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            list.PopLeft();
        }
        list.Dispose();
    }

    [BenchmarkCategory("List"), Benchmark]
    public void List_Mixed_Large_Managed()
    {
        var list = new RedisList();
        for (int i = 0; i < LargeSize; i++)
        {
            if (i % 2 == 0)
                list.PushRight(_testData[i]);
            else
                list.PushLeft(_testData[i]);
            
            if (i % 10 == 0)
            {
                list.PopLeft();
            }
        }
    }

    [BenchmarkCategory("List"), Benchmark]
    public void List_Mixed_Large_Unsafe()
    {
        var list = new UnsafeRedisListAdapter();
        for (int i = 0; i < LargeSize; i++)
        {
            if (i % 2 == 0)
                list.PushRight(_testData[i]);
            else
                list.PushLeft(_testData[i]);
            
            if (i % 10 == 0)
            {
                list.PopLeft();
            }
        }
        list.Dispose();
    }

    #endregion

    #region RedisSortedSet Benchmarks

    [BenchmarkCategory("SortedSet"), Benchmark]
    public void SortedSet_Add_Small_Managed()
    {
        var zset = new RedisSortedSet();
        var random = new Random(42);
        for (int i = 0; i < SmallSize; i++)
        {
            zset.Add(_testStrings[i], random.NextDouble() * 100);
        }
    }

    [BenchmarkCategory("SortedSet"), Benchmark]
    public void SortedSet_Add_Small_Unsafe()
    {
        var zset = new UnsafeRedisSortedSetAdapter();
        var random = new Random(42);
        for (int i = 0; i < SmallSize; i++)
        {
            zset.Add(_testStrings[i], random.NextDouble() * 100);
        }
        zset.Dispose();
    }

    [BenchmarkCategory("SortedSet"), Benchmark]
    public void SortedSet_GetScore_Medium_Managed()
    {
        var zset = new RedisSortedSet();
        var random = new Random(42);
        for (int i = 0; i < MediumSize; i++)
        {
            zset.Add(_testStrings[i], random.NextDouble() * 100);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            zset.GetScore(_testStrings[i]);
        }
    }

    [BenchmarkCategory("SortedSet"), Benchmark]
    public void SortedSet_GetScore_Medium_Unsafe()
    {
        var zset = new UnsafeRedisSortedSetAdapter();
        var random = new Random(42);
        for (int i = 0; i < MediumSize; i++)
        {
            zset.Add(_testStrings[i], random.NextDouble() * 100);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            zset.GetScore(_testStrings[i]);
        }
        zset.Dispose();
    }

    [BenchmarkCategory("SortedSet"), Benchmark]
    public void SortedSet_Mixed_Large_Managed()
    {
        var zset = new RedisSortedSet();
        var random = new Random(42);
        for (int i = 0; i < LargeSize; i++)
        {
            zset.Add(_testStrings[i], random.NextDouble() * 100);
            if (i % 10 == 0)
            {
                zset.GetScore(_testStrings[i]);
            }
            if (i % 100 == 0)
            {
                zset.IncrBy(_testStrings[i - 50], 10.0);
            }
        }
    }

    [BenchmarkCategory("SortedSet"), Benchmark]
    public void SortedSet_Mixed_Large_Unsafe()
    {
        var zset = new UnsafeRedisSortedSetAdapter();
        var random = new Random(42);
        for (int i = 0; i < LargeSize; i++)
        {
            zset.Add(_testStrings[i], random.NextDouble() * 100);
            if (i % 10 == 0)
            {
                zset.GetScore(_testStrings[i]);
            }
            if (i % 100 == 0)
            {
                zset.IncrBy(_testStrings[i - 50], 10.0);
            }
        }
        zset.Dispose();
    }

    #endregion

    #region Factory-based Benchmarks (Real-world usage)

    [BenchmarkCategory("Factory"), Benchmark]
    public void Factory_Hash_Operations_Managed()
    {
        var factory = new ManagedDataStructureFactory();
        var hash = factory.CreateHash();
        
        for (int i = 0; i < MediumSize; i++)
        {
            hash.Set($"field_{i}", _testData[i]);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            hash.Get($"field_{i}");
        }
    }

    [BenchmarkCategory("Factory"), Benchmark]
    public void Factory_Hash_Operations_Unsafe()
    {
        var factory = new UnsafeDataStructureFactory();
        var hash = factory.CreateHash();
        
        for (int i = 0; i < MediumSize; i++)
        {
            hash.Set($"field_{i}", _testData[i]);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            hash.Get($"field_{i}");
        }
        
        if (hash is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    [BenchmarkCategory("Factory"), Benchmark]
    public void Factory_Set_Operations_Managed()
    {
        var factory = new ManagedDataStructureFactory();
        var set = factory.CreateSet();
        
        for (int i = 0; i < MediumSize; i++)
        {
            set.Add(_testStrings[i]);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            set.Contains(_testStrings[i]);
        }
    }

    [BenchmarkCategory("Factory"), Benchmark]
    public void Factory_Set_Operations_Unsafe()
    {
        var factory = new UnsafeDataStructureFactory();
        var set = factory.CreateSet();
        
        for (int i = 0; i < MediumSize; i++)
        {
            set.Add(_testStrings[i]);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            set.Contains(_testStrings[i]);
        }
        
        if (set is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    [BenchmarkCategory("Factory"), Benchmark]
    public void Factory_List_Operations_Managed()
    {
        var factory = new ManagedDataStructureFactory();
        var list = factory.CreateList();
        
        for (int i = 0; i < MediumSize; i++)
        {
            list.PushRight(_testData[i]);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            list.PopLeft();
        }
    }

    [BenchmarkCategory("Factory"), Benchmark]
    public void Factory_List_Operations_Unsafe()
    {
        var factory = new UnsafeDataStructureFactory();
        var list = factory.CreateList();
        
        for (int i = 0; i < MediumSize; i++)
        {
            list.PushRight(_testData[i]);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            list.PopLeft();
        }
        
        if (list is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    [BenchmarkCategory("Factory"), Benchmark]
    public void Factory_SortedSet_Operations_Managed()
    {
        var factory = new ManagedDataStructureFactory();
        var zset = factory.CreateSortedSet();
        var random = new Random(42);
        
        for (int i = 0; i < MediumSize; i++)
        {
            zset.Add(_testStrings[i], random.NextDouble() * 100);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            zset.GetScore(_testStrings[i]);
        }
    }

    [BenchmarkCategory("Factory"), Benchmark]
    public void Factory_SortedSet_Operations_Unsafe()
    {
        var factory = new UnsafeDataStructureFactory();
        var zset = factory.CreateSortedSet();
        var random = new Random(42);
        
        for (int i = 0; i < MediumSize; i++)
        {
            zset.Add(_testStrings[i], random.NextDouble() * 100);
        }
        
        for (int i = 0; i < MediumSize; i++)
        {
            zset.GetScore(_testStrings[i]);
        }
        
        if (zset is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    #endregion
}
