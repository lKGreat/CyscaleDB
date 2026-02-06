using System.Collections.Concurrent;
using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;

namespace CysRedis.Core.Memory;

/// <summary>
/// 内存淘汰管理器 - 实现LRU/LFU策略
/// </summary>
public class EvictionManager
{
    private readonly EvictionPolicy _policy;
    private readonly long _maxMemory;
    private readonly ConcurrentDictionary<string, EvictionMetadata> _metadata;
    private long _approximateMemoryUsage;
    private readonly object _evictionLock = new();

    public EvictionManager(EvictionPolicy policy = EvictionPolicy.NoEviction, long maxMemory = 0)
    {
        _policy = policy;
        _maxMemory = maxMemory;
        _metadata = new ConcurrentDictionary<string, EvictionMetadata>(StringComparer.Ordinal);
        _approximateMemoryUsage = 0;
    }

    /// <summary>
    /// 当前内存使用量（近似值）
    /// </summary>
    public long ApproximateMemoryUsage => Interlocked.Read(ref _approximateMemoryUsage);

    /// <summary>
    /// 最大内存限制
    /// </summary>
    public long MaxMemory => _maxMemory;

    /// <summary>
    /// 淘汰策略
    /// </summary>
    public EvictionPolicy Policy => _policy;

    /// <summary>
    /// 记录键访问
    /// </summary>
    public void OnKeyAccess(string key)
    {
        if (_policy == EvictionPolicy.NoEviction)
            return;

        var metadata = _metadata.GetOrAdd(key, _ => new EvictionMetadata());
        metadata.UpdateAccess(_policy);
    }

    /// <summary>
    /// 记录键写入
    /// </summary>
    public void OnKeySet(string key, long estimatedSize)
    {
        Interlocked.Add(ref _approximateMemoryUsage, estimatedSize);
        
        var metadata = _metadata.GetOrAdd(key, _ => new EvictionMetadata());
        metadata.EstimatedSize = estimatedSize;
        metadata.UpdateAccess(_policy);
    }

    /// <summary>
    /// 记录键删除
    /// </summary>
    public void OnKeyDelete(string key)
    {
        if (_metadata.TryRemove(key, out var metadata))
        {
            Interlocked.Add(ref _approximateMemoryUsage, -metadata.EstimatedSize);
        }
    }

    /// <summary>
    /// 检查是否需要淘汰
    /// </summary>
    public bool NeedsEviction()
    {
        if (_maxMemory == 0 || _policy == EvictionPolicy.NoEviction)
            return false;

        return ApproximateMemoryUsage > _maxMemory;
    }

    /// <summary>
    /// 执行内存淘汰
    /// </summary>
    public List<string> Evict(RedisDatabase database, int maxEvictions = 100)
    {
        if (!NeedsEviction())
            return new List<string>();

        lock (_evictionLock)
        {
            var evicted = new List<string>();
            var targetMemory = (long)(_maxMemory * 0.9); // 淘汰到90%

            // 根据策略选择要淘汰的键
            var candidates = SelectEvictionCandidates(database);

            foreach (var key in candidates)
            {
                if (evicted.Count >= maxEvictions || ApproximateMemoryUsage <= targetMemory)
                    break;

                if (database.Delete(key))
                {
                    OnKeyDelete(key);
                    evicted.Add(key);
                    Logger.Debug("Evicted key: {0}", key);
                }
            }

            Logger.Info("Evicted {0} keys, memory usage: {1} / {2}", 
                evicted.Count, ApproximateMemoryUsage, _maxMemory);

            return evicted;
        }
    }

    /// <summary>
    /// Number of random keys to sample per eviction cycle (Redis default: 5).
    /// </summary>
    private const int EvictionSampleSize = 5;

    /// <summary>
    /// 选择淘汰候选键 - 使用Redis风格的随机采样法。
    /// 随机取N个key，从中选出最佳淘汰候选，避免全量遍历。
    /// </summary>
    private List<string> SelectEvictionCandidates(RedisDatabase database)
    {
        bool volatileOnly = _policy is EvictionPolicy.VolatileLru or EvictionPolicy.VolatileLfu
            or EvictionPolicy.VolatileRandom or EvictionPolicy.VolatileTtl;

        // Sample random keys from database
        var samples = database.GetRandomKeys(EvictionSampleSize * 4); // Over-sample, then filter

        if (volatileOnly)
        {
            samples = samples.Where(k => database.GetExpire(k).HasValue).ToList();
        }

        if (samples.Count == 0)
            return new List<string>();

        return _policy switch
        {
            EvictionPolicy.AllKeysLru or EvictionPolicy.VolatileLru => SelectBestByLru(samples),
            EvictionPolicy.AllKeysLfu or EvictionPolicy.VolatileLfu => SelectBestByLfu(samples),
            EvictionPolicy.AllKeysRandom or EvictionPolicy.VolatileRandom => samples.Take(EvictionSampleSize).ToList(),
            EvictionPolicy.VolatileTtl => SelectBestByTtl(samples, database),
            _ => new List<string>()
        };
    }

    /// <summary>
    /// LRU选择：从采样中选择空闲时间最长的键
    /// </summary>
    private List<string> SelectBestByLru(List<string> samples)
    {
        return samples
            .Where(k => _metadata.ContainsKey(k))
            .OrderByDescending(k => _metadata[k].IdleTimeSeconds)
            .Take(EvictionSampleSize)
            .ToList();
    }

    /// <summary>
    /// LFU选择：从采样中选择访问频率最低的键
    /// </summary>
    private List<string> SelectBestByLfu(List<string> samples)
    {
        return samples
            .Where(k => _metadata.ContainsKey(k))
            .OrderBy(k => _metadata[k].AccessFrequency)
            .ThenByDescending(k => _metadata[k].IdleTimeSeconds)
            .Take(EvictionSampleSize)
            .ToList();
    }

    /// <summary>
    /// TTL选择：从采样中选择最快过期的键
    /// </summary>
    private List<string> SelectBestByTtl(List<string> samples, RedisDatabase database)
    {
        return samples
            .Select(k => new { Key = k, Expire = database.GetExpire(k) })
            .Where(x => x.Expire.HasValue)
            .OrderBy(x => x.Expire!.Value)
            .Take(EvictionSampleSize)
            .Select(x => x.Key)
            .ToList();
    }

    /// <summary>
    /// 估算对象大小
    /// </summary>
    public static long EstimateSize(RedisObject obj)
    {
        return obj switch
        {
            RedisString str => 24 + str.Length,
            RedisList list => 24 + list.Count * 16,
            RedisSet set => 24 + set.Count * 16,
            RedisSortedSet zset => 24 + zset.Count * 32,
            RedisHash hash => 24 + hash.Count * 24,
            _ => 24
        };
    }
}

/// <summary>
/// 淘汰策略
/// </summary>
public enum EvictionPolicy
{
    /// <summary>不淘汰，达到maxmemory时返回错误</summary>
    NoEviction,
    
    /// <summary>所有键的LRU淘汰</summary>
    AllKeysLru,
    
    /// <summary>只淘汰设置了过期时间的键(LRU)</summary>
    VolatileLru,
    
    /// <summary>所有键的LFU淘汰</summary>
    AllKeysLfu,
    
    /// <summary>只淘汰设置了过期时间的键(LFU)</summary>
    VolatileLfu,
    
    /// <summary>随机淘汰所有键</summary>
    AllKeysRandom,
    
    /// <summary>随机淘汰设置了过期时间的键</summary>
    VolatileRandom,
    
    /// <summary>淘汰TTL最短的键</summary>
    VolatileTtl
}

/// <summary>
/// 淘汰元数据
/// </summary>
internal class EvictionMetadata
{
    public uint Lru { get; private set; } // 24-bit LRU clock value
    public long AccessCount { get; private set; }
    public byte LfuCounter { get; private set; }
    public long EstimatedSize { get; set; }

    public EvictionMetadata()
    {
        Lru = LruClock.GetClock();
        AccessCount = 0;
        LfuCounter = 5; // 初始频率计数器
        EstimatedSize = 0;
    }

    /// <summary>
    /// 更新访问记录
    /// </summary>
    public void UpdateAccess(EvictionPolicy policy)
    {
        Lru = LruClock.GetClock();
        AccessCount++;

        // LFU计数器更新（使用Morris计数器算法）
        if (policy == EvictionPolicy.AllKeysLfu || policy == EvictionPolicy.VolatileLfu)
        {
            var probability = 1.0 / (LfuCounter * 10 + 1);
            if (Random.Shared.NextDouble() < probability)
            {
                if (LfuCounter < 255)
                    LfuCounter++;
            }
        }
    }

    /// <summary>
    /// 获取空闲时间（秒）
    /// </summary>
    public long IdleTimeSeconds => LruClock.EstimateIdleTime(Lru);

    /// <summary>
    /// 访问频率（对数化的访问次数）
    /// </summary>
    public double AccessFrequency
    {
        get
        {
            // 考虑时间衰减
            var ageMinutes = IdleTimeSeconds / 60.0;
            var decay = Math.Exp(-ageMinutes / 60.0); // 1小时半衰期
            return LfuCounter * decay;
        }
    }
}
