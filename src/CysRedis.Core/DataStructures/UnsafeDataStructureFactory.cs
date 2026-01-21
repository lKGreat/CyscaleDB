using CysRedis.Core.DataStructures.Adapters;

namespace CysRedis.Core.DataStructures;

/// <summary>
/// Factory for creating unsafe (high-performance) Redis data structures.
/// </summary>
public class UnsafeDataStructureFactory : IDataStructureFactory
{
    /// <summary>
    /// Creates a new unsafe Redis hash wrapped in an adapter.
    /// </summary>
    public RedisHash CreateHash() => new UnsafeRedisHashAdapter();

    /// <summary>
    /// Creates a new unsafe Redis set wrapped in an adapter.
    /// </summary>
    public RedisSet CreateSet() => new UnsafeRedisSetAdapter();

    /// <summary>
    /// Creates a new unsafe Redis list wrapped in an adapter.
    /// </summary>
    public RedisList CreateList() => new UnsafeRedisListAdapter();

    /// <summary>
    /// Creates a new unsafe Redis sorted set wrapped in an adapter.
    /// </summary>
    public RedisSortedSet CreateSortedSet() => new UnsafeRedisSortedSetAdapter();
}
