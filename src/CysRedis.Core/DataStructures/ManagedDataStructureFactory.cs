namespace CysRedis.Core.DataStructures;

/// <summary>
/// Factory for creating managed (safe) Redis data structures.
/// </summary>
public class ManagedDataStructureFactory : IDataStructureFactory
{
    /// <summary>
    /// Creates a new managed Redis hash.
    /// </summary>
    public RedisHash CreateHash() => new RedisHash();

    /// <summary>
    /// Creates a new managed Redis set.
    /// </summary>
    public RedisSet CreateSet() => new RedisSet();

    /// <summary>
    /// Creates a new managed Redis list.
    /// </summary>
    public RedisList CreateList() => new RedisList();

    /// <summary>
    /// Creates a new managed Redis sorted set.
    /// </summary>
    public RedisSortedSet CreateSortedSet() => new RedisSortedSet();
}
