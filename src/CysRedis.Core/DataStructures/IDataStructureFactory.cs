namespace CysRedis.Core.DataStructures;

/// <summary>
/// Factory interface for creating Redis data structures.
/// Allows switching between managed and unsafe implementations.
/// </summary>
public interface IDataStructureFactory
{
    /// <summary>
    /// Creates a new Redis hash.
    /// </summary>
    RedisHash CreateHash();

    /// <summary>
    /// Creates a new Redis set.
    /// </summary>
    RedisSet CreateSet();

    /// <summary>
    /// Creates a new Redis list.
    /// </summary>
    RedisList CreateList();

    /// <summary>
    /// Creates a new Redis sorted set.
    /// </summary>
    RedisSortedSet CreateSortedSet();
}
