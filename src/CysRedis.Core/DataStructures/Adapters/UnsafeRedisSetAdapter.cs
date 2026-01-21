using System.Text;
using CysRedis.Core.Unsafe.DataStructures;

namespace CysRedis.Core.DataStructures.Adapters;

/// <summary>
/// Adapter that wraps UnsafeRedisSet to implement RedisSet interface.
/// </summary>
public sealed class UnsafeRedisSetAdapter : RedisSet, IDisposable
{
    private readonly UnsafeRedisSet _unsafeSet;
    private readonly HashSet<string> _memberCache = new(StringComparer.Ordinal);

    /// <summary>
    /// Creates a new adapter wrapping an unsafe set.
    /// </summary>
    public UnsafeRedisSetAdapter()
    {
        _unsafeSet = new UnsafeRedisSet();
    }

    /// <summary>
    /// Number of members in the set.
    /// </summary>
    public new int Count => _unsafeSet.Count;

    /// <summary>
    /// Adds a member to the set.
    /// </summary>
    public new bool Add(string member)
    {
        var memberBytes = Encoding.UTF8.GetBytes(member);
        if (_unsafeSet.Add(memberBytes))
        {
            _memberCache.Add(member);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Removes a member from the set.
    /// </summary>
    public new bool Remove(string member)
    {
        _memberCache.Remove(member);
        var memberBytes = Encoding.UTF8.GetBytes(member);
        return _unsafeSet.Remove(memberBytes);
    }

    /// <summary>
    /// Checks if a member exists in the set.
    /// </summary>
    public new bool Contains(string member)
    {
        var memberBytes = Encoding.UTF8.GetBytes(member);
        return _unsafeSet.Contains(memberBytes);
    }

    /// <summary>
    /// Gets all members.
    /// </summary>
    public new IEnumerable<string> Members => _memberCache;

    /// <summary>
    /// Pops a random member from the set.
    /// </summary>
    public new string? Pop()
    {
        // Note: UnsafeRedisSet doesn't expose Pop, so we need to iterate
        // For now, use cache if available
        if (_memberCache.Count == 0)
            return null;
        
        var member = _memberCache.First();
        Remove(member);
        return member;
    }

    /// <summary>
    /// Gets a random member without removing it.
    /// </summary>
    public new string? RandomMember()
    {
        if (_memberCache.Count == 0)
            return null;
        return _memberCache.ElementAt(Random.Shared.Next(_memberCache.Count));
    }

    /// <summary>
    /// Computes the union of this set with another set.
    /// </summary>
    public new HashSet<string> Union(RedisSet other)
    {
        var result = new HashSet<string>(_memberCache, StringComparer.Ordinal);
        result.UnionWith(other.Members);
        return result;
    }

    /// <summary>
    /// Computes the intersection of this set with another set.
    /// </summary>
    public new HashSet<string> Intersect(RedisSet other)
    {
        var result = new HashSet<string>(_memberCache, StringComparer.Ordinal);
        result.IntersectWith(other.Members);
        return result;
    }

    /// <summary>
    /// Computes the difference of this set with another set.
    /// </summary>
    public new HashSet<string> Difference(RedisSet other)
    {
        var result = new HashSet<string>(_memberCache, StringComparer.Ordinal);
        result.ExceptWith(other.Members);
        return result;
    }

    /// <summary>
    /// Disposes the adapter and underlying unsafe set.
    /// </summary>
    public new void Dispose()
    {
        _unsafeSet?.Dispose();
    }
}
