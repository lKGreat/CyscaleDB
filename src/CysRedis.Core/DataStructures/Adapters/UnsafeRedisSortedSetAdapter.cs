using System.Text;
using CysRedis.Core.Unsafe.DataStructures;

namespace CysRedis.Core.DataStructures.Adapters;

/// <summary>
/// Adapter that wraps UnsafeRedisSortedSet to implement RedisSortedSet interface.
/// </summary>
public sealed class UnsafeRedisSortedSetAdapter : RedisSortedSet, IDisposable
{
    private readonly UnsafeRedisSortedSet _unsafeSortedSet;
    private readonly Dictionary<string, double> _memberScores = new(StringComparer.Ordinal);

    /// <summary>
    /// Creates a new adapter wrapping an unsafe sorted set.
    /// </summary>
    public UnsafeRedisSortedSetAdapter()
    {
        _unsafeSortedSet = new UnsafeRedisSortedSet();
    }

    /// <summary>
    /// Number of members in the sorted set.
    /// </summary>
    public new int Count => _unsafeSortedSet.Count;

    /// <summary>
    /// Adds a member with a score. Returns true if new member, false if updated.
    /// </summary>
    public new bool Add(string member, double score)
    {
        var memberBytes = Encoding.UTF8.GetBytes(member);
        bool isNew = _unsafeSortedSet.Add(memberBytes, score);
        _memberScores[member] = score;
        return isNew;
    }

    /// <summary>
    /// Removes a member.
    /// </summary>
    public new bool Remove(string member)
    {
        _memberScores.Remove(member);
        var memberBytes = Encoding.UTF8.GetBytes(member);
        // Note: UnsafeRedisSortedSet doesn't expose Remove
        // This would need to be implemented
        return false;
    }

    /// <summary>
    /// Gets the score of a member.
    /// </summary>
    public new double? GetScore(string member)
    {
        return _memberScores.TryGetValue(member, out var score) ? score : null;
    }

    /// <summary>
    /// Increments the score of a member.
    /// </summary>
    public new double IncrBy(string member, double increment)
    {
        double newScore;
        if (_memberScores.TryGetValue(member, out var oldScore))
        {
            newScore = oldScore + increment;
        }
        else
        {
            newScore = increment;
        }
        Add(member, newScore);
        return newScore;
    }

    /// <summary>
    /// Gets the rank of a member (0-based).
    /// </summary>
    public new long? GetRank(string member, bool reverse = false)
    {
        // Note: UnsafeRedisSortedSet doesn't expose GetRank
        // This would need to be implemented
        return null;
    }

    /// <summary>
    /// Gets members in a range by rank.
    /// </summary>
    public new IEnumerable<(string Member, double Score)> GetRange(long start, long stop, bool reverse = false)
    {
        // Note: UnsafeRedisSortedSet doesn't expose GetRange
        // This would need to be implemented
        yield break;
    }

    /// <summary>
    /// Gets members in a score range.
    /// </summary>
    public new IEnumerable<(string Member, double Score)> GetRangeByScore(double min, double max, bool reverse = false)
    {
        // Note: UnsafeRedisSortedSet doesn't expose GetRangeByScore
        // This would need to be implemented
        yield break;
    }

    /// <summary>
    /// Counts members in a score range.
    /// </summary>
    public new int CountByScore(double min, double max)
    {
        return _memberScores.Values.Count(s => s >= min && s <= max);
    }

    /// <summary>
    /// Checks if a member exists.
    /// </summary>
    public new bool Contains(string member)
    {
        return _memberScores.ContainsKey(member);
    }

    /// <summary>
    /// Gets all members.
    /// </summary>
    public new IEnumerable<string> Members => _memberScores.Keys;

    /// <summary>
    /// Disposes the adapter and underlying unsafe sorted set.
    /// </summary>
    public new void Dispose()
    {
        _unsafeSortedSet?.Dispose();
    }
}
