namespace CysRedis.Core.DataStructures;

/// <summary>
/// Base class for all Redis data types.
/// </summary>
public abstract class RedisObject
{
    /// <summary>
    /// Gets the Redis type name (string, list, set, zset, hash, stream).
    /// </summary>
    public abstract string TypeName { get; }
}

/// <summary>
/// Redis string type with zero-copy support.
/// </summary>
public class RedisString : RedisObject
{
    public override string TypeName => "string";

    private Memory<byte> _value;

    /// <summary>
    /// The raw bytes value (for backward compatibility).
    /// </summary>
    public byte[] Value => _value.ToArray();
    
    /// <summary>
    /// Gets the value as a Memory<byte> for zero-copy operations.
    /// </summary>
    public Memory<byte> ValueMemory => _value;
    
    /// <summary>
    /// Gets the value as a ReadOnlySpan<byte> for zero-copy operations.
    /// </summary>
    public ReadOnlySpan<byte> ValueSpan => _value.Span;

    public RedisString(byte[] value)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        _value = value;
    }
    
    public RedisString(Memory<byte> value)
    {
        _value = value;
    }

    public RedisString(string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value ?? string.Empty);
        _value = bytes;
    }

    /// <summary>
    /// Gets the string value.
    /// </summary>
    public string GetString() => System.Text.Encoding.UTF8.GetString(_value.Span);

    /// <summary>
    /// Gets the value as an integer.
    /// </summary>
    public bool TryGetInt64(out long result)
    {
        return long.TryParse(GetString(), out result);
    }

    /// <summary>
    /// Gets the value as a double.
    /// </summary>
    public bool TryGetDouble(out double result)
    {
        return double.TryParse(GetString(), System.Globalization.NumberStyles.Float, 
            System.Globalization.CultureInfo.InvariantCulture, out result);
    }

    /// <summary>
    /// Sets the value from an integer.
    /// </summary>
    public void SetInt64(long value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value.ToString());
        _value = bytes;
    }

    /// <summary>
    /// Sets the value from a double.
    /// </summary>
    public void SetDouble(double value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(
            value.ToString("G17", System.Globalization.CultureInfo.InvariantCulture));
        _value = bytes;
    }

    /// <summary>
    /// Gets the length in bytes.
    /// </summary>
    public int Length => _value.Length;
}

/// <summary>
/// Redis list type.
/// </summary>
public class RedisList : RedisObject
{
    public override string TypeName => "list";

    private readonly LinkedList<byte[]> _list = new();

    public int Count => _list.Count;

    public void PushLeft(byte[] value) => _list.AddFirst(value);
    public void PushRight(byte[] value) => _list.AddLast(value);

    public byte[]? PopLeft()
    {
        if (_list.Count == 0) return null;
        var value = _list.First!.Value;
        _list.RemoveFirst();
        return value;
    }

    public byte[]? PopRight()
    {
        if (_list.Count == 0) return null;
        var value = _list.Last!.Value;
        _list.RemoveLast();
        return value;
    }

    public byte[]? GetByIndex(int index)
    {
        if (index < 0) index = _list.Count + index;
        if (index < 0 || index >= _list.Count) return null;

        var node = _list.First;
        for (int i = 0; i < index && node != null; i++)
            node = node.Next;

        return node?.Value;
    }

    public bool SetByIndex(int index, byte[] value)
    {
        if (index < 0) index = _list.Count + index;
        if (index < 0 || index >= _list.Count) return false;

        var node = _list.First;
        for (int i = 0; i < index && node != null; i++)
            node = node.Next;

        if (node != null)
        {
            node.Value = value;
            return true;
        }
        return false;
    }

    public List<byte[]> GetRange(int start, int stop)
    {
        if (start < 0) start = Math.Max(0, _list.Count + start);
        if (stop < 0) stop = _list.Count + stop;
        if (start > stop || start >= _list.Count) return new List<byte[]>();
        
        stop = Math.Min(stop, _list.Count - 1);

        var result = new List<byte[]>();
        var node = _list.First;
        for (int i = 0; node != null && i <= stop; i++)
        {
            if (i >= start)
                result.Add(node.Value);
            node = node.Next;
        }
        return result;
    }

    public void Trim(int start, int stop)
    {
        if (start < 0) start = Math.Max(0, _list.Count + start);
        if (stop < 0) stop = _list.Count + stop;

        if (start > stop || start >= _list.Count)
        {
            _list.Clear();
            return;
        }

        stop = Math.Min(stop, _list.Count - 1);

        // Remove from end
        while (_list.Count > stop + 1)
            _list.RemoveLast();

        // Remove from start
        for (int i = 0; i < start && _list.Count > 0; i++)
            _list.RemoveFirst();
    }
}

/// <summary>
/// Redis set type.
/// </summary>
public class RedisSet : RedisObject
{
    public override string TypeName => "set";

    private readonly HashSet<string> _set = new(StringComparer.Ordinal);

    public int Count => _set.Count;

    public bool Add(string member) => _set.Add(member);
    public bool Remove(string member) => _set.Remove(member);
    public bool Contains(string member) => _set.Contains(member);
    public IEnumerable<string> Members => _set;

    public string? Pop()
    {
        if (_set.Count == 0) return null;
        var member = _set.First();
        _set.Remove(member);
        return member;
    }

    public string? RandomMember()
    {
        if (_set.Count == 0) return null;
        return _set.ElementAt(Random.Shared.Next(_set.Count));
    }

    public HashSet<string> Union(RedisSet other)
    {
        var result = new HashSet<string>(_set, StringComparer.Ordinal);
        result.UnionWith(other._set);
        return result;
    }

    public HashSet<string> Intersect(RedisSet other)
    {
        var result = new HashSet<string>(_set, StringComparer.Ordinal);
        result.IntersectWith(other._set);
        return result;
    }

    public HashSet<string> Difference(RedisSet other)
    {
        var result = new HashSet<string>(_set, StringComparer.Ordinal);
        result.ExceptWith(other._set);
        return result;
    }
}

/// <summary>
/// Redis sorted set type.
/// </summary>
public class RedisSortedSet : RedisObject
{
    public override string TypeName => "zset";

    // Dictionary for O(1) member->score lookup
    private readonly Dictionary<string, double> _memberScores = new(StringComparer.Ordinal);
    // SkipList for sorted traversal
    private readonly SkipList<(double Score, string Member), string> _skipList;

    public RedisSortedSet()
    {
        _skipList = new SkipList<(double Score, string Member), string>(
            Comparer<(double Score, string Member)>.Create((a, b) =>
            {
                int cmp = a.Score.CompareTo(b.Score);
                if (cmp != 0) return cmp;
                return string.CompareOrdinal(a.Member, b.Member);
            }));
    }

    public int Count => _memberScores.Count;

    /// <summary>
    /// Adds a member with a score. Returns true if new member, false if updated.
    /// </summary>
    public bool Add(string member, double score)
    {
        if (_memberScores.TryGetValue(member, out var oldScore))
        {
            // Update existing
            if (Math.Abs(oldScore - score) > double.Epsilon)
            {
                _skipList.Remove((oldScore, member));
                _skipList.Insert((score, member), member);
                _memberScores[member] = score;
            }
            return false;
        }

        // New member
        _memberScores[member] = score;
        _skipList.Insert((score, member), member);
        return true;
    }

    /// <summary>
    /// Removes a member.
    /// </summary>
    public bool Remove(string member)
    {
        if (!_memberScores.TryGetValue(member, out var score))
            return false;

        _memberScores.Remove(member);
        _skipList.Remove((score, member));
        return true;
    }

    /// <summary>
    /// Gets the score of a member.
    /// </summary>
    public double? GetScore(string member)
    {
        return _memberScores.TryGetValue(member, out var score) ? score : null;
    }

    /// <summary>
    /// Increments the score of a member.
    /// </summary>
    public double IncrBy(string member, double increment)
    {
        double newScore;
        if (_memberScores.TryGetValue(member, out var oldScore))
        {
            newScore = oldScore + increment;
            _skipList.Remove((oldScore, member));
        }
        else
        {
            newScore = increment;
        }

        _memberScores[member] = newScore;
        _skipList.Insert((newScore, member), member);
        return newScore;
    }

    /// <summary>
    /// Gets the rank of a member (0-based).
    /// </summary>
    public long? GetRank(string member, bool reverse = false)
    {
        if (!_memberScores.TryGetValue(member, out var score))
            return null;

        var rank = _skipList.GetRank((score, member));
        if (rank < 0) return null;

        return reverse ? _memberScores.Count - 1 - rank : rank;
    }

    /// <summary>
    /// Gets members in a range by rank.
    /// </summary>
    public IEnumerable<(string Member, double Score)> GetRange(long start, long stop, bool reverse = false)
    {
        foreach (var item in _skipList.GetRange(start, stop, reverse))
        {
            yield return (item.Key.Member, item.Key.Score);
        }
    }

    /// <summary>
    /// Gets members in a score range.
    /// </summary>
    public IEnumerable<(string Member, double Score)> GetRangeByScore(double min, double max, bool reverse = false)
    {
        foreach (var item in _skipList.GetAll(reverse))
        {
            if (item.Key.Score >= min && item.Key.Score <= max)
                yield return (item.Key.Member, item.Key.Score);
            else if (!reverse && item.Key.Score > max)
                break;
            else if (reverse && item.Key.Score < min)
                break;
        }
    }

    /// <summary>
    /// Counts members in a score range.
    /// </summary>
    public int CountByScore(double min, double max)
    {
        int count = 0;
        foreach (var item in _skipList.GetAll())
        {
            if (item.Key.Score >= min && item.Key.Score <= max)
                count++;
            else if (item.Key.Score > max)
                break;
        }
        return count;
    }

    /// <summary>
    /// Checks if a member exists.
    /// </summary>
    public bool Contains(string member) => _memberScores.ContainsKey(member);

    /// <summary>
    /// Gets all members.
    /// </summary>
    public IEnumerable<string> Members => _memberScores.Keys;
}

/// <summary>
/// Redis hash type with field-level expiration support (Redis 8.0+).
/// </summary>
public class RedisHash : RedisObject
{
    public override string TypeName => "hash";

    private readonly Dictionary<string, byte[]> _hash = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTime> _fieldExpires = new(StringComparer.Ordinal);

    public int Count => _hash.Count;

    public void Set(string field, byte[] value)
    {
        _hash[field] = value;
    }
    
    public bool SetNx(string field, byte[] value)
    {
        if (_hash.ContainsKey(field)) return false;
        _hash[field] = value;
        return true;
    }

    public byte[]? Get(string field)
    {
        // Check if field has expired
        if (IsFieldExpired(field))
        {
            Delete(field);
            return null;
        }
        
        _hash.TryGetValue(field, out var value);
        return value;
    }

    public bool Delete(string field)
    {
        _fieldExpires.Remove(field);
        return _hash.Remove(field);
    }
    
    public bool Exists(string field)
    {
        if (IsFieldExpired(field))
        {
            Delete(field);
            return false;
        }
        return _hash.ContainsKey(field);
    }

    public IEnumerable<string> Keys => _hash.Keys.Where(k => !IsFieldExpired(k));
    public IEnumerable<byte[]> Values => _hash.Where(kvp => !IsFieldExpired(kvp.Key)).Select(kvp => kvp.Value);
    public IEnumerable<KeyValuePair<string, byte[]>> Entries => _hash.Where(kvp => !IsFieldExpired(kvp.Key));

    public long IncrBy(string field, long increment)
    {
        if (!_hash.TryGetValue(field, out var existing))
        {
            _hash[field] = System.Text.Encoding.UTF8.GetBytes(increment.ToString());
            return increment;
        }

        var str = System.Text.Encoding.UTF8.GetString(existing);
        if (!long.TryParse(str, out var value))
            throw new Common.NotIntegerException();

        value += increment;
        _hash[field] = System.Text.Encoding.UTF8.GetBytes(value.ToString());
        return value;
    }

    public double IncrByFloat(string field, double increment)
    {
        if (!_hash.TryGetValue(field, out var existing))
        {
            _hash[field] = System.Text.Encoding.UTF8.GetBytes(
                increment.ToString("G17", System.Globalization.CultureInfo.InvariantCulture));
            return increment;
        }

        var str = System.Text.Encoding.UTF8.GetString(existing);
        if (!double.TryParse(str, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var value))
            throw new Common.NotFloatException();

        value += increment;
        _hash[field] = System.Text.Encoding.UTF8.GetBytes(
            value.ToString("G17", System.Globalization.CultureInfo.InvariantCulture));
        return value;
    }
    
    /// <summary>
    /// Sets field expiration time.
    /// </summary>
    public bool SetFieldExpire(string field, DateTime expireAt)
    {
        if (!_hash.ContainsKey(field))
            return false;
        _fieldExpires[field] = expireAt;
        return true;
    }
    
    /// <summary>
    /// Gets field expiration time.
    /// </summary>
    public DateTime? GetFieldExpire(string field)
    {
        return _fieldExpires.TryGetValue(field, out var expiry) ? expiry : null;
    }
    
    /// <summary>
    /// Removes field expiration.
    /// </summary>
    public bool PersistField(string field)
    {
        return _fieldExpires.Remove(field);
    }
    
    /// <summary>
    /// Gets field TTL in seconds.
    /// </summary>
    public long? GetFieldTtl(string field)
    {
        if (!_hash.ContainsKey(field))
            return -2; // Field doesn't exist
        
        if (!_fieldExpires.TryGetValue(field, out var expiry))
            return -1; // No expiration
        
        var remaining = (expiry - DateTime.UtcNow).TotalSeconds;
        return remaining > 0 ? (long)remaining : -2;
    }
    
    /// <summary>
    /// Gets field TTL in milliseconds.
    /// </summary>
    public long? GetFieldPttl(string field)
    {
        if (!_hash.ContainsKey(field))
            return -2;
        
        if (!_fieldExpires.TryGetValue(field, out var expiry))
            return -1;
        
        var remaining = (expiry - DateTime.UtcNow).TotalMilliseconds;
        return remaining > 0 ? (long)remaining : -2;
    }
    
    /// <summary>
    /// Checks if a field has expired.
    /// </summary>
    private bool IsFieldExpired(string field)
    {
        if (_fieldExpires.TryGetValue(field, out var expiry))
            return DateTime.UtcNow >= expiry;
        return false;
    }
    
    /// <summary>
    /// Cleans up expired fields.
    /// </summary>
    public int CleanupExpiredFields()
    {
        int count = 0;
        var now = DateTime.UtcNow;
        var expiredFields = _fieldExpires.Where(kvp => now >= kvp.Value).Select(kvp => kvp.Key).ToList();
        
        foreach (var field in expiredFields)
        {
            if (Delete(field))
                count++;
        }
        
        return count;
    }
}
