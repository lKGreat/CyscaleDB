namespace CysRedis.Core.DataStructures;

/// <summary>
/// Redis object encoding types, matching Redis's encoding names.
/// </summary>
public enum RedisEncoding
{
    /// <summary>Raw byte encoding (for strings).</summary>
    Raw,
    /// <summary>Integer encoding (for integer strings).</summary>
    Int,
    /// <summary>Hashtable encoding.</summary>
    Hashtable,
    /// <summary>LinkedList encoding (legacy).</summary>
    LinkedList,
    /// <summary>SkipList encoding (for sorted sets).</summary>
    SkipList,
    /// <summary>Listpack encoding (compact for small collections).</summary>
    Listpack,
    /// <summary>QuickList encoding (for lists).</summary>
    QuickList,
    /// <summary>IntSet encoding (compact integer set).</summary>
    IntSet
}

/// <summary>
/// Base class for all Redis data types.
/// </summary>
public abstract class RedisObject
{
    /// <summary>
    /// Gets the Redis type name (string, list, set, zset, hash, stream).
    /// </summary>
    public abstract string TypeName { get; }

    /// <summary>
    /// Gets the current encoding of this object.
    /// </summary>
    public virtual RedisEncoding Encoding => RedisEncoding.Raw;
}

/// <summary>
/// Redis string type with zero-copy support.
/// </summary>
public class RedisString : RedisObject
{
    public override string TypeName => "string";
    
    /// <summary>
    /// Returns Int encoding if the string is a valid integer, Raw otherwise.
    /// </summary>
    public override RedisEncoding Encoding
    {
        get
        {
            if (_value.Length <= 20 && TryGetInt64(out _))
                return RedisEncoding.Int;
            return RedisEncoding.Raw;
        }
    }

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
/// Redis list type with dual encoding:
/// - Listpack (List&lt;byte[]&gt;) for small lists: O(1) index access, compact memory
/// - LinkedList for large lists: O(1) push/pop at both ends
/// Auto-converts from Listpack to LinkedList when size exceeds threshold.
/// </summary>
public class RedisList : RedisObject
{
    public override string TypeName => "list";

    /// <summary>
    /// Threshold for converting from listpack (array) to linkedlist encoding.
    /// When element count exceeds this or any element > 64 bytes, converts.
    /// </summary>
    private const int ListpackMaxEntries = 128;
    private const int ListpackMaxElementSize = 64;

    // Dual encoding: one or the other is active
    private List<byte[]>? _arrayList; // Listpack encoding (compact, O(1) index)
    private LinkedList<byte[]>? _linkedList; // LinkedList encoding (O(1) push/pop)
    private bool _useLinkedList;

    public override RedisEncoding Encoding => _useLinkedList ? RedisEncoding.QuickList : RedisEncoding.Listpack;

    public RedisList()
    {
        _arrayList = new List<byte[]>();
        _useLinkedList = false;
    }

    public int Count => _useLinkedList ? _linkedList!.Count : _arrayList!.Count;

    /// <summary>
    /// Checks if we need to convert from listpack to linkedlist encoding.
    /// </summary>
    private void CheckConvert(byte[]? newElement = null)
    {
        if (_useLinkedList) return;

        bool shouldConvert = _arrayList!.Count >= ListpackMaxEntries
            || (newElement != null && newElement.Length > ListpackMaxElementSize);

        if (shouldConvert)
        {
            _linkedList = new LinkedList<byte[]>(_arrayList!);
            _arrayList = null;
            _useLinkedList = true;
        }
    }

    public void PushLeft(byte[] value)
    {
        CheckConvert(value);
        if (_useLinkedList)
            _linkedList!.AddFirst(value);
        else
            _arrayList!.Insert(0, value);
    }

    public void PushRight(byte[] value)
    {
        CheckConvert(value);
        if (_useLinkedList)
            _linkedList!.AddLast(value);
        else
            _arrayList!.Add(value);
    }

    public byte[]? PopLeft()
    {
        if (_useLinkedList)
        {
            if (_linkedList!.Count == 0) return null;
            var value = _linkedList.First!.Value;
            _linkedList.RemoveFirst();
            return value;
        }
        else
        {
            if (_arrayList!.Count == 0) return null;
            var value = _arrayList[0];
            _arrayList.RemoveAt(0);
            return value;
        }
    }

    public byte[]? PopRight()
    {
        if (_useLinkedList)
        {
            if (_linkedList!.Count == 0) return null;
            var value = _linkedList.Last!.Value;
            _linkedList.RemoveLast();
            return value;
        }
        else
        {
            if (_arrayList!.Count == 0) return null;
            var idx = _arrayList.Count - 1;
            var value = _arrayList[idx];
            _arrayList.RemoveAt(idx);
            return value;
        }
    }

    public byte[]? GetByIndex(int index)
    {
        var count = Count;
        if (index < 0) index = count + index;
        if (index < 0 || index >= count) return null;

        if (!_useLinkedList)
        {
            // O(1) index access for listpack encoding
            return _arrayList![index];
        }

        // O(n) for linkedlist
        var node = _linkedList!.First;
        for (int i = 0; i < index && node != null; i++)
            node = node.Next;
        return node?.Value;
    }

    public bool SetByIndex(int index, byte[] value)
    {
        var count = Count;
        if (index < 0) index = count + index;
        if (index < 0 || index >= count) return false;

        if (!_useLinkedList)
        {
            _arrayList![index] = value;
            CheckConvert(value);
            return true;
        }

        var node = _linkedList!.First;
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
        var count = Count;
        if (start < 0) start = Math.Max(0, count + start);
        if (stop < 0) stop = count + stop;
        if (start > stop || start >= count) return new List<byte[]>();
        stop = Math.Min(stop, count - 1);

        if (!_useLinkedList)
        {
            // Fast range access for listpack encoding
            var rangeLen = stop - start + 1;
            return _arrayList!.GetRange(start, rangeLen);
        }

        var result = new List<byte[]>();
        var node = _linkedList!.First;
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
        var count = Count;
        if (start < 0) start = Math.Max(0, count + start);
        if (stop < 0) stop = count + stop;

        if (start > stop || start >= count)
        {
            if (_useLinkedList) _linkedList!.Clear();
            else _arrayList!.Clear();
            return;
        }

        stop = Math.Min(stop, count - 1);

        if (!_useLinkedList)
        {
            // Efficient trim for arraylist
            var newList = _arrayList!.GetRange(start, stop - start + 1);
            _arrayList.Clear();
            _arrayList.AddRange(newList);
            return;
        }

        // LinkedList trim
        while (_linkedList!.Count > stop + 1)
            _linkedList.RemoveLast();
        for (int i = 0; i < start && _linkedList.Count > 0; i++)
            _linkedList.RemoveFirst();
    }
}

/// <summary>
/// Redis set type with dual encoding:
/// - IntSet: compact sorted array for small integer-only sets (up to 512 elements)
/// - Hashtable: standard HashSet for general use
/// Auto-converts from IntSet to Hashtable when a non-integer member is added or size exceeds threshold.
/// </summary>
public class RedisSet : RedisObject
{
    public override string TypeName => "set";

    private const int IntSetMaxEntries = 512;

    private HashSet<string>? _set;
    private SortedSet<long>? _intSet; // IntSet encoding: compact integer set
    private bool _useIntSet;

    public override RedisEncoding Encoding => _useIntSet ? RedisEncoding.IntSet : RedisEncoding.Hashtable;

    public RedisSet()
    {
        _intSet = new SortedSet<long>();
        _useIntSet = true;
    }

    public int Count => _useIntSet ? _intSet!.Count : _set!.Count;

    /// <summary>
    /// Converts from IntSet to Hashtable encoding.
    /// </summary>
    private void ConvertToHashtable()
    {
        if (!_useIntSet) return;
        _set = new HashSet<string>(StringComparer.Ordinal);
        foreach (var val in _intSet!)
            _set.Add(val.ToString());
        _intSet = null;
        _useIntSet = false;
    }

    public bool Add(string member)
    {
        if (_useIntSet)
        {
            if (long.TryParse(member, out var intVal) && _intSet!.Count < IntSetMaxEntries)
            {
                return _intSet.Add(intVal);
            }
            // Non-integer or too large: convert to hashtable
            ConvertToHashtable();
        }
        return _set!.Add(member);
    }

    public bool Remove(string member)
    {
        if (_useIntSet)
        {
            if (long.TryParse(member, out var intVal))
                return _intSet!.Remove(intVal);
            return false; // Not in intset if not integer
        }
        return _set!.Remove(member);
    }

    public bool Contains(string member)
    {
        if (_useIntSet)
        {
            return long.TryParse(member, out var intVal) && _intSet!.Contains(intVal);
        }
        return _set!.Contains(member);
    }

    public IEnumerable<string> Members => _useIntSet
        ? _intSet!.Select(v => v.ToString())
        : _set!;

    public string? Pop()
    {
        if (_useIntSet)
        {
            if (_intSet!.Count == 0) return null;
            var val = _intSet.Min;
            _intSet.Remove(val);
            return val.ToString();
        }
        if (_set!.Count == 0) return null;
        var member = _set.First();
        _set.Remove(member);
        return member;
    }

    public string? RandomMember()
    {
        if (_useIntSet)
        {
            if (_intSet!.Count == 0) return null;
            return _intSet.ElementAt(Random.Shared.Next(_intSet.Count)).ToString();
        }
        if (_set!.Count == 0) return null;
        return _set.ElementAt(Random.Shared.Next(_set.Count));
    }

    /// <summary>
    /// Gets the underlying HashSet (converting if needed). Used for set operations.
    /// </summary>
    private HashSet<string> GetHashSet()
    {
        if (_useIntSet)
        {
            return new HashSet<string>(_intSet!.Select(v => v.ToString()), StringComparer.Ordinal);
        }
        return _set!;
    }

    public HashSet<string> Union(RedisSet other)
    {
        var result = new HashSet<string>(GetHashSet(), StringComparer.Ordinal);
        result.UnionWith(other.GetHashSet());
        return result;
    }

    public HashSet<string> Intersect(RedisSet other)
    {
        var result = new HashSet<string>(GetHashSet(), StringComparer.Ordinal);
        result.IntersectWith(other.GetHashSet());
        return result;
    }

    public HashSet<string> Difference(RedisSet other)
    {
        var result = new HashSet<string>(GetHashSet(), StringComparer.Ordinal);
        result.ExceptWith(other.GetHashSet());
        return result;
    }
}

/// <summary>
/// Redis sorted set type.
/// </summary>
public class RedisSortedSet : RedisObject
{
    public override string TypeName => "zset";
    public override RedisEncoding Encoding => RedisEncoding.SkipList;

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
    public override RedisEncoding Encoding => RedisEncoding.Hashtable;

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
