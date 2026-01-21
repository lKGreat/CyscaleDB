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
/// Redis string type.
/// </summary>
public class RedisString : RedisObject
{
    public override string TypeName => "string";

    /// <summary>
    /// The raw bytes value.
    /// </summary>
    public byte[] Value { get; set; }

    public RedisString(byte[] value)
    {
        Value = value ?? throw new ArgumentNullException(nameof(value));
    }

    public RedisString(string value)
    {
        Value = System.Text.Encoding.UTF8.GetBytes(value ?? string.Empty);
    }

    /// <summary>
    /// Gets the string value.
    /// </summary>
    public string GetString() => System.Text.Encoding.UTF8.GetString(Value);

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
        Value = System.Text.Encoding.UTF8.GetBytes(value.ToString());
    }

    /// <summary>
    /// Sets the value from a double.
    /// </summary>
    public void SetDouble(double value)
    {
        Value = System.Text.Encoding.UTF8.GetBytes(
            value.ToString("G17", System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Gets the length in bytes.
    /// </summary>
    public int Length => Value.Length;
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
/// Redis hash type.
/// </summary>
public class RedisHash : RedisObject
{
    public override string TypeName => "hash";

    private readonly Dictionary<string, byte[]> _hash = new(StringComparer.Ordinal);

    public int Count => _hash.Count;

    public void Set(string field, byte[] value) => _hash[field] = value;
    
    public bool SetNx(string field, byte[] value)
    {
        if (_hash.ContainsKey(field)) return false;
        _hash[field] = value;
        return true;
    }

    public byte[]? Get(string field)
    {
        _hash.TryGetValue(field, out var value);
        return value;
    }

    public bool Delete(string field) => _hash.Remove(field);
    public bool Exists(string field) => _hash.ContainsKey(field);

    public IEnumerable<string> Keys => _hash.Keys;
    public IEnumerable<byte[]> Values => _hash.Values;
    public IEnumerable<KeyValuePair<string, byte[]>> Entries => _hash;

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
}
