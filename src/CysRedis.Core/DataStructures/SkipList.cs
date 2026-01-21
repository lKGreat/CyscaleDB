namespace CysRedis.Core.DataStructures;

/// <summary>
/// Skip list implementation for sorted sets.
/// Based on Redis skiplist implementation.
/// </summary>
public class SkipList<TKey, TValue> where TKey : notnull
{
    private const int MaxLevel = 32;
    private const double Probability = 0.25;
    
    private readonly IComparer<TKey> _comparer;
    private readonly Random _random = new();
    private SkipListNode<TKey, TValue>? _head;
    private SkipListNode<TKey, TValue>? _tail;
    private int _level;
    private int _count;

    /// <summary>
    /// Number of elements in the skip list.
    /// </summary>
    public int Count => _count;

    /// <summary>
    /// Creates a new skip list.
    /// </summary>
    public SkipList(IComparer<TKey>? comparer = null)
    {
        _comparer = comparer ?? Comparer<TKey>.Default;
        _head = CreateNode(MaxLevel, default!, default!);
        _level = 1;
        _count = 0;
    }

    /// <summary>
    /// Inserts or updates an element.
    /// </summary>
    public bool Insert(TKey key, TValue value)
    {
        var update = new SkipListNode<TKey, TValue>?[MaxLevel];
        var rank = new int[MaxLevel];
        var current = _head!;

        // Find the position to insert
        for (int i = _level - 1; i >= 0; i--)
        {
            rank[i] = i == _level - 1 ? 0 : rank[i + 1];
            
            while (current.Levels[i].Forward != null &&
                   _comparer.Compare(current.Levels[i].Forward.Key, key) < 0)
            {
                rank[i] += current.Levels[i].Span;
                current = current.Levels[i].Forward;
            }
            update[i] = current;
        }

        // Check if key already exists
        current = current.Levels[0].Forward!;
        if (current != null && _comparer.Compare(current.Key, key) == 0)
        {
            // Update existing
            current.Value = value;
            return false;
        }

        // Generate random level
        int level = RandomLevel();
        if (level > _level)
        {
            for (int i = _level; i < level; i++)
            {
                rank[i] = 0;
                update[i] = _head;
                update[i]!.Levels[i].Span = _count;
            }
            _level = level;
        }

        // Create new node
        var newNode = CreateNode(level, key, value);
        for (int i = 0; i < level; i++)
        {
            newNode.Levels[i].Forward = update[i]!.Levels[i].Forward;
            update[i]!.Levels[i].Forward = newNode;

            newNode.Levels[i].Span = update[i]!.Levels[i].Span - (rank[0] - rank[i]);
            update[i]!.Levels[i].Span = rank[0] - rank[i] + 1;
        }

        // Increment span for untouched levels
        for (int i = level; i < _level; i++)
        {
            update[i]!.Levels[i].Span++;
        }

        // Set backward pointer
        newNode.Backward = update[0] == _head ? null : update[0];
        if (newNode.Levels[0].Forward != null)
            newNode.Levels[0].Forward.Backward = newNode;
        else
            _tail = newNode;

        _count++;
        return true;
    }

    /// <summary>
    /// Removes an element by key.
    /// </summary>
    public bool Remove(TKey key)
    {
        var update = new SkipListNode<TKey, TValue>?[MaxLevel];
        var current = _head!;

        for (int i = _level - 1; i >= 0; i--)
        {
            while (current.Levels[i].Forward != null &&
                   _comparer.Compare(current.Levels[i].Forward.Key, key) < 0)
            {
                current = current.Levels[i].Forward;
            }
            update[i] = current;
        }

        current = current.Levels[0].Forward!;
        if (current == null || _comparer.Compare(current.Key, key) != 0)
            return false;

        // Delete node
        DeleteNode(current, update);
        return true;
    }

    /// <summary>
    /// Finds an element by key.
    /// </summary>
    public TValue? Find(TKey key)
    {
        var current = _head!;

        for (int i = _level - 1; i >= 0; i--)
        {
            while (current.Levels[i].Forward != null &&
                   _comparer.Compare(current.Levels[i].Forward.Key, key) < 0)
            {
                current = current.Levels[i].Forward;
            }
        }

        current = current.Levels[0].Forward!;
        if (current != null && _comparer.Compare(current.Key, key) == 0)
            return current.Value;

        return default;
    }

    /// <summary>
    /// Checks if a key exists.
    /// </summary>
    public bool Contains(TKey key)
    {
        var current = _head!;

        for (int i = _level - 1; i >= 0; i--)
        {
            while (current.Levels[i].Forward != null &&
                   _comparer.Compare(current.Levels[i].Forward.Key, key) < 0)
            {
                current = current.Levels[i].Forward;
            }
        }

        current = current.Levels[0].Forward!;
        return current != null && _comparer.Compare(current.Key, key) == 0;
    }

    /// <summary>
    /// Gets the rank of a key (0-based).
    /// </summary>
    public long GetRank(TKey key)
    {
        long rank = 0;
        var current = _head!;

        for (int i = _level - 1; i >= 0; i--)
        {
            while (current.Levels[i].Forward != null &&
                   _comparer.Compare(current.Levels[i].Forward.Key, key) <= 0)
            {
                rank += current.Levels[i].Span;
                current = current.Levels[i].Forward;
            }

            if (current != _head && _comparer.Compare(current.Key, key) == 0)
                return rank - 1;
        }

        return -1; // Not found
    }

    /// <summary>
    /// Gets element by rank (0-based).
    /// </summary>
    public (TKey Key, TValue Value)? GetByRank(long rank)
    {
        if (rank < 0 || rank >= _count)
            return null;

        long traversed = 0;
        var current = _head!;

        for (int i = _level - 1; i >= 0; i--)
        {
            while (current.Levels[i].Forward != null &&
                   traversed + current.Levels[i].Span <= rank + 1)
            {
                traversed += current.Levels[i].Span;
                current = current.Levels[i].Forward;
            }

            if (traversed == rank + 1)
                return (current.Key, current.Value);
        }

        return null;
    }

    /// <summary>
    /// Gets a range of elements by rank.
    /// </summary>
    public IEnumerable<(TKey Key, TValue Value)> GetRange(long start, long stop, bool reverse = false)
    {
        if (start < 0) start = Math.Max(0, _count + start);
        if (stop < 0) stop = _count + stop;
        if (start > stop || start >= _count) yield break;
        stop = Math.Min(stop, _count - 1);

        if (reverse)
        {
            // Start from the end
            var current = _tail;
            long rank = _count - 1;

            while (current != null && rank > stop)
            {
                current = current.Backward;
                rank--;
            }

            while (current != null && rank >= start)
            {
                yield return (current.Key, current.Value);
                current = current.Backward;
                rank--;
            }
        }
        else
        {
            // Find start position
            var startNode = GetByRank(start);
            if (startNode == null) yield break;

            var current = _head!;
            long traversed = 0;

            for (int i = _level - 1; i >= 0; i--)
            {
                while (current.Levels[i].Forward != null &&
                       traversed + current.Levels[i].Span <= start + 1)
                {
                    traversed += current.Levels[i].Span;
                    current = current.Levels[i].Forward;
                }
            }

            long rank = start;
            while (current != null && rank <= stop)
            {
                yield return (current.Key, current.Value);
                current = current.Levels[0].Forward!;
                rank++;
            }
        }
    }

    /// <summary>
    /// Gets all elements in order.
    /// </summary>
    public IEnumerable<(TKey Key, TValue Value)> GetAll(bool reverse = false)
    {
        if (reverse)
        {
            var current = _tail;
            while (current != null)
            {
                yield return (current.Key, current.Value);
                current = current.Backward;
            }
        }
        else
        {
            var current = _head!.Levels[0].Forward;
            while (current != null)
            {
                yield return (current.Key, current.Value);
                current = current.Levels[0].Forward;
            }
        }
    }

    private void DeleteNode(SkipListNode<TKey, TValue> node, SkipListNode<TKey, TValue>?[] update)
    {
        for (int i = 0; i < _level; i++)
        {
            if (update[i]!.Levels[i].Forward == node)
            {
                update[i]!.Levels[i].Span += node.Levels[i].Span - 1;
                update[i]!.Levels[i].Forward = node.Levels[i].Forward;
            }
            else
            {
                update[i]!.Levels[i].Span--;
            }
        }

        if (node.Levels[0].Forward != null)
            node.Levels[0].Forward.Backward = node.Backward;
        else
            _tail = node.Backward;

        while (_level > 1 && _head!.Levels[_level - 1].Forward == null)
            _level--;

        _count--;
    }

    private int RandomLevel()
    {
        int level = 1;
        while (_random.NextDouble() < Probability && level < MaxLevel)
            level++;
        return level;
    }

    private static SkipListNode<TKey, TValue> CreateNode(int level, TKey key, TValue value)
    {
        return new SkipListNode<TKey, TValue>(level, key, value);
    }
}

/// <summary>
/// Skip list node.
/// </summary>
internal class SkipListNode<TKey, TValue>
{
    public TKey Key { get; }
    public TValue Value { get; set; }
    public SkipListNode<TKey, TValue>? Backward { get; set; }
    public SkipListLevel<TKey, TValue>[] Levels { get; }

    public SkipListNode(int level, TKey key, TValue value)
    {
        Key = key;
        Value = value;
        Levels = new SkipListLevel<TKey, TValue>[level];
        for (int i = 0; i < level; i++)
            Levels[i] = new SkipListLevel<TKey, TValue>();
    }
}

/// <summary>
/// Skip list level.
/// </summary>
internal class SkipListLevel<TKey, TValue>
{
    public SkipListNode<TKey, TValue>? Forward { get; set; }
    public int Span { get; set; }
}
