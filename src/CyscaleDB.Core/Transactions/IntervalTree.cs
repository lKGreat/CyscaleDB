namespace CyscaleDB.Core.Transactions;

/// <summary>
/// An interval tree implementation for efficient range query operations.
/// Used for gap lock optimization to achieve O(log n) lookup instead of O(n).
/// </summary>
/// <typeparam name="TKey">The type of the interval endpoints, must be comparable</typeparam>
/// <typeparam name="TValue">The type of values stored at each interval</typeparam>
public sealed class IntervalTree<TKey, TValue> where TKey : IComparable<TKey>
{
    private Node? _root;
    private readonly object _lock = new();
    private int _count;

    /// <summary>
    /// Gets the number of intervals in the tree.
    /// </summary>
    public int Count => _count;

    /// <summary>
    /// Inserts an interval with the specified value into the tree.
    /// </summary>
    /// <param name="low">The lower bound of the interval</param>
    /// <param name="high">The upper bound of the interval</param>
    /// <param name="value">The value associated with the interval</param>
    public void Insert(TKey low, TKey high, TValue value)
    {
        var interval = new Interval(low, high, value);
        
        lock (_lock)
        {
            _root = Insert(_root, interval);
            _count++;
        }
    }

    /// <summary>
    /// Removes an interval from the tree.
    /// </summary>
    /// <param name="low">The lower bound of the interval</param>
    /// <param name="high">The upper bound of the interval</param>
    /// <returns>True if the interval was found and removed</returns>
    public bool Remove(TKey low, TKey high)
    {
        lock (_lock)
        {
            var (newRoot, removed) = Remove(_root, low, high);
            _root = newRoot;
            if (removed)
            {
                _count--;
            }
            return removed;
        }
    }

    /// <summary>
    /// Finds all intervals that overlap with the specified point.
    /// </summary>
    /// <param name="point">The point to query</param>
    /// <returns>List of values for intervals containing the point</returns>
    public List<TValue> Query(TKey point)
    {
        var results = new List<TValue>();
        
        lock (_lock)
        {
            Query(_root, point, results);
        }
        
        return results;
    }

    /// <summary>
    /// Finds all intervals that overlap with the specified range.
    /// </summary>
    /// <param name="low">The lower bound of the query range</param>
    /// <param name="high">The upper bound of the query range</param>
    /// <returns>List of values for overlapping intervals</returns>
    public List<TValue> QueryRange(TKey low, TKey high)
    {
        var results = new List<TValue>();
        
        lock (_lock)
        {
            QueryRange(_root, low, high, results);
        }
        
        return results;
    }

    /// <summary>
    /// Checks if any interval overlaps with the specified range.
    /// </summary>
    /// <param name="low">The lower bound of the query range</param>
    /// <param name="high">The upper bound of the query range</param>
    /// <returns>True if any interval overlaps</returns>
    public bool HasOverlap(TKey low, TKey high)
    {
        lock (_lock)
        {
            return HasOverlap(_root, low, high);
        }
    }

    /// <summary>
    /// Clears all intervals from the tree.
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            _root = null;
            _count = 0;
        }
    }

    /// <summary>
    /// Gets all intervals in the tree.
    /// </summary>
    public List<(TKey Low, TKey High, TValue Value)> GetAllIntervals()
    {
        var results = new List<(TKey, TKey, TValue)>();
        
        lock (_lock)
        {
            CollectAll(_root, results);
        }
        
        return results;
    }

    #region Private Implementation

    private Node Insert(Node? node, Interval interval)
    {
        if (node == null)
        {
            return new Node(interval);
        }

        // Insert based on low endpoint
        if (interval.Low.CompareTo(node.Interval.Low) < 0)
        {
            node.Left = Insert(node.Left, interval);
        }
        else
        {
            node.Right = Insert(node.Right, interval);
        }

        // Update max high value in subtree
        UpdateMax(node);

        return node;
    }

    private (Node?, bool) Remove(Node? node, TKey low, TKey high)
    {
        if (node == null)
        {
            return (null, false);
        }

        int cmp = low.CompareTo(node.Interval.Low);

        if (cmp < 0)
        {
            var (newLeft, removed) = Remove(node.Left, low, high);
            node.Left = newLeft;
            if (removed) UpdateMax(node);
            return (node, removed);
        }
        else if (cmp > 0)
        {
            var (newRight, removed) = Remove(node.Right, low, high);
            node.Right = newRight;
            if (removed) UpdateMax(node);
            return (node, removed);
        }
        else
        {
            // Found matching low - check high
            if (high.CompareTo(node.Interval.High) == 0)
            {
                // Found the interval to remove
                if (node.Left == null)
                {
                    return (node.Right, true);
                }
                if (node.Right == null)
                {
                    return (node.Left, true);
                }

                // Node has two children - replace with min from right subtree
                var minNode = FindMin(node.Right);
                node.Interval = minNode.Interval;
                var (newRight, _) = Remove(node.Right, minNode.Interval.Low, minNode.Interval.High);
                node.Right = newRight;
                UpdateMax(node);
                return (node, true);
            }
            else
            {
                // Same low but different high - continue searching
                var (newRight, removed) = Remove(node.Right, low, high);
                node.Right = newRight;
                if (removed) UpdateMax(node);
                return (node, removed);
            }
        }
    }

    private static Node FindMin(Node node)
    {
        while (node.Left != null)
        {
            node = node.Left;
        }
        return node;
    }

    private void Query(Node? node, TKey point, List<TValue> results)
    {
        if (node == null)
        {
            return;
        }

        // If point is to the right of all intervals in this subtree, skip
        if (node.Max.CompareTo(point) < 0)
        {
            return;
        }

        // Check left subtree
        Query(node.Left, point, results);

        // Check current interval
        if (node.Interval.Contains(point))
        {
            results.Add(node.Interval.Value);
        }

        // Check right subtree if point could be there
        if (point.CompareTo(node.Interval.Low) >= 0)
        {
            Query(node.Right, point, results);
        }
    }

    private void QueryRange(Node? node, TKey low, TKey high, List<TValue> results)
    {
        if (node == null)
        {
            return;
        }

        // If all intervals end before query starts, skip
        if (node.Max.CompareTo(low) < 0)
        {
            return;
        }

        // Check left subtree
        QueryRange(node.Left, low, high, results);

        // Check current interval
        if (node.Interval.Overlaps(low, high))
        {
            results.Add(node.Interval.Value);
        }

        // Check right subtree if query could overlap
        if (high.CompareTo(node.Interval.Low) >= 0)
        {
            QueryRange(node.Right, low, high, results);
        }
    }

    private bool HasOverlap(Node? node, TKey low, TKey high)
    {
        if (node == null)
        {
            return false;
        }

        // If all intervals end before query starts, skip
        if (node.Max.CompareTo(low) < 0)
        {
            return false;
        }

        // Check current interval
        if (node.Interval.Overlaps(low, high))
        {
            return true;
        }

        // Check left subtree
        if (HasOverlap(node.Left, low, high))
        {
            return true;
        }

        // Check right subtree if query could overlap
        if (high.CompareTo(node.Interval.Low) >= 0)
        {
            return HasOverlap(node.Right, low, high);
        }

        return false;
    }

    private void CollectAll(Node? node, List<(TKey, TKey, TValue)> results)
    {
        if (node == null)
        {
            return;
        }

        CollectAll(node.Left, results);
        results.Add((node.Interval.Low, node.Interval.High, node.Interval.Value));
        CollectAll(node.Right, results);
    }

    private static void UpdateMax(Node node)
    {
        node.Max = node.Interval.High;
        
        if (node.Left != null && node.Left.Max.CompareTo(node.Max) > 0)
        {
            node.Max = node.Left.Max;
        }
        
        if (node.Right != null && node.Right.Max.CompareTo(node.Max) > 0)
        {
            node.Max = node.Right.Max;
        }
    }

    #endregion

    #region Nested Types

    private sealed class Node
    {
        public Interval Interval { get; set; }
        public TKey Max { get; set; }
        public Node? Left { get; set; }
        public Node? Right { get; set; }

        public Node(Interval interval)
        {
            Interval = interval;
            Max = interval.High;
        }
    }

    private sealed class Interval
    {
        public TKey Low { get; }
        public TKey High { get; }
        public TValue Value { get; }

        public Interval(TKey low, TKey high, TValue value)
        {
            Low = low;
            High = high;
            Value = value;
        }

        public bool Contains(TKey point)
        {
            return Low.CompareTo(point) <= 0 && point.CompareTo(High) <= 0;
        }

        public bool Overlaps(TKey low, TKey high)
        {
            return Low.CompareTo(high) <= 0 && low.CompareTo(High) <= 0;
        }
    }

    #endregion
}
