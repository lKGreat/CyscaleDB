using CysRedis.Core.DataStructures;

namespace CysRedis.Tests;

/// <summary>
/// Tests for SkipList data structure used by Redis sorted sets.
/// </summary>
public class SkipListTests
{
    [Fact]
    public void Insert_SingleElement()
    {
        var sl = new SkipList<double, string>();
        Assert.True(sl.Insert(1.0, "a"));
        Assert.Equal(1, sl.Count);
    }

    [Fact]
    public void Insert_DuplicateKey_Updates()
    {
        var sl = new SkipList<double, string>();
        sl.Insert(1.0, "a");
        Assert.False(sl.Insert(1.0, "b")); // Update
        Assert.Equal(1, sl.Count);
    }

    [Fact]
    public void Insert_MultipleElements_Ordered()
    {
        var sl = new SkipList<int, string>();
        sl.Insert(3, "c");
        sl.Insert(1, "a");
        sl.Insert(2, "b");

        // GetRange uses rank-based indexing: 0, 1, 2
        var range = sl.GetRange(0, 2).ToList();
        Assert.Equal(3, range.Count);
        Assert.Equal("a", range[0].Value);
        Assert.Equal("b", range[1].Value);
        Assert.Equal("c", range[2].Value);
    }

    [Fact]
    public void Delete_ExistingElement()
    {
        var sl = new SkipList<int, string>();
        sl.Insert(1, "a");
        sl.Insert(2, "b");
        Assert.True(sl.Remove(1));
        Assert.Equal(1, sl.Count);
    }

    [Fact]
    public void Delete_NonExisting_ReturnsFalse()
    {
        var sl = new SkipList<int, string>();
        sl.Insert(1, "a");
        Assert.False(sl.Remove(99));
    }

    [Fact]
    public void Find_ExistingKey()
    {
        var sl = new SkipList<string, int>(StringComparer.Ordinal);
        sl.Insert("key1", 100);
        sl.Insert("key2", 200);

        var value = sl.Find("key1");
        Assert.Equal(100, value);
    }

    [Fact]
    public void Find_NonExistingKey_ReturnsDefault()
    {
        var sl = new SkipList<string, int>(StringComparer.Ordinal);
        Assert.Equal(default, sl.Find("nope"));
    }

    [Fact]
    public void Contains_ExistingKey()
    {
        var sl = new SkipList<int, string>();
        sl.Insert(10, "a");
        Assert.True(sl.Contains(10));
        Assert.False(sl.Contains(99));
    }

    [Fact]
    public void GetRank()
    {
        var sl = new SkipList<int, string>();
        sl.Insert(10, "a");
        sl.Insert(20, "b");
        sl.Insert(30, "c");
        sl.Insert(40, "d");

        Assert.Equal(0, sl.GetRank(10));
        Assert.Equal(1, sl.GetRank(20));
        Assert.Equal(3, sl.GetRank(40));
        Assert.Equal(-1, sl.GetRank(99)); // Not found
    }

    [Fact]
    public void GetByRank()
    {
        var sl = new SkipList<int, string>();
        sl.Insert(10, "a");
        sl.Insert(20, "b");
        sl.Insert(30, "c");

        var first = sl.GetByRank(0);
        Assert.NotNull(first);
        Assert.Equal(10, first!.Value.Key);
        Assert.Equal("a", first.Value.Value);

        var last = sl.GetByRank(2);
        Assert.NotNull(last);
        Assert.Equal(30, last!.Value.Key);

        Assert.Null(sl.GetByRank(99)); // Out of range
    }

    [Fact]
    public void GetRange_Subset()
    {
        var sl = new SkipList<int, string>();
        for (int i = 0; i < 10; i++)
            sl.Insert(i, $"v{i}");

        // Rank-based: get ranks 3 through 6
        var range = sl.GetRange(3, 6).ToList();
        Assert.Equal(4, range.Count);
        Assert.Equal("v3", range[0].Value);
        Assert.Equal("v6", range[3].Value);
    }

    [Fact]
    public void GetRange_Empty()
    {
        var sl = new SkipList<int, string>();
        var range = sl.GetRange(0, 10).ToList();
        Assert.Empty(range);
    }

    [Fact]
    public void GetRange_Reverse()
    {
        var sl = new SkipList<int, string>();
        sl.Insert(1, "a");
        sl.Insert(2, "b");
        sl.Insert(3, "c");

        var reversed = sl.GetRange(0, 2, reverse: true).ToList();
        Assert.Equal(3, reversed.Count);
        Assert.Equal("c", reversed[0].Value);
        Assert.Equal("a", reversed[2].Value);
    }

    [Fact]
    public void LargeInsertAndDelete()
    {
        var sl = new SkipList<int, string>();
        const int n = 1000;
        for (int i = 0; i < n; i++)
            sl.Insert(i, $"value-{i}");

        Assert.Equal(n, sl.Count);

        // Delete even numbers
        for (int i = 0; i < n; i += 2)
            Assert.True(sl.Remove(i));

        Assert.Equal(n / 2, sl.Count);

        // Verify remaining are odd numbers via range
        var all = sl.GetRange(0, sl.Count - 1).ToList();
        foreach (var kv in all)
            Assert.True(kv.Key % 2 == 1, $"Expected odd key, got {kv.Key}");
    }

    [Fact]
    public void NegativeKeys()
    {
        var sl = new SkipList<int, string>();
        sl.Insert(-5, "a");
        sl.Insert(0, "b");
        sl.Insert(5, "c");

        // Rank-based: 3 elements, ranks 0-2
        var all = sl.GetRange(0, 2).ToList();
        Assert.Equal(3, all.Count);
        Assert.Equal(-5, all[0].Key);
        Assert.Equal(0, all[1].Key);
        Assert.Equal(5, all[2].Key);
    }

    [Fact]
    public void InsertUpdatesValue()
    {
        var sl = new SkipList<int, string>();
        sl.Insert(1, "original");
        sl.Insert(1, "updated"); // Should update
        Assert.Equal("updated", sl.Find(1));
    }
}
