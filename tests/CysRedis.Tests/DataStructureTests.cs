using CysRedis.Core.DataStructures;
using CysRedis.Core.Memory;

namespace CysRedis.Tests;

/// <summary>
/// Tests for Redis data structures including encoding optimizations.
/// </summary>
public class DataStructureTests
{
    #region RedisString Tests

    [Fact]
    public void RedisString_BasicGetSet()
    {
        var str = new RedisString("hello");
        Assert.Equal("hello", str.GetString());
        Assert.Equal(5, str.Length);
    }

    [Fact]
    public void RedisString_IntegerEncoding()
    {
        var str = new RedisString("12345");
        Assert.Equal(RedisEncoding.Int, str.Encoding);

        var str2 = new RedisString("hello");
        Assert.Equal(RedisEncoding.Raw, str2.Encoding);
    }

    [Fact]
    public void RedisString_IncrDecr()
    {
        var str = new RedisString("100");
        Assert.True(str.TryGetInt64(out var val));
        Assert.Equal(100, val);

        str.SetInt64(200);
        Assert.Equal("200", str.GetString());
    }

    [Fact]
    public void RedisString_Float()
    {
        var str = new RedisString("3.14");
        Assert.True(str.TryGetDouble(out var val));
        Assert.Equal(3.14, val, 2);
    }

    #endregion

    #region RedisList Tests (Dual Encoding)

    [Fact]
    public void RedisList_StartsWithListpackEncoding()
    {
        var list = new RedisList();
        Assert.Equal(RedisEncoding.Listpack, list.Encoding);
    }

    [Fact]
    public void RedisList_PushAndPop()
    {
        var list = new RedisList();
        list.PushRight("hello"u8.ToArray());
        list.PushRight("world"u8.ToArray());
        list.PushLeft("first"u8.ToArray());

        Assert.Equal(3, list.Count);
        Assert.Equal("first", System.Text.Encoding.UTF8.GetString(list.PopLeft()!));
        Assert.Equal("world", System.Text.Encoding.UTF8.GetString(list.PopRight()!));
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(list.PopLeft()!));
        Assert.Equal(0, list.Count);
    }

    [Fact]
    public void RedisList_IndexAccess_Listpack()
    {
        var list = new RedisList();
        for (int i = 0; i < 10; i++)
            list.PushRight(System.Text.Encoding.UTF8.GetBytes($"item{i}"));

        // Listpack encoding should give O(1) index access
        Assert.Equal(RedisEncoding.Listpack, list.Encoding);
        Assert.Equal("item5", System.Text.Encoding.UTF8.GetString(list.GetByIndex(5)!));
        Assert.Equal("item9", System.Text.Encoding.UTF8.GetString(list.GetByIndex(-1)!));
    }

    [Fact]
    public void RedisList_ConvertsToQuickList_OnLargeSize()
    {
        var list = new RedisList();
        for (int i = 0; i < 200; i++)
            list.PushRight(System.Text.Encoding.UTF8.GetBytes($"item{i}"));

        Assert.Equal(RedisEncoding.QuickList, list.Encoding);
        Assert.Equal(200, list.Count);
        // Index access should still work
        Assert.Equal("item100", System.Text.Encoding.UTF8.GetString(list.GetByIndex(100)!));
    }

    [Fact]
    public void RedisList_ConvertsToQuickList_OnLargeElement()
    {
        var list = new RedisList();
        list.PushRight(new byte[100]); // large element > 64 bytes

        Assert.Equal(RedisEncoding.QuickList, list.Encoding);
    }

    [Fact]
    public void RedisList_GetRange()
    {
        var list = new RedisList();
        for (int i = 0; i < 5; i++)
            list.PushRight(System.Text.Encoding.UTF8.GetBytes($"v{i}"));

        var range = list.GetRange(1, 3);
        Assert.Equal(3, range.Count);
        Assert.Equal("v1", System.Text.Encoding.UTF8.GetString(range[0]));
        Assert.Equal("v3", System.Text.Encoding.UTF8.GetString(range[2]));
    }

    [Fact]
    public void RedisList_Trim()
    {
        var list = new RedisList();
        for (int i = 0; i < 10; i++)
            list.PushRight(System.Text.Encoding.UTF8.GetBytes($"v{i}"));

        list.Trim(2, 5);
        Assert.Equal(4, list.Count);
        Assert.Equal("v2", System.Text.Encoding.UTF8.GetString(list.GetByIndex(0)!));
        Assert.Equal("v5", System.Text.Encoding.UTF8.GetString(list.GetByIndex(3)!));
    }

    [Fact]
    public void RedisList_SetByIndex()
    {
        var list = new RedisList();
        list.PushRight("a"u8.ToArray());
        list.PushRight("b"u8.ToArray());

        Assert.True(list.SetByIndex(1, "c"u8.ToArray()));
        Assert.Equal("c", System.Text.Encoding.UTF8.GetString(list.GetByIndex(1)!));
    }

    #endregion

    #region RedisSet Tests (IntSet + Hashtable Encoding)

    [Fact]
    public void RedisSet_StartsWithIntSetEncoding()
    {
        var set = new RedisSet();
        set.Add("1");
        set.Add("2");
        set.Add("3");
        Assert.Equal(RedisEncoding.IntSet, set.Encoding);
    }

    [Fact]
    public void RedisSet_ConvertsToHashtable_OnNonInteger()
    {
        var set = new RedisSet();
        set.Add("1");
        set.Add("hello"); // non-integer -> convert
        Assert.Equal(RedisEncoding.Hashtable, set.Encoding);
        Assert.Equal(2, set.Count);
        Assert.True(set.Contains("1"));
        Assert.True(set.Contains("hello"));
    }

    [Fact]
    public void RedisSet_ConvertsToHashtable_OnLargeSize()
    {
        var set = new RedisSet();
        for (int i = 0; i < 600; i++)
            set.Add(i.ToString());
        Assert.Equal(RedisEncoding.Hashtable, set.Encoding);
        Assert.Equal(600, set.Count);
    }

    [Fact]
    public void RedisSet_Operations()
    {
        var set = new RedisSet();
        Assert.True(set.Add("a"));
        Assert.False(set.Add("a")); // duplicate
        Assert.True(set.Contains("a"));
        Assert.True(set.Remove("a"));
        Assert.False(set.Contains("a"));
    }

    [Fact]
    public void RedisSet_Union_Intersect_Difference()
    {
        var s1 = new RedisSet();
        s1.Add("a"); s1.Add("b"); s1.Add("c");

        var s2 = new RedisSet();
        s2.Add("b"); s2.Add("c"); s2.Add("d");

        var union = s1.Union(s2);
        Assert.Equal(4, union.Count);

        var inter = s1.Intersect(s2);
        Assert.Equal(2, inter.Count);
        Assert.Contains("b", inter);
        Assert.Contains("c", inter);

        var diff = s1.Difference(s2);
        Assert.Single(diff);
        Assert.Contains("a", diff);
    }

    #endregion

    #region RedisSortedSet Tests

    [Fact]
    public void RedisSortedSet_AddAndScore()
    {
        var zset = new RedisSortedSet();
        Assert.True(zset.Add("a", 1.0));
        Assert.True(zset.Add("b", 2.0));
        Assert.False(zset.Add("a", 3.0)); // update returns false

        Assert.Equal(3.0, zset.GetScore("a"));
        Assert.Equal(2.0, zset.GetScore("b"));
    }

    [Fact]
    public void RedisSortedSet_Rank()
    {
        var zset = new RedisSortedSet();
        zset.Add("a", 1.0);
        zset.Add("b", 2.0);
        zset.Add("c", 3.0);

        Assert.Equal(0L, zset.GetRank("a"));
        Assert.Equal(2L, zset.GetRank("c"));
        Assert.Equal(0L, zset.GetRank("c", reverse: true));
        Assert.Null(zset.GetRank("nonexistent"));
    }

    [Fact]
    public void RedisSortedSet_Remove()
    {
        var zset = new RedisSortedSet();
        zset.Add("a", 1.0);
        Assert.True(zset.Remove("a"));
        Assert.False(zset.Remove("nonexistent"));
        Assert.Equal(0, zset.Count);
    }

    #endregion

    #region RedisHash Tests

    [Fact]
    public void RedisHash_BasicOps()
    {
        var hash = new RedisHash();
        hash.Set("field1", "value1"u8.ToArray());
        hash.Set("field2", "value2"u8.ToArray());

        Assert.Equal(2, hash.Count);
        Assert.Equal("value1", System.Text.Encoding.UTF8.GetString(hash.Get("field1")!));
        Assert.True(hash.Exists("field1"));
        Assert.True(hash.Delete("field1"));
        Assert.False(hash.Exists("field1"));
    }

    [Fact]
    public void RedisHash_IncrBy()
    {
        var hash = new RedisHash();
        Assert.Equal(10, hash.IncrBy("counter", 10));
        Assert.Equal(15, hash.IncrBy("counter", 5));
        Assert.Equal(10, hash.IncrBy("counter", -5));
    }

    [Fact]
    public void RedisHash_FieldExpiration()
    {
        var hash = new RedisHash();
        hash.Set("field1", "value1"u8.ToArray());
        hash.SetFieldExpire("field1", DateTime.UtcNow.AddMilliseconds(-1));

        // Field should be expired
        Assert.Null(hash.Get("field1"));
    }

    #endregion
}
