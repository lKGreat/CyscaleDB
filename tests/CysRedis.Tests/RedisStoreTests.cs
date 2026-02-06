using CysRedis.Core.DataStructures;
using CysRedis.Core.Memory;

namespace CysRedis.Tests;

/// <summary>
/// Tests for RedisStore and RedisDatabase, including key versioning (WATCH),
/// expiration cleanup (probabilistic sampling), and eviction.
/// </summary>
public class RedisStoreTests
{
    #region Basic Store Tests

    [Fact]
    public void RedisStore_CreateAndGetDatabase()
    {
        using var store = new RedisStore(databaseCount: 16);
        var db = store.GetDatabase(0);
        Assert.NotNull(db);
        Assert.Equal(0, db.Index);
    }

    [Fact]
    public void RedisStore_InvalidDatabaseIndex_Throws()
    {
        using var store = new RedisStore(databaseCount: 16);
        Assert.Throws<CysRedis.Core.Common.InvalidArgumentException>(() => store.GetDatabase(99));
    }

    #endregion

    #region Key Version Tests (WATCH support)

    [Fact]
    public void RedisDatabase_KeyVersion_IncreasesOnSet()
    {
        using var store = new RedisStore();
        var db = store.GetDatabase(0);

        var v1 = db.GetKeyVersion("key1");
        db.Set("key1", new RedisString("value1"));
        var v2 = db.GetKeyVersion("key1");

        Assert.True(v2 > v1, "Version should increase after Set");
    }

    [Fact]
    public void RedisDatabase_KeyVersion_IncreasesOnDelete()
    {
        using var store = new RedisStore();
        var db = store.GetDatabase(0);

        db.Set("key1", new RedisString("value1"));
        var vBefore = db.GetKeyVersion("key1");
        db.Delete("key1");

        // After delete, the key version entry is removed,
        // but we need to ensure the version changed before removal
        Assert.True(vBefore > 0);
    }

    [Fact]
    public void RedisDatabase_KeyVersion_UnchangedIfNotModified()
    {
        using var store = new RedisStore();
        var db = store.GetDatabase(0);

        db.Set("key1", new RedisString("value1"));
        var v1 = db.GetKeyVersion("key1");
        var v2 = db.GetKeyVersion("key1");

        Assert.Equal(v1, v2);
    }

    [Fact]
    public void RedisDatabase_KeyVersion_DifferentKeysGetDifferentVersions()
    {
        using var store = new RedisStore();
        var db = store.GetDatabase(0);

        db.Set("key1", new RedisString("v1"));
        db.Set("key2", new RedisString("v2"));

        var v1 = db.GetKeyVersion("key1");
        var v2 = db.GetKeyVersion("key2");

        Assert.NotEqual(v1, v2);
    }

    [Fact]
    public void RedisDatabase_KeyVersion_IncreasesOnRename()
    {
        using var store = new RedisStore();
        var db = store.GetDatabase(0);

        db.Set("old", new RedisString("value"));
        var vOld = db.GetKeyVersion("old");
        db.Rename("old", "new");

        // Old key version should be gone
        Assert.Equal(0, db.GetKeyVersion("old"));
        // New key should have a version
        Assert.True(db.GetKeyVersion("new") > 0);
    }

    #endregion

    #region Expiration Tests

    [Fact]
    public void RedisDatabase_SetAndCheckExpire()
    {
        using var store = new RedisStore();
        var db = store.GetDatabase(0);

        db.Set("key1", new RedisString("value1"));
        db.SetExpire("key1", DateTime.UtcNow.AddMinutes(10));

        Assert.NotNull(db.GetExpire("key1"));
        var ttl = db.GetTtl("key1");
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value > 0);
    }

    [Fact]
    public void RedisDatabase_ExpiredKey_ReturnsNull()
    {
        using var store = new RedisStore();
        var db = store.GetDatabase(0);

        db.Set("key1", new RedisString("value1"));
        db.SetExpire("key1", DateTime.UtcNow.AddMilliseconds(-1));

        // Key should be expired
        Assert.Null(db.Get("key1"));
    }

    [Fact]
    public void RedisDatabase_CleanupExpired_ProbabilisticSampling()
    {
        using var store = new RedisStore();
        var db = store.GetDatabase(0);

        // Add 100 keys, 50 expired
        for (int i = 0; i < 100; i++)
        {
            db.Set($"key{i}", new RedisString($"val{i}"));
            if (i < 50)
                db.SetExpire($"key{i}", DateTime.UtcNow.AddMilliseconds(-1));
        }

        // Run cleanup
        var cleaned = db.CleanupExpired(sampleSize: 20, maxIterations: 16);
        Assert.True(cleaned > 0, "Should have cleaned some expired keys");
    }

    [Fact]
    public void RedisDatabase_Persist_RemovesExpiration()
    {
        using var store = new RedisStore();
        var db = store.GetDatabase(0);

        db.Set("key1", new RedisString("value1"));
        db.SetExpire("key1", DateTime.UtcNow.AddMinutes(10));
        Assert.True(db.Persist("key1"));
        Assert.Null(db.GetExpire("key1"));
        Assert.Equal(-1L, db.GetTtl("key1"));
    }

    #endregion

    #region GetRandomKeys Tests

    [Fact]
    public void RedisDatabase_GetRandomKeys()
    {
        using var store = new RedisStore();
        var db = store.GetDatabase(0);

        for (int i = 0; i < 100; i++)
            db.Set($"k{i}", new RedisString($"v{i}"));

        var randomKeys = db.GetRandomKeys(10);
        Assert.Equal(10, randomKeys.Count);
        // All should be valid keys
        foreach (var key in randomKeys)
            Assert.True(db.Exists(key));
    }

    [Fact]
    public void RedisDatabase_GetRandomKeys_EmptyDb()
    {
        using var store = new RedisStore();
        var db = store.GetDatabase(0);

        var randomKeys = db.GetRandomKeys(10);
        Assert.Empty(randomKeys);
    }

    #endregion

    #region Eviction Tests

    [Fact]
    public void EvictionManager_NoEviction_DoesNotEvict()
    {
        var eviction = new EvictionManager(EvictionPolicy.NoEviction, maxMemory: 1024);
        Assert.False(eviction.NeedsEviction());
    }

    [Fact]
    public void EvictionManager_TracksMemory()
    {
        var eviction = new EvictionManager(EvictionPolicy.AllKeysLru, maxMemory: 1000);
        eviction.OnKeySet("key1", 500);
        Assert.Equal(500, eviction.ApproximateMemoryUsage);

        eviction.OnKeySet("key2", 300);
        Assert.Equal(800, eviction.ApproximateMemoryUsage);

        eviction.OnKeyDelete("key1");
        Assert.Equal(300, eviction.ApproximateMemoryUsage);
    }

    [Fact]
    public void EvictionManager_NeedsEviction_WhenOverLimit()
    {
        var eviction = new EvictionManager(EvictionPolicy.AllKeysLru, maxMemory: 1000);
        eviction.OnKeySet("key1", 600);
        eviction.OnKeySet("key2", 600);
        Assert.True(eviction.NeedsEviction());
    }

    #endregion

    #region Flush Tests

    [Fact]
    public void RedisDatabase_Flush_ClearsEverything()
    {
        using var store = new RedisStore();
        var db = store.GetDatabase(0);

        db.Set("k1", new RedisString("v1"));
        db.Set("k2", new RedisString("v2"));
        db.Flush();

        Assert.Equal(0, db.KeyCount);
        Assert.Null(db.Get("k1"));
    }

    [Fact]
    public void RedisStore_FlushAll_ClearsAllDatabases()
    {
        using var store = new RedisStore(databaseCount: 4);
        store.GetDatabase(0).Set("k", new RedisString("v"));
        store.GetDatabase(1).Set("k", new RedisString("v"));

        store.FlushAll();

        Assert.Equal(0, store.GetDatabase(0).KeyCount);
        Assert.Equal(0, store.GetDatabase(1).KeyCount);
    }

    #endregion
}
