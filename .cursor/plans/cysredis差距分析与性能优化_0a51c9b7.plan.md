---
name: CysRedis差距分析与性能优化
overview: 全面对比 CysRedis 与 Redis 8.4.0 的实现差距，列出最小可闭环的实现案例，并提供极致内存性能优化方案。
todos:
  - id: set-ifeq
    content: 实现 SET 命令的 IFEQ/IFNE/IFDEQ/IFDNE 参数 (Redis 8.4.0 新特性)
    status: completed
  - id: smismember
    content: 实现 SMISMEMBER 命令 - 批量成员检查
    status: completed
  - id: lpos
    content: 实现 LPOS 命令 - 列表元素位置查找
    status: completed
  - id: copy
    content: 实现 COPY 命令 - 键复制
    status: completed
  - id: zmscore
    content: 实现 ZMSCORE 命令 - 批量分数获取
    status: completed
  - id: lru-clock
    content: 优化 LRU 时钟，使用 24 位时钟减少内存和计算开销
    status: completed
  - id: object-pool
    content: 实现 RedisObject 对象池减少 GC 压力
    status: completed
  - id: zero-copy
    content: 使用 Span/Memory 实现零拷贝命令解析
    status: completed
  - id: intset
    content: 实现 IntSet 紧凑整数集合
    status: completed
  - id: listpack
    content: 实现 Listpack 紧凑列表结构
    status: completed
  - id: compact-string
    content: 实现类 SDS 的紧凑字符串结构
    status: completed
  - id: hash-field-expire
    content: 实现 Hash 字段过期 (HEXPIRE/HTTL/HPERSIST)
    status: completed
---

# CysRedis 与 Redis 8.4.0 差距分析及性能优化计划

## 一、核心实现差距分析

### 1. 数据结构编码差距

| 特性 | Redis 8.4.0 | CysRedis 现状 | 差距评估 |

|------|-------------|---------------|----------|

| SDS (Simple Dynamic String) | 5种头大小(sdshdr5/8/16/32/64)，按字符串长度选择最小头 | 使用 `byte[]`，无紧凑头 | **高** |

| intset | 整数集合紧凑编码(16/32/64位自动升级) | 使用 `HashSet<string>` | **高** |

| listpack | 替代 ziplist 的紧凑列表 | 未实现 | **高** |

| quicklist | 链表 + listpack 混合结构 | 使用 `LinkedList<byte[]>` | **高** |

| dict | 增量 rehash + 2表结构 | 使用 `ConcurrentDictionary` | **中** |

| skiplist | 已实现 | 已实现，符合 Redis 设计 | **低** |

### 2. Redis 8.4.0 新特性差距

**SET 命令新参数 (8.4.0):**

- `IFEQ <value>` - 当前值等于指定值时才设置
- `IFNE <value>` - 当前值不等于指定值时才设置  
- `IFDEQ <digest>` - 当前值的 XXH3 哈希等于指定摘要时才设置
- `IFDNE <digest>` - 当前值的 XXH3 哈希不等于指定摘要时才设置

**Hash 字段过期 (8.0+):**

- `HEXPIRE` / `HPEXPIRE` - 设置字段过期
- `HTTL` / `HPTTL` - 获取字段 TTL
- `HPERSIST` - 移除字段过期
- `HGETDEL` / `HGETEX` - 获取并删除/设置过期

### 3. 缺失的核心命令

**String 命令:**

- `LCS` - 最长公共子序列

**List 命令:**

- `LPOS` - 查找元素位置
- `LMPOP` - 多列表弹出

**Set 命令:**

- `SINTERCARD` - 交集基数
- `SMISMEMBER` - 批量成员检查

**Sorted Set 命令:**

- `ZMPOP` / `ZRANGESTORE` / `ZINTERCARD` / `ZDIFF` / `ZDIFFSTORE`
- `ZMSCORE` / `ZRANDMEMBER`

**通用命令:**

- `COPY` - 键复制
- `SORT` / `SORT_RO` - 排序
- `DUMP` / `RESTORE` - 序列化
- `OBJECT ENCODING/FREQ/IDLETIME`
- `MEMORY USAGE/DOCTOR`

---

## 二、最小可闭环实现案例 (保证编译通过)

### Case 1: SET 命令 IFEQ/IFNE 支持

**文件:** `src/CysRedis.Core/Commands/StringCommands.cs`

```csharp
// 在 SetCommand.ExecuteAsync 中添加:
case "IFEQ":
    i++;
    if (i >= context.ArgCount) throw new SyntaxErrorException();
    var existingForIfeq = db.Get<RedisString>(key);
    if (existingForIfeq == null || existingForIfeq.GetString() != context.GetArg(i))
    {
        await context.Client.WriteNullAsync(cancellationToken);
        return;
    }
    break;
case "IFNE":
    i++;
    if (i >= context.ArgCount) throw new SyntaxErrorException();
    var existingForIfne = db.Get<RedisString>(key);
    if (existingForIfne != null && existingForIfne.GetString() == context.GetArg(i))
    {
        await context.Client.WriteNullAsync(cancellationToken);
        return;
    }
    break;
```

### Case 2: SMISMEMBER 命令

**文件:** `src/CysRedis.Core/Commands/SetCommands.cs`

```csharp
public class SMIsMemberCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var set = context.Database.Get<RedisSet>(key);
        
        var results = new RespValue[context.ArgCount - 1];
        for (int i = 1; i < context.ArgCount; i++)
        {
            var member = context.GetArg(i);
            results[i - 1] = RespValue.Integer(set?.Contains(member) == true ? 1 : 0);
        }
        
        return context.Client.WriteArrayAsync(results, cancellationToken);
    }
}
```

### Case 3: LPOS 命令

**文件:** `src/CysRedis.Core/Commands/ListCommands.cs`

```csharp
public class LPosCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var element = context.GetArgBytes(1);
        var list = context.Database.Get<RedisList>(key);
        
        if (list == null)
            return context.Client.WriteNullAsync(cancellationToken);
        
        int rank = 1, count = 1, maxlen = 0;
        // 解析 RANK/COUNT/MAXLEN 参数...
        
        var positions = new List<long>();
        for (int i = 0; i < list.Count && (maxlen == 0 || i < maxlen); i++)
        {
            if (list.GetByIndex(i)?.SequenceEqual(element) == true)
            {
                positions.Add(i);
                if (positions.Count >= count) break;
            }
        }
        
        if (count == 1)
            return positions.Count > 0 
                ? context.Client.WriteIntegerAsync(positions[0], cancellationToken)
                : context.Client.WriteNullAsync(cancellationToken);
        
        return context.Client.WriteArrayAsync(
            positions.Select(p => RespValue.Integer(p)).ToArray(), cancellationToken);
    }
}
```

### Case 4: COPY 命令

**文件:** `src/CysRedis.Core/Commands/KeyCommands.cs`

```csharp
public class CopyCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var sourceKey = context.GetArg(0);
        var destKey = context.GetArg(1);
        bool replace = false;
        int destDb = context.Client.DatabaseIndex;
        
        // 解析 REPLACE/DB 参数
        for (int i = 2; i < context.ArgCount; i++)
        {
            var opt = context.GetArg(i).ToUpperInvariant();
            if (opt == "REPLACE") replace = true;
            else if (opt == "DB") { i++; destDb = context.GetArgAsInt(i); }
        }
        
        var sourceDb = context.Database;
        var targetDb = context.Server.Store.GetDatabase(destDb);
        var source = sourceDb.Get(sourceKey);
        
        if (source == null)
            return context.Client.WriteIntegerAsync(0, cancellationToken);
        
        if (!replace && targetDb.Exists(destKey))
            return context.Client.WriteIntegerAsync(0, cancellationToken);
        
        // 深拷贝对象
        var copy = DeepCopyRedisObject(source);
        targetDb.Set(destKey, copy);
        
        // 复制过期时间
        var expire = sourceDb.GetExpire(sourceKey);
        if (expire.HasValue) targetDb.SetExpire(destKey, expire.Value);
        
        return context.Client.WriteIntegerAsync(1, cancellationToken);
    }
    
    private RedisObject DeepCopyRedisObject(RedisObject obj) => obj switch
    {
        RedisString s => new RedisString((byte[])s.Value.Clone()),
        RedisList l => CopyList(l),
        RedisSet s => CopySet(s),
        // ... 其他类型
        _ => throw new NotSupportedException()
    };
}
```

### Case 5: ZMSCORE 命令

**文件:** `src/CysRedis.Core/Commands/SortedSetCommands.cs`

```csharp
public class ZMScoreCommand : ICommandHandler
{
    public Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var key = context.GetArg(0);
        var zset = context.Database.Get<RedisSortedSet>(key);
        
        var results = new RespValue[context.ArgCount - 1];
        for (int i = 1; i < context.ArgCount; i++)
        {
            var member = context.GetArg(i);
            var score = zset?.GetScore(member);
            results[i - 1] = score.HasValue 
                ? RespValue.BulkString(score.Value.ToString("G17")) 
                : RespValue.Null;
        }
        
        return context.Client.WriteArrayAsync(results, cancellationToken);
    }
}
```

---

## 三、极致内存性能优化方案

### 1. 紧凑字符串实现 (类 SDS)

**问题:** 当前使用 `byte[]` 存储字符串，每个数组对象有 24 字节开销。

**优化方案:** 实现类似 Redis SDS 的紧凑字符串结构。

```csharp
// 新文件: src/CysRedis.Core/DataStructures/CompactString.cs
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct CompactString
{
    // 1字节: 低3位=类型, 高5位=短字符串长度(<=31)
    private readonly byte _flags;
    // 数据紧跟在 flags 之后
    
    public static CompactString Create(ReadOnlySpan<byte> data)
    {
        // 根据长度选择最优编码
        if (data.Length <= 31) return CreateType5(data);
        if (data.Length <= 255) return CreateType8(data);
        // ...
    }
}
```

**预期收益:** 小字符串内存减少 50-70%

### 2. 整数集合 (IntSet)

**问题:** `RedisSet` 对纯整数集合使用 `HashSet<string>`，内存浪费严重。

**优化方案:**

```csharp
// 新文件: src/CysRedis.Core/DataStructures/IntSet.cs
public class IntSet
{
    private byte _encoding; // 2=int16, 4=int32, 8=int64
    private int _length;
    private byte[] _contents;
    
    public bool Add(long value)
    {
        var reqEncoding = GetRequiredEncoding(value);
        if (reqEncoding > _encoding) UpgradeAndAdd(value);
        else BinaryInsert(value);
        return true;
    }
    
    // 使用二分查找，O(log n) 查询
    public bool Contains(long value) => BinarySearch(value) >= 0;
}
```

**预期收益:** 整数集合内存减少 80%+

### 3. Listpack 紧凑列表

**问题:** `RedisList` 使用 `LinkedList<byte[]>`，每节点 40+ 字节开销。

**优化方案:**

```csharp
// 新文件: src/CysRedis.Core/DataStructures/Listpack.cs
public class Listpack
{
    private byte[] _buffer;
    private int _totalBytes;
    private int _numElements;
    
    // 元素编码: 整数用 varint，字符串用长度前缀
    public void Append(ReadOnlySpan<byte> data)
    {
        // 自动选择最优编码
        if (TryParseInteger(data, out var intVal))
            AppendInteger(intVal);
        else
            AppendString(data);
    }
}
```

**预期收益:** 小列表内存减少 60-80%

### 4. 对象池与内存复用

**问题:** 频繁创建 `RedisObject` 导致 GC 压力。

**优化方案:**

```csharp
// 增强 BufferPool.cs
public static class ObjectPool<T> where T : class, new()
{
    private static readonly ConcurrentQueue<T> _pool = new();
    private const int MaxPoolSize = 1024;
    
    public static T Rent()
    {
        if (_pool.TryDequeue(out var obj)) return obj;
        return new T();
    }
    
    public static void Return(T obj)
    {
        if (_pool.Count < MaxPoolSize)
        {
            // 重置对象状态
            if (obj is IResettable r) r.Reset();
            _pool.Enqueue(obj);
        }
    }
}
```

### 5. Span/Memory 零拷贝优化

**问题:** 命令解析和响应构建存在多次内存拷贝。

**优化方案:**

```csharp
// 优化 RespPipeReader 使用 ReadOnlySequence
public bool TryParseCommand(ref ReadOnlySequence<byte> buffer, 
    out ReadOnlySpan<byte>[] args)
{
    // 直接在 buffer 上解析，避免拷贝
    // 使用 SequenceReader<byte> 高效遍历
}

// 优化 RedisString 使用 Memory<byte> 替代 byte[]
public class RedisString : RedisObject
{
    private Memory<byte> _value;
    public ReadOnlySpan<byte> Value => _value.Span;
}
```

### 6. LRU 时钟优化

**问题:** 当前每次访问都更新 `DateTime.UtcNow`，开销大。

**优化方案:**

```csharp
// 使用 Redis 的 LRU 时钟方案
public static class LruClock
{
    private static uint _clock;
    private const int LruBits = 24;
    private const uint LruClockMax = (1 << LruBits) - 1;
    private const int LruClockResolution = 1000; // 1秒分辨率
    
    // 每秒更新一次，而非每次访问
    public static uint GetClock() => _clock;
    
    public static void UpdateClock()
    {
        _clock = (uint)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 
            LruClockResolution) & LruClockMax;
    }
}

// 对象只存储 24 位时钟值
public class EvictionMetadata
{
    public uint Lru { get; set; } // 24 bits
}
```

### 7. 内存预取 (Memory Prefetch)

**问题:** 大量随机键访问导致 CPU 缓存未命中。

**优化方案:**

```csharp
// 参考 Redis 8.4.0 的 memory_prefetch.c
public static class MemoryPrefetch
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void PrefetchReadonly(IntPtr ptr)
    {
        // 使用 SSE 预取指令
        if (Sse.IsSupported)
            Sse.Prefetch0((void*)ptr);
    }
    
    // 批量命令时预取多个键
    public static void PrefetchKeys(RedisDatabase db, string[] keys)
    {
        foreach (var key in keys)
        {
            var hash = key.GetHashCode();
            // 预取字典桶...
        }
    }
}
```

---

## 四、实施优先级

### P0 - 立即实现 (保证闭环)

1. SET IFEQ/IFNE 参数
2. SMISMEMBER 命令
3. LPOS 命令
4. COPY 命令
5. ZMSCORE 命令

### P1 - 性能关键

6. LRU 时钟优化
7. 对象池实现
8. Span/Memory 零拷贝

### P2 - 内存优化

9. IntSet 紧凑整数集合
10. Listpack 紧凑列表
11. CompactString 紧凑字符串

### P3 - 高级特性

12. Hash 字段过期
13. SORT 命令
14. DUMP/RESTORE 序列化