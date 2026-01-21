---
name: Redis 8.4.0 深度对比
overview: 深入对比 CysRedis 与 Redis 8.4.0 在内存管理、GC 处理、性能优化等方面的差距，提供完整的优化方案。
todos:
  - id: server-cron
    content: 实现 ServerCron 定时任务（100ms 周期维护）
    status: completed
  - id: memory-prefetch
    content: 实现 MemoryPrefetch CPU 缓存预取
    status: completed
  - id: lazy-free
    content: 实现 LazyFreeManager 惰性删除
    status: completed
  - id: gc-tuning
    content: 配置服务器 GC 和低延迟模式
    status: completed
  - id: kvstore-sharding
    content: 实现 KvStore 分片字典（16384 槽位）
    status: completed
  - id: shared-objects
    content: 实现共享对象池（0-9999 整数）
    status: completed
  - id: batch-write
    content: 增强 RespPipeWriter 批量写入
    status: completed
  - id: memory-tracker
    content: 实现多线程内存统计 MemoryTracker
    status: completed
---

# CysRedis 与 Redis 8.4.0 深度对比及优化方案

## 已完成的改进总结

### 1. 新命令实现（已完成✅）

- **SET 命令 Redis 8.4.0 新特性**: IFEQ/IFNE/IFDEQ/IFDNE 条件设置
- **SMISMEMBER**: 批量成员检查
- **LPOS**: 列表元素位置查找（支持 RANK/COUNT/MAXLEN）
- **COPY**: 键复制（支持 REPLACE/DB 参数）
- **ZMSCORE**: 批量获取有序集合分数
- **Hash 字段过期**: HEXPIRE/HPEXPIRE/HTTL/HPTTL/HPERSIST

### 2. 内存优化（已完成✅）

- **LRU 时钟优化**: 24 位时钟，1 秒分辨率
- **对象池**: ObjectPool<T> 减少 GC 压力
- **零拷贝**: RedisString 使用 Memory<byte>
- **IntSet**: 紧凑整数集合（16/32/64 位自动升级）
- **Listpack**: 紧凑列表编码
- **CompactString**: 类 SDS 的紧凑字符串（5 种头大小）

---

## 一、内存管理核心差距

### 1. 内存分配器集成 ⚠️ 高优先级

**Redis 8.4.0 实现:**

```c
// zmalloc.c - 集成 jemalloc/tcmalloc
void *zmalloc_usable(size_t size, size_t *usable) {
    void *ptr = je_malloc_with_usize(size, &usable);
    update_zmalloc_stat_alloc(*usable);  // 跟踪每次分配
    return ptr;
}

// 多线程内存统计（避免锁竞争）
static used_memory_entry used_memory[MAX_THREADS]; // 每线程独立计数器
```

**CysRedis 现状:**

- 使用 .NET GC 托管堆
- 无法精确控制内存分配位置
- 无法获取实际分配大小（malloc_usable_size）
- 无多线程内存统计

**影响:**

- 内存碎片无法精确测量
- 无法实现主动内存碎片整理
- 内存统计不够精确

**解决方案:**

```csharp
// 新文件: src/CysRedis.Core/Memory/MemoryTracker.cs
public static class MemoryTracker
{
    // 每线程内存统计（避免锁竞争）
    [ThreadStatic] private static long _threadMemory;
    private static long[] _perThreadMemory = new long[Environment.ProcessorCount];
    
    public static void TrackAllocation(long size)
    {
        var threadId = Environment.CurrentManagedThreadId % _perThreadMemory.Length;
        Interlocked.Add(ref _perThreadMemory[threadId], size);
    }
    
    public static long GetTotalMemory()
    {
        return _perThreadMemory.Sum();
    }
    
    // 周期性与 GC 实际内存同步
    public static void SyncWithGC()
    {
        var gcMemory = GC.GetTotalMemory(false);
        var tracked = GetTotalMemory();
        // 调整偏差...
    }
}
```

### 2. 惰性删除（Lazy Free）⚠️ 高优先级

**Redis 8.4.0 实现:**

```c
// lazyfree.c - 后台线程删除大对象
void lazyfreeFreeObject(void *args[]) {
    robj *o = (robj *) args[0];
    decrRefCount(o);  // 在后台线程释放
}

// 根据对象大小决定是否异步删除
size_t lazyfreeGetFreeEffort(robj *key, robj *obj, int dbid) {
    if (obj->type == OBJ_LIST && encoding == QUICKLIST) {
        return ql->len;  // 列表节点数
    }
    // ... 其他类型的复杂度计算
}

// DEL 命令自动使用后台删除
if (effort > LAZYFREE_THRESHOLD) {
    bioCreateLazyFreeJob(lazyfreeFreeObject, 1, obj);
}
```

**CysRedis 现状:**

- 直接同步删除，阻塞主线程
- 大集合/大列表删除会造成延迟尖峰
- 无后台 GC 线程

**解决方案:**

```csharp
// 新文件: src/CysRedis.Core/Memory/LazyFreeManager.cs
public class LazyFreeManager
{
    private readonly Channel<Action> _freeQueue;
    private const int FreeEffortThreshold = 64; // 元素数超过64异步释放
    
    public void QueueFree(RedisObject obj)
    {
        var effort = EstimateFreeEffort(obj);
        if (effort > FreeEffortThreshold)
        {
            _freeQueue.Writer.TryWrite(() => {
                // 在后台线程释放
                if (obj is IDisposable d) d.Dispose();
            });
        }
        else
        {
            // 小对象同步释放
            if (obj is IDisposable d) d.Dispose();
        }
    }
    
    private int EstimateFreeEffort(RedisObject obj) => obj switch
    {
        RedisList l => l.Count,
        RedisSet s => s.Count,
        RedisSortedSet z => z.Count,
        RedisHash h => h.Count,
        _ => 1
    };
}
```

### 3. 内存碎片整理（Defragmentation）⚠️ 中优先级

**Redis 8.4.0 实现:**

```c
// defrag.c - 主动内存碎片整理
int je_get_defrag_hint(void* ptr);  // jemalloc 特性

void *activeDefragAlloc(void *ptr) {
    if (je_get_defrag_hint(ptr)) {
        // 重新分配到更好的位置
        void *newptr = zmalloc(zmalloc_size(ptr));
        memcpy(newptr, ptr, zmalloc_size(ptr));
        zfree(ptr);
        return newptr;
    }
    return ptr;
}
```

**CysRedis 现状:**

- .NET GC 自动管理碎片
- 无主动碎片整理能力
- 依赖 GC 的 Gen2 压缩

**评估:**

- .NET 环境下无需实现（GC 已优化）
- 可通过 `GCSettings.LargeObjectHeapCompactionMode` 控制
- 定期调用 `GC.Collect(2, GCCollectionMode.Optimized)` 即可

### 4. 内存预取（Memory Prefetch）⚠️ 高优先级

**Redis 8.4.0 实现:**

```c
// memory_prefetch.c - 批量命令预取
void prefetchCommands(void) {
    // 1. 预取命令参数
    for (size_t i = 0; i < batch->client_count; i++) {
        for (int j = 1; j < c->argc; j++) {
            redis_prefetch_read(c->argv[j]);  // SSE PREFETCH 指令
        }
    }
    
    // 2. 预取字典键
    dictPrefetch(batch->keys_dicts, getObjectValuePtr);
}

// 使用 SSE 指令
#define redis_prefetch_read(addr) _mm_prefetch((addr), _MM_HINT_T0)
```

**CysRedis 现状:**

- 无批量命令预取
- 无 CPU 缓存预取优化
- 依赖 CPU 自动预取

**解决方案:**

```csharp
// 新文件: src/CysRedis.Core/Memory/MemoryPrefetch.cs
using System.Runtime.Intrinsics.X86;

public static class MemoryPrefetch
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe void PrefetchRead(void* address)
    {
        if (Sse.IsSupported)
            Sse.Prefetch0(address);
    }
    
    public static void PrefetchKeys(RedisDatabase db, string[] keys)
    {
        // Pipeline 模式批量预取
        foreach (var key in keys)
        {
            // 触发字典查找预取
            _ = db.Get(key);
        }
    }
}
```

---

## 二、GC 优化策略

### 1. 服务器 GC 模式配置

**优化方案:**

```csharp
// Program.cs 启动时配置
public static void Main(string[] args)
{
    // 1. 启用服务器 GC（多线程并行 GC）
    GCSettings.IsServerGC = true;
    
    // 2. 降低 GC 延迟优先级
    GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
    
    // 3. 配置大对象堆压缩
    GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
    
    var server = new RedisServer(options);
    server.Start();
}
```

### 2. 定期 GC 调优任务

**优化方案:**

```csharp
// 新增到 RedisServer.cs
private Task? _gcTuningTask;

private async Task RunGcTuningAsync()
{
    while (!_cts.Token.IsCancellationRequested)
    {
        await Task.Delay(TimeSpan.FromMinutes(5), _cts.Token);
        
        // 1. 检查内存使用
        var gen0 = GC.CollectionCount(0);
        var gen1 = GC.CollectionCount(1);
        var gen2 = GC.CollectionCount(2);
        
        // 2. Gen2 过多时主动触发压缩
        if (gen2 > 100 && (DateTime.UtcNow - _lastCompaction).TotalMinutes > 10)
        {
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(2, GCCollectionMode.Optimized);
            _lastCompaction = DateTime.UtcNow;
        }
    }
}
```

### 3. 对象复用策略

**已实现 ObjectPool<T>**，需进一步应用：

```csharp
// 应用对象池到命令执行
public class MGetCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        // 从池中租用列表
        var resultList = ObjectPool<List<RespValue>>.Rent();
        try
        {
            // 使用 resultList...
        }
        finally
        {
            // 清空并归还
            resultList.Clear();
            ObjectPool<List<RespValue>>.Return(resultList);
        }
    }
}
```

---

## 三、Redis 对象结构差距

### 1. kvobj 嵌入式对象 ⚠️ 高优先级

**Redis 8.4.0 实现:**

Redis 使用 `kvobj` 将键、过期时间、值嵌入到一个连续内存块：

```
+--------------+--------------+--------------+--------------------+
| robj (16B)   | Expiry (8B)  | key-hdr (1B) | sdshdr5 "key" (7B) |
+--------------+--------------+--------------+--------------------+
```

**优点:**

- 一次分配包含键+值+元数据
- 缓存局部性好
- 减少指针跳转

**CysRedis 现状:**

- `ConcurrentDictionary<string, RedisObject>` - 键和值分离
- `ConcurrentDictionary<string, DateTime>` - 过期时间分离
- 3 次内存分配，3 次指针跳转

**解决方案:**

```csharp
// 新文件: src/CysRedis.Core/DataStructures/KvObject.cs
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct KvObject
{
    public uint Lru;              // 24 bits LRU + 8 bits refcount
    public DateTime Expiry;       // 8 bytes
    public CompactString Key;     // 变长
    public RedisObject Value;     // 变长
    
    // 使用非托管内存分配
    public static unsafe KvObject* Create(string key, RedisObject value, DateTime? expiry)
    {
        var keyBytes = Encoding.UTF8.GetBytes(key);
        var size = sizeof(KvObject) + keyBytes.Length + EstimateValueSize(value);
        
        var ptr = (KvObject*)NativeMemory.Alloc((nuint)size);
        // 初始化结构...
        return ptr;
    }
}
```

**注意:** 使用非托管内存需要手动管理生命周期，权衡利弊后建议保持当前方案。

### 2. 共享对象（Shared Objects）

**Redis 8.4.0 实现:**

```c
// 预分配常用对象
struct sharedObjectsStruct {
    robj *ok, *err, *emptybulk, *czero, *cone;
    robj *integers[OBJ_SHARED_INTEGERS];  // -1 到 9999
    robj *mbulkhdr[OBJ_SHARED_BULKHDR_LEN];
};
```

**CysRedis 现状:**

- RespValue 有静态常量（Ok/Pong/Zero/One）
- 但 RedisString 每次都创建新对象

**优化方案:**

```csharp
// 扩展 RespValue 共享对象
public static class SharedObjects
{
    // 预分配 0-9999 的整数字符串
    private static readonly RedisString[] _sharedIntegers;
    
    static SharedObjects()
    {
        _sharedIntegers = new RedisString[10000];
        for (int i = 0; i < 10000; i++)
        {
            _sharedIntegers[i] = new RedisString(i.ToString());
        }
    }
    
    public static RedisString? GetSharedInteger(long value)
    {
        if (value >= 0 && value < 10000)
            return _sharedIntegers[value];
        return null;
    }
}

// 在 IncrCommand 中使用
var newValue = currentValue + increment;
var sharedStr = SharedObjects.GetSharedInteger(newValue);
db.Set(key, sharedStr ?? new RedisString(newValue.ToString()));
```

---

## 四、kvstore 分片字典

**Redis 8.4.0 实现:**

```c
// kvstore - 槽位分片字典
typedef struct _kvstore {
    dict **dicts;          // 字典数组（16384 个槽位）
    int num_dicts_bits;    // 字典数量的位数
    int flags;
} kvstore;

// 优势：
// 1. 集群模式每个槽位独立字典
// 2. Rehash 只影响单个槽位
// 3. 并发访问不同槽位无锁
```

**CysRedis 现状:**

- 单个 `ConcurrentDictionary` 存储所有键
- 集群模式下需要全局锁
- Rehash 影响所有键

**优化方案:**

```csharp
// 新文件: src/CysRedis.Core/DataStructures/KvStore.cs
public class KvStore
{
    private readonly ConcurrentDictionary<string, RedisObject>[] _slots;
    private const int SlotBits = 14; // 16384 slots
    private const int NumSlots = 1 << SlotBits;
    
    public KvStore()
    {
        _slots = new ConcurrentDictionary<string, RedisObject>[NumSlots];
        for (int i = 0; i < NumSlots; i++)
        {
            _slots[i] = new ConcurrentDictionary<string, RedisObject>();
        }
    }
    
    public RedisObject? Get(string key)
    {
        var slot = Cluster.ClusterManager.GetSlot(key);
        return _slots[slot].TryGetValue(key, out var value) ? value : null;
    }
    
    // 优势：每个槽位独立锁，减少锁竞争
}
```

---

## 五、事件循环架构差距

### 1. ae 事件驱动模型

**Redis 8.4.0 实现:**

```c
// ae.c - 高性能事件循环
aeEventLoop *aeCreateEventLoop(int setsize) {
    // epoll/kqueue/evport/select 多路复用
    eventLoop->events = zmalloc(sizeof(aeFileEvent)*setsize);
    aeApiCreate(eventLoop);  // 平台特定实现
}

// serverCron - 定时任务（100ms）
int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    // 1. 更新 LRU 时钟
    server.lruclock = getLRUClock();
    
    // 2. 增量 rehash
    for (j = 0; j < dbs_per_call; j++) {
        int work_done = kvstoreIncrementallyRehash(server.db[db].keys, 100);
    }
    
    // 3. 主动过期检查
    activeExpireCycle(ACTIVE_EXPIRE_CYCLE_SLOW);
}
```

**CysRedis 现状:**

- async/await 异步模型
- 无统一事件循环
- 无定时任务机制（serverCron）

**优化方案:**

```csharp
// 新文件: src/CysRedis.Core/Threading/ServerCron.cs
public class ServerCron
{
    private readonly RedisServer _server;
    private const int IntervalMs = 100; // 100ms
    
    public Task Start(CancellationToken cancellationToken)
    {
        return Task.Run(async () =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var startTime = Stopwatch.GetTimestamp();
                
                // 1. 更新 LRU 时钟
                LruClock.UpdateClock();
                
                // 2. 清理过期键（每次处理部分）
                foreach (var db in _server.Store.GetAllDatabases())
                {
                    db.CleanupExpired(); // 限制每次清理数量
                }
                
                // 3. 触发 GC 调优（如果需要）
                TuneGcIfNeeded();
                
                // 4. 更新统计信息
                _server.NetworkMetrics.UpdateRates();
                
                var elapsed = (Stopwatch.GetTimestamp() - startTime) * 1000 / Stopwatch.Frequency;
                var remaining = IntervalMs - (int)elapsed;
                if (remaining > 0)
                    await Task.Delay(remaining, cancellationToken);
            }
        }, cancellationToken);
    }
    
    private void TuneGcIfNeeded()
    {
        // 根据内存压力动态调整
        var memory = GC.GetTotalMemory(false);
        var maxMemory = _server.Options.MaxMemory;
        
        if (maxMemory > 0 && memory > maxMemory * 0.95)
        {
            GC.Collect(1, GCCollectionMode.Optimized, false);
        }
    }
}
```

---

## 六、网络 I/O 优化差距

### 1. I/O 线程写入批处理

**Redis 8.4.0 实现:**

```c
// iothread.c - 批量写入优化
int IOThreadBeforeSleep(IOThread *t) {
    // 批量处理客户端写入
    listIter li;
    client *c;
    listRewind(t->clients_pending_write, &li);
    while ((c = listNext(&li)) != NULL) {
        if (c->bufpos > 0 || listLength(c->reply) > 0)
            writeToClient(c, 0);  // 批量刷新
    }
}
```

**CysRedis 现状:**

- 每个命令独立 FlushAsync
- 无批量写入缓冲
- Pipeline 效率不够高

**优化方案:**

```csharp
// 增强 RespPipeWriter.cs
public class RespPipeWriter
{
    private bool _batchMode;
    
    public void BeginBatch() => _batchMode = true;
    
    public async ValueTask EndBatch(CancellationToken ct)
    {
        _batchMode = false;
        await FlushAsync(ct);
    }
    
    // WriteValue 不自动 flush
    public void WriteValue(in RespValue value)
    {
        // ... 写入逻辑
        if (!_batchMode)
            FlushAsync().GetAwaiter().GetResult(); // 同步刷新
    }
}

// 在 Pipeline 执行时使用
writer.BeginBatch();
foreach (var cmd in pipeline)
{
    await ExecuteCommand(cmd);
}
await writer.EndBatch(ct);
```

### 2. 零拷贝发送（Scatter-Gather I/O）

**Redis 8.4.0 实现:**

```c
// networking.c - 使用 writev 批量发送
ssize_t _writeToClient(client *c, ssize_t *nwritten) {
    struct iovec iov[IOV_MAX];
    int iovcnt = 0;
    
    // 批量发送多个缓冲区（零拷贝）
    nwritten = writev(c->fd, iov, iovcnt);
}
```

**CysRedis 现状:**

- Socket.SendAsync 单次发送
- 无 scatter-gather I/O

**.NET 优化方案:**

```csharp
// 使用 Socket.SendAsync(IList<ArraySegment<byte>>)
public async ValueTask FlushBatchAsync(List<ArraySegment<byte>> buffers)
{
    await _socket.SendAsync(buffers, SocketFlags.None);
}
```

---

## 七、进一步的性能优化建议

### 1. 使用 ArrayPool 代替 new byte[]

**当前问题:**

很多地方还在使用 `new byte[]` 分配数组。

**优化:**

```csharp
// 所有命令中使用 ArrayPool
var buffer = ArrayPool<byte>.Shared.Rent(size);
try {
    // 使用 buffer
} finally {
    ArrayPool<byte>.Shared.Return(buffer);
}
```

### 2. String Intern 优化

**优化方案:**

```csharp
// 对频繁出现的键使用 String.Intern
public class RedisDatabase
{
    private static readonly StringPool _keyPool = new(maxSize: 10000);
    
    public void Set(string key, RedisObject value)
    {
        // 复用字符串对象
        var internedKey = _keyPool.GetOrAdd(key);
        _data[internedKey] = value;
    }
}

class StringPool
{
    private readonly LruCache<string, string> _cache;
    
    public string GetOrAdd(string str)
    {
        if (_cache.TryGet(str, out var cached))
            return cached;
        
        var interned = string.Copy(str); // 创建独立副本
        _cache.Add(str, interned);
        return interned;
    }
}
```

### 3. 减少 LINQ 分配

**优化前:**

```csharp
var results = positions.Select(p => new RespValue(p)).ToArray();
```

**优化后:**

```csharp
var results = new RespValue[positions.Count];
for (int i = 0; i < positions.Count; i++)
{
    results[i] = new RespValue(positions[i]);
}
```

---

## 八、功能完整性检查

### 缺失的 Redis 8.4.0 特性

| 功能 | Redis 8.4.0 | CysRedis | 优先级 |

|------|-------------|----------|--------|

| RESP3 协议完整支持 | ✅ Map/Set/Attribute | ⚠️ 部分支持 | P1 |

| Client-side caching | ✅ CLIENT TRACKING | ❌ 未实现 | P2 |

| Modules 系统 | ✅ 动态加载 | ❌ 未实现 | P3 |

| FUNCTION (Lua 库) | ✅ 持久化函数 | ⚠️ 仅 EVAL | P1 |

| Hash 字段过期 | ✅ 完整支持 | ✅ 已实现 | ✅ |

| Stream XAUTOCLAIM | ✅ 完整 | ✅ 已实现 | ✅ |

### 命令完整性对比

**仍缺失的命令:**

```
String: LCS (最长公共子序列)
List: LMPOP (多列表弹出)
Set: SINTERCARD (交集基数)
ZSet: ZMPOP, ZRANGESTORE, ZINTERCARD, ZDIFF, ZDIFFSTORE, ZRANDMEMBER
通用: SORT, DUMP/RESTORE, OBJECT ENCODING/FREQ/IDLETIME, MEMORY USAGE
```

---

## 九、实施建议优先级

### P0 - 立即实施（已完成）

✅ SET IFEQ/IFNE/IFDEQ/IFDNE

✅ SMISMEMBER, LPOS, COPY, ZMSCORE

✅ LRU 时钟优化

✅ 对象池

✅ IntSet/Listpack/CompactString

### P1 - 性能关键

1. **ServerCron 定时任务**

   - 文件: `src/CysRedis.Core/Threading/ServerCron.cs`
   - 100ms 周期执行维护任务

2. **MemoryPrefetch 预取**

   - 文件: `src/CysRedis.Core/Memory/MemoryPrefetch.cs`
   - Pipeline 批量命令预取

3. **LazyFreeManager 惰性删除**

   - 文件: `src/CysRedis.Core/Memory/LazyFreeManager.cs`
   - 大对象后台删除

4. **GC 调优配置**

   - 文件: `src/CysRedis.Server/Program.cs`
   - 启用服务器 GC + 低延迟模式

### P2 - 架构优化

5. **KvStore 分片字典**

   - 文件: `src/CysRedis.Core/DataStructures/KvStore.cs`
   - 16384 槽位分片减少锁竞争

6. **共享对象池**

   - 文件: `src/CysRedis.Core/Protocol/SharedObjects.cs`
   - 预分配常用整数/字符串

7. **批量写入缓冲**

   - 文件: `src/CysRedis.Core/Protocol/RespPipeWriter.cs`
   - BeginBatch/EndBatch API

### P3 - 功能补全

8. RESP3 完整支持（Map/Set/Attribute 类型）
9. CLIENT TRACKING 客户端缓存
10. 剩余缺失命令（LCS/LMPOP/SORT 等）

---

## 十、内存优化效果预估

| 优化项 | 预期收益 |

|--------|----------|

| IntSet 整数集合 | 内存减少 80%+ |

| Listpack 列表 | 内存减少 60-70% |

| CompactString | 内存减少 50-60% |

| 共享对象 | 常用值内存减少 100% |

| KvStore 分片 | 锁竞争减少 90%+ |

| 对象池 | GC 压力减少 50%+ |

| LazyFree | 大对象删除延迟降低 95%+ |

| 内存预取 | Pipeline 吞吐提升 20-30% |

---

## 总结

CysRedis 已实现：

- ✅ 核心命令完整性 95%+
- ✅ LRU/LFU 内存淘汰策略
- ✅ 紧凑数据结构（IntSet/Listpack/CompactString）
- ✅ 多线程 I/O

仍需优化：

- ⚠️ 惰性删除（避免大对象阻塞）
- ⚠️ 内存预取（Pipeline 性能）
- ⚠️ ServerCron 定时维护
- ⚠️ KvStore 分片字典（集群性能）
- ⚠️ GC 调优配置

**关键区别:**

Redis 使用 C 语言手动内存管理 + jemalloc，CysRedis 使用 .NET GC。这是架构级差异，但通过：

1. 对象池减少分配
2. Span/Memory 零拷贝  
3. 服务器 GC 模式
4. 紧凑数据结构

可以接近原生 Redis 的内存性能。