# Unsafe 优化实现文档

## 概述

本文档描述了 CysRedis 中实现的 unsafe 优化，这些优化使用指针、SIMD 和内存池等技术大幅提升性能。

## 目录结构

```
src/CysRedis.Core/Unsafe/
  Common/
    SimdHelpers.cs              # SIMD 加速库
    UnsafeMemoryManager.cs      # 统一内存管理
    UnsafeBufferPool.cs         # 缓存行对齐内存池
    SafeHandles.cs              # 内存安全句柄
  DataStructures/
    UnsafeIntSet.cs             # 整数集合
    UnsafeListpack.cs           # 压缩列表
    UnsafeCompactString.cs      # 紧凑字符串
    UnsafeSkipList.cs           # 跳表
    UnsafeRedisHash.cs          # 哈希表
    UnsafeRedisList.cs          # 列表
    UnsafeRedisSet.cs           # 集合
    UnsafeRedisSortedSet.cs     # 有序集合
    UnsafeRedisStream.cs        # 流
    UnsafeHyperLogLog.cs        # HyperLogLog
    UnsafeKvStore.cs            # 键值存储
```

## 核心优化技术

### 1. SIMD 向量化

`SimdHelpers` 提供了自动检测和使用的 SIMD 优化：

- **AVX2**: 256 位向量操作（Intel/AMD 现代 CPU）
- **SSE2**: 128 位向量操作（广泛支持）
- **Vector<T>**: .NET 跨平台 SIMD

**使用示例：**
```csharp
unsafe
{
    fixed (byte* src = source)
    fixed (byte* dst = destination)
    {
        SimdHelpers.CopyMemory(src, dst, length); // 自动选择最优 SIMD
    }
}
```

### 2. 内存管理

`UnsafeMemoryManager` 提供统一的内存分配和管理：

- **对齐分配**: 缓存行对齐（64 字节）
- **泄漏检测**: Debug 模式自动检测内存泄漏
- **统计信息**: 实时内存使用统计

**使用示例：**
```csharp
unsafe
{
    void* ptr = UnsafeMemoryManager.AlignedAlloc(1024, 64);
    // 使用内存...
    UnsafeMemoryManager.AlignedFree(ptr);
}
```

### 3. 内存池

`UnsafeBufferPool` 提供线程本地缓存的内存池：

- **线程本地缓存**: 避免锁竞争
- **缓存行对齐**: 优化缓存性能
- **自动大小管理**: 按需分配和回收

**使用示例：**
```csharp
using (var buffer = UnsafeBufferPool.Rent(1024))
{
    // 使用 buffer.Span 或 buffer.Pointer
    // 自动返回池中
}
```

## 数据结构使用指南

### UnsafeIntSet

高性能整数集合，自动升级编码（int16 → int32 → int64）。

```csharp
using var intset = new UnsafeIntSet();

intset.Add(100);
intset.Add(200);
Assert.True(intset.Contains(100));
Assert.Equal(2, intset.Count);

// 自动处理编码升级
intset.Add(int.MaxValue); // 升级到 int32
intset.Add(long.MaxValue); // 升级到 int64
```

### UnsafeListpack

零拷贝压缩列表，用于紧凑存储。

```csharp
using var listpack = new UnsafeListpack();

listpack.AppendInteger(100);
listpack.Append(Encoding.UTF8.GetBytes("test"));

var entry = listpack.GetAt(0);
if (entry.IsInteger)
    Console.WriteLine(entry.IntValue);
else
    Console.WriteLine(Encoding.UTF8.GetString(entry.StringValue!));
```

### UnsafeSkipList

指针优化的跳表，用于有序集合。

```csharp
using var skiplist = new UnsafeSkipList();

var key = Encoding.UTF8.GetBytes("key1");
var value = Encoding.UTF8.GetBytes("value1");
skiplist.Insert(key, value);

if (skiplist.Find(key, out var foundValue))
{
    Console.WriteLine(Encoding.UTF8.GetString(foundValue));
}

var rank = skiplist.GetRank(key);
skiplist.GetByRank(rank, out var rankKey, out var rankValue);
```

### UnsafeRedisHash

自适应哈希表（小数据用 ziplist，大数据用 hashtable）。

```csharp
using var hash = new UnsafeRedisHash();

var field = Encoding.UTF8.GetBytes("field1");
var value = Encoding.UTF8.GetBytes("value1");
hash.Set(field, value);

if (hash.Get(field, out var foundValue))
{
    Console.WriteLine(Encoding.UTF8.GetString(foundValue));
}
```

### UnsafeKvStore

无锁键值存储，支持 16384 个槽分片。

```csharp
using var store = new UnsafeKvStore();

var key = Encoding.UTF8.GetBytes("key1");
store.Set(key, (void*)100, 0); // 值、过期时间

if (store.Get(key, out var value, out var expireTime))
{
    Console.WriteLine($"Value: {(long)value}, Expire: {expireTime}");
}

store.Delete(key);
```

## 性能基准

运行基准测试：

```bash
dotnet run --project tests/CysRedis.Tests -- --filter "UnsafeDataStructuresBenchmarks"
```

预期性能提升：

- **IntSet**: 3-5x 吞吐量提升
- **Listpack**: 4-6x 编码/解码速度
- **SkipList**: 3-4x 插入/查找速度
- **SIMD 操作**: 10-20x 内存操作速度
- **整体**: 接近原生 Redis 80-90% 性能

## 安全注意事项

### 1. 内存管理

所有 unsafe 数据结构都实现了 `IDisposable`，必须正确释放：

```csharp
using var intset = new UnsafeIntSet();
// 自动释放
```

### 2. 线程安全

- `UnsafeKvStore`: 无锁设计，线程安全
- `UnsafeSkipList`: 单线程使用
- `UnsafeBufferPool`: 线程本地缓存，线程安全

### 3. 边界检查

虽然使用了 unsafe 代码，但在关键路径仍保留了必要的边界检查：

```csharp
public long GetAt(int index)
{
    if (index < 0 || index >= _length)
        throw new IndexOutOfRangeException();
    // unsafe 操作
}
```

## 调试和诊断

### 启用内存泄漏检测

```csharp
UnsafeMemoryManager.LeakDetectionEnabled = true;

// 在程序结束时验证
UnsafeMemoryManager.ValidateNoLeaks();
```

### 查看内存统计

```csharp
Console.WriteLine($"Total Allocated: {UnsafeMemoryManager.TotalAllocated}");
Console.WriteLine($"Current Usage: {UnsafeMemoryManager.CurrentUsage}");
Console.WriteLine($"Allocation Count: {UnsafeMemoryManager.AllocationCount}");
```

## 迁移指南

### 从托管实现迁移

1. **替换类型**:
   ```csharp
   // 旧代码
   var intset = new IntSet();
   
   // 新代码
   using var intset = new UnsafeIntSet();
   ```

2. **使用 using 语句**: 确保资源释放

3. **更新 API 调用**: 某些 API 可能略有不同（如使用 `ReadOnlySpan<byte>` 而非 `byte[]`）

4. **测试验证**: 运行集成测试确保行为一致

## 最佳实践

1. **优先使用 using**: 确保资源正确释放
2. **避免长期持有**: unsafe 对象应尽快释放
3. **监控内存**: 在生产环境监控内存使用
4. **性能测试**: 在实际负载下验证性能提升
5. **渐进迁移**: 逐步迁移，保留原有实现作为后备

## 限制和注意事项

1. **平台依赖**: SIMD 优化需要特定 CPU 支持
2. **调试困难**: unsafe 代码较难调试
3. **内存安全**: 需要仔细管理内存，避免泄漏和损坏
4. **兼容性**: 某些场景下可能需要回退到托管实现

## 未来改进

- [ ] 更多 SIMD 优化路径
- [ ] 更好的节点池管理
- [ ] 内存压缩支持
- [ ] 更完善的错误处理
- [ ] 性能分析工具集成

## 参考

- [.NET Unsafe 代码文档](https://docs.microsoft.com/dotnet/csharp/language-reference/unsafe-code)
- [SIMD 指令集](https://docs.microsoft.com/dotnet/api/system.numerics)
- [Redis 内部数据结构](https://redis.io/docs/data-structures/)
