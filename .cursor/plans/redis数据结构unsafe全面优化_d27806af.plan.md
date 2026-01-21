---
name: Redis数据结构unsafe全面优化
overview: 对 CysRedis 的所有核心数据结构进行激进的 unsafe 优化,使用指针、stackalloc、固定内存等技术最大化性能,同时保持公共 API 兼容性。目标是达到接近原生 Redis 的性能水平。
todos:
  - id: skiplist_unsafe
    content: 实现 UnsafeSkipList - 使用非托管内存和指针优化跳表
    status: completed
  - id: listpack_unsafe
    content: 实现 UnsafeListpack - 零拷贝编码解码优化
    status: completed
  - id: intset_unsafe
    content: 实现 UnsafeIntSet - SIMD 优化整数集合操作
    status: completed
  - id: compactstring_unsafe
    content: 实现 UnsafeCompactString - SDS 优化和 stackalloc 小字符串
    status: completed
  - id: hash_unsafe
    content: 实现 UnsafeRedisHash - ziplist/hashtable 混合编码
    status: completed
  - id: list_unsafe
    content: 实现 UnsafeRedisList - quicklist 结构优化
    status: completed
  - id: set_unsafe
    content: 实现 UnsafeRedisSet - intset/hashtable 自适应编码
    status: completed
  - id: sortedset_unsafe
    content: 实现 UnsafeRedisSortedSet - 优化的 skiplist+hashtable
    status: completed
  - id: stream_unsafe
    content: 实现 UnsafeRedisStream - radix tree + listpack 优化
    status: completed
  - id: hll_unsafe
    content: 实现 UnsafeHyperLogLog - SIMD 优化寄存器计算
    status: completed
  - id: respwriter_unsafe
    content: 优化 RespPipeWriter - stackalloc 和 SIMD 加速编码
    status: completed
  - id: bufferpool_unsafe
    content: 实现 UnsafeBufferPool - 缓存行对齐内存池
    status: completed
  - id: kvstore_unsafe
    content: 实现 UnsafeKvStore - 无锁哈希表优化
    status: completed
  - id: simd_helpers
    content: 创建 SimdHelpers 通用 SIMD 加速库
    status: completed
  - id: memory_manager
    content: 实现 UnsafeMemoryManager 统一内存管理
    status: completed
  - id: safe_handles
    content: 创建 SafeHandle 包装防止内存泄漏
    status: completed
  - id: benchmarks
    content: 创建性能基准测试套件对比优化效果
    status: completed
  - id: integration_tests
    content: 验证 unsafe 实现的正确性和兼容性
    status: completed
---

# Redis 数据结构 unsafe 全面优化计划

## 优化策略概览

采用激进的 unsafe 优化策略,在保持 API 兼容的前提下,重写所有数据结构的内部实现。主要技术包括:

- **指针直接操作** - 绕过边界检查和托管内存开销
- **栈内存分配** (stackalloc) - 小对象使用栈内存避免 GC
- **固定内存块** (pinned memory) - 大对象固定避免复制
- **SIMD 优化** - 向量化处理提升吞吐
- **内存对齐** - 优化缓存行访问
- **内联关键方法** - 减少调用开销

## 第一阶段:基础数据结构优化 (核心热点路径)

### 1.1 SkipList 指针化重构

**当前问题:**

- [`SkipList.cs:355-370`](d:\Code\CyscaleDB\src\CysRedis.Core\DataStructures\SkipList.cs) 使用引用类型节点,大量对象分配
- 链表遍历涉及多次指针追踪,缓存不友好
- Level 数组每个节点单独分配

**优化方案:**

创建 `UnsafeSkipList<TKey, TValue>` 内部实现:

```csharp
// 使用非托管内存块存储所有节点
unsafe struct SkipListNode
{
    public fixed byte Key[256];      // 内联键数据
    public fixed long Span[32];      // 内联 span 数组
    public SkipListNode** Forward;   // 指针数组
    public SkipListNode* Backward;   // 反向指针
    public int KeyLength;
    public int Level;
}

// 节点池使用大块连续内存
private void* _nodePool;
private int _nodePoolSize;
private int _nodePoolCapacity;
```

**关键优化点:**

- 使用 `NativeMemory.Alloc/Free` 管理节点内存
- 节点在连续内存块中,提升缓存局部性
- 短键直接内联存储,避免额外分配
- 使用指针数组替代引用数组

### 1.2 Listpack 零拷贝优化

**当前问题:**

- [`Listpack.cs:1-337`](d:\Code\CyscaleDB\src\CysRedis.Core\DataStructures\Listpack.cs) 编码/解码有大量边界检查
- 数组扩容涉及复制开销

**优化方案:**

```csharp
unsafe class UnsafeListpack
{
    private byte* _buffer;         // 非托管内存
    private int _capacity;
    private int _length;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long DecodeIntegerFast(byte* ptr)
    {
        // 使用指针直接读取,无边界检查
        byte firstByte = *ptr;
        if ((firstByte & 0x80) == 0)
            return firstByte;
        
        // 使用 Unsafe.ReadUnaligned 快速读取
        if (firstByte == 0xF4)
            return Unsafe.ReadUnaligned<long>(ptr + 1);
        // ... 其他编码
    }
    
    // 使用 SIMD 加速批量操作
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CopyMemoryFast(byte* src, byte* dst, int length)
    {
        if (Vector.IsHardwareAccelerated && length >= 32)
        {
            // 使用 SIMD 批量复制
            // ...
        }
        else
        {
            Buffer.MemoryCopy(src, dst, length, length);
        }
    }
}
```

### 1.3 IntSet 位操作优化

**当前问题:**

- [`IntSet.cs:1-300`](d:\Code\CyscaleDB\src\CysRedis.Core\DataStructures\IntSet.cs) 二分查找和插入涉及多次边界检查
- 数组扩容需要复制

**优化方案:**

```csharp
unsafe class UnsafeIntSet
{
    private void* _data;
    private int _encoding;  // 2/4/8 bytes
    private int _length;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long GetValueUnsafe(int index)
    {
        byte* ptr = (byte*)_data + index * _encoding;
        return _encoding switch
        {
            2 => *(short*)ptr,
            4 => *(int*)ptr,
            8 => *(long*)ptr,
            _ => 0
        };
    }
    
    // 使用 SIMD 优化二分查找
    private int BinarySearchSIMD(long value)
    {
        if (Vector256.IsHardwareAccelerated && _encoding == 8 && _length >= 4)
        {
            // 使用 AVX2 并行比较
            // ...
        }
        return BinarySearchStandard(value);
    }
}
```

### 1.4 CompactString SDS 优化

**当前问题:**

- [`CompactString.cs:1-288`](d:\Code\CyscaleDB\src\CysRedis.Core\DataStructures\CompactString.cs) 头部和数据分离存储
- 字符串操作有托管开销

**优化方案:**

```csharp
unsafe struct UnsafeCompactString
{
    private byte* _data;      // 头部+数据连续存储
    private int _allocSize;
    
    // 头部直接内联在数据前
    // [len:4][alloc:4][flags:1][data...]
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> AsSpan()
    {
        int len = *(int*)_data;
        return new ReadOnlySpan<byte>(_data + 9, len);
    }
    
    // 使用 stackalloc 优化小字符串
    public static UnsafeCompactString CreateSmall(ReadOnlySpan<byte> data)
    {
        Span<byte> stack = stackalloc byte[64];
        if (data.Length <= 55)
        {
            // 小字符串使用栈内存
            // ...
        }
        // ...
    }
}
```

## 第二阶段:复杂数据结构优化

### 2.1 RedisHash 内存布局优化

**当前问题:**

- [`RedisObject.cs:416-591`](d:\Code\CyscaleDB\src\CysRedis.Core\DataStructures\RedisObject.cs) RedisHash 使用 Dictionary,每个键值对单独分配
- 字段过期检查有性能开销

**优化方案:**

```csharp
unsafe class UnsafeRedisHash
{
    // 小哈希使用 ziplist 编码 (连续内存)
    private byte* _ziplist;
    private int _ziplistSize;
    
    // 大哈希使用开放寻址哈希表
    private HashEntry* _table;
    private int _tableSize;
    private int _tableMask;
    
    struct HashEntry
    {
        public fixed byte Key[128];
        public fixed byte Value[512];
        public ushort KeyLen;
        public ushort ValueLen;
        public long ExpireTime;  // Unix timestamp ms
        public uint HashCode;
    }
    
    // 使用 SIMD 批量过期检查
    private void CleanupExpiredBatch()
    {
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        
        if (Vector256.IsHardwareAccelerated)
        {
            Vector256<long> nowVec = Vector256.Create(now);
            // 批量比较过期时间
            // ...
        }
    }
}
```

### 2.2 RedisList 双端队列优化

**当前问题:**

- [`RedisObject.cs:105-201`](d:\Code\CyscaleDB\src\CysRedis.Core\DataStructures\RedisObject.cs) 使用 LinkedList,指针追踪慢
- 每个节点单独分配

**优化方案:**

```csharp
unsafe class UnsafeRedisList
{
    // quicklist 结构: ziplist 节点的双向链表
    private QuickListNode* _head;
    private QuickListNode* _tail;
    
    struct QuickListNode
    {
        public QuickListNode* Prev;
        public QuickListNode* Next;
        public byte* Ziplist;         // 压缩列表
        public int ZiplistBytes;
        public int Count;
        public int Encoding;          // RAW/LZF
    }
    
    // 小列表使用单个 ziplist
    // 大列表使用 quicklist (ziplist 链表)
    // 节点池优化分配
}
```

### 2.3 RedisSet 哈希表优化

**当前问题:**

- [`RedisObject.cs:206-253`](d:\Code\CyscaleDB\src\CysRedis.Core\DataStructures\RedisObject.cs) 使用托管 HashSet

**优化方案:**

```csharp
unsafe class UnsafeRedisSet
{
    // 小集合使用 intset (整数集合)
    private UnsafeIntSet* _intset;
    
    // 大集合使用自定义哈希表
    private SetEntry* _table;
    private int _size;
    private int _mask;
    
    struct SetEntry
    {
        public fixed byte Member[256];
        public ushort Length;
        public uint HashCode;
    }
    
    // 使用线性探测开放寻址
    // SIMD 优化查找
}
```

### 2.4 RedisSortedSet 组合优化

**当前问题:**

- [`RedisObject.cs:258-411`](d:\Code\CyscaleDB\src\CysRedis.Core\DataStructures\RedisObject.cs) SkipList + Dictionary 双重存储

**优化方案:**

```csharp
unsafe class UnsafeRedisSortedSet
{
    // 小有序集合使用 ziplist
    private byte* _ziplist;
    
    // 大有序集合使用 skiplist + hashtable
    private UnsafeSkipList* _skiplist;
    private HashEntry* _hashTable;  // member -> score 快速查找
    
    // 共享内存,避免重复存储
    // skiplist 节点直接指向 hashtable entry
}
```

### 2.5 RedisStream 日志结构优化

**当前问题:**

- [`RedisStream.cs:1-305`](d:\Code\CyscaleDB\src\CysRedis.Core\DataStructures\RedisStream.cs) 使用 List<StreamEntry>,大量小对象

**优化方案:**

```csharp
unsafe class UnsafeRedisStream
{
    // Radix tree + listpack 混合结构
    private RadixTreeNode* _root;
    
    struct RadixTreeNode
    {
        public fixed byte Key[16];    // 时间戳前缀
        public byte* Listpack;        // 同一时间戳的条目
        public RadixTreeNode** Children;
        public int NumChildren;
    }
    
    // 条目使用 listpack 紧凑编码
    // 大幅减少内存占用
}
```

### 2.6 HyperLogLog 寄存器优化

**当前问题:**

- [`HyperLogLog.cs:1-137`](d:\Code\CyscaleDB\src\CysRedis.Core\DataStructures\HyperLogLog.cs) Count 计算有大量浮点运算

**优化方案:**

```csharp
unsafe class UnsafeHyperLogLog
{
    private fixed byte _registers[16384];
    
    // 使用查找表替代 pow(2, -x) 计算
    private static readonly double[] PowLookup = new double[64];
    
    // SIMD 批量计算
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long CountSIMD()
    {
        double sum = 0;
        int zeros = 0;
        
        fixed (byte* ptr = _registers)
        {
            if (Vector256.IsHardwareAccelerated)
            {
                // 使用 AVX2 并行累加
                for (int i = 0; i < 16384; i += 32)
                {
                    // 向量化计算
                    // ...
                }
            }
        }
        
        return CalculateCardinality(sum, zeros);
    }
}
```

## 第三阶段:协议层优化

### 3.1 RespPipeWriter 编码优化

**当前问题:**

- [`RespPipeWriter.cs:1-464`](d:\Code\CyscaleDB\src\CysRedis.Core\Protocol\RespPipeWriter.cs) 整数格式化有字符串转换开销

**优化方案:**

```csharp
unsafe partial class RespPipeWriter
{
    // 使用 stackalloc 优化小缓冲区
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteIntegerFast(long value)
    {
        Span<byte> buffer = stackalloc byte[24];
        fixed (byte* ptr = buffer)
        {
            int len = FormatIntegerUnsafe(ptr, value);
            WriteRaw(new ReadOnlySpan<byte>(ptr, len));
        }
    }
    
    // 使用位操作快速格式化
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int FormatIntegerUnsafe(byte* ptr, long value)
    {
        *ptr++ = (byte)':';
        
        if (value < 0)
        {
            *ptr++ = (byte)'-';
            value = -value;
        }
        
        // 快速整数转字符串 (无分支)
        int pos = 0;
        byte* digits = ptr;
        
        do
        {
            digits[pos++] = (byte)('0' + value % 10);
            value /= 10;
        } while (value > 0);
        
        // 反转数字
        for (int i = 0; i < pos / 2; i++)
        {
            byte tmp = digits[i];
            digits[i] = digits[pos - 1 - i];
            digits[pos - 1 - i] = tmp;
        }
        
        ptr += pos;
        *ptr++ = (byte)'\r';
        *ptr++ = (byte)'\n';
        
        return (int)(ptr - digits + 1);
    }
    
    // SIMD 优化批量写入
    public void WriteArrayFast(ReadOnlySpan<byte>[] elements)
    {
        // 使用向量化批量编码
        // ...
    }
}
```

### 3.2 BufferPool 内存对齐优化

**当前问题:**

- [`BufferPool.cs:1-256`](d:\Code\CyscaleDB\src\CysRedis.Core\Common\BufferPool.cs) 使用托管 ArrayPool

**优化方案:**

```csharp
unsafe static class UnsafeBufferPool
{
    // 按缓存行对齐的内存池
    private const int CacheLineSize = 64;
    
    // 使用 NativeMemory 分配对齐内存
    public static byte* RentAligned(int size, out int actualSize)
    {
        actualSize = (size + CacheLineSize - 1) & ~(CacheLineSize - 1);
        return (byte*)NativeMemory.AlignedAlloc((nuint)actualSize, CacheLineSize);
    }
    
    public static void ReturnAligned(byte* ptr)
    {
        NativeMemory.AlignedFree(ptr);
    }
    
    // 使用线程本地缓存避免锁竞争
    [ThreadStatic]
    private static ThreadLocalPool* _threadPool;
}
```

## 第四阶段:KvStore 存储层优化

### 4.1 KvStore 无锁哈希表

**当前问题:**

- [`KvStore.cs:1-218`](d:\Code\CyscaleDB\src\CysRedis.Core\DataStructures\KvStore.cs) 使用 ConcurrentDictionary,有锁开销

**优化方案:**

```csharp
unsafe class UnsafeKvStore
{
    // 每个 slot 使用无锁哈希表
    private SlotHashTable* _slots;
    
    struct SlotHashTable
    {
        public HashBucket* Buckets;
        public int Size;
        public int Mask;
        public long Version;  // 版本号用于无锁读取
    }
    
    struct HashBucket
    {
        public fixed byte Key[256];
        public RedisObject* Value;
        public long ExpireTime;
        public ushort KeyLen;
        public uint HashCode;
        public volatile int State;  // 0=empty, 1=filled, 2=deleted
    }
    
    // 使用 CAS 操作实现无锁更新
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TrySetAtomic(int slot, ReadOnlySpan<byte> key, RedisObject* value)
    {
        SlotHashTable* table = _slots + slot;
        uint hash = ComputeHash(key);
        int index = (int)(hash & table->Mask);
        
        HashBucket* bucket = table->Buckets + index;
        
        // 无锁 CAS 更新
        int expected = 0;
        if (Interlocked.CompareExchange(ref bucket->State, 1, expected) == expected)
        {
            // 成功占用桶
            key.CopyTo(new Span<byte>(bucket->Key, key.Length));
            bucket->KeyLen = (ushort)key.Length;
            bucket->Value = value;
            bucket->HashCode = hash;
            return true;
        }
        
        return false; // 冲突,需要探测
    }
}
```

## 第五阶段:性能关键辅助优化

### 5.1 内存池统一管理

创建 `UnsafeMemoryManager` 统一管理所有非托管内存:

```csharp
unsafe static class UnsafeMemoryManager
{
    // 大页内存支持
    // 内存池统计
    // 泄漏检测 (debug 模式)
    // 内存使用监控
}
```

### 5.2 SIMD 加速库

创建 `SimdHelpers` 提供通用 SIMD 操作:

```csharp
static class SimdHelpers
{
    // 批量比较
    // 批量复制
    // 批量哈希计算
    // 批量编码/解码
}
```

### 5.3 性能基准测试

创建完整的性能基准:

```csharp
// BenchmarkDotNet 测试套件
// 对比原生 Redis
// 对比托管版本
// 微基准测试
```

## 实施注意事项

### 安全性考虑

1. **内存泄漏防护**

   - 实现 RAII 模式 (IDisposable)
   - 使用 `SafeHandle` 包装关键资源
   - Debug 模式启用泄漏检测

2. **边界检查**

   - 在 unsafe 代码入口进行一次性检查
   - 使用 Debug.Assert 验证不变量
   - 关键路径保留最小检查

3. **线程安全**

   - 使用 volatile 和 Interlocked 保证可见性
   - 无锁算法需要仔细验证
   - 提供线程安全保证文档

### 性能验证

1. **基准测试目标**

   - SkipList 插入/查找: 提升 3-5x
   - Listpack 编码/解码: 提升 4-6x
   - IntSet 操作: 提升 2-3x
   - RespWriter 编码: 提升 3-4x
   - 整体吞吐: 接近原生 Redis 80-90%

2. **内存使用**

   - 保持与原生 Redis 相当
   - 某些场景下更优 (紧凑编码)

### 兼容性保证

1. **API 层不变**

   - 保持所有公共接口签名
   - 内部实现完全重写
   - 行为语义完全兼容

2. **渐进式迁移**

   - 先实现 unsafe 版本
   - 通过配置开关切换
   - 保留原有实现作为后备

## 文件组织

```
src/CysRedis.Core/
  Unsafe/
    DataStructures/
      UnsafeSkipList.cs
      UnsafeListpack.cs
      UnsafeIntSet.cs
      UnsafeCompactString.cs
      UnsafeRedisHash.cs
      UnsafeRedisList.cs
      UnsafeRedisSet.cs
      UnsafeRedisSortedSet.cs
      UnsafeRedisStream.cs
      UnsafeHyperLogLog.cs
      UnsafeKvStore.cs
    Protocol/
      UnsafeRespWriter.cs
    Common/
      UnsafeBufferPool.cs
      UnsafeMemoryManager.cs
      SimdHelpers.cs
      SafeHandles.cs
```

## 预期收益

- **吞吐量提升**: 3-5x
- **延迟降低**: 50-70%
- **内存效率**: 提升 20-30%
- **GC 压力**: 减少 80-90%
- **缓存命中率**: 提升 30-40% (内存局部性改善)

这将使 CysRedis 性能接近原生 Redis,并充分发挥 .NET 8+ 的现代特性。