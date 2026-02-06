---
name: CysRedis性能功能提升
overview: 基于对 CysRedis 全部 98 个源文件的深度代码审查，从关键 Bug 修复、性能优化、功能完善三个维度，制定系统性的提升方案。
todos:
  - id: p0-watch-bug
    content: 修复 WATCH 实现：将 GetHashCode 改为版本号机制
    status: completed
  - id: p0-cluster-slot
    content: 修复 ClusterManager._slotToNode 数组未填充的 bug
    status: completed
  - id: p0-race-condition
    content: 修复 RedisServer 连接接受竞态条件
    status: completed
  - id: p1-expiry-perf
    content: 过期键清理改为概率采样 + 优先队列
    status: completed
  - id: p1-iothread-spin
    content: IoThread 空转改为事件驱动唤醒
    status: completed
  - id: p1-resp-alloc
    content: RespPipeReader 减少堆分配（ArrayPool/stackalloc）
    status: completed
  - id: p1-aof-lock
    content: AOF 写入改为无锁缓冲队列 + fsync 策略
    status: completed
  - id: p1-eviction-sample
    content: 淘汰候选选择改为随机采样法
    status: completed
  - id: p1-thread-safety
    content: 事务状态、RDB 保存、ServerCron 统计线程安全加固
    status: completed
  - id: p2-encoding-opt
    content: 数据结构编码优化（QuickList/IntSet/Listpack）
    status: completed
  - id: p2-replication
    content: 实现主从复制核心逻辑
    status: completed
  - id: p2-client-cmds
    content: 补全 CLIENT/MEMORY 命令族
    status: completed
  - id: p2-resp3
    content: 完善 RESP3 协议支持
    status: completed
  - id: p3-backpressure
    content: 添加背压机制和大 Key 防护
    status: completed
  - id: p3-tests
    content: 补充集成测试和协议兼容性测试
    status: completed
isProject: false
---

# CysRedis 性能与功能提升方案

经过对项目全部核心模块（Protocol、Commands、Threading、Memory、Storage、DataStructures、Replication、Cluster 等）的深度审查，以下按 **优先级** 从高到低列出需要提升的领域。

---

## 一、关键 Bug 修复（P0 - 必须修复）

### 1. WATCH 实现错误 — RedisClient.cs

- 当前使用 `GetHashCode()` 来检测 key 是否被修改，这是**根本性的设计缺陷**
- `GetHashCode()` 只保证"相等对象哈希相同"，不保证"不同值哈希不同"
- 应改为**版本号机制**：每个 key 维护一个单调递增的版本号，WATCH 记录版本号，EXEC 时比较
- 参考 Redis 实现：每次写操作递增 key 的 version counter

### 2. ClusterManager._slotToNode 数组从未填充

- [ClusterManager.cs](src/CysRedis.Core/Cluster/ClusterManager.cs) 中 `_slotToNode` 数组在 `AddSlots()` 时从未更新
- 导致 `GetNodeForSlot()` 退化为遍历所有节点 O(n)，而非预期的 O(1)
- 修复：在 `AddSlots()` 中同步更新 `_slotToNode[slot] = nodeId`

### 3. 连接接受时的竞态条件 — RedisServer.cs

- `_clients.Count` 检查与 `TryAdd` 之间存在 TOCTOU 竞态
- 应使用 `Interlocked` 计数器或在 `TryAdd` 后检查并移除

---

## 二、性能优化（P1 - 显著提升吞吐/延迟）

### 4. 过期键清理性能 — ServerCron.cs / RedisStore.cs

- **当前问题**：`CleanupExpired()` 遍历所有带过期时间的 key，O(n) 复杂度
- **建议**：
  - 使用 **SortedSet/优先队列** 按过期时间排序，只扫描即将过期的 key
  - 采用 Redis 的**概率采样策略**：每次随机取 20 个有 TTL 的 key，检查是否过期；若过期比例 > 25%，再取一批
  - 将同步调用改为后台增量处理

### 5. IoThread 空转浪费 CPU — IoThread.cs

- `Thread.Sleep(1)` 在无任务时仍导致频繁上下文切换
- **建议**：使用 `ManualResetEventSlim` 或 `SemaphoreSlim` 替代 sleep，在有新任务时唤醒线程
- 同时考虑使用 `SpinWait` 做短暂自旋后再阻塞

### 6. RespPipeReader 内存分配 — RespPipeReader.cs

- 多段 `ReadOnlySequence` 调用 `ToArray()` 产生不必要的堆分配
- Bulk string 解析每次分配新 byte[]
- **建议**：
  - 使用 `ArrayPool<byte>.Shared.Rent()` 替代 `new byte[]`
  - 对小字符串使用 `stackalloc` + `Span<byte>`
  - 引入 `RespValue` 对象池

### 7. AofPersistence 写入锁争用 — AofPersistence.cs

- 每条命令写 AOF 都要获取 `_lock`，高并发下是瓶颈
- **建议**：
  - 使用 **Channel&lt;T&gt;** 或 **ConcurrentQueue** 做写入缓冲，单线程消费写盘
  - 支持 `appendfsync` 策略：`always`（每条 fsync）、`everysec`（每秒 fsync）、`no`（OS 决定）
  - 批量合并写入减少系统调用次数

### 8. EvictionManager 候选选择效率 — EvictionManager.cs

- `SelectEvictionCandidates()` 调用 `database.Keys()` 获取全部 key，再排序
- **建议**：采用 Redis 的**随机采样法**：随机取 N 个 key（默认 5），淘汰其中 idle time 最大的
- 避免全量遍历 + 排序，将 O(n log n) 降为 O(k)，k 为采样数

### 9. RDB 持久化 CRC64 计算 — RdbPersistence.cs

- 当前将整个文件读入内存计算 CRC64
- **建议**：使用流式 CRC64 计算，边写边算，避免双倍内存占用

### 10. 数据结构编码优化 — RedisObject.cs

- 当前 List 使用 `LinkedList<byte[]>`，索引访问 O(n)
- 当前 Hash/Set 无论大小都用 Dictionary/HashSet
- **建议**：
  - 实现 **QuickList**（ziplist + 双向链表混合）替代纯 LinkedList
  - 小 Hash（元素数 < 128 且值 < 64 字节）使用 **Listpack** 编码
  - 小 Set（全部为整数且数量 < 512）使用 **IntSet** 编码
  - 小 Sorted Set 使用 Listpack 编码
  - 在元素数量超过阈值时自动转换编码

---

## 三、功能完善（P2 - 功能性缺失）

### 11. Replication 主从复制 — ReplicationManager.cs

- `ReplicaOfAsync()` 是空壳，没有实际连接 master 的逻辑
- **需要实现**：
  - TCP 连接到 master
  - PSYNC 握手协议
  - RDB 全量同步
  - 命令流增量同步
  - 断线重连 + 部分重同步（backlog）

### 12. Unsafe 适配器补全 — DataStructures/Adapters/

- 多数方法是 stub（抛 NotImplementedException 或返回空）
- `GetByIndex`、`SetByIndex`、`GetRange`、`Remove`、`GetRank` 等均未实现
- **建议**：要么完整实现，要么暂时移除 unsafe 模式选项避免运行时错误

### 13. CLIENT 命令族完善

- 缺少：`CLIENT LIST`（完整信息）、`CLIENT KILL`（按条件）、`CLIENT PAUSE`、`CLIENT TRACKING`
- `CLIENT TRACKING` 对客户端缓存至关重要（Redis 6.0+ 核心特性）

### 14. MEMORY 命令支持

- 缺少：`MEMORY USAGE key`、`MEMORY STATS`、`MEMORY DOCTOR`
- 对运维和调试非常重要

### 15. Cluster 核心功能

- 缺少 Gossip 协议、故障转移、槽迁移、MOVED/ASK 重定向
- 当前 Cluster 功能基本不可用

### 16. RESP3 协议完整支持

- 缺少：Map/Set 序列化、Push 消息、Verbatim String、Attribute 类型
- RESP3 是 Redis 6.0+ 的默认协议，客户端兼容性重要

### 17. AOF 持久化配置

- 缺少 `appendfsync` 策略配置（always/everysec/no）
- 缺少 `auto-aof-rewrite-min-size` / `auto-aof-rewrite-percentage` 配置
- 缺少 AOF 加载进度报告

---

## 四、线程安全加固（P1）

### 18. 事务状态保护 — RedisClient.cs

- `_transactionQueue` 和 `_watchedKeys` 无同步保护
- 在 I/O 多线程模式下可能被并发访问
- **建议**：使用 `ConcurrentQueue` 或在事务操作时加锁

### 19. RDB 并发保存保护 — RdbPersistence.cs

- `_isSaving` 标志非线程安全
- **建议**：使用 `SemaphoreSlim(1,1)` 确保同一时间只有一个保存操作

### 20. ServerCron 统计字段

- `CyclesExecuted`、`LastCycleDurationMs` 等字段无原子保护
- **建议**：使用 `Interlocked` 或 `volatile`

---

## 五、可观测性与运维（P2）

### 21. INFO 命令增强

- 补充 `INFO memory`：已用内存、峰值内存、碎片率、eviction 统计
- 补充 `INFO threads`：I/O 线程状态、队列深度
- 补充 `INFO replication`：复制偏移量、连接状态
- 补充 `INFO persistence`：RDB/AOF 状态、最后保存时间

### 22. 完善 Slow Log

- 增加命令参数记录（当前可能只记录命令名）
- 增加客户端信息（IP、端口）
- 支持 `CONFIG SET slowlog-log-slower-than`

### 23. 连接级别统计

- 每个连接的命令数、流量、最后活跃时间
- 支持 `CLIENT INFO` 输出完整 client list

---

## 六、生产就绪性（P3）

### 24. 优雅降级与背压

- RespPipeWriter 缺少写超时和慢客户端检测
- **建议**：设置 client-output-buffer-limit，慢客户端超限断开
- IoThread 缺少背压机制，队列无限增长可能 OOM

### 25. 大 Key 防护

- 无最大命令大小限制（`proto-max-bulk-len`）
- 无最大 key 数量报警
- **建议**：在 RespPipeReader 中添加 `MaxBulkLength` 检查

### 26. 测试覆盖

- 当前测试几乎为空（`UnitTest1.cs` 是占位符）
- **建议**：
  - 为每个命令组添加集成测试
  - 使用实际 Redis 客户端（StackExchange.Redis）进行协议兼容性测试
  - 添加并发/压力测试

---

## 建议实施顺序

```
Phase 1 (关键修复):  #1 WATCH bug, #2 Cluster slot bug, #3 竞态条件
Phase 2 (性能核心):  #4 过期键, #5 IoThread, #6 内存分配, #7 AOF锁
Phase 3 (数据结构):  #10 编码优化, #8 淘汰采样, #9 RDB流式CRC
Phase 4 (功能补全):  #11 复制, #13 CLIENT, #14 MEMORY, #16 RESP3
Phase 5 (生产就绪):  #18-20 线程安全, #24-25 背压/限制, #26 测试
Phase 6 (高级功能):  #12 Unsafe适配器, #15 Cluster, #17 AOF配置
```