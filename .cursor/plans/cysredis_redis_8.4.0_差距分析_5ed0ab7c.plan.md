---
name: CysRedis Redis 8.4.0 差距分析
overview: 对比 Redis 8.4.0 源码与 CysRedis 当前实现，识别未实现功能和实现不完善之处，划分优先级
todos:
  - id: blocking-cmds
    content: 实现阻塞命令 (BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX/WAIT)
    status: completed
  - id: replication-psync
    content: 实现 PSYNC 增量复制协议和复制积压缓冲区
    status: completed
  - id: lua-execution
    content: 集成 MoonSharp 实现真实 Lua 脚本执行
    status: completed
  - id: eviction-policies
    content: 实现 LRU/LFU 内存淘汰策略
    status: completed
  - id: stream-enhance
    content: 完善 Stream (XINFO/XCLAIM/XAUTOCLAIM/XPENDING)
    status: completed
  - id: cluster-mode
    content: 实现 Redis Cluster 16384 槽位和 Gossip 协议
    status: completed
  - id: rdb-enhance
    content: 增强 RDB (LZF压缩/Stream存储/CRC校验)
    status: completed
  - id: aof-rewrite
    content: 实现完整 AOF 重写和混合持久化
    status: completed
  - id: acl-enhance
    content: 完善 ACL (持久化/日志/频道权限)
    status: completed
  - id: keyspace-notify
    content: 实现 Keyspace 事件通知
    status: completed
---

# CysRedis 对比 Redis 8.4.0 差距分析报告

## 一、未实现的核心功能

### 1. 集群模式 (Cluster) - 高优先级

Redis 8.4.0 源码位置: `cluster.c`, `cluster.h`, `cluster_legacy.c`, `cluster_slot_stats.c`

CysRedis **完全缺失**集群功能:

- 16384 槽位分配机制 (slot hashing)
- 节点间通信协议 (Gossip)
- `CLUSTER` 系列命令 (CLUSTER SLOTS/NODES/INFO/MEET/ADDSLOTS 等)
- 槽位迁移 (MIGRATE/IMPORTING/MIGRATING)
- 故障检测与自动故障转移 (Failover)
- `-MOVED` 和 `-ASK` 重定向
- 跨槽操作检测 `-CROSSSLOT`

### 2. 阻塞命令 (Blocking Operations) - 高优先级

Redis 8.4.0 源码位置: `blocked.c`

CysRedis **完全缺失**阻塞操作:

- `BLPOP` / `BRPOP` / `BLMOVE` / `BLMPOP` - 阻塞式列表弹出
- `BZPOPMIN` / `BZPOPMAX` / `BZMPOP` - 阻塞式有序集合弹出
- `BRPOPLPUSH` - 阻塞式列表转移
- `BLMOVE` - 阻塞式列表移动
- `WAIT` / `WAITAOF` - 等待复制/AOF同步
- 客户端阻塞状态管理
- 超时处理机制

### 3. Redis Functions (Lua函数库) - 中优先级

Redis 8.4.0 源码位置: `functions.c`, `functions.h`, `function_lua.c`

CysRedis 仅有脚本占位:

- `FUNCTION LOAD` / `DELETE` / `LIST` / `FLUSH`
- `FCALL` / `FCALL_RO` - 函数调用
- 函数库持久化到 RDB
- 函数引擎注册机制

### 4. Lua 脚本真实执行 - 中优先级

CysRedis 当前 `ScriptManager.cs` 仅存储脚本，不实际执行:

- 需集成 MoonSharp 或 NLua
- `redis.call()` / `redis.pcall()` 实现
- 脚本原子性保证
- `SCRIPT KILL` 超时中断

### 5. Module 模块系统 - 低优先级

Redis 8.4.0 源码位置: `module.c`, `redismodule.h`

CysRedis **完全缺失**:

- 动态加载模块 (.so/.dll)
- RedisModule API
- 模块命令注册
- 模块数据类型
- 模块钩子 (hooks)

---

## 二、实现不完善的功能

### 1. 复制 (Replication) - 高优先级

当前 `ReplicationManager.cs` 仅框架代码:

- 缺失 PSYNC/PSYNC2 增量同步协议
- 缺失 RDB 传输 (fullresync)
- 缺失复制积压缓冲区 (backlog)
- 缺失 `WAIT` 命令实现
- 缺失主从切换逻辑

### 2. RDB 持久化 - 中优先级

当前 `RdbPersistence.cs` 简化实现 (RDB_VERSION=10):

- 缺失 LZF 压缩 (`RDB_ENC_LZF`)
- 缺失 Stream 类型存储 (`RDB_TYPE_STREAM_LISTPACKS_3`)
- 缺失模块数据存储 (`RDB_TYPE_MODULE_2`)
- 缺失 Hash 字段过期 (`RDB_TYPE_HASH_METADATA`)
- 缺失后台 fork 保存 (BGSAVE 子进程)
- 缺失 CRC64 校验
- 缺失增量 RDB (无损持久化)

### 3. AOF 持久化 - 中优先级

当前 `AofPersistence.cs`:

- 缺失 AOF 重写 (BGREWRITEAOF 完整实现)
- 缺失多部分 AOF (Multi Part AOF)
- 缺失 AOF-RDB 混合模式
- 缺失 AOF 校验和修复
- fsync 策略不完整

### 4. Stream 数据结构 - 中优先级

当前 `RedisStream.cs` 基础实现:

- 缺失 `XINFO` 命令
- 缺失 `XCLAIM` / `XAUTOCLAIM` - 消息认领
- 缺失 `XPENDING` 完整实现
- 缺失 `XSETID` - ID 设置
- 缺失 `XREADGROUP` 阻塞模式
- 缺失消费者活跃时间跟踪
- 缺失 Radix Tree 存储优化

### 5. ACL 访问控制 - 中优先级

当前 `AclManager.cs`:

- 缺失 `ACL LOAD` / `ACL SAVE` - 持久化
- 缺失 `ACL LOG` - 访问日志
- 缺失 `ACL DRYRUN` - 权限测试
- 缺失 Pub/Sub 频道权限 (`&channel`)
- 缺失 Selector 选择器语法
- 缺失密码哈希存储 (SHA256)

### 6. 内存管理 - 中优先级

CysRedis 缺失:

- LRU/LFU 淘汰策略 (`evict.c`)
- 内存碎片整理 (`defrag.c`)
- 惰性删除 (`lazyfree.c`)
- 内存使用统计
- 客户端输出缓冲区限制
- maxmemory 策略

### 7. Keyspace 通知 - 低优先级

Redis 8.4.0 源码: `notify.c`

CysRedis 缺失:

- `__keyspace@*__` / `__keyevent@*__` 通知
- `notify-keyspace-events` 配置
- 事件类型 (g$lshzxeKEtmdn)

---

## 三、缺失的命令 (按类别)

### String 命令

- `LCS` - 最长公共子序列
- `GETDEL` - 获取并删除 (已有但需验证)
- `SETEX` 原子语义

### List 命令

- `LPOS` - 查找元素位置
- `LMPOP` - 多列表弹出
- `LMOVE` - 原子移动

### Set 命令

- `SINTERCARD` - 交集基数
- `SMISMEMBER` - 批量成员检查

### Sorted Set 命令

- `ZMPOP` - 多集合弹出
- `ZRANGESTORE` - 范围存储
- `ZINTER` / `ZUNION` - 交集/并集
- `ZINTERCARD` - 交集基数
- `ZDIFF` / `ZDIFFSTORE` - 差集
- `ZMSCORE` - 批量分数
- `ZRANDMEMBER` - 随机成员

### Hash 命令

- `HRANDFIELD` - 随机字段
- `HSCAN` - 迭代扫描
- `HEXPIRE` / `HPEXPIRE` - 字段过期 (Redis 8 新特性)
- `HTTL` / `HPTTL` - 字段 TTL
- `HPERSIST` - 字段持久化

### 通用命令

- `OBJECT ENCODING/FREQ/IDLETIME/REFCOUNT`
- `MEMORY USAGE/DOCTOR/MALLOC-SIZE`
- `CLIENT PAUSE/UNPAUSE/NO-EVICT`
- `DEBUG DIGEST/STRUCTSIZE`
- `DUMP` / `RESTORE` - 序列化
- `MIGRATE` - 键迁移
- `COPY` - 键复制
- `TOUCH` - 更新访问时间
- `UNLINK` - 异步删除
- `SORT` / `SORT_RO` - 排序

---

## 四、性能与架构差距

### 1. 数据结构编码

- 缺失 listpack 紧凑编码 (替代 ziplist)
- 缺失 intset 整数集合
- 缺失 quicklist 优化列表
- 缺失 radix tree (用于 Stream)

### 2. 事件循环

- 当前使用 async/await，缺失 ae 事件驱动模型
- 缺失定时任务 (serverCron)
- 缺失增量式 rehash

### 3. 网络优化

- 缺失 RESP3 协议完整支持
- 缺失客户端缓存 (Client-side caching)
- 缺失客户端追踪 (CLIENT TRACKING)

---

## 五、建议实施优先级

| 优先级 | 功能 | 工作量 |
|--------|------|--------|
| P0 | 阻塞命令 (BLPOP等) | 中 |
| P0 | 复制增量同步 | 高 |
| P1 | Lua 脚本执行 | 中 |
| P1 | 内存淘汰策略 | 中 |
| P1 | Stream 完善 | 中 |
| P2 | 集群模式 | 高 |
| P2 | RDB/AOF 增强 | 中 |
| P2 | ACL 完善 | 低 |
| P3 | Functions | 中 |
| P3 | Keyspace 通知 | 低 |
| P4 | Module 系统 | 高 |