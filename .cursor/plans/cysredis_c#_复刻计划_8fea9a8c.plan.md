---
name: CysRedis C# 复刻计划
overview: 使用 C# 参考 CyscaleDB.Server 架构，复刻 Redis 8.4.0 的完整功能，支持标准 RESP 协议，兼容 Navicat 和各类 Redis SDK 连接。
todos:
  - id: phase-1.1
    content: "Phase 1.1: 创建项目骨架 - 解决方案、项目结构、基础配置"
    status: completed
  - id: phase-1.2a
    content: "Phase 1.2a: RESP 协议读取器 - 解析 Simple Strings/Errors/Integers"
    status: completed
  - id: phase-1.2b
    content: "Phase 1.2b: RESP 协议读取器 - 解析 Bulk Strings/Arrays"
    status: completed
  - id: phase-1.2c
    content: "Phase 1.2c: RESP 协议写入器 - 生成所有响应类型"
    status: completed
  - id: phase-1.3a
    content: "Phase 1.3a: TCP 服务器 - 异步连接接受与客户端管理"
    status: completed
  - id: phase-1.3b
    content: "Phase 1.3b: 命令框架 - 解析器、分发器、PING/ECHO 命令"
    status: completed
  - id: phase-2.1a
    content: "Phase 2.1a: String 基础 - GET/SET/MGET/MSET 实现"
    status: completed
  - id: phase-2.1b
    content: "Phase 2.1b: String 进阶 - INCR/DECR/APPEND/STRLEN 等"
    status: completed
  - id: phase-2.2a
    content: "Phase 2.2a: Hash 基础 - HSET/HGET/HMSET/HMGET/HGETALL"
    status: completed
  - id: phase-2.2b
    content: "Phase 2.2b: Hash 进阶 - HDEL/HINCRBY/HSCAN 等"
    status: completed
  - id: phase-2.3a
    content: "Phase 2.3a: List 基础 - LPUSH/RPUSH/LPOP/RPOP/LRANGE"
    status: completed
  - id: phase-2.3b
    content: "Phase 2.3b: List 进阶 - LINDEX/LINSERT/BLPOP 阻塞操作"
    status: completed
  - id: phase-2.4
    content: "Phase 2.4: Set 类型 - SADD/SREM/SMEMBERS/集合运算"
    status: completed
  - id: phase-2.5a
    content: "Phase 2.5a: 跳表数据结构实现"
    status: in_progress
  - id: phase-2.5b
    content: "Phase 2.5b: Sorted Set 命令实现"
    status: pending
  - id: phase-3.1
    content: "Phase 3.1: 键空间操作 - DEL/EXISTS/TYPE/KEYS/SCAN"
    status: completed
  - id: phase-3.2
    content: "Phase 3.2: 过期机制 - EXPIRE/TTL + 惰性删除 + 定期删除"
    status: completed
  - id: phase-4.1
    content: "Phase 4.1: RDB 持久化 - 文件格式 + SAVE/BGSAVE"
    status: pending
  - id: phase-4.2
    content: "Phase 4.2: AOF 持久化 - 命令日志 + 重写"
    status: pending
  - id: phase-5.1
    content: "Phase 5.1: 事务支持 - MULTI/EXEC/WATCH"
    status: pending
  - id: phase-5.2
    content: "Phase 5.2: Lua 脚本 - EVAL/EVALSHA"
    status: pending
  - id: phase-6.1
    content: "Phase 6.1: Pub/Sub - SUBSCRIBE/PUBLISH/PSUBSCRIBE"
    status: pending
  - id: phase-6.2
    content: "Phase 6.2: Stream 类型 - XADD/XREAD/消费者组"
    status: pending
  - id: phase-7.1
    content: "Phase 7.1: 主从复制 - REPLICAOF + 全量/增量同步"
    status: pending
  - id: phase-7.2
    content: "Phase 7.2: Redis Cluster - 槽位分配 + 节点通信"
    status: pending
  - id: phase-8.1
    content: "Phase 8.1: ACL 权限控制"
    status: pending
  - id: phase-8.2
    content: "Phase 8.2: 扩展类型 - HyperLogLog/Geo/Bitmap"
    status: pending
  - id: phase-8.3
    content: "Phase 8.3: 监控管理 - INFO/CLIENT/CONFIG/SLOWLOG"
    status: pending
---

# CysRedis - Redis C# 复刻实施计划

## 项目架构设计

参考 CyscaleDB 的分层架构，设计如下结构：

```
CysRedis/
├── src/
│   ├── CysRedis.Core/           # 核心库
│   │   ├── Protocol/            # RESP 协议解析
│   │   ├── Commands/            # 命令处理器
│   │   ├── DataStructures/      # 数据结构实现
│   │   ├── Storage/             # 存储引擎 (RDB/AOF)
│   │   ├── Cluster/             # 集群支持
│   │   ├── Replication/         # 主从复制
│   │   ├── PubSub/              # 发布订阅
│   │   ├── Scripting/           # Lua 脚本
│   │   ├── Auth/                # ACL 权限
│   │   └── Common/              # 公共工具
│   ├── CysRedis.Server/         # 服务器入口
│   └── CysRedis.Cli/            # 命令行工具
└── tests/
    └── CysRedis.Tests/          # 测试项目
```

---

## 第一阶段：基础框架与 RESP 协议 (P0 - 核心)

### Phase 1.1: 项目骨架搭建

- 创建解决方案和项目结构
- 配置依赖和构建系统
- 参考 [CyscaleDB.Server/Program.cs](src/CyscaleDB.Server/Program.cs) 创建服务器入口

### Phase 1.2: RESP 协议实现

Redis 使用 RESP (Redis Serialization Protocol) 协议，需完整实现：

- **RespReader**: 解析客户端请求
  - Simple Strings (+OK\r\n)
  - Errors (-ERR message\r\n)
  - Integers (:1000\r\n)
  - Bulk Strings ($6\r\nfoobar\r\n)
  - Arrays (*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n)
  - RESP3 扩展类型 (Map, Set, Boolean, Double, Null 等)

- **RespWriter**: 生成响应数据

### Phase 1.3: 事件驱动网络层

参考 Redis `ae.c` 和 CyscaleDB `MySqlServer.cs`：

- 基于 `System.Net.Sockets` 的异步 TCP 服务器
- 客户端连接管理 (Client 结构)
- 命令解析与分发框架

---

## 第二阶段：核心数据结构 (P0 - 核心)

### Phase 2.1: String 类型

参考 `t_string.c`，实现命令：

- SET/GET/GETSET/MSET/MGET
- INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT
- APPEND/STRLEN/GETRANGE/SETRANGE
- SETNX/SETEX/PSETEX/GETEX/GETDEL

### Phase 2.2: Hash 类型

参考 `t_hash.c`，实现命令：

- HSET/HGET/HMSET/HMGET/HGETALL
- HDEL/HEXISTS/HLEN/HKEYS/HVALS
- HINCRBY/HINCRBYFLOAT
- HSCAN/HRANDFIELD

### Phase 2.3: List 类型

参考 `t_list.c`，实现命令：

- LPUSH/RPUSH/LPOP/RPOP
- LRANGE/LINDEX/LSET/LLEN
- LINSERT/LREM/LTRIM
- BLPOP/BRPOP (阻塞操作)
- LMOVE/BLMOVE

### Phase 2.4: Set 类型

参考 `t_set.c`，实现命令：

- SADD/SREM/SMEMBERS/SISMEMBER
- SCARD/SPOP/SRANDMEMBER
- SUNION/SINTER/SDIFF
- SUNIONSTORE/SINTERSTORE/SDIFFSTORE
- SSCAN/SMOVE

### Phase 2.5: Sorted Set 类型

参考 `t_zset.c`，需实现跳表 (SkipList)：

- ZADD/ZREM/ZSCORE/ZRANK/ZREVRANK
- ZRANGE/ZREVRANGE/ZRANGEBYSCORE
- ZINCRBY/ZCARD/ZCOUNT
- ZUNION/ZINTER/ZDIFF
- BZPOPMIN/BZPOPMAX (阻塞操作)

---

## 第三阶段：键空间管理与过期 (P0 - 核心)

### Phase 3.1: 键空间基础操作

参考 `db.c`：

- DEL/EXISTS/TYPE/RENAME/RENAMENX
- KEYS/SCAN/RANDOMKEY/DBSIZE
- SELECT (多数据库支持)
- FLUSHDB/FLUSHALL

### Phase 3.2: 过期机制

参考 `expire.c`：

- EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT
- TTL/PTTL/PERSIST/EXPIRETIME
- 惰性删除 (访问时检查)
- 定期删除 (后台扫描)

---

## 第四阶段：持久化 (P1 - 重要)

### Phase 4.1: RDB 快照

参考 `rdb.c`：

- SAVE/BGSAVE 命令
- RDB 文件格式解析与生成
- 后台 fork 保存 (或异步任务模拟)
- 自动保存策略 (save 配置)

### Phase 4.2: AOF 日志

参考 `aof.c`：

- 命令追加写入
- AOF 重写 (BGREWRITEAOF)
- fsync 策略 (always/everysec/no)
- AOF-RDB 混合持久化

---

## 第五阶段：事务与脚本 (P1 - 重要)

### Phase 5.1: 事务支持

参考 `multi.c`：

- MULTI/EXEC/DISCARD
- WATCH/UNWATCH (乐观锁)
- 事务队列管理
- 原子性保证

### Phase 5.2: Lua 脚本

参考 `eval.c`：

- 集成 NLua 或 MoonSharp
- EVAL/EVALSHA 命令
- SCRIPT LOAD/EXISTS/FLUSH
- redis.call/redis.pcall API

---

## 第六阶段：发布订阅 (P1 - 重要)

### Phase 6.1: Pub/Sub 基础

参考 `pubsub.c`：

- SUBSCRIBE/UNSUBSCRIBE
- PSUBSCRIBE/PUNSUBSCRIBE (模式匹配)
- PUBLISH
- PUBSUB CHANNELS/NUMSUB/NUMPAT

### Phase 6.2: Stream 数据类型

参考 `t_stream.c`：

- XADD/XREAD/XRANGE/XLEN
- XGROUP CREATE/DESTROY
- XREADGROUP/XACK/XCLAIM
- 消费者组管理

---

## 第七阶段：复制与集群 (P2 - 扩展)

### Phase 7.1: 主从复制

参考 `replication.c`：

- SLAVEOF/REPLICAOF 命令
- 全量同步 (RDB 传输)
- 增量同步 (命令传播)
- 复制积压缓冲区

### Phase 7.2: Redis Cluster

参考 `cluster.c`：

- 16384 槽位分配
- CLUSTER 命令族
- 节点间 gossip 协议
- 自动故障转移

---

## 第八阶段：高级特性 (P2 - 扩展)

### Phase 8.1: 访问控制 (ACL)

参考 `acl.c`：

- 用户管理 (ACL SETUSER/DELUSER)
- 命令权限控制
- 密钥模式权限

### Phase 8.2: 扩展数据类型

- HyperLogLog (PFADD/PFCOUNT/PFMERGE)
- Geo (GEOADD/GEODIST/GEOSEARCH)
- Bitmap (SETBIT/GETBIT/BITCOUNT/BITOP)

### Phase 8.3: 监控与管理

- INFO 命令 (完整统计信息)
- CLIENT LIST/KILL/PAUSE
- CONFIG GET/SET/REWRITE
- SLOWLOG/DEBUG/MEMORY

---

## 关键技术决策

| 组件 | Redis 实现 | C# 实现方案 |

|------|-----------|------------|

| 事件循环 | epoll/kqueue (ae.c) | async/await + SocketAsyncEventArgs |

| 哈希表 | dict.c | Dictionary + 渐进式 rehash |

| 跳表 | t_zset.c | 自定义 SkipList 实现 |

| 内存分配 | jemalloc | .NET GC + ArrayPool |

| 字符串 | sds.c | ReadOnlyMemory/Span |

| 压缩列表 | listpack.c | 自定义紧凑数组 |

---

## 验收标准

每个 Phase 完成后需满足：

1. 代码可独立编译 (`dotnet build`)
2. 单元测试覆盖核心逻辑
3. 可通过 redis-cli 验证命令
4. 兼容标准 Redis 客户端 (StackExchange.Redis)