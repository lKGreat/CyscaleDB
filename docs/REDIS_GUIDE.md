# CysRedis 使用指南

CysRedis 是一个高性能的 Redis 协议兼容服务器，使用 C# 和 .NET 构建。

## 目录

- [快速开始](#快速开始)
- [启动服务器](#启动服务器)
- [客户端连接](#客户端连接)
- [基本命令](#基本命令)
- [性能优化](#性能优化)
- [配置选项](#配置选项)
- [故障排除](#故障排除)

## 快速开始

### 1. 编译项目

```bash
# 编译整个解决方案
dotnet build CyscaleDB.sln --configuration Release

# 或只编译 Redis 服务器
dotnet build src/CysRedis.Server/CysRedis.Server.csproj --configuration Release
```

### 2. 启动服务器

```bash
# 使用默认配置启动（端口 6379）
dotnet run --project src/CysRedis.Server

# 或使用编译后的可执行文件
dotnet src/CysRedis.Server/bin/Release/net10.0/CysRedis.Server.dll
```

### 3. 测试连接

```bash
# 使用 redis-cli 连接（如果已安装）
redis-cli -h localhost -p 6379

# 或使用 telnet
telnet localhost 6379
```

## 启动服务器

### 基本启动命令

```bash
# 默认端口 6379
dotnet run --project src/CysRedis.Server

# 指定端口
dotnet run --project src/CysRedis.Server -- --port 6380

# 使用短参数
dotnet run --project src/CysRedis.Server -- -p 6380
```

### 启动参数

| 参数 | 短参数 | 说明 | 默认值 |
|------|--------|------|--------|
| `--port` | `-p` | 服务器端口 | 6379 |
| `--bind` | - | 绑定地址 | 0.0.0.0 |
| `--datadir` | - | 数据目录路径 | - |
| `--maxclients` | - | 最大客户端连接数 | 10000 |
| `--timeout` | - | 客户端空闲超时（秒） | 300 |
| `--tcp-keepalive` | - | TCP keep-alive 间隔（秒） | 60 |
| `--low-latency` | - | 低延迟优化模式 | - |
| `--high-throughput` | - | 高吞吐量优化模式 | - |
| `--unsafe` | - | 使用 unsafe 数据结构（高性能） | false |
| `--use-unsafe` | - | 同 `--unsafe` | false |
| `--help` | `-h` | 显示帮助信息 | - |

### 启动示例

#### 1. 基本启动

```bash
# 使用默认配置
dotnet run --project src/CysRedis.Server
```

#### 2. 自定义端口

```bash
# 在端口 6380 上启动
dotnet run --project src/CysRedis.Server -- --port 6380
```

#### 3. 绑定到特定地址

```bash
# 只监听本地回环地址
dotnet run --project src/CysRedis.Server -- --bind 127.0.0.1

# 监听所有网络接口（默认）
dotnet run --project src/CysRedis.Server -- --bind 0.0.0.0
```

#### 4. 指定数据目录

```bash
# 指定持久化数据目录
dotnet run --project src/CysRedis.Server -- --datadir ./data
```

#### 5. 性能优化模式

```bash
# 低延迟模式（适合实时应用）
dotnet run --project src/CysRedis.Server -- --low-latency

# 高吞吐量模式（适合批量处理）
dotnet run --project src/CysRedis.Server -- --high-throughput
```

#### 6. 使用 Unsafe 数据结构（高性能）

```bash
# 启用 unsafe 指针数据结构以获得最佳性能
dotnet run --project src/CysRedis.Server -- --unsafe
```

#### 7. 组合参数

```bash
# 组合多个参数
dotnet run --project src/CysRedis.Server -- \
  --port 6380 \
  --bind 127.0.0.1 \
  --datadir ./redis-data \
  --maxclients 5000 \
  --unsafe \
  --low-latency
```

#### 8. 查看帮助

```bash
# 显示所有可用参数
dotnet run --project src/CysRedis.Server -- --help
```

### 启动输出示例

成功启动后，你会看到类似以下的输出：

```
===========================================
  CysRedis Server v1.0.0
  Redis Protocol Compatible Server
  with High-Performance Network Stack
===========================================
Data structures: Managed (safe)
GC Mode: Server GC (Parallel)
GC Latency Mode: SustainedLowLatency
LOH Compaction: Configured (will compact on demand)
GC Max Generation: 2
GC Total Memory: 1,234,567 bytes
Starting server on port 6379...
Server is ready to accept connections.
Press Ctrl+C to shutdown.
```

## 客户端连接

### 使用 redis-cli

```bash
# 连接到默认端口
redis-cli

# 连接到指定端口
redis-cli -p 6380

# 连接到远程服务器
redis-cli -h 192.168.1.100 -p 6379
```

### 使用 .NET 客户端

```csharp
using StackExchange.Redis;

var connection = ConnectionMultiplexer.Connect("localhost:6379");
var db = connection.GetDatabase();

// 设置值
await db.StringSetAsync("key", "value");

// 获取值
var value = await db.StringGetAsync("key");
Console.WriteLine(value);
```

### 使用 Python 客户端

```python
import redis

r = redis.Redis(host='localhost', port=6379, db=0)

# 设置值
r.set('key', 'value')

# 获取值
value = r.get('key')
print(value)
```

### 使用 Node.js 客户端

```javascript
const redis = require('redis');
const client = redis.createClient({
    host: 'localhost',
    port: 6379
});

// 设置值
client.set('key', 'value', (err, reply) => {
    console.log(reply);
});

// 获取值
client.get('key', (err, reply) => {
    console.log(reply);
});
```

## 基本命令

### 字符串操作

```redis
# 设置值
SET mykey "Hello World"

# 获取值
GET mykey

# 设置多个值
MSET key1 "value1" key2 "value2"

# 获取多个值
MGET key1 key2

# 递增
INCR counter

# 递减
DECR counter

# 增加指定值
INCRBY counter 10
```

### Hash 操作

```redis
# 设置字段
HSET user:1 name "Alice" age 30

# 获取字段
HGET user:1 name

# 获取所有字段
HGETALL user:1

# 获取多个字段
HMGET user:1 name age

# 检查字段是否存在
HEXISTS user:1 name

# 删除字段
HDEL user:1 age
```

### List 操作

```redis
# 从右侧推入
RPUSH mylist "item1" "item2"

# 从左侧推入
LPUSH mylist "item0"

# 从左侧弹出
LPOP mylist

# 从右侧弹出
RPOP mylist

# 获取列表长度
LLEN mylist

# 获取列表元素
LRANGE mylist 0 -1
```

### Set 操作

```redis
# 添加成员
SADD myset "member1" "member2"

# 检查成员是否存在
SISMEMBER myset "member1"

# 获取所有成员
SMEMBERS myset

# 获取集合大小
SCARD myset

# 删除成员
SREM myset "member1"
```

### Sorted Set 操作

```redis
# 添加成员和分数
ZADD leaderboard 100 "player1" 200 "player2"

# 获取分数
ZSCORE leaderboard "player1"

# 获取排名
ZRANK leaderboard "player1"

# 获取范围
ZRANGE leaderboard 0 -1 WITHSCORES

# 增加分数
ZINCRBY leaderboard 50 "player1"
```

### 键操作

```redis
# 检查键是否存在
EXISTS mykey

# 删除键
DEL mykey

# 设置过期时间（秒）
EXPIRE mykey 60

# 设置过期时间（时间戳）
EXPIREAT mykey 1609459200

# 获取剩余时间
TTL mykey

# 获取键类型
TYPE mykey

# 列出所有键（谨慎使用）
KEYS *
```

### 服务器信息

```redis
# 获取服务器信息
INFO

# 获取特定信息
INFO server
INFO memory
INFO stats

# Ping 服务器
PING

# 获取当前数据库键数量
DBSIZE

# 清空当前数据库
FLUSHDB

# 清空所有数据库
FLUSHALL
```

## 性能优化

### 1. 使用 Unsafe 数据结构

```bash
# 启动时启用 unsafe 模式
dotnet run --project src/CysRedis.Server -- --unsafe
```

**优势：**
- 更高的性能（通常快 20-50%）
- 更低的内存分配
- 减少 GC 压力

**注意事项：**
- 需要启用 `AllowUnsafeBlocks`
- 适合生产环境的高性能需求

### 2. 低延迟模式

```bash
# 优化网络设置以降低延迟
dotnet run --project src/CysRedis.Server -- --low-latency
```

**优化内容：**
- TCP_NODELAY 启用
- 较小的缓冲区（32KB）
- 适合实时应用

### 3. 高吞吐量模式

```bash
# 优化网络设置以提高吞吐量
dotnet run --project src/CysRedis.Server -- --high-throughput
```

**优化内容：**
- TCP_NODELAY 禁用（允许 Nagle 算法）
- 较大的缓冲区（128KB）
- 适合批量处理

### 4. 调整客户端连接数

```bash
# 限制最大连接数
dotnet run --project src/CysRedis.Server -- --maxclients 5000
```

### 5. 调整超时设置

```bash
# 设置客户端空闲超时（秒）
dotnet run --project src/CysRedis.Server -- --timeout 600

# 禁用超时
dotnet run --project src/CysRedis.Server -- --timeout 0
```

## 配置选项

### 网络配置

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `--port` | 服务器端口 | 6379 |
| `--bind` | 绑定地址 | 0.0.0.0 |
| `--tcp-keepalive` | TCP keep-alive 间隔（秒） | 60 |

### 连接配置

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `--maxclients` | 最大客户端连接数 | 10000 |
| `--timeout` | 客户端空闲超时（秒） | 300 |

### 数据配置

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `--datadir` | 数据目录路径 | - |

### 性能配置

| 选项 | 说明 |
|------|------|
| `--low-latency` | 低延迟优化模式 |
| `--high-throughput` | 高吞吐量优化模式 |
| `--unsafe` | 使用 unsafe 数据结构 |

## 故障排除

### 1. 端口已被占用

**错误：**
```
Address already in use
```

**解决方案：**
```bash
# 使用其他端口
dotnet run --project src/CysRedis.Server -- --port 6380

# 或查找占用端口的进程并关闭
# Windows
netstat -ano | findstr :6379
taskkill /PID <进程ID> /F

# Linux/Mac
lsof -i :6379
kill -9 <进程ID>
```

### 2. 无法连接

**检查清单：**
1. 确认服务器正在运行
2. 检查防火墙设置
3. 验证端口是否正确
4. 检查绑定地址设置

```bash
# 检查服务器是否运行
netstat -an | findstr 6379

# 测试连接
telnet localhost 6379
```

### 3. 性能问题

**优化建议：**
1. 启用 `--unsafe` 模式
2. 使用 `--low-latency` 或 `--high-throughput`
3. 调整 `--maxclients` 限制
4. 检查系统资源（CPU、内存）

### 4. 内存不足

**解决方案：**
```bash
# 限制最大客户端数
dotnet run --project src/CysRedis.Server -- --maxclients 1000

# 启用 unsafe 模式减少内存分配
dotnet run --project src/CysRedis.Server -- --unsafe
```

### 5. 查看日志

服务器会在控制台输出详细的日志信息，包括：
- GC 模式
- 连接状态
- 错误信息
- 性能指标

## 生产环境建议

### 1. 使用 Release 模式

```bash
# 编译 Release 版本
dotnet build --configuration Release

# 运行 Release 版本
dotnet run --configuration Release --project src/CysRedis.Server
```

### 2. 启用 Server GC

在 `CysRedis.Server.csproj` 中添加：

```xml
<PropertyGroup>
  <ServerGarbageCollection>true</ServerGarbageCollection>
</PropertyGroup>
```

### 3. 使用系统服务

**Windows (使用 NSSM)：**
```bash
nssm install CysRedis "dotnet" "D:\Path\To\CysRedis.Server.dll"
nssm set CysRedis AppParameters "--port 6379 --unsafe"
nssm start CysRedis
```

**Linux (使用 systemd)：**
```ini
[Unit]
Description=CysRedis Server
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/dotnet /path/to/CysRedis.Server.dll --port 6379 --unsafe
Restart=always

[Install]
WantedBy=multi-user.target
```

### 4. 监控和日志

- 使用 `INFO` 命令监控服务器状态
- 定期检查内存使用情况
- 监控连接数和性能指标

## 相关文档

- [Redis 命令示例](REDIS_EXAMPLES.md)
- [性能基准测试](PERFORMANCE_BENCHMARKING.md)
- [Unsafe 优化文档](UNSAFE_OPTIMIZATIONS.md)
- [项目状态](PROJECT_STATUS.md)

## 获取帮助

```bash
# 查看启动参数帮助
dotnet run --project src/CysRedis.Server -- --help

# 查看服务器信息
redis-cli INFO
```

## 版本信息

当前版本：1.0.0

支持的 Redis 协议版本：Redis 6.x/7.x/8.x 兼容
