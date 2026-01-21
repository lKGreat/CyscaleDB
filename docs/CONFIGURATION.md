# CyscaleDB 配置指南

## 概述

CyscaleDB 使用 JSON 格式的配置文件来管理数据库引擎的各项参数。配置文件位于项目根目录：`cyscaledb.config.json`。

## 配置文件位置

- **默认位置**: `./cyscaledb.config.json`
- **自定义位置**: 通过启动参数指定 `--config path/to/config.json`

如果配置文件不存在，系统会自动创建一个包含默认值的配置文件。

## 配置文件格式

```json
{
  "BufferPoolSizePages": 1024,
  "BufferPoolYoungRatio": 0.625,
  "RecursiveCteMaxIterations": 1000,
  "DefaultIsolationLevel": "RepeatableRead",
  "LockWaitTimeoutMs": 5000,
  "DeadlockCheckIntervalMs": 1000,
  "MinimumLogLevel": "Info",
  "EnableSlowQueryLog": true,
  "SlowQueryThresholdMs": 1000,
  "CheckpointIntervalSeconds": 300,
  "EnableOnlineDdl": true,
  "EnableMetrics": true
}
```

## 配置项详解

### Buffer Pool 配置

#### BufferPoolSizePages
- **类型**: Integer
- **默认值**: 1024 (16MB)
- **范围**: >= 16
- **说明**: Buffer pool 的大小，以页为单位（每页 16KB）
- **建议值**:
  - 开发环境: 512 (8MB)
  - 生产环境: 8192 (128MB) 或更多

```json
"BufferPoolSizePages": 8192
```

#### BufferPoolYoungRatio
- **类型**: Double
- **默认值**: 0.625 (62.5%)
- **范围**: 0.1 - 0.9
- **说明**: Young 区域占 Buffer Pool 的比例
- **说明**: Young 区域用于热数据，Old 区域用于一次性扫描

```json
"BufferPoolYoungRatio": 0.625
```

### CTE 配置

#### RecursiveCteMaxIterations
- **类型**: Integer
- **默认值**: 1000
- **范围**: >= 10
- **说明**: 递归 CTE 的最大迭代次数，防止无限递归
- **运行时可修改**: 是

```json
"RecursiveCteMaxIterations": 1000
```

```sql
-- 运行时修改
SET GLOBAL cte_max_recursion_depth = 2000;
```

### 事务配置

#### DefaultIsolationLevel
- **类型**: Enum
- **默认值**: "RepeatableRead"
- **可选值**: 
  - "ReadUncommitted"
  - "ReadCommitted"
  - "RepeatableRead"
  - "Serializable"
- **说明**: 新事务的默认隔离级别

```json
"DefaultIsolationLevel": "RepeatableRead"
```

### 锁配置

#### LockWaitTimeoutMs
- **类型**: Integer
- **默认值**: 5000 (5秒)
- **范围**: >= 0 (0 表示无限等待)
- **说明**: 等待锁的超时时间（毫秒）

```json
"LockWaitTimeoutMs": 5000
```

#### DeadlockCheckIntervalMs
- **类型**: Integer
- **默认值**: 1000 (1秒)
- **范围**: > 0
- **说明**: 死锁检测的时间间隔（毫秒）

```json
"DeadlockCheckIntervalMs": 1000
```

### 日志配置

#### MinimumLogLevel
- **类型**: Enum
- **默认值**: "Info"
- **可选值**: "Trace", "Debug", "Info", "Warning", "Error", "Fatal"
- **说明**: 最小日志级别

```json
"MinimumLogLevel": "Info"
```

#### EnableSlowQueryLog
- **类型**: Boolean
- **默认值**: false
- **说明**: 是否启用慢查询日志

```json
"EnableSlowQueryLog": true
```

#### SlowQueryThresholdMs
- **类型**: Integer
- **默认值**: 1000 (1秒)
- **范围**: >= 0
- **说明**: 慢查询阈值（毫秒），超过此时间的查询会被记录

```json
"SlowQueryThresholdMs": 1000
```

#### SlowQueryLogPath
- **类型**: String
- **默认值**: "cyscaledb-slow.log"
- **说明**: 慢查询日志文件路径

```json
"SlowQueryLogPath": "logs/cyscaledb-slow.log"
```

### Checkpoint 配置

#### CheckpointIntervalSeconds
- **类型**: Integer
- **默认值**: 300 (5分钟)
- **范围**: >= 10
- **说明**: 自动 checkpoint 的时间间隔（秒）

```json
"CheckpointIntervalSeconds": 300
```

#### CheckpointMaxDirtyPages
- **类型**: Integer
- **默认值**: 100
- **范围**: > 0
- **说明**: 触发 checkpoint 的脏页数量阈值

```json
"CheckpointMaxDirtyPages": 100
```

### WAL 配置

#### WalSegmentSizeBytes
- **类型**: Long
- **默认值**: 16777216 (16MB)
- **范围**: >= 1MB
- **说明**: WAL 段文件大小（字节）

```json
"WalSegmentSizeBytes": 16777216
```

#### WalBufferSizeBytes
- **类型**: Integer
- **默认值**: 262144 (256KB)
- **范围**: > 0
- **说明**: WAL 缓冲区大小（字节）

```json
"WalBufferSizeBytes": 262144
```

#### WalSyncAfterWrite
- **类型**: Boolean
- **默认值**: true
- **说明**: 写入 WAL 后是否立即同步到磁盘（保证持久性）

```json
"WalSyncAfterWrite": true
```

### 在线 DDL 配置

#### EnableOnlineDdl
- **类型**: Boolean
- **默认值**: true
- **说明**: 是否启用在线 DDL 功能

```json
"EnableOnlineDdl": true
```

#### OnlineDdlMaxConcurrentOperations
- **类型**: Integer
- **默认值**: 1
- **范围**: >= 1
- **说明**: 允许同时执行的在线 DDL 操作数

```json
"OnlineDdlMaxConcurrentOperations": 1
```

### 性能监控配置

#### EnableMetrics
- **类型**: Boolean
- **默认值**: true
- **说明**: 是否启用性能指标收集

```json
"EnableMetrics": true
```

#### MetricsUpdateIntervalMs
- **类型**: Integer
- **默认值**: 1000 (1秒)
- **范围**: > 0
- **说明**: 指标更新间隔（毫秒）

```json
"MetricsUpdateIntervalMs": 1000
```

## 运行时配置修改

部分配置可以在运行时通过 SQL 命令修改：

### 全局变量（影响所有会话）

```sql
SET GLOBAL cte_max_recursion_depth = 2000;
SET GLOBAL lock_wait_timeout = 10000;
```

### 会话变量（仅影响当前会话）

```sql
SET SESSION transaction_isolation = 'READ COMMITTED';
SET SESSION lock_wait_timeout = 3000;
```

### 查看当前配置

```sql
SHOW VARIABLES;
SHOW VARIABLES LIKE '%timeout%';
SHOW GLOBAL VARIABLES;
```

## 预设配置

CyscaleDB 提供三种预设配置：

### 生产环境配置

```json
{
  "BufferPoolSizePages": 8192,
  "EnableSlowQueryLog": true,
  "SlowQueryThresholdMs": 500,
  "CheckpointIntervalSeconds": 600,
  "MinimumLogLevel": "Warning",
  "EnableMetrics": true
}
```

### 开发环境配置

```json
{
  "BufferPoolSizePages": 512,
  "EnableSlowQueryLog": true,
  "SlowQueryThresholdMs": 100,
  "MinimumLogLevel": "Debug",
  "EnableMetrics": true
}
```

### 测试环境配置

```json
{
  "BufferPoolSizePages": 128,
  "EnableSlowQueryLog": false,
  "MinimumLogLevel": "Warning",
  "CheckpointIntervalSeconds": 30,
  "EnableMetrics": false
}
```

## 性能调优建议

### Buffer Pool 调优

**规则**: 设置为可用内存的 50-80%

```json
// 系统内存 4GB，分配 2GB (2048MB = 131072 pages)
"BufferPoolSizePages": 131072
```

### 慢查询日志调优

**生产环境**:
```json
"EnableSlowQueryLog": true,
"SlowQueryThresholdMs": 500  // 500ms 以上算慢查询
```

**开发环境**:
```json
"EnableSlowQueryLog": true,
"SlowQueryThresholdMs": 100  // 更敏感，发现所有慢查询
```

### Checkpoint 调优

**高写入负载**:
```json
"CheckpointIntervalSeconds": 180,  // 更频繁的 checkpoint
"CheckpointMaxDirtyPages": 200
```

**低写入负载**:
```json
"CheckpointIntervalSeconds": 600,  // 较少的 checkpoint
"CheckpointMaxDirtyPages": 50
```

## 配置验证

加载配置时会自动验证：

```csharp
var config = CyscaleDbConfiguration.LoadFromFile("cyscaledb.config.json");
var errors = config.Validate();

if (errors.Count > 0)
{
    foreach (var error in errors)
    {
        Console.WriteLine($"Config error: {error}");
    }
}
```

## 故障排查

### 配置文件未生效

1. 检查文件路径是否正确
2. 检查 JSON 格式是否有效
3. 检查文件权限

### 配置值无效

查看启动日志中的验证错误：

```
Config error: BufferPoolSizePages must be at least 16
Config error: SlowQueryThresholdMs cannot be negative
```

### 性能问题

1. 增加 `BufferPoolSizePages`
2. 调整 `CheckpointIntervalSeconds`
3. 启用慢查询日志分析瓶颈

## 相关文档

- [在线 DDL](ONLINE_DDL.md)
- [性能监控](MONITORING.md)
- [项目状态](PROJECT_STATUS.md)
