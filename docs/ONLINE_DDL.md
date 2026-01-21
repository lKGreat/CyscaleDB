# CyscaleDB 在线 DDL

## 概述

在线 DDL（Data Definition Language）允许在不阻塞并发 DML（Data Manipulation Language）操作的情况下修改表结构。这是生产环境数据库的关键特性，可以最小化维护窗口时间。

## 支持的操作

CyscaleDB 支持以下在线 DDL 操作：

| 操作 | 支持的算法 | 锁定模式 | 说明 |
|------|-----------|----------|------|
| ADD COLUMN | INPLACE, COPY | NONE, SHARED | 添加新列，旧行延迟填充默认值 |
| DROP COLUMN | INPLACE, COPY | SHARED | 删除列，元数据立即更新 |
| MODIFY COLUMN | COPY | SHARED | 修改列定义 |
| ADD INDEX | INPLACE | NONE | 后台构建索引，支持并发 DML |
| DROP INDEX | INPLACE | NONE | 删除索引，快速操作 |
| RENAME TABLE | INPLACE | NONE | 重命名表，仅元数据更新 |

## ALGORITHM 选项

```sql
ALTER TABLE table_name 
  ADD COLUMN new_col INT DEFAULT 0
  ALGORITHM = INPLACE;
```

### ALGORITHM 类型

- **INPLACE**: 在线操作，不复制整个表
  - 最小化锁定时间
  - 允许并发 DML
  - DDL 期间记录变更，完成时应用

- **COPY**: 传统方法，复制整个表
  - 需要更多磁盘空间
  - DDL 期间表被锁定
  - 适用于需要重建表的操作

- **DEFAULT**: 自动选择最优算法

## LOCK 选项

```sql
ALTER TABLE table_name 
  ADD INDEX idx_name (column)
  ALGORITHM = INPLACE,
  LOCK = NONE;
```

### LOCK 模式

- **NONE**: 无锁
  - 允许并发读和写
  - 最高并发性
  - 仅适用于某些操作

- **SHARED**: 共享锁
  - 允许并发读
  - 阻塞并发写
  - 默认模式

- **EXCLUSIVE**: 排他锁
  - 阻塞所有并发访问
  - 最安全但最慢

- **DEFAULT**: 使用操作的默认锁模式

## 使用示例

### 添加列（在线）

```sql
-- 在线添加列，不阻塞查询
ALTER TABLE users 
  ADD COLUMN email VARCHAR(255) DEFAULT '' 
  ALGORITHM = INPLACE,
  LOCK = NONE;

-- 旧行读取时自动返回默认值
SELECT id, name, email FROM users;  -- email 为 ''
```

### 添加索引（在线）

```sql
-- 在后台构建索引
ALTER TABLE orders 
  ADD INDEX idx_order_date (order_date)
  ALGORITHM = INPLACE,
  LOCK = NONE;

-- DDL 执行期间，INSERT/UPDATE/DELETE 正常工作
INSERT INTO orders (order_date, amount) VALUES (NOW(), 100.00);
```

### 修改列（需要锁）

```sql
-- 修改列类型需要重建表
ALTER TABLE products 
  MODIFY COLUMN price DECIMAL(10,2)
  ALGORITHM = COPY,
  LOCK = SHARED;
```

## 工作原理

### ADD COLUMN 在线执行

1. **元数据更新**: 立即更新表结构定义
2. **延迟填充**: 旧行标记新列为"延迟"
3. **读取时填充**: 访问旧行时返回默认值
4. **后台回填**: 可选的异步任务逐步更新旧行

```
┌──────────────────────────────────────────┐
│ Old Row (before ADD COLUMN)              │
│ ┌─────┬─────┬─────┐                      │
│ │ id  │name │age  │                      │
│ └─────┴─────┴─────┘                      │
└──────────────────────────────────────────┘
          ↓ ADD COLUMN email
┌──────────────────────────────────────────┐
│ Row (after ADD COLUMN)                   │
│ ┌─────┬─────┬─────┬────────────────────┐ │
│ │ id  │name │age  │ email (lazy)       │ │
│ └─────┴─────┴─────┴────────────────────┘ │
│                      ↓ 读取时返回 ''      │
└──────────────────────────────────────────┘
```

### ADD INDEX 在线执行

1. **创建影子索引**: 在独立文件中构建索引
2. **扫描数据**: 后台遍历表，构建索引条目
3. **记录 DML**: DDL 期间的 INSERT/UPDATE/DELETE 记录到日志
4. **应用变更**: 索引构建完成后，应用日志中的 DML
5. **原子切换**: 用新索引替换旧索引（如果有）

```
┌─────────────────────────────────────────────┐
│ Timeline                                    │
├─────────────────────────────────────────────┤
│ T1: Begin Online DDL                        │
│     - Create shadow index                   │
│     - Start change log                      │
├─────────────────────────────────────────────┤
│ T2: Build index (background)                │
│     - Scan existing rows                    │
│     - Insert into shadow index              │
│     [Concurrent DML allowed]                │
│     - INSERT INTO table ...  ← logged       │
│     - UPDATE table ...       ← logged       │
├─────────────────────────────────────────────┤
│ T3: Finalize                                │
│     - Apply logged changes to shadow index  │
│     - Atomic swap: shadow → primary         │
│     - Commit                                │
└─────────────────────────────────────────────┘
```

## 性能影响

### 在线 DDL

- **DML 性能**: 下降约 5-10%（需要记录到变更日志）
- **查询性能**: 几乎无影响
- **磁盘使用**: 需要额外空间存储索引/日志
- **执行时间**: 比传统 DDL 稍长

### 传统 DDL（COPY）

- **DML 性能**: 完全阻塞
- **查询性能**: 阻塞（LOCK=EXCLUSIVE）
- **磁盘使用**: 需要 2 倍表空间
- **执行时间**: 取决于表大小

## 限制和注意事项

### 不支持在线执行的操作

以下操作必须使用 `ALGORITHM=COPY`:
- 修改列数据类型（扩大例外）
- 修改主键
- 更改字符集/排序规则

### 磁盘空间要求

在线 DDL 需要额外磁盘空间：
- **ADD INDEX**: ~索引大小
- **ADD COLUMN**: 最小（仅日志）
- **变更日志**: 取决于并发 DML 量

### 并发限制

- 默认只允许 1 个在线 DDL 同时执行
- 可通过配置调整: `OnlineDdlMaxConcurrentOperations`

## 配置选项

在 `cyscaledb.config.json` 中：

```json
{
  "EnableOnlineDdl": true,
  "OnlineDdlMaxConcurrentOperations": 1
}
```

## 监控在线 DDL

### 查看正在进行的 DDL

```sql
-- TODO: 实现 SHOW PROCESSLIST
SHOW PROCESSLIST;
```

### 查看 DDL 指标

```sql
SHOW STATUS LIKE 'online_ddl%';
```

输出示例：
```
+---------------------------+-------+
| Variable_name             | Value |
+---------------------------+-------+
| online_ddl_operations     | 42    |
| online_ddl_dml_changes    | 1523  |
+---------------------------+-------+
```

## 最佳实践

1. **优先使用在线 DDL**
   ```sql
   ALTER TABLE ... ALGORITHM=INPLACE, LOCK=NONE;
   ```

2. **在低峰期执行**
   - 即使是在线 DDL 也会消耗资源
   - 选择业务低峰期执行

3. **监控磁盘空间**
   - 确保有足够空间存储索引和日志

4. **测试先行**
   - 在测试环境验证 DDL 影响
   - 评估执行时间和性能影响

5. **逐步执行**
   - 一次只执行一个 DDL 操作
   - 避免多个大型 DDL 同时进行

## 故障排查

### DDL 执行缓慢

**问题**: 在线 DDL 执行时间过长

**解决方案**:
- 检查并发 DML 负载
- 增加 buffer pool 大小
- 确保有足够的磁盘 I/O 能力

### DDL 失败

**问题**: 在线 DDL 中途失败

**解决方案**:
- 检查磁盘空间
- 查看错误日志
- 尝试使用 `ALGORITHM=COPY`

### 性能下降

**问题**: DDL 期间查询性能明显下降

**解决方案**:
- 检查是否使用了正确的 LOCK 模式
- 评估是否需要在更低峰期执行
- 考虑分批次执行 DDL

## 相关文档

- [配置指南](CONFIGURATION.md)
- [性能监控](MONITORING.md)
- [项目状态](PROJECT_STATUS.md)
