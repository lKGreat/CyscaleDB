# CyscaleDB 功能能力清单

## 支持的 SQL 语句

### DDL (数据定义语言)

| 语句 | 状态 | 示例 |
|------|------|------|
| CREATE DATABASE | ✅ | `CREATE DATABASE mydb` |
| DROP DATABASE | ✅ | `DROP DATABASE IF EXISTS mydb` |
| CREATE TABLE | ✅ | `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))` |
| DROP TABLE | ✅ | `DROP TABLE IF EXISTS users` |
| CREATE INDEX | ✅ | `CREATE INDEX idx_name ON users (name)` |
| CREATE UNIQUE INDEX | ✅ | `CREATE UNIQUE INDEX idx_email ON users (email)` |
| DROP INDEX | ✅ | `DROP INDEX idx_name ON users` |
| CREATE VIEW | ✅ | `CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1` |
| DROP VIEW | ✅ | `DROP VIEW IF EXISTS active_users` |
| OPTIMIZE TABLE | ✅ | `OPTIMIZE TABLE users` |

### DML (数据操作语言)

| 语句 | 状态 | 说明 |
|------|------|------|
| SELECT | ✅ | 支持完整的查询功能 |
| INSERT | ✅ | 单行/多行插入 |
| UPDATE | ✅ | 支持 WHERE 条件 |
| DELETE | ✅ | 支持 WHERE 条件 |

### 事务控制

| 语句 | 状态 |
|------|------|
| BEGIN | ✅ |
| COMMIT | ✅ |
| ROLLBACK | ✅ |

### 工具语句

| 语句 | 状态 |
|------|------|
| USE database | ✅ |
| SHOW DATABASES | ✅ |
| SHOW TABLES | ✅ |
| DESCRIBE table | ✅ |

### information_schema 虚拟表 (MySQL 8.0 完整支持)

#### 核心元数据表

| 表名 | 状态 | 说明 |
|------|------|------|
| SCHEMATA | ✅ | 数据库列表 |
| TABLES | ✅ | 表列表 (21列完整) |
| COLUMNS | ✅ | 列信息 (22列完整) |
| STATISTICS | ✅ | 索引统计 |
| ENGINES | ✅ | 存储引擎信息 |
| VIEWS | ✅ | 视图定义 |

#### 字符集与排序规则表

| 表名 | 状态 | 说明 |
|------|------|------|
| CHARACTER_SETS | ✅ | 可用字符集列表 |
| COLLATIONS | ✅ | 排序规则列表 |
| COLLATION_CHARACTER_SET_APPLICABILITY | ✅ | 字符集-排序规则映射 |

#### 约束表

| 表名 | 状态 | 说明 |
|------|------|------|
| TABLE_CONSTRAINTS | ✅ | 表约束 (PRIMARY KEY, UNIQUE) |
| CHECK_CONSTRAINTS | ✅ | CHECK 约束 (空表桩) |
| KEY_COLUMN_USAGE | ✅ | 键列使用 (空表桩) |
| REFERENTIAL_CONSTRAINTS | ✅ | 外键约束 (空表桩) |

#### 系统表

| 表名 | 状态 | 说明 |
|------|------|------|
| PROCESSLIST | ✅ | 进程列表 |
| PLUGINS | ✅ | 服务器插件 |
| EVENTS | ✅ | 事件调度器 (空表桩) |
| PARTITIONS | ✅ | 分区信息 (空表桩) |

#### 例程表

| 表名 | 状态 | 说明 |
|------|------|------|
| ROUTINES | ✅ | 存储过程/函数 (空表桩) |
| PARAMETERS | ✅ | 存储过程参数 (空表桩) |
| TRIGGERS | ✅ | 触发器定义 (空表桩) |

#### 权限表

| 表名 | 状态 | 说明 |
|------|------|------|
| COLUMN_PRIVILEGES | ✅ | 列级权限 (空表桩) |
| TABLE_PRIVILEGES | ✅ | 表级权限 (空表桩) |
| SCHEMA_PRIVILEGES | ✅ | 库级权限 (空表桩) |
| USER_PRIVILEGES | ✅ | 用户全局权限 |
| USER_ATTRIBUTES | ✅ | 用户属性 |

#### 角色表

| 表名 | 状态 | 说明 |
|------|------|------|
| ADMINISTRABLE_ROLE_AUTHORIZATIONS | ✅ | 可授予角色 (空表桩) |
| APPLICABLE_ROLES | ✅ | 适用角色 (空表桩) |
| ENABLED_ROLES | ✅ | 已启用角色 (空表桩) |
| ROLE_COLUMN_GRANTS | ✅ | 角色列授权 (空表桩) |
| ROLE_ROUTINE_GRANTS | ✅ | 角色例程授权 (空表桩) |
| ROLE_TABLE_GRANTS | ✅ | 角色表授权 (空表桩) |

#### 扩展表

| 表名 | 状态 | 说明 |
|------|------|------|
| SCHEMATA_EXTENSIONS | ✅ | 库扩展属性 |
| TABLES_EXTENSIONS | ✅ | 表扩展属性 |
| COLUMNS_EXTENSIONS | ✅ | 列扩展属性 (空表桩) |
| TABLE_CONSTRAINTS_EXTENSIONS | ✅ | 约束扩展 (空表桩) |
| TABLESPACES | ✅ | 表空间 (空表桩) |
| TABLESPACES_EXTENSIONS | ✅ | 表空间扩展 (空表桩) |
| VIEW_ROUTINE_USAGE | ✅ | 视图引用例程 (空表桩) |
| VIEW_TABLE_USAGE | ✅ | 视图引用表 (空表桩) |

#### 其他系统表

| 表名 | 状态 | 说明 |
|------|------|------|
| COLUMN_STATISTICS | ✅ | 直方图统计 (空表桩) |
| KEYWORDS | ✅ | MySQL 关键字列表 |
| OPTIMIZER_TRACE | ✅ | 优化器追踪 (空表桩) |
| PROFILING | ✅ | 语句分析 (空表桩) |
| RESOURCE_GROUPS | ✅ | 资源组 |
| FILES | ✅ | 表空间文件 (空表桩) |

#### InnoDB Buffer Pool 表

| 表名 | 状态 | 说明 |
|------|------|------|
| INNODB_BUFFER_PAGE | ✅ | 缓冲页 (空表桩) |
| INNODB_BUFFER_PAGE_LRU | ✅ | LRU 缓冲页 (空表桩) |
| INNODB_BUFFER_POOL_STATS | ✅ | 缓冲池统计 |
| INNODB_CACHED_INDEXES | ✅ | 缓存索引 (空表桩) |

#### InnoDB 压缩表

| 表名 | 状态 | 说明 |
|------|------|------|
| INNODB_CMP | ✅ | 压缩统计 (空表桩) |
| INNODB_CMP_RESET | ✅ | 压缩统计重置 (空表桩) |
| INNODB_CMP_PER_INDEX | ✅ | 按索引压缩统计 (空表桩) |
| INNODB_CMP_PER_INDEX_RESET | ✅ | 按索引压缩重置 (空表桩) |
| INNODB_CMPMEM | ✅ | 压缩内存统计 (空表桩) |
| INNODB_CMPMEM_RESET | ✅ | 压缩内存重置 (空表桩) |

#### InnoDB 元数据表

| 表名 | 状态 | 说明 |
|------|------|------|
| INNODB_COLUMNS | ✅ | InnoDB 列信息 |
| INNODB_DATAFILES | ✅ | 数据文件 (空表桩) |
| INNODB_FIELDS | ✅ | 索引字段 (空表桩) |
| INNODB_FOREIGN | ✅ | 外键 (空表桩) |
| INNODB_FOREIGN_COLS | ✅ | 外键列 (空表桩) |
| INNODB_INDEXES | ✅ | 索引 (空表桩) |
| INNODB_TABLES | ✅ | InnoDB 表信息 |

#### InnoDB 全文索引表

| 表名 | 状态 | 说明 |
|------|------|------|
| INNODB_FT_BEING_DELETED | ✅ | 正在删除 (空表桩) |
| INNODB_FT_CONFIG | ✅ | 全文配置 (空表桩) |
| INNODB_FT_DEFAULT_STOPWORD | ✅ | 默认停用词 |
| INNODB_FT_DELETED | ✅ | 已删除 (空表桩) |
| INNODB_FT_INDEX_CACHE | ✅ | 索引缓存 (空表桩) |
| INNODB_FT_INDEX_TABLE | ✅ | 索引表 (空表桩) |

#### InnoDB 表空间与事务表

| 表名 | 状态 | 说明 |
|------|------|------|
| INNODB_METRICS | ✅ | 性能指标 (空表桩) |
| INNODB_SESSION_TEMP_TABLESPACES | ✅ | 会话临时表空间 (空表桩) |
| INNODB_TABLESPACES | ✅ | 表空间 (空表桩) |
| INNODB_TABLESPACES_BRIEF | ✅ | 表空间简要 (空表桩) |
| INNODB_TABLESTATS | ✅ | 表统计 |
| INNODB_TEMP_TABLE_INFO | ✅ | 临时表信息 (空表桩) |
| INNODB_TRX | ✅ | 事务 (空表桩) |

---

## SELECT 查询能力

### 基础功能

| 功能 | 状态 | 示例 |
|------|------|------|
| SELECT * | ✅ | `SELECT * FROM users` |
| 列选择 | ✅ | `SELECT id, name FROM users` |
| 列别名 | ✅ | `SELECT name AS user_name FROM users` |
| DISTINCT | ✅ | `SELECT DISTINCT status FROM orders` |
| 无FROM查询 | ✅ | `SELECT 1+1, NOW()` |

### WHERE 条件

| 操作符 | 状态 |
|--------|------|
| 比较 (=, !=, <, >, <=, >=) | ✅ |
| 逻辑 (AND, OR, NOT) | ✅ |
| LIKE | ✅ |
| IN | ✅ |
| BETWEEN | ✅ |
| IS NULL / IS NOT NULL | ✅ |

### JOIN

| 类型 | 状态 |
|------|------|
| INNER JOIN | ✅ |
| LEFT JOIN | ✅ |
| RIGHT JOIN | ✅ |
| FULL JOIN | ✅ |
| CROSS JOIN | ✅ |

### 分组与排序

| 功能 | 状态 | 示例 |
|------|------|------|
| GROUP BY | ✅ | `SELECT status, COUNT(*) FROM orders GROUP BY status` |
| HAVING | ✅ | `SELECT status, COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 5` |
| ORDER BY | ✅ | `SELECT * FROM users ORDER BY created_at DESC` |
| LIMIT | ✅ | `SELECT * FROM users LIMIT 10` |
| OFFSET | ✅ | `SELECT * FROM users LIMIT 10 OFFSET 20` |
| LIMIT offset,count | ✅ | `SELECT * FROM users LIMIT 20, 10` (MySQL语法) |

### 聚合函数

| 函数 | 状态 |
|------|------|
| COUNT(*) | ✅ |
| COUNT(column) | ✅ |
| SUM(column) | ✅ |
| AVG(column) | ✅ |
| MIN(column) | ✅ |
| MAX(column) | ✅ |

---

## 内置函数

### 时间函数

| 函数 | 状态 | 说明 |
|------|------|------|
| NOW() | ✅ | 当前日期时间 |
| CURRENT_TIMESTAMP | ✅ | 同 NOW() |
| CURDATE() | ✅ | 当前日期 |
| CURRENT_DATE | ✅ | 同 CURDATE() |
| CURTIME() | ✅ | 当前时间 |
| CURRENT_TIME | ✅ | 同 CURTIME() |

### 字符串函数

| 函数 | 状态 | 说明 |
|------|------|------|
| UPPER(str) | ✅ | 转大写 |
| LOWER(str) | ✅ | 转小写 |
| LENGTH(str) | ✅ | 字符串长度 |
| CHAR_LENGTH(str) | ✅ | 字符长度 |
| CONCAT(str1, str2, ...) | ✅ | 字符串连接 |

### 控制流函数

| 函数 | 状态 | 说明 |
|------|------|------|
| IF(cond, true_val, false_val) | ✅ | 条件判断 |
| IFNULL(expr, default) | ✅ | NULL 替换 |
| COALESCE(expr, default) | ✅ | 同 IFNULL |
| ISNULL(expr) | ✅ | 判断是否为NULL (返回1或0) |
| FIELD(str, str1, str2, ...) | ✅ | 返回str在列表中的位置 (1-based) |

### 系统信息函数

| 函数 | 状态 | 说明 |
|------|------|------|
| DATABASE() | ✅ | 当前数据库名 |
| VERSION() | ✅ | 服务器版本 |
| USER() | ✅ | 当前用户 |
| CONNECTION_ID() | ✅ | 连接ID |
| LAST_INSERT_ID() | ✅ | 最后插入ID |

---

## 支持的数据类型

### 整数类型

| 类型 | 大小 | 范围 |
|------|------|------|
| TINYINT | 1字节 | -128 ~ 127 |
| SMALLINT | 2字节 | -32,768 ~ 32,767 |
| MEDIUMINT | 3字节 | -8,388,608 ~ 8,388,607 |
| INT | 4字节 | -2,147,483,648 ~ 2,147,483,647 |
| BIGINT | 8字节 | -9,223,372,036,854,775,808 ~ 9,223,372,036,854,775,807 |
| BIT(n) | 1-8字节 | 位类型 (1-64位) |

### 浮点类型

| 类型 | 大小 | 说明 |
|------|------|------|
| FLOAT | 4字节 | 单精度浮点 |
| DOUBLE | 8字节 | 双精度浮点 |
| DECIMAL(p, s) | 变长 | 定点数 |

### 字符类型

| 类型 | 说明 |
|------|------|
| CHAR(n) | 定长字符串 |
| VARCHAR(n) | 变长字符串 |
| TINYTEXT | 小文本 (最大255字节) |
| TEXT | 大文本 (最大64KB) |
| MEDIUMTEXT | 中等文本 (最大16MB) |
| LONGTEXT | 超大文本 (最大4GB) |

### 二进制类型

| 类型 | 说明 |
|------|------|
| BINARY(n) | 定长二进制 |
| VARBINARY(n) | 变长二进制 |
| TINYBLOB | 小二进制对象 (最大255字节) |
| BLOB | 二进制大对象 (最大64KB) |
| MEDIUMBLOB | 中等二进制对象 (最大16MB) |
| LONGBLOB | 超大二进制对象 (最大4GB) |

### 时间类型

| 类型 | 说明 |
|------|------|
| DATE | 日期 (YYYY-MM-DD) |
| TIME | 时间 (HH:MM:SS) |
| DATETIME | 日期时间 |
| TIMESTAMP | 时间戳 |
| YEAR | 年份 (1901-2155) |

### 其他类型

| 类型 | 说明 |
|------|------|
| BOOLEAN | 布尔值 |
| JSON | JSON数据 |
| ENUM('a','b',...) | 枚举类型 |
| SET('a','b',...) | 集合类型 |
| GEOMETRY | 几何/空间数据 |

---

## 列约束与属性

### 基本约束

| 约束 | 状态 |
|------|------|
| PRIMARY KEY | ✅ |
| NOT NULL | ✅ |
| AUTO_INCREMENT | ✅ |
| DEFAULT | ✅ |
| UNIQUE | ✅ |

### 数值类型修饰符

| 修饰符 | 状态 | 说明 |
|------|------|------|
| UNSIGNED | ✅ | 无符号 (仅正数) |
| ZEROFILL | ✅ | 零填充显示 |
| SIGNED | ✅ | 有符号 (默认) |

### 字符集与排序规则

| 属性 | 状态 | 示例 |
|------|------|------|
| CHARACTER SET | ✅ | `VARCHAR(255) CHARACTER SET utf8mb4` |
| CHARSET | ✅ | `VARCHAR(255) CHARSET utf8mb4` |
| COLLATE | ✅ | `VARCHAR(255) COLLATE utf8mb4_general_ci` |

### 其他列属性

| 属性 | 状态 | 说明 |
|------|------|------|
| COMMENT | ✅ | 列注释 `COMMENT '用户ID'` |
| ON UPDATE CURRENT_TIMESTAMP | ✅ | 自动更新时间戳 |

---

## 存储引擎特性

| 特性 | 说明 |
|------|------|
| 页面大小 | 4KB 固定大小 |
| 缓冲池 | LRU 缓存策略 |
| WAL日志 | 预写日志，支持崩溃恢复 |
| 事务隔离 | 表级锁 |
| 文件格式 | .cdb 数据文件 |
| 索引文件 | .idx (B-Tree), .hidx (Hash) |
| 日志轮转 | 自动轮转超过阈值的 WAL 文件 |
| 日志归档 | 支持 gzip 压缩旧日志 |
| 检查点 | 定期刷新脏页，支持快速恢复 |
| 数据收缩 | OPTIMIZE TABLE 回收删除空间 |

---

## 协议支持

| 协议 | 状态 | 说明 |
|------|------|------|
| MySQL Protocol | ✅ | 可使用标准 MySQL 客户端连接 |
| 默认端口 | 3306 | |
| 认证 | ⚠️ | MVP阶段跳过密码验证 |
| 多语句支持 | ✅ | 支持用分号分隔的多条SQL语句 |

### MySQL 客户端兼容性

| 特性 | 状态 | 说明 |
|------|------|------|
| BINARY 关键字 | ✅ | 解析支持，作为透传处理 (无排序规则区分) |
| LIMIT offset,count | ✅ | 支持MySQL简写语法 |
| information_schema | ✅ | 支持常用元数据表 |

---

## 索引支持

### 索引类型

| 类型 | 状态 | 说明 |
|------|------|------|
| B-Tree | ✅ | 支持等值和范围查询 |
| Hash | ✅ | 仅支持等值查询，性能更优 |

### 索引语法

```sql
-- 创建 B-Tree 索引 (默认)
CREATE INDEX idx_name ON users (name);

-- 创建 Hash 索引
CREATE INDEX idx_id ON users (id) USING HASH;

-- 创建唯一索引
CREATE UNIQUE INDEX idx_email ON users (email);

-- 创建复合索引
CREATE INDEX idx_composite ON orders (customer_id, order_date);

-- 删除索引
DROP INDEX idx_name ON users;
```

### 索引选择器

查询优化器会根据 WHERE 条件自动选择最优索引：
- 等值查询优先使用 Hash 索引
- 范围查询使用 B-Tree 索引
- 复合索引支持最左前缀匹配

---

## 视图支持

| 功能 | 状态 | 示例 |
|------|------|------|
| 创建视图 | ✅ | `CREATE VIEW v1 AS SELECT ...` |
| 替换视图 | ✅ | `CREATE OR REPLACE VIEW v1 AS SELECT ...` |
| 删除视图 | ✅ | `DROP VIEW v1` |
| 查询视图 | ✅ | `SELECT * FROM v1` |
| 视图列别名 | ✅ | `CREATE VIEW v1 (col1, col2) AS SELECT ...` |

---

## 高级查询功能

### CASE WHEN 表达式

| 语法 | 状态 | 示例 |
|------|------|------|
| 简单 CASE | ✅ | `CASE status WHEN 1 THEN 'active' ELSE 'inactive' END` |
| 搜索 CASE | ✅ | `CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END` |
| 嵌套 CASE | ✅ | 支持嵌套 |

### CTE (WITH 子句)

| 功能 | 状态 | 示例 |
|------|------|------|
| 基本 CTE | ✅ | `WITH cte AS (SELECT ...) SELECT * FROM cte` |
| 多个 CTE | ✅ | `WITH cte1 AS (...), cte2 AS (...) SELECT ...` |
| 递归 CTE | ✅ | `WITH RECURSIVE cte AS (...) SELECT ...` |

### 窗口函数

| 函数 | 状态 | 说明 |
|------|------|------|
| ROW_NUMBER() | ✅ | 行号 |
| RANK() | ✅ | 排名 (有间隙) |
| DENSE_RANK() | ✅ | 排名 (无间隙) |
| NTILE(n) | ✅ | 分桶 |
| LAG(expr, offset, default) | ✅ | 前行值 |
| LEAD(expr, offset, default) | ✅ | 后行值 |
| FIRST_VALUE(expr) | ✅ | 窗口首值 |
| LAST_VALUE(expr) | ✅ | 窗口末值 |
| SUM() OVER | ✅ | 窗口求和 |
| AVG() OVER | ✅ | 窗口平均 |
| MIN() OVER | ✅ | 窗口最小值 |
| MAX() OVER | ✅ | 窗口最大值 |
| COUNT() OVER | ✅ | 窗口计数 |

### 窗口规范

| 语法 | 状态 |
|------|------|
| PARTITION BY | ✅ |
| ORDER BY | ✅ |
| ROWS BETWEEN | ✅ |

---

## InnoDB 存储引擎特性

### MVCC (多版本并发控制)

| 特性 | 状态 | 说明 |
|------|------|------|
| ReadView | ✅ | 事务快照视图 |
| 版本链 | ✅ | 通过 Roll Pointer 链接历史版本 |
| 快照读 | ✅ | 普通 SELECT 不加锁 |
| 当前读 | ✅ | SELECT FOR UPDATE/FOR SHARE |

### 事务隔离级别

| 级别 | 状态 | 说明 |
|------|------|------|
| READ UNCOMMITTED | ✅ | 读未提交 |
| READ COMMITTED | ✅ | 读已提交 (每次读创建新 ReadView) |
| REPEATABLE READ | ✅ | 可重复读 (事务开始创建 ReadView) |
| SERIALIZABLE | ✅ | 串行化 |

### 锁机制

| 锁类型 | 状态 | 说明 |
|------|------|------|
| 表级锁 | ✅ | S/X 锁 |
| 行级锁 (Record Lock) | ✅ | 锁定索引记录 |
| 间隙锁 (Gap Lock) | ✅ | 防止幻读 |
| 临键锁 (Next-Key Lock) | ✅ | Record + Gap |
| 意向锁 (IS/IX/SIX) | ✅ | 表级意向锁 |

### 日志系统

| 组件 | 状态 | 说明 |
|------|------|------|
| Redo Log (WAL) | ✅ | 崩溃恢复 |
| Undo Log | ✅ | 事务回滚、MVCC |
| Mini-transaction | ✅ | 原子页面操作 |
| Checkpoint | ✅ | 定期刷脏页 |
| Doublewrite Buffer | ✅ | 防止部分写 |

### Buffer Pool

| 特性 | 状态 | 说明 |
|------|------|------|
| LRU 管理 | ✅ | 页面缓存淘汰 |
| Flush List | ✅ | 脏页管理 |
| 预读 | ✅ | 线性/随机预读 |

---

## 外键约束

| 功能 | 状态 | 说明 |
|------|------|------|
| 解析支持 | ✅ | CREATE TABLE 外键语法 |
| 运行时检查 | ✅ | INSERT/UPDATE 时验证 |
| ON DELETE CASCADE | ✅ | 级联删除 |
| ON DELETE SET NULL | ✅ | 设为 NULL |
| ON DELETE RESTRICT | ✅ | 拒绝操作 |
| ON UPDATE CASCADE | ✅ | 级联更新 |
| ON UPDATE SET NULL | ✅ | 设为 NULL |
| ON UPDATE RESTRICT | ✅ | 拒绝操作 |

---

## 行级锁定读

| 语法 | 状态 | 说明 |
|------|------|------|
| FOR UPDATE | ✅ | 排他锁 |
| FOR SHARE | ✅ | 共享锁 |
| FOR UPDATE NOWAIT | ✅ | 获取不到立即失败 |
| FOR UPDATE SKIP LOCKED | ✅ | 跳过已锁定行 |

---

## 已知限制

1. **存储过程**: 不支持
2. **触发器**: 不支持
3. **事件调度器**: 不支持
4. **JSON 函数**: 不支持
5. **空间数据类型**: 不支持
6. **全文索引**: 不支持
7. **视图更新**: 暂不支持 INSERT/UPDATE/DELETE 到视图

---

## 执行算子

| 算子 | 说明 |
|------|------|
| TableScanOperator | 全表扫描 (支持 MVCC) |
| IndexScanOperator | 索引扫描 (支持 MVCC、回表) |
| FilterOperator | WHERE 条件过滤 |
| ProjectOperator | 列投影 |
| NestedLoopJoinOperator | 嵌套循环连接 |
| GroupByOperator | 分组聚合 |
| OrderByOperator | 排序 |
| LimitOperator | 限制结果数量 |
| DistinctOperator | 去重 |
| WindowOperator | 窗口函数计算 |
| CteOperator | CTE 结果读取 |
| DualOperator | 虚拟表 (无FROM查询) |
| AliasOperator | 表/列别名处理 |
| InformationSchemaOperator | 系统表查询 |

---

## MySQL 8.4 兼容性增强 (2026-02-06)

### 新增内置函数 (170+)

| 类别 | 函数数量 | 代表函数 |
|------|----------|----------|
| 数学函数 | 25+ | ABS, CEIL, FLOOR, ROUND, TRUNCATE, MOD, POW, SQRT, EXP, LOG, SIN, COS, TAN, PI, RAND, SIGN, CRC32, CONV |
| 字符串函数 | 40+ | SUBSTRING, LEFT, RIGHT, TRIM, LPAD, RPAD, REPLACE, LOCATE, REPEAT, REVERSE, ASCII, HEX, UNHEX, CONCAT_WS, FROM_BASE64, TO_BASE64, SOUNDEX, ELT, FIND_IN_SET, EXPORT_SET |
| 日期时间函数 | 45+ | YEAR, MONTH, DAY, DATE_ADD, DATE_SUB, DATEDIFF, TIMESTAMPDIFF, DATE_FORMAT, STR_TO_DATE, UNIX_TIMESTAMP, FROM_UNIXTIME, LAST_DAY, MAKEDATE, CONVERT_TZ |
| 聚合函数 | 12+ | BIT_AND, BIT_OR, BIT_XOR, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP, JSON_ARRAYAGG, JSON_OBJECTAGG |
| 加密/哈希函数 | 10+ | MD5, SHA1, SHA2, AES_ENCRYPT, AES_DECRYPT, COMPRESS, UNCOMPRESS, RANDOM_BYTES |
| 正则函数 | 4 | REGEXP_LIKE, REGEXP_INSTR, REGEXP_REPLACE, REGEXP_SUBSTR |
| UUID函数 | 5 | UUID, UUID_SHORT, UUID_TO_BIN, BIN_TO_UUID, IS_UUID |
| 锁定函数 | 5 | GET_LOCK, RELEASE_LOCK, RELEASE_ALL_LOCKS, IS_FREE_LOCK, IS_USED_LOCK |
| 其他函数 | 20+ | SLEEP, BENCHMARK, ANY_VALUE, BIT_COUNT, NULLIF, GREATEST, LEAST, CAST, INET_ATON, INET_NTOA |

### 新增 DML/DDL

| 语句 | 状态 |
|------|------|
| INSERT IGNORE | ✅ |
| REPLACE INTO | ✅ |
| INSERT ... ON DUPLICATE KEY UPDATE | ✅ |
| INSERT ... SELECT | ✅ |
| LOAD DATA INFILE | ✅ |
| SELECT ... INTO OUTFILE | ✅ |
| RENAME TABLE | ✅ |
| PREPARE / EXECUTE / DEALLOCATE | ✅ |
| CHECK TABLE | ✅ |
| REPAIR TABLE | ✅ |
| CHECKSUM TABLE | ✅ |
| BACKUP DATABASE / RESTORE DATABASE | ✅ |

### 新增 SHOW 命令

| 命令 | 状态 |
|------|------|
| SHOW PROCESSLIST / SHOW FULL PROCESSLIST | ✅ |
| SHOW GRANTS [FOR user] | ✅ |
| SHOW CREATE DATABASE | ✅ |
| SHOW CREATE VIEW | ✅ |
| SHOW CREATE PROCEDURE/FUNCTION | ✅ |
| SHOW PROCEDURE STATUS / FUNCTION STATUS | ✅ |
| SHOW TRIGGERS | ✅ |
| SHOW EVENTS | ✅ |
| SHOW PLUGINS | ✅ |
| SHOW ENGINES | ✅ |
| SHOW ENGINE INNODB STATUS | ✅ |
| SHOW PRIVILEGES | ✅ |
| SHOW MASTER STATUS / SHOW BINARY LOG STATUS | ✅ |
| SHOW REPLICA STATUS | ✅ |
| SHOW BINARY LOGS | ✅ |
| SHOW OPEN TABLES | ✅ |

### 存储过程执行引擎

| 功能 | 状态 |
|------|------|
| DECLARE CURSOR | ✅ |
| OPEN / FETCH / CLOSE cursor | ✅ |
| DECLARE HANDLER (CONTINUE/EXIT/UNDO) | ✅ |
| SIGNAL / RESIGNAL | ✅ |
| 事件调度器 (EventScheduler) | ✅ |

### 管理命令

| 命令 | 状态 |
|------|------|
| FLUSH TABLES / PRIVILEGES / LOGS / STATUS / HOSTS | ✅ |
| FLUSH BINARY LOGS / ENGINE LOGS / ERROR LOGS | ✅ |
| RESET MASTER / RESET REPLICA | ✅ |
| KILL [CONNECTION\|QUERY] id | ✅ |

### 用户管理

| 功能 | 状态 |
|------|------|
| CREATE USER (持久化) | ✅ |
| ALTER USER (密码/锁定/资源限制) | ✅ |
| DROP USER | ✅ |
| CREATE ROLE / DROP ROLE | ✅ |
| GRANT role TO user | ✅ |
| REVOKE role FROM user | ✅ |
| 角色继承权限检查 | ✅ |
| 用户数据 JSON 持久化 | ✅ |

### 系统变量 (500+)

| 类别 | 数量 |
|------|------|
| InnoDB 变量 | ~100 |
| 性能与缓冲 | ~60 |
| 安全与 SSL | ~30 |
| 日志相关 | ~25 |
| 复制相关 | ~50 |
| 会话变量 | ~50 |
| Performance Schema | ~40 |
| 服务器杂项 | ~50 |

### 复制与 Binlog

| 功能 | 状态 |
|------|------|
| BinlogManager (事件记录) | ✅ |
| GTID 管理 (GtidManager) | ✅ |
| CHANGE REPLICATION SOURCE TO | ✅ |
| START REPLICA / STOP REPLICA | ✅ |
| RESET REPLICA [ALL] | ✅ |

### performance_schema (100+ 虚拟表)

| 类别 | 表数量 |
|------|--------|
| 设置表 (setup_*) | 5 |
| 实例表 (*_instances) | 5 |
| 等待事件表 (events_waits_*) | 6 |
| 阶段事件表 (events_stages_*) | 5 |
| 语句事件表 (events_statements_*) | 6 |
| 事务事件表 (events_transactions_*) | 5 |
| 连接表 (threads/accounts/users/hosts) | 6 |
| 内存表 (memory_summary_*) | 3 |
| 锁表 (metadata_locks/data_locks) | 3 |
| 复制表 (replication_*) | 6 |
| 变量/状态表 | 7 |
| 其他摘要表 | ~10 |

### sys 库 (40+ 诊断视图)

| 类别 | 代表视图 |
|------|----------|
| 主机摘要 | host_summary, host_summary_by_statement_latency |
| IO 分析 | io_by_thread_by_latency, io_global_by_file_by_bytes |
| InnoDB | innodb_buffer_stats_by_schema, innodb_lock_waits |
| 内存 | memory_global_total, memory_by_thread_by_current_bytes |
| 会话 | processlist, session |
| Schema 分析 | schema_table_statistics, schema_unused_indexes |
| 语句分析 | statement_analysis, statements_with_errors_or_warnings |
| 用户摘要 | user_summary, user_summary_by_statement_latency |
| 等待分析 | waits_global_by_latency |

### 分区表

| 功能 | 状态 |
|------|------|
| RANGE 分区 | ✅ |
| RANGE COLUMNS 分区 | ✅ |
| LIST 分区 | ✅ |
| LIST COLUMNS 分区 | ✅ |
| HASH 分区 | ✅ |
| LINEAR HASH 分区 | ✅ |
| KEY 分区 | ✅ |
| LINEAR KEY 分区 | ✅ |
| 子分区 | ✅ |
| 分区裁剪 (Partition Pruning) | ✅ |
| ADD/DROP/REORGANIZE PARTITION | ✅ |

### 高级特性

| 功能 | 状态 |
|------|------|
| 生成列 (VIRTUAL) | ✅ |
| 生成列 (STORED) | ✅ |
| 不可见索引 (INVISIBLE INDEX) | ✅ |
| 降序索引 | ✅ |
| 函数索引 (Functional Index) | ✅ |

### 查询执行引擎增强

| 功能 | 状态 |
|------|------|
| 外部排序 (ExternalSortOperator) | ✅ |
| sort_buffer_size 管理 + 磁盘溢写 | ✅ |
| K-Way Merge Sort | ✅ |
| Hash Join (HashJoinOperator) | ✅ |
| GROUP BY WITH ROLLUP | ✅ |
| 可溢写哈希聚合 (SpillableHashAggOperator) | ✅ |
| 可溢写去重 (SpillableDistinctOperator) | ✅ |
| 内部临时表引擎 (TempTableEngine) | ✅ |
| 磁盘溢写文件 (SpillFile) | ✅ |

### SQL Server TDS 协议

| 功能 | 状态 |
|------|------|
| TDS 7.4 协议栈 (PreLogin/LOGIN7/Token Stream) | ✅ |
| TDS 包读写 (TdsPacketReader/TdsPacketWriter) | ✅ |
| SQL Batch 命令处理 | ✅ |
| T-SQL 方言转换 (TsqlTranslator) | ✅ |
| sys.* 系统视图 (databases/tables/columns/objects/types/schemas/indexes) | ✅ |
| sp_* 系统存储过程 (sp_databases/sp_tables/sp_columns/sp_helpdb/sp_who) | ✅ |
| SERVERPROPERTY() 函数 | ✅ |
| @@VERSION/@@SERVERNAME/@@SPID 系统变量 | ✅ |
| SSMS Object Explorer 兼容 | ✅ |
| SQL Server 格式备份还原 (BACKUP/RESTORE DATABASE) | ✅ |

### 存储引擎增强 (InnoDB 兼容)

| 功能 | 状态 |
|------|------|
| 自适应哈希索引 (Adaptive Hash Index) | ✅ |
| 变更缓冲 (Change Buffer) | ✅ |
| 表空间加密 (AES-256-CBC) | ✅ |
| 主密钥轮换 (Master Key Rotation) | ✅ |

### 查询优化器

| 功能 | 状态 |
|------|------|
| Cost-based Optimizer (CBO) | ✅ |
| 统计信息管理 (StatisticsManager) | ✅ |
| 表扫描 / 索引扫描代价估算 | ✅ |
| JOIN 顺序优化 | ✅ |
| JOIN 算法选择 (NL vs Hash) | ✅ |
| 谓词选择性估算 | ✅ |

### 千亿级大数据量支持

| 功能 | 状态 |
|------|------|
| 全局内存预算管理器 (MemoryBudgetManager) | ✅ |
| 并行扫描 (ParallelScanOperator) | ✅ |
| 并行聚合 (ParallelAggregateOperator) | ✅ |
| 异步页面管理器 (AsyncPageManager) | ✅ |
| 大块预读 (Read-Ahead) | ✅ |
| Bloom Filter + 运行时下推 | ✅ |
| Zone Map (MIN/MAX 页面索引) | ✅ |

### 文件组与多磁盘分布式存储

| 功能 | 状态 |
|------|------|
| 文件组数据模型 (FileGroupInfo/DataFileInfo) | ✅ |
| 多文件页面管理器 (MultiFilePageManager) | ✅ |
| IPageManager 接口 (单文件/多文件统一) | ✅ |
| 页面分配策略 (ProportionalFill/RoundRobin/Striped) | ✅ |
| Extent 分配器 (8 页连续分配) | ✅ |
| 文件组级并行 I/O 调度器 (FileGroupIoScheduler) | ✅ |
| 在线添加/移除数据文件 | ✅ |

---

> 此文档记录了 CyscaleDB 当前支持的所有功能。在添加新功能时请同步更新此文档。
