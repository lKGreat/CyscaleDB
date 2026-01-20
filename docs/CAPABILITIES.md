# CyscaleDB 功能能力清单

## 支持的 SQL 语句

### DDL (数据定义语言)

| 语句 | 状态 | 示例 |
|------|------|------|
| CREATE DATABASE | ✅ | `CREATE DATABASE mydb` |
| DROP DATABASE | ✅ | `DROP DATABASE IF EXISTS mydb` |
| CREATE TABLE | ✅ | `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))` |
| DROP TABLE | ✅ | `DROP TABLE IF EXISTS users` |

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

### information_schema 虚拟表

| 表名 | 状态 | 说明 |
|------|------|------|
| SCHEMATA | ✅ | 数据库列表 |
| TABLES | ✅ | 表列表 |
| COLUMNS | ✅ | 列信息 |
| STATISTICS | ✅ | 索引统计 |
| ENGINES | ✅ | 存储引擎信息 |

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
| INT | 4字节 | -2,147,483,648 ~ 2,147,483,647 |
| BIGINT | 8字节 | -9,223,372,036,854,775,808 ~ 9,223,372,036,854,775,807 |

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
| TEXT | 大文本 |

### 时间类型

| 类型 | 说明 |
|------|------|
| DATE | 日期 (YYYY-MM-DD) |
| TIME | 时间 (HH:MM:SS) |
| DATETIME | 日期时间 |
| TIMESTAMP | 时间戳 |

### 其他类型

| 类型 | 说明 |
|------|------|
| BOOLEAN | 布尔值 |
| BLOB | 二进制大对象 |

---

## 列约束

| 约束 | 状态 |
|------|------|
| PRIMARY KEY | ✅ |
| NOT NULL | ✅ |
| AUTO_INCREMENT | ✅ |
| DEFAULT | ✅ |
| UNIQUE | ✅ |

---

## 存储引擎特性

| 特性 | 说明 |
|------|------|
| 页面大小 | 4KB 固定大小 |
| 缓冲池 | LRU 缓存策略 |
| WAL日志 | 预写日志，支持崩溃恢复 |
| 事务隔离 | 表级锁 |
| 文件格式 | .cdb 数据文件 |

---

## 协议支持

| 协议 | 状态 | 说明 |
|------|------|------|
| MySQL Protocol | ✅ | 可使用标准 MySQL 客户端连接 |
| 默认端口 | 3306 | |
| 认证 | ⚠️ | MVP阶段跳过密码验证 |
| 多语句支持 | ✅ | 支持用分号分隔的多条SQL语句 |

---

## 已知限制

1. **索引**: 目前不支持索引，所有查询为全表扫描
2. **子查询**: 基础支持，复杂嵌套查询可能不完整
3. **存储过程**: 不支持
4. **视图**: 不支持
5. **触发器**: 不支持
6. **外键约束**: 解析支持，运行时不强制

---

## 执行算子

| 算子 | 说明 |
|------|------|
| TableScanOperator | 全表扫描 |
| FilterOperator | WHERE 条件过滤 |
| ProjectOperator | 列投影 |
| NestedLoopJoinOperator | 嵌套循环连接 |
| GroupByOperator | 分组聚合 |
| OrderByOperator | 排序 |
| LimitOperator | 限制结果数量 |
| DistinctOperator | 去重 |
| DualOperator | 虚拟表 (无FROM查询) |

---

> 此文档记录了 CyscaleDB 当前支持的所有功能。在添加新功能时请同步更新此文档。
