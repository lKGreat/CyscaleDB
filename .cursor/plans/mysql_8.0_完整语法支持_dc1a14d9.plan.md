---
name: MySQL 8.0 完整语法与InnoDB支持
overview: 为 CyscaleDB 添加 MySQL 8.0 完整 SQL 语法支持和 InnoDB 存储引擎特性。每个 case 都是可闭环的最小单元，完成后编译通过并有测试用例。
todos:
  - id: 0.1.1
    content: Row 添加 TRX_ID/ROLL_PTR 字段 - 修改 Row.cs 添加事务元数据，测试序列化/反序列化
    status: completed
  - id: 0.1.2
    content: ReadView 数据结构 - 新建 ReadView.cs，记录活跃事务列表，测试 ReadView 创建
    status: completed
  - id: 0.1.3
    content: 可见性判断 - ReadView.IsVisible() 方法，测试不同事务ID的可见性
    status: completed
  - id: 0.1.4
    content: 版本链基础 - 新建 VersionChain.cs，测试版本链存储和遍历
    status: completed
  - id: 0.1.5
    content: TableScan 集成 MVCC - 修改 TableScanOperator 使用 ReadView 过滤，测试快照读
    status: completed
  - id: 0.2.1
    content: UndoRecord 数据结构 - 新建 UndoRecord.cs，定义 Insert/Update Undo 格式
    status: completed
  - id: 0.2.2
    content: UndoLog 文件管理 - 新建 UndoLog.cs，支持写入和读取 Undo 记录
    status: completed
  - id: 0.2.3
    content: Insert Undo 写入 - INSERT 时写入 Undo，测试 Undo 记录生成
    status: completed
  - id: 0.2.4
    content: Update Undo 写入 - UPDATE/DELETE 时写入 Undo，测试旧值保存
    status: completed
  - id: 0.2.5
    content: Rollback 使用 Undo - ROLLBACK 时应用 Undo 记录，测试事务回滚
    status: completed
  - id: 0.2.6
    content: MVCC 使用 Undo - 版本链通过 Undo 获取历史版本，测试历史数据读取
    status: completed
  - id: 0.4.1
    content: Transaction 添加隔离级别属性 - 修改 Transaction.cs，测试隔离级别设置
    status: completed
  - id: 0.4.2
    content: SET TRANSACTION ISOLATION LEVEL 解析 - Parser 支持语法，测试解析
    status: completed
  - id: 0.4.3
    content: READ COMMITTED 实现 - 每次读创建新 ReadView，测试 RC 隔离
    status: completed
  - id: 0.4.4
    content: REPEATABLE READ 实现 - 事务首次读创建 ReadView，测试 RR 隔离
    status: completed
  - id: 0.4.5
    content: READ UNCOMMITTED 实现 - 不使用 ReadView，测试脏读
    status: completed
  - id: 0.4.6
    content: SERIALIZABLE 实现 - 读加锁，测试串行化隔离
    status: completed
  - id: 0.5.1
    content: ClusteredIndex 基础结构 - 新建 ClusteredIndex.cs，叶子节点存储完整行
    status: completed
  - id: 0.5.2
    content: 主键索引创建 - CREATE TABLE 时自动创建聚簇索引，测试索引创建
    status: completed
  - id: 0.5.3
    content: 聚簇索引查询 - 通过主键查询直接返回行数据，测试主键查询
    status: completed
  - id: 0.5.4
    content: SecondaryIndex 结构 - 新建 SecondaryIndex.cs，叶子存储主键值
    status: completed
  - id: 0.5.5
    content: 二级索引回表 - 二级索引查询后回表，测试回表查询
    status: completed
  - id: 0.7.1
    content: RecordLock 结构 - 新建 RecordLock.cs，锁定索引记录
    status: completed
  - id: 0.7.2
    content: GapLock 结构 - 新建 GapLock.cs，锁定索引间隙
    status: completed
  - id: 0.7.3
    content: NextKeyLock 实现 - 组合记录锁和间隙锁，测试临键锁
    status: completed
  - id: 0.7.4
    content: 意向锁实现 - 表级 IS/IX 锁，测试意向锁兼容性
    status: completed
  - id: 0.7.5
    content: SELECT FOR UPDATE 支持 - 解析和执行加锁读，测试当前读
    status: completed
  - id: 0.8.1
    content: ForeignKeyManager 基础 - 新建 ForeignKeyManager.cs，管理外键定义
    status: completed
  - id: 0.8.2
    content: 外键 RESTRICT 检查 - INSERT/UPDATE/DELETE 前检查，测试约束拒绝
    status: completed
  - id: 0.8.3
    content: 外键 CASCADE DELETE - 删除时级联删除子表，测试级联删除
    status: completed
  - id: 0.8.4
    content: 外键 CASCADE UPDATE - 更新时级联更新子表，测试级联更新
    status: completed
  - id: 0.8.5
    content: 外键 SET NULL - 删除/更新时子表设为 NULL，测试 SET NULL
    status: completed
  - id: 0.3.1
    content: MiniTransaction 结构 - 新建 MiniTransaction.cs，原子页面操作
    status: completed
  - id: 0.3.2
    content: DoublewriteBuffer - 新建，防止部分写，测试崩溃恢复
    status: completed
  - id: 0.6.1
    content: BufferPool LRU 优化 - Young/Old 区域分离，测试热点数据保留
    status: completed
  - id: 0.6.2
    content: FlushList 脏页管理 - 新建 FlushList.cs，测试脏页刷盘
    status: completed
  - id: 0.6.3
    content: 预读实现 - 线性预读，测试顺序扫描性能
    status: completed
  - id: 0.9.1
    content: ALTER TABLE ADD COLUMN 在线 - 不锁表添加列，测试并发 DML
    status: completed
  - id: 0.9.2
    content: ALTER TABLE DROP COLUMN 在线 - 不锁表删除列，测试并发 DML
    status: completed
  - id: 0.9.3
    content: ALTER TABLE ADD INDEX 在线 - 不锁表创建索引，测试并发 DML
    status: completed
  - id: 1.1.1
    content: CASE/WHEN/THEN/ELSE/END 关键字 - 添加到 Token.cs，测试词法分析
    status: completed
  - id: 1.1.2
    content: CaseExpression AST - 新建 AST 节点，测试节点创建
    status: completed
  - id: 1.1.3
    content: Simple CASE 解析 - CASE expr WHEN val THEN result，测试解析
    status: completed
  - id: 1.1.4
    content: Searched CASE 解析 - CASE WHEN cond THEN result，测试解析
    status: completed
  - id: 1.1.5
    content: CASE 表达式执行 - Executor 中求值 CASE，测试执行结果
    status: completed
  - id: 1.2.1
    content: WITH/RECURSIVE 关键字 - 添加到 Token.cs，测试词法分析
    status: completed
  - id: 1.2.2
    content: WithClause/CteDefinition AST - 新建 AST 节点
    status: completed
  - id: 1.2.3
    content: 非递归 CTE 解析 - WITH cte AS (SELECT...)，测试解析
    status: completed
  - id: 1.2.4
    content: 非递归 CTE 执行 - 物化 CTE 结果，测试查询
    status: completed
  - id: 1.2.5
    content: 递归 CTE 解析 - WITH RECURSIVE cte AS (...)，测试解析
    status: completed
  - id: 1.2.6
    content: 递归 CTE 执行 - 迭代执行直到不产生新行，测试递归查询
    status: completed
  - id: 1.3.1
    content: OVER/PARTITION/ROWS/RANGE 关键字 - 添加到 Token.cs
    status: completed
  - id: 1.3.2
    content: WindowSpec AST - 新建窗口规范 AST 节点
    status: completed
  - id: 1.3.3
    content: OVER 子句解析 - func() OVER (PARTITION BY ... ORDER BY ...)，测试解析
    status: completed
  - id: 1.3.4
    content: WindowOperator 基础 - 新建算子，分区和排序数据
    status: completed
  - id: 1.3.5
    content: ROW_NUMBER 实现 - 测试行号计算
    status: completed
  - id: 1.3.6
    content: RANK/DENSE_RANK 实现 - 测试排名计算
    status: completed
  - id: 1.3.7
    content: LAG/LEAD 实现 - 测试前后行访问
    status: completed
  - id: 1.3.8
    content: SUM/AVG OVER 实现 - 窗口聚合函数，测试累计计算
    status: completed
  - id: 2.1.1
    content: ALTER 关键字 - 添加到 Token.cs
    status: completed
  - id: 2.1.2
    content: AlterTableStatement AST - 新建 AST 节点
    status: completed
  - id: 2.1.3
    content: ALTER TABLE ADD COLUMN 解析 - 测试解析
    status: completed
  - id: 2.1.4
    content: ALTER TABLE ADD COLUMN 执行 - 测试添加列
    status: completed
  - id: 2.1.5
    content: ALTER TABLE DROP COLUMN 解析执行 - 测试删除列
    status: completed
  - id: 2.1.6
    content: ALTER TABLE MODIFY COLUMN 解析执行 - 测试修改列
    status: completed
  - id: 2.1.7
    content: ALTER TABLE ADD INDEX 解析执行 - 测试添加索引
    status: completed
  - id: 2.1.8
    content: ALTER TABLE DROP INDEX 解析执行 - 测试删除索引
    status: completed
  - id: 2.1.9
    content: ALTER TABLE RENAME 解析执行 - 测试重命名表
    status: completed
  - id: 2.2.1
    content: ON DELETE/UPDATE 关键字 - 添加到 Token.cs
    status: completed
  - id: 2.2.2
    content: ForeignKeyConstraint AST 增强 - 添加 OnDelete/OnUpdate 属性
    status: completed
  - id: 2.2.3
    content: 外键完整语法解析 - REFERENCES t(c) ON DELETE CASCADE ON UPDATE SET NULL
    status: completed
  - id: 2.2.4
    content: 外键元数据存储 - Catalog 保存外键定义，测试持久化
    status: completed
  - id: 2.3.1
    content: CHECK 约束 AST - 添加 CheckConstraint 节点
    status: completed
  - id: 2.3.2
    content: CHECK 约束解析 - CHECK (age > 0 AND age < 150)
    status: completed
  - id: 2.3.3
    content: CHECK 约束 INSERT 验证 - 插入时检查约束，测试拒绝
    status: completed
  - id: 2.3.4
    content: CHECK 约束 UPDATE 验证 - 更新时检查约束，测试拒绝
    status: completed
  - id: 3.1.1
    content: PROCEDURE/CALL/DECLARE 等关键字 - 添加到 Token.cs
    status: pending
  - id: 3.1.2
    content: CreateProcedureStatement AST - 新建 AST 节点
    status: pending
  - id: 3.1.3
    content: CREATE PROCEDURE 解析 - 解析过程定义
    status: pending
  - id: 3.1.4
    content: 过程体 BEGIN...END 解析 - 解析过程体语句
    status: pending
  - id: 3.1.5
    content: DECLARE 变量解析 - 解析局部变量声明
    status: pending
  - id: 3.1.6
    content: SET 变量赋值 - 过程内变量赋值
    status: pending
  - id: 3.1.7
    content: IF...THEN...ELSE 解析执行 - 条件分支
    status: pending
  - id: 3.1.8
    content: WHILE...DO 解析执行 - 循环语句
    status: pending
  - id: 3.1.9
    content: CALL 语句解析执行 - 调用存储过程，测试过程调用
    status: pending
  - id: 3.1.10
    content: DROP PROCEDURE 解析执行 - 删除过程
    status: pending
  - id: 3.2.1
    content: FUNCTION/RETURNS 关键字 - 添加到 Token.cs
    status: pending
  - id: 3.2.2
    content: CreateFunctionStatement AST - 新建 AST 节点
    status: pending
  - id: 3.2.3
    content: CREATE FUNCTION 解析 - 解析函数定义
    status: pending
  - id: 3.2.4
    content: RETURN 语句解析 - 解析返回值
    status: pending
  - id: 3.2.5
    content: 函数执行和返回值 - 测试函数调用
    status: pending
  - id: 3.2.6
    content: 表达式中调用函数 - SELECT my_func(col)，测试
    status: pending
  - id: 3.3.1
    content: TRIGGER/BEFORE/AFTER 关键字 - 添加到 Token.cs
    status: pending
  - id: 3.3.2
    content: CreateTriggerStatement AST - 新建 AST 节点
    status: pending
  - id: 3.3.3
    content: CREATE TRIGGER 解析 - BEFORE INSERT ON t FOR EACH ROW
    status: pending
  - id: 3.3.4
    content: NEW/OLD 伪记录 - 触发器中访问新旧值
    status: pending
  - id: 3.3.5
    content: BEFORE INSERT 触发器执行 - 测试插入前触发
    status: pending
  - id: 3.3.6
    content: AFTER INSERT 触发器执行 - 测试插入后触发
    status: pending
  - id: 3.3.7
    content: BEFORE/AFTER UPDATE 触发器 - 测试更新触发
    status: pending
  - id: 3.3.8
    content: BEFORE/AFTER DELETE 触发器 - 测试删除触发
    status: pending
  - id: 3.4.1
    content: EVENT/SCHEDULE/EVERY 关键字 - 添加到 Token.cs
    status: pending
  - id: 3.4.2
    content: CreateEventStatement AST - 新建 AST 节点
    status: pending
  - id: 3.4.3
    content: CREATE EVENT 解析 - 解析事件定义和调度
    status: pending
  - id: 3.4.4
    content: 事件调度器 - 后台线程执行定时事件
    status: pending
  - id: 3.4.5
    content: 事件执行 - 测试定时任务执行
    status: pending
  - id: 4.1.1
    content: INTERSECT/EXCEPT 关键字 - 添加到 Token.cs
    status: completed
  - id: 4.1.2
    content: INTERSECT 解析 - SELECT ... INTERSECT SELECT ...
    status: completed
  - id: 4.1.3
    content: INTERSECT 执行 - 返回交集，测试
    status: completed
  - id: 4.1.4
    content: EXCEPT 解析执行 - 返回差集，测试
    status: completed
  - id: 4.1.5
    content: 嵌套集合操作 - (A UNION B) INTERSECT C，测试
    status: completed
  - id: 4.2.1
    content: NATURAL/USING 关键字 - 添加到 Token.cs
    status: completed
  - id: 4.2.2
    content: NATURAL JOIN 解析 - 自动匹配同名列
    status: completed
  - id: 4.2.3
    content: NATURAL JOIN 执行 - 测试自然连接
    status: completed
  - id: 4.2.4
    content: USING 子句解析 - JOIN ... USING (col1, col2)
    status: completed
  - id: 4.2.5
    content: USING 子句执行 - 测试 USING 连接
    status: completed
  - id: 4.3.1
    content: ALL/ANY/SOME 关键字 - 添加到 Token.cs
    status: completed
  - id: 4.3.2
    content: 比较子查询 AST - col > ALL (SELECT ...)
    status: completed
  - id: 4.3.3
    content: ALL 子查询执行 - 与所有值比较，测试
    status: completed
  - id: 4.3.4
    content: ANY/SOME 子查询执行 - 与任一值比较，测试
    status: completed
  - id: 4.3.5
    content: 相关子查询优化 - 减少重复执行，测试性能
    status: pending
  - id: 5.1.1
    content: JSON 数据类型 - 添加到 DataType.cs
    status: completed
  - id: 5.1.2
    content: "->/->> 操作符 - Lexer 支持 JSON 路径操作符"
    status: pending
  - id: 5.1.3
    content: JSON_EXTRACT 函数 - 提取 JSON 值，测试
    status: completed
  - id: 5.1.4
    content: "-> 操作符执行 - col->'$.key'，测试"
    status: pending
  - id: 5.1.5
    content: "->> 操作符执行 - col->>'$.key' 返回文本，测试"
    status: pending
  - id: 5.1.6
    content: JSON_SET/JSON_INSERT 函数 - 修改 JSON，测试
    status: completed
  - id: 5.1.7
    content: JSON_ARRAY/JSON_OBJECT 函数 - 创建 JSON，测试
    status: completed
  - id: 5.2.1
    content: GEOMETRY/POINT 等类型 - 添加到 DataType.cs
    status: pending
  - id: 5.2.2
    content: ST_GeomFromText 函数 - 解析 WKT 创建几何对象
    status: pending
  - id: 5.2.3
    content: ST_AsText 函数 - 几何对象转 WKT
    status: pending
  - id: 5.2.4
    content: ST_Distance 函数 - 计算两点距离，测试
    status: pending
  - id: 5.2.5
    content: ST_Contains 函数 - 几何包含判断，测试
    status: pending
  - id: 6.1.1
    content: USER/GRANT/REVOKE 等关键字 - 添加到 Token.cs
    status: completed
  - id: 6.1.2
    content: CreateUserStatement AST - 新建 AST 节点
    status: completed
  - id: 6.1.3
    content: CREATE USER 解析执行 - 创建用户，测试
    status: completed
  - id: 6.1.4
    content: DROP USER 解析执行 - 删除用户，测试
    status: completed
  - id: 6.1.5
    content: ALTER USER 解析执行 - 修改密码，测试
    status: completed
  - id: 6.1.6
    content: GrantStatement AST - 新建 AST 节点
    status: completed
  - id: 6.1.7
    content: GRANT 解析执行 - 授予权限，测试
    status: completed
  - id: 6.1.8
    content: REVOKE 解析执行 - 撤销权限，测试
    status: completed
  - id: 6.1.9
    content: 权限检查集成 - 执行语句前检查权限，测试
    status: pending
  - id: 6.2.1
    content: ANALYZE/FLUSH/LOCK 关键字 - 添加到 Token.cs
    status: pending
  - id: 6.2.2
    content: ANALYZE TABLE 解析执行 - 更新统计信息，测试
    status: pending
  - id: 6.2.3
    content: FLUSH 解析执行 - FLUSH TABLES/PRIVILEGES，测试
    status: pending
  - id: 6.2.4
    content: LOCK TABLES 解析执行 - 表级锁定，测试
    status: pending
  - id: 6.2.5
    content: UNLOCK TABLES 解析执行 - 释放锁定，测试
    status: pending
---

# MySQL 8.0 完整语法与 InnoDB 存储引擎实施计划

## 当前状态分析

**已支持:**

- 基础 DDL/DML (CREATE/DROP TABLE/DATABASE, SELECT/INSERT/UPDATE/DELETE)
- JOIN (INNER/LEFT/RIGHT/FULL/CROSS)
- 基础子查询、UNION、聚合函数 (COUNT/SUM/AVG/MIN/MAX)
- 约束 (PRIMARY KEY, FOREIGN KEY, UNIQUE, NOT NULL, DEFAULT 解析支持)
- B-Tree 索引基础实现
- WAL 日志、事务管理基础框架
- 表级锁和行级锁基础
- 约 100 个关键字

**需添加:**

- **InnoDB 存储引擎核心特性** (MVCC, Undo Log, 聚簇索引等)
- MySQL 8.0 约 700+ 关键字
- 窗口函数、CTE、CASE WHEN
- 存储过程、函数、触发器、事件
- ALTER TABLE 完整语法
- JSON 函数、空间函数
- 用户管理、权限控制

---

## Phase 0: InnoDB 存储引擎 (最高优先级)

### 0.1 MVCC 多版本并发控制

**核心概念:**

- ReadView: 事务快照，记录活跃事务列表
- 版本链: 每行数据的历史版本链表
- 可见性判断: 根据 ReadView 判断版本是否可见

**需修改/新建文件:**

- 新建 `Storage/Mvcc/ReadView.cs`: ReadView 数据结构
- 新建 `Storage/Mvcc/VersionChain.cs`: 版本链管理
- 修改 `Storage/Row.cs`: 添加事务ID、回滚指针字段
- 修改 `Execution/Operators/TableScanOperator.cs`: 快照读支持

**数据结构:**

```
Row Header:
+------------+------------+------------+
| TRX_ID(8B) | ROLL_PTR(7B) | ROW_DATA |
+------------+------------+------------+
```

### 0.2 Undo Log

**功能:**

- Insert Undo: 记录插入行的主键，回滚时删除
- Update Undo: 记录修改前的旧值，回滚时恢复
- MVCC 支持: 提供历史版本数据

**需新建文件:**

- `Transactions/UndoLog.cs`: Undo 日志管理
- `Transactions/UndoRecord.cs`: Undo 记录格式
- `Transactions/UndoSegment.cs`: Undo 段管理

### 0.3 Redo Log 增强

**增强内容:**

- Mini-transaction (mtr): 原子性页面操作
- 双写缓冲 (Doublewrite Buffer): 防止部分写问题
- Checkpoint 优化: 异步刷脏页

**需修改文件:**

- `Transactions/WalLog.cs`: 增强为完整 Redo Log
- 新建 `Transactions/MiniTransaction.cs`
- 新建 `Storage/DoublewriteBuffer.cs`

### 0.4 事务隔离级别

| 隔离级别 | 脏读 | 不可重复读 | 幻读 | 实现方式 |

|----------|------|------------|------|----------|

| READ UNCOMMITTED | 是 | 是 | 是 | 当前读，无快照 |

| READ COMMITTED | 否 | 是 | 是 | 每次读创建 ReadView |

| REPEATABLE READ | 否 | 否 | 否* | 事务开始创建 ReadView |

| SERIALIZABLE | 否 | 否 | 否 | 加锁读 |

**需修改文件:**

- `Transactions/Transaction.cs`: 支持不同隔离级别
- `Transactions/TransactionManager.cs`: 隔离级别处理
- `Execution/Executor.cs`: 按隔离级别执行查询

### 0.5 聚簇索引

**InnoDB 索引结构:**

- 聚簇索引 (Primary Key): 叶子节点存储完整行数据
- 二级索引: 叶子节点存储主键值，需要回表查询

**需修改文件:**

- `Storage/Index/BTreeIndex.cs`: 支持聚簇索引模式
- 新建 `Storage/Index/ClusteredIndex.cs`
- 新建 `Storage/Index/SecondaryIndex.cs`
- `Execution/Operators/IndexScanOperator.cs`: 回表查询

### 0.6 Buffer Pool 增强

**增强内容:**

- LRU 列表优化: Young/Old 区域分离
- 脏页管理: Flush List
- 预读: 线性预读、随机预读
- Change Buffer: 缓存二级索引修改

**需修改文件:**

- `Storage/BufferPool.cs`: LRU 优化
- 新建 `Storage/FlushList.cs`
- 新建 `Storage/ChangeBuffer.cs`

### 0.7 行级锁完善

**锁类型:**

- 记录锁 (Record Lock): 锁定索引记录
- 间隙锁 (Gap Lock): 锁定索引间隙，防止幻读
- 临键锁 (Next-Key Lock): 记录锁 + 间隙锁
- 意向锁 (Intent Lock): 表级意向锁

**需修改文件:**

- `Transactions/LockManager.cs`: 实现完整锁类型
- 新建 `Transactions/RecordLock.cs`
- 新建 `Transactions/GapLock.cs`

### 0.8 外键运行时执行

**级联操作:**

- CASCADE: 级联删除/更新
- SET NULL: 设置为 NULL
- RESTRICT: 拒绝操作
- NO ACTION: 延迟检查

**需修改文件:**

- `Execution/Executor.cs`: DELETE/UPDATE 时检查外键
- 新建 `Storage/ForeignKeyManager.cs`

### 0.9 在线 DDL

**支持:**

- `ALTER TABLE ... ALGORITHM=INPLACE`: 不复制表
- `ALTER TABLE ... LOCK=NONE`: 不阻塞 DML

**需修改文件:**

- `Execution/Executor.cs`: ALTER TABLE 在线执行
- 新建 `Storage/OnlineDdl/OnlineDdlManager.cs`

---

## Phase 1: 核心查询增强

### 1.1 CASE WHEN 表达式

- 文件: [Token.cs](src/CyscaleDB.Core/Parsing/Token.cs), [Expressions.cs](src/CyscaleDB.Core/Parsing/Ast/Expressions.cs), [Parser.cs](src/CyscaleDB.Core/Parsing/Parser.cs), [Executor.cs](src/CyscaleDB.Core/Execution/Executor.cs)

### 1.2 CTE (WITH 子句)

- 添加 `WITH`, `RECURSIVE` 关键字
- 新增 `WithClause`, `CteDefinition` AST 节点
- 支持递归 CTE

### 1.3 窗口函数

- 添加 `OVER`, `PARTITION BY`, `ROWS`, `RANGE` 等关键字
- 新增 `WindowFunctionCall`, `WindowSpec` AST 节点
- 实现 ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD 等

---

## Phase 2: DDL 完整支持

### 2.1 ALTER TABLE

- ADD/DROP/MODIFY COLUMN
- ADD/DROP INDEX
- ADD/DROP CONSTRAINT (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK)
- RENAME TABLE/COLUMN

### 2.2 外键约束完整支持

- ON DELETE/UPDATE (CASCADE, SET NULL, RESTRICT, NO ACTION)
- 运行时外键检查

### 2.3 CHECK 约束

- 解析和执行 CHECK 约束

---

## Phase 3: 存储程序

### 3.1 存储过程

- CREATE/DROP/ALTER PROCEDURE
- CALL 语句
- 流程控制: IF/ELSE, WHILE, LOOP, REPEAT, CASE
- DECLARE 变量

### 3.2 存储函数

- CREATE/DROP/ALTER FUNCTION
- RETURNS 子句
- DETERMINISTIC/NOT DETERMINISTIC

### 3.3 触发器

- CREATE/DROP TRIGGER
- BEFORE/AFTER INSERT/UPDATE/DELETE

### 3.4 事件

- CREATE/DROP EVENT
- 事件调度器

---

## Phase 4: 高级查询特性

### 4.1 集合操作

- INTERSECT, EXCEPT
- 集合操作嵌套

### 4.2 更多 JOIN 语法

- NATURAL JOIN
- USING 子句
- 自连接优化

### 4.3 子查询增强

- 相关子查询优化
- ALL/ANY/SOME 子查询

---

## Phase 5: JSON 和空间数据

### 5.1 JSON 函数

- JSON_EXTRACT, JSON_SET, JSON_INSERT
- JSON_ARRAY, JSON_OBJECT
- JSON 路径表达式 (->>, ->)

### 5.2 空间数据

- GEOMETRY 类型
- ST_* 函数

---

## Phase 6: 管理和安全

### 6.1 用户管理

- CREATE/DROP/ALTER USER
- GRANT/REVOKE

### 6.2 其他管理语句

- ANALYZE TABLE
- FLUSH
- LOCK/UNLOCK TABLES

---

## 关键文件修改清单

### InnoDB 存储引擎 (新建文件)

| 文件 | 功能 |

|------|------|

| `Storage/Mvcc/ReadView.cs` | MVCC 读视图 |

| `Storage/Mvcc/VersionChain.cs` | 行版本链管理 |

| `Storage/Index/ClusteredIndex.cs` | 聚簇索引 |

| `Storage/Index/SecondaryIndex.cs` | 二级索引 |

| `Storage/DoublewriteBuffer.cs` | 双写缓冲 |

| `Storage/FlushList.cs` | 脏页列表 |

| `Storage/ChangeBuffer.cs` | Change Buffer |

| `Storage/ForeignKeyManager.cs` | 外键约束执行 |

| `Storage/OnlineDdl/OnlineDdlManager.cs` | 在线 DDL |

| `Transactions/UndoLog.cs` | Undo 日志 |

| `Transactions/UndoRecord.cs` | Undo 记录 |

| `Transactions/MiniTransaction.cs` | Mini-transaction |

| `Transactions/RecordLock.cs` | 记录锁 |

| `Transactions/GapLock.cs` | 间隙锁 |

### InnoDB 存储引擎 (修改文件)

| 文件 | 修改内容 |

|------|----------|

| `Storage/Row.cs` | 添加 TRX_ID、ROLL_PTR 字段 |

| `Storage/BufferPool.cs` | LRU 优化、Young/Old 区域 |

| `Storage/Index/BTreeIndex.cs` | 支持聚簇索引模式 |

| `Transactions/Transaction.cs` | 事务隔离级别支持 |

| `Transactions/TransactionManager.cs` | 隔离级别处理 |

| `Transactions/LockManager.cs` | 完整锁类型 |

| `Transactions/WalLog.cs` | 增强为完整 Redo Log |

| `Execution/Operators/TableScanOperator.cs` | MVCC 快照读 |

| `Execution/Operators/IndexScanOperator.cs` | 回表查询 |

### SQL 语法支持

| 文件 | 修改内容 |

|------|----------|

| `Token.cs` | 添加 500+ 新关键字 |

| `Lexer.cs` | 支持新操作符 (->>, ->) |

| `Statements.cs` | 添加 30+ 新语句 AST 节点 |

| `Expressions.cs` | 添加 CASE, 窗口函数等表达式 |

| `Parser.cs` | 添加所有新语法解析方法 |

| `Executor.cs` | 添加所有新语句执行逻辑 |

| 新建 `WindowOperator.cs` | 窗口函数算子 |

| 新建 `Procedure.cs` | 存储过程运行时 |

---

## 架构图: InnoDB 存储引擎

```
+------------------------------------------------------------------+
|                        SQL Layer                                  |
|  Parser -> Executor -> Operators (TableScan, IndexScan, etc.)    |
+------------------------------+-----------------------------------+
                               |
+------------------------------v-----------------------------------+
|                   Transaction Layer                               |
|  +-----------+  +-------------+  +-------------+                 |
|  |Transaction|  | LockManager |  |  ReadView   |                 |
|  |  Manager  |  | (Row Lock)  |  |   (MVCC)    |                 |
|  +-----------+  +-------------+  +-------------+                 |
+------------------------------+-----------------------------------+
                               |
+------------------------------v-----------------------------------+
|                    Storage Layer                                  |
|  +-----------+  +-------------+  +-------------+                 |
|  | Clustered |  | Secondary   |  | Buffer Pool |                 |
|  |   Index   |  |   Index     |  | (LRU/Flush) |                 |
|  +-----------+  +-------------+  +-------------+                 |
|  +-----------+  +-------------+  +-------------+                 |
|  | Undo Log  |  | Redo Log    |  | Doublewrite |                 |
|  | (Rollback)|  |   (WAL)     |  |   Buffer    |                 |
|  +-----------+  +-------------+  +-------------+                 |
+------------------------------+-----------------------------------+
                               |
+------------------------------v-----------------------------------+
|                      File Layer                                   |
|  +-----------+  +-------------+  +-------------+                 |
|  | .cdb Data |  | .wal Redo   |  | .undo Undo  |                 |
|  |   Files   |  |    Log      |  |    Log      |                 |
|  +-----------+  +-------------+  +-------------+                 |
+------------------------------------------------------------------+
```

---

## 实施顺序建议

1. **Phase 0 (InnoDB)** 先于其他 Phase 实施，因为：

   - 外键约束执行依赖 InnoDB 运行时检查
   - 存储过程/触发器需要正确的事务支持
   - 高级查询需要 MVCC 保证一致性

2. **推荐实施顺序:**

   - 0.1 MVCC -> 0.2 Undo Log -> 0.4 隔离级别 (核心)
   - 0.5 聚簇索引 -> 0.7 行级锁 (性能)
   - 0.3 Redo 增强 -> 0.6 Buffer Pool (可靠性)
   - 0.8 外键执行 -> 0.9 在线 DDL (功能)
   - Phase 1-6 按顺序实施