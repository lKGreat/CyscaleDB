---
name: MySQL 8.0 完整语法支持
overview: 为 CyscaleDB 添加 MySQL 8.0 完整 SQL 语法支持，包括所有关键字、复杂嵌套查询、多表联查、主键外键约束、存储程序、窗口函数、CTE、JSON 等高级特性，分阶段实施。
todos:
  - id: phase1-case-when
    content: "Phase 1.1: 实现 CASE WHEN 表达式 - 添加 CASE/WHEN/THEN/ELSE/END 关键字、CaseExpression AST、解析和执行"
    status: pending
  - id: phase1-cte
    content: "Phase 1.2: 实现 CTE (WITH 子句) - 添加 WITH/RECURSIVE 关键字、WithClause AST、支持递归 CTE"
    status: pending
  - id: phase1-window
    content: "Phase 1.3: 实现窗口函数 - OVER/PARTITION BY、WindowOperator 算子、ROW_NUMBER/RANK/LAG/LEAD 等"
    status: pending
  - id: phase2-alter-table
    content: "Phase 2.1: 实现 ALTER TABLE - ADD/DROP/MODIFY COLUMN、ADD/DROP INDEX/CONSTRAINT、RENAME"
    status: pending
  - id: phase2-foreign-key
    content: "Phase 2.2: 外键约束完整支持 - ON DELETE/UPDATE CASCADE/SET NULL/RESTRICT，运行时检查"
    status: pending
  - id: phase2-check-constraint
    content: "Phase 2.3: CHECK 约束 - 解析 CHECK 子句，INSERT/UPDATE 时验证"
    status: pending
  - id: phase3-procedure
    content: "Phase 3.1: 存储过程 - CREATE/DROP PROCEDURE、CALL、IF/WHILE/LOOP 流程控制"
    status: pending
  - id: phase3-function
    content: "Phase 3.2: 存储函数 - CREATE/DROP FUNCTION、RETURNS、在表达式中调用"
    status: pending
  - id: phase3-trigger
    content: "Phase 3.3: 触发器 - CREATE/DROP TRIGGER、BEFORE/AFTER INSERT/UPDATE/DELETE"
    status: pending
  - id: phase3-event
    content: "Phase 3.4: 事件 - CREATE/DROP EVENT、事件调度器"
    status: pending
  - id: phase4-set-ops
    content: "Phase 4.1: 集合操作 - INTERSECT、EXCEPT、嵌套集合操作"
    status: pending
  - id: phase4-join-enhance
    content: "Phase 4.2: JOIN 增强 - NATURAL JOIN、USING 子句"
    status: pending
  - id: phase4-subquery
    content: "Phase 4.3: 子查询增强 - ALL/ANY/SOME、相关子查询优化"
    status: pending
  - id: phase5-json
    content: "Phase 5.1: JSON 函数 - JSON_EXTRACT/->/->>/JSON_SET/JSON_ARRAY 等"
    status: pending
  - id: phase5-spatial
    content: "Phase 5.2: 空间数据 - GEOMETRY 类型、ST_* 函数"
    status: pending
  - id: phase6-user-mgmt
    content: "Phase 6.1: 用户管理 - CREATE/DROP/ALTER USER、GRANT/REVOKE 权限"
    status: pending
  - id: phase6-admin
    content: "Phase 6.2: 管理语句 - ANALYZE TABLE、FLUSH、LOCK/UNLOCK TABLES"
    status: pending
  - id: keywords-expansion
    content: "贯穿任务: 扩展 Token.cs 关键字列表 (从约 100 个扩展到 700+)"
    status: pending
---

# MySQL 8.0 完整语法支持实施计划

## 当前状态分析

**已支持:**

- 基础 DDL/DML (CREATE/DROP TABLE/DATABASE, SELECT/INSERT/UPDATE/DELETE)
- JOIN (INNER/LEFT/RIGHT/FULL/CROSS)
- 基础子查询、UNION、聚合函数 (COUNT/SUM/AVG/MIN/MAX)
- 约束 (PRIMARY KEY, FOREIGN KEY, UNIQUE, NOT NULL, DEFAULT 解析支持)
- 约 100 个关键字

**需添加:**

- MySQL 8.0 约 700+ 关键字
- 窗口函数、CTE、CASE WHEN
- 存储过程、函数、触发器、事件
- ALTER TABLE 完整语法
- JSON 函数、空间函数
- 用户管理、权限控制

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