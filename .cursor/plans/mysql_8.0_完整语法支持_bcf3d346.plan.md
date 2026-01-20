---
name: MySQL 8.0 完整语法支持
overview: 为 CyscaleDB 添加 MySQL 8.0 完整 SQL 语法支持，包括所有关键字、复杂嵌套查询、多表联查、主键外键约束、存储程序、窗口函数、CTE、JSON 等高级特性。分 6 个阶段实施。
todos: []
---

# MySQL 8.0 完整语法支持实施计划

## 项目现状分析

当前 CyscaleDB 已支持基础 SQL 功能：

- 基础 DDL/DML (CREATE/DROP/SELECT/INSERT/UPDATE/DELETE)
- JOIN (INNER/LEFT/RIGHT/FULL/CROSS)
- 基础子查询、UNION、聚合函数
- 约束 (PRIMARY KEY, FOREIGN KEY 解析支持)

需要添加 MySQL 8.0 的 **500+ 关键字** 和大量高级特性。

---

## Phase 1: 核心查询增强 (高优先级)

### 1.1 CTE (Common Table Expressions)

**需修改文件:**

- [Token.cs](src/CyscaleDB.Core/Parsing/Token.cs): 添加 `WITH`, `RECURSIVE` 关键字
- [Statements.cs](src/CyscaleDB.Core/Parsing/Ast/Statements.cs): 添加 `WithClause`, `CteDefinition` AST 节点
- [Parser.cs](src/CyscaleDB.Core/Parsing/Parser.cs): 添加 `ParseWithClause()` 方法
- [Executor.cs](src/CyscaleDB.Core/Execution/Executor.cs): 实现 CTE 执行逻辑

**语法示例:**

```sql
WITH cte AS (SELECT * FROM t1)
SELECT * FROM cte;

WITH RECURSIVE cte AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM cte WHERE n < 10
)
SELECT * FROM cte;
```

### 1.2 CASE WHEN 表达式

**需修改文件:**

- [Token.cs](src/CyscaleDB.Core/Parsing/Token.cs): 添加 `CASE`, `WHEN`, `THEN`, `ELSE`, `END` 关键字
- [Expressions.cs](src/CyscaleDB.Core/Parsing/Ast/Expressions.cs): 添加 `CaseExpression` AST 节点
- [Parser.cs](src/CyscaleDB.Core/Parsing/Parser.cs): 添加 `ParseCaseExpression()` 方法
- [Executor.cs](src/CyscaleDB.Core/Execution/Executor.cs): 在 `BuildExpression()` 中处理 CASE

**语法示例:**

```sql
SELECT CASE status WHEN 1 THEN 'Active' ELSE 'Inactive' END FROM users;
SELECT CASE WHEN age > 18 THEN 'Adult' ELSE 'Minor' END FROM users;
```

### 1.3 窗口函数

**需修改文件:**

- [Token.cs](src/CyscaleDB.Core/Parsing/Token.cs): 添加 `OVER`, `PARTITION`, `ROWS`, `RANGE`, `UNBOUNDED`, `PRECEDING`, `FOLLOWING`, `CURRENT`, `ROW` 等关键字
- [Expressions.cs](src/CyscaleDB.Core/Parsing/Ast/Expressions.cs): 添加 `WindowFunctionCall`, `WindowSpec` AST 节点
- [Parser.cs](src/CyscaleDB.Core/Parsing/Parser.cs): 修改 `ParseFunctionCall()` 支持 OVER 子句
- 新建 [Operators/WindowOperator.cs](src/CyscaleDB.Core/Execution