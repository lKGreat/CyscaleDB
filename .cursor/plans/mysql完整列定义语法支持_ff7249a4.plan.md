---
name: MySQL完整列定义语法支持
overview: 为CyscaleDB添加完整的MySQL列定义语法支持，包括CHARACTER SET、COLLATE、UNSIGNED、ZEROFILL等修饰符，以及缺失的数据类型。
todos:
  - id: add-datatypes
    content: "DataType.cs: 添加MediumInt, TinyText, MediumText, LongText, TinyBlob, MediumBlob, LongBlob, VarBinary, Binary, Year, Bit等新数据类型"
    status: completed
  - id: add-tokens
    content: "Token.cs: 添加MEDIUMINT, TINYTEXT, MEDIUMTEXT, LONGTEXT, TINYBLOB, MEDIUMBLOB, LONGBLOB, VARBINARY, YEAR, BIT, UNSIGNED, ZEROFILL, SIGNED, CHARACTER, COMMENT等TokenType和Keywords映射"
    status: completed
  - id: extend-columndef
    content: "Statements.cs: 在ColumnDef类中添加CharacterSet, Collation, IsUnsigned, IsZerofill, Comment, OnUpdateCurrentTimestamp属性"
    status: completed
  - id: update-parser-datatype
    content: "Parser.cs: 修改ParseDataType方法支持新数据类型，并解析UNSIGNED/ZEROFILL/SIGNED修饰符"
    status: completed
  - id: update-parser-columndef
    content: "Parser.cs: 修改ParseColumnDefinition方法支持CHARACTER SET、COLLATE、COMMENT、ON UPDATE CURRENT_TIMESTAMP语法"
    status: completed
  - id: update-docs
    content: "CAPABILITIES.md: 更新文档记录新增的数据类型和列定义语法支持"
    status: completed
---

# MySQL 完整列定义语法支持

## 问题分析

当前解析器在处理以下SQL时失败：

```sql
ALTER TABLE `user` MODIFY COLUMN `id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' FIRST
```

错误原因：`ParseColumnDefinition` 方法不识别 `CHARACTER SET` 和 `COLLATE` 关键字。

## 需要支持的语法

### 1. 字符集和排序规则（字符串类型）

- `CHARACTER SET charset_name` 或 `CHARSET charset_name`
- `COLLATE collation_name`

### 2. 数值类型修饰符

- `UNSIGNED` - 无符号
- `ZEROFILL` - 零填充（自动隐含UNSIGNED）
- `SIGNED` - 有符号（默认，解析后忽略）

### 3. 列注释

- `COMMENT 'comment_text'`

### 4. 时间戳自动更新

- `ON UPDATE CURRENT_TIMESTAMP`

### 5. 缺失的数据类型

- `MEDIUMINT` - 3字节整数
- `TINYTEXT`, `MEDIUMTEXT`, `LONGTEXT` - 不同大小的文本类型
- `TINYBLOB`, `MEDIUMBLOB`, `LONGBLOB` - 不同大小的二进制类型
- `VARBINARY(n)` - 变长二进制
- `BINARY(n)` - 定长二进制（作为数据类型）
- `YEAR` - 年份类型
- `BIT(n)` - 位类型

## 实现方案

### 文件修改清单

1. **[src/CyscaleDB.Core/Common/DataType.cs](src/CyscaleDB.Core/Common/DataType.cs)** - 添加新数据类型枚举

2. **[src/CyscaleDB.Core/Parsing/Token.cs](src/CyscaleDB.Core/Parsing/Token.cs)** - 添加新关键字Token

3. **[src/CyscaleDB.Core/Parsing/Ast/Statements.cs](src/CyscaleDB.Core/Parsing/Ast/Statements.cs)** - 扩展ColumnDef类

4. **[src/CyscaleDB.Core/Parsing/Parser.cs](src/CyscaleDB.Core/Parsing/Parser.cs)** - 修改解析逻辑

5. **[docs/CAPABILITIES.md](docs/CAPABILITIES.md)** - 更新文档

### 详细修改

#### A. DataType.cs - 新增数据类型

```csharp
MediumInt = 5,      // 3字节整数
TinyText = 13,      // 小文本
MediumText = 14,    // 中等文本  
LongText = 15,      // 大文本
TinyBlob = 52,      // 小二进制
MediumBlob = 53,    // 中等二进制
LongBlob = 54,      // 大二进制
VarBinary = 55,     // 变长二进制
Binary = 56,        // 定长二进制
Year = 34,          // 年份
Bit = 80,           // 位类型
```

#### B. Token.cs - 新增关键字

```csharp
// TokenType枚举新增
MEDIUMINT, TINYTEXT, MEDIUMTEXT, LONGTEXT, 
TINYBLOB, MEDIUMBLOB, LONGBLOB, VARBINARY, 
YEAR, BIT, UNSIGNED, ZEROFILL, SIGNED, 
CHARACTER, COMMENT, ON

// Keywords字典新增对应映射
```

#### C. ColumnDef类 - 新增属性

```csharp
public string? CharacterSet { get; set; }    // 字符集
public string? Collation { get; set; }        // 排序规则
public bool IsUnsigned { get; set; }          // 无符号
public bool IsZerofill { get; set; }          // 零填充
public string? Comment { get; set; }          // 列注释
public bool OnUpdateCurrentTimestamp { get; set; }  // 自动更新时间戳
```

#### D. Parser.cs - 解析逻辑修改

**ParseDataType方法**:

- 添加新数据类型的识别
- 在类型后解析 `UNSIGNED`/`ZEROFILL`/`SIGNED`

**ParseColumnDefinition方法**:

- 在约束解析循环中添加:
  - `CHARACTER SET` / `CHARSET` 解析
  - `COLLATE` 解析  
  - `COMMENT` 解析
  - `ON UPDATE CURRENT_TIMESTAMP` 解析

关键代码位置: `Parser.cs` 第1189-1238行