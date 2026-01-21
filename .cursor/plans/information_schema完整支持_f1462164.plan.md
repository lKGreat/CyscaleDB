---
name: information_schema完整支持
overview: 为CyscaleDB实现MySQL 8.0 information_schema数据库的完整支持，包括所有通用元数据表、权限表和InnoDB特定表，共计70+个虚拟表的实现。
todos:
  - id: phase1-core
    content: "Phase 1: 完善TABLES/COLUMNS表列定义，实现CHARACTER_SETS/COLLATIONS/COLLATION_CHARACTER_SET_APPLICABILITY表"
    status: completed
  - id: phase2-constraint-system
    content: "Phase 2: 实现TABLE_CONSTRAINTS/CHECK_CONSTRAINTS/PROCESSLIST/PLUGINS/EVENTS/PARTITIONS表"
    status: completed
  - id: phase3-privileges
    content: "Phase 3: 实现COLUMN_PRIVILEGES/TABLE_PRIVILEGES/SCHEMA_PRIVILEGES/USER_PRIVILEGES/USER_ATTRIBUTES表"
    status: completed
  - id: phase4-roles
    content: "Phase 4: 实现角色相关表(ADMINISTRABLE_ROLE_AUTHORIZATIONS等6个表)"
    status: completed
  - id: phase5-extensions
    content: "Phase 5: 实现扩展表(*_EXTENSIONS/VIEW_*_USAGE/KEYWORDS等)"
    status: completed
  - id: phase6-innodb-buffer
    content: "Phase 6: 实现InnoDB Buffer Pool相关表(INNODB_BUFFER_*/INNODB_CACHED_INDEXES)"
    status: completed
  - id: phase7-innodb-metadata
    content: "Phase 7: 实现InnoDB元数据表(INNODB_COLUMNS/TABLES/INDEXES/FOREIGN等)"
    status: completed
  - id: phase8-innodb-cmp-ft
    content: "Phase 8: 实现InnoDB压缩表(INNODB_CMP*)和全文索引表(INNODB_FT_*)"
    status: completed
  - id: phase9-innodb-tablespace
    content: "Phase 9: 实现InnoDB表空间和事务表(INNODB_TABLESPACES/INNODB_TRX/INNODB_METRICS等)"
    status: completed
  - id: test-validation
    content: "测试验证: 单元测试 + Navicat/DBeaver/MySQL Workbench兼容性验证"
    status: in_progress
---

# MySQL 8.0 information_schema 完整支持实施计划

## 当前状态分析

已实现的表（12个）：

- SCHEMATA, TABLES, COLUMNS, STATISTICS, ENGINES, VIEWS
- ROUTINES, PARAMETERS, FILES, KEY_COLUMN_USAGE, REFERENTIAL_CONSTRAINTS, TRIGGERS（均为空表桩）

需新增的表（60+个），分为以下类别：

---

## 一、核心元数据表（高优先级 - 客户端工具必需）

### 1.1 字符集与排序规则（3个表）

| 表名 | 用途 | 列数 |

|------|------|------|

| CHARACTER_SETS | 可用字符集列表 | 4 |

| COLLATIONS | 排序规则列表 | 6 |

| COLLATION_CHARACTER_SET_APPLICABILITY | 字符集-排序规则映射 | 2 |

**实现要点**：返回静态数据，包含utf8mb4、utf8、latin1等常用字符集

### 1.2 约束信息（2个表）

| 表名 | 用途 | 需要数据 |

|------|------|----------|

| TABLE_CONSTRAINTS | 所有表约束 | PRIMARY KEY、UNIQUE、FOREIGN KEY |

| CHECK_CONSTRAINTS | CHECK约束 | 从表定义获取 |

### 1.3 进程与系统信息（4个表）

| 表名 | 用途 |

|------|------|

| PROCESSLIST | 当前连接/线程 |

| PLUGINS | 服务器插件 |

| EVENTS | 事件调度器事件 |

| PARTITIONS | 分区信息 |

---

## 二、权限与角色表（中优先级）

### 2.1 权限表（4个表）

| 表名 | 用途 |

|------|------|

| COLUMN_PRIVILEGES | 列级权限 |

| TABLE_PRIVILEGES | 表级权限 |

| SCHEMA_PRIVILEGES | 库级权限 |

| USER_PRIVILEGES | 用户全局权限 |

### 2.2 角色表（6个表）

| 表名 | 用途 |

|------|------|

| USER_ATTRIBUTES | 用户属性 |

| ADMINISTRABLE_ROLE_AUTHORIZATIONS | 可授予角色 |

| APPLICABLE_ROLES | 适用角色 |

| ENABLED_ROLES | 已启用角色 |

| ROLE_COLUMN_GRANTS | 角色列授权 |

| ROLE_ROUTINE_GRANTS | 角色例程授权 |

| ROLE_TABLE_GRANTS | 角色表授权 |

---

## 三、扩展元数据表（中优先级）

### 3.1 Extension表（5个表）

| 表名 | 用途 |

|------|------|

| SCHEMATA_EXTENSIONS | 库扩展属性 |

| TABLES_EXTENSIONS | 表扩展属性 |

| COLUMNS_EXTENSIONS | 列扩展属性 |

| TABLE_CONSTRAINTS_EXTENSIONS | 约束扩展属性 |

| TABLESPACES_EXTENSIONS | 表空间扩展属性 |

### 3.2 视图依赖表（2个表）

| 表名 | 用途 |

|------|------|

| VIEW_ROUTINE_USAGE | 视图引用的例程 |

| VIEW_TABLE_USAGE | 视图引用的表 |

### 3.3 其他系统表（4个表）

| 表名 | 用途 |

|------|------|

| COLUMN_STATISTICS | 直方图统计 |

| KEYWORDS | MySQL关键字列表 |

| OPTIMIZER_TRACE | 优化器追踪 |

| PROFILING | 语句分析 |

| RESOURCE_GROUPS | 资源组 |

| TABLESPACES | 表空间 |

---

## 四、InnoDB特定表（30个表）

### 4.1 Buffer Pool相关（4个表）

- INNODB_BUFFER_PAGE
- INNODB_BUFFER_PAGE_LRU  
- INNODB_BUFFER_POOL_STATS
- INNODB_CACHED_INDEXES

### 4.2 压缩相关（6个表）

- INNODB_CMP / INNODB_CMP_RESET
- INNODB_CMP_PER_INDEX / INNODB_CMP_PER_INDEX_RESET
- INNODB_CMPMEM / INNODB_CMPMEM_RESET

### 4.3 内部元数据（7个表）

- INNODB_COLUMNS
- INNODB_DATAFILES
- INNODB_FIELDS
- INNODB_FOREIGN
- INNODB_FOREIGN_COLS
- INNODB_INDEXES
- INNODB_TABLES

### 4.4 全文索引（6个表）

- INNODB_FT_BEING_DELETED
- INNODB_FT_CONFIG
- INNODB_FT_DEFAULT_STOPWORD
- INNODB_FT_DELETED
- INNODB_FT_INDEX_CACHE
- INNODB_FT_INDEX_TABLE

### 4.5 表空间与事务（7个表）

- INNODB_METRICS
- INNODB_SESSION_TEMP_TABLESPACES
- INNODB_TABLESPACES
- INNODB_TABLESPACES_BRIEF
- INNODB_TABLESTATS
- INNODB_TEMP_TABLE_INFO
- INNODB_TRX

---

## 五、实现架构

### 5.1 代码结构扩展

```
src/CyscaleDB.Core/Storage/InformationSchema/
├── InformationSchemaProvider.cs          # 主入口（已存在，需扩展）
├── Tables/
│   ├── CharacterSetTables.cs             # CHARACTER_SETS, COLLATIONS 等
│   ├── ConstraintTables.cs               # TABLE_CONSTRAINTS, CHECK_CONSTRAINTS
│   ├── PrivilegeTables.cs                # *_PRIVILEGES 表
│   ├── RoleTables.cs                     # *_ROLE* 表
│   ├── SystemTables.cs                   # PROCESSLIST, PLUGINS, EVENTS 等
│   ├── ExtensionTables.cs                # *_EXTENSIONS 表
│   └── InnoDbTables.cs                   # INNODB_* 表
└── Helpers/
    └── SchemaDefinitions.cs              # 表结构定义常量
```

### 5.2 核心修改文件

| 文件 | 修改内容 |

|------|----------|

| [`InformationSchemaProvider.cs`](src/CyscaleDB.Core/Storage/InformationSchema/InformationSchemaProvider.cs) | 扩展 SupportedTables 列表，添加新表分派逻辑 |

| [`Executor.cs`](src/CyscaleDB.Core/Execution/Executor.cs) | 无需修改（已支持通用查询） |

| [`docs/CAPABILITIES.md`](docs/CAPABILITIES.md) | 更新支持的表列表 |

### 5.3 TABLES表列完善

当前TABLES表仅7列，需扩展到21列以匹配MySQL 8.0：

```csharp
// 需要添加的列：
VERSION, ROW_FORMAT, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, 
MAX_DATA_LENGTH, INDEX_LENGTH, DATA_FREE, AUTO_INCREMENT,
CREATE_TIME, UPDATE_TIME, CHECK_TIME, CHECKSUM, CREATE_OPTIONS
```

### 5.4 COLUMNS表列完善

当前COLUMNS表11列，需扩展到22列：

```csharp
// 需要添加的列：
CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION,
NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME,
PRIVILEGES, GENERATION_EXPRESSION, SRS_ID
```

---

## 六、实施阶段

### Phase 1: 核心表完善与字符集表（估计改动：~500行）

1. 完善TABLES表（增加14列）
2. 完善COLUMNS表（增加11列）
3. 实现CHARACTER_SETS表
4. 实现COLLATIONS表
5. 实现COLLATION_CHARACTER_SET_APPLICABILITY表

### Phase 2: 约束与系统表（估计改动：~600行）

1. 实现TABLE_CONSTRAINTS表
2. 实现CHECK_CONSTRAINTS表
3. 实现PROCESSLIST表
4. 实现PLUGINS表
5. 实现EVENTS表
6. 实现PARTITIONS表

### Phase 3: 权限表（估计改动：~400行）

1. 实现COLUMN_PRIVILEGES表
2. 实现TABLE_PRIVILEGES表
3. 实现SCHEMA_PRIVILEGES表
4. 实现USER_PRIVILEGES表
5. 实现USER_ATTRIBUTES表

### Phase 4: 角色表（估计改动：~500行）

1. 实现ADMINISTRABLE_ROLE_AUTHORIZATIONS表
2. 实现APPLICABLE_ROLES表
3. 实现ENABLED_ROLES表
4. 实现ROLE_COLUMN_GRANTS表
5. 实现ROLE_ROUTINE_GRANTS表
6. 实现ROLE_TABLE_GRANTS表

### Phase 5: 扩展表（估计改动：~600行）

1. 实现SCHEMATA_EXTENSIONS表
2. 实现TABLES_EXTENSIONS表
3. 实现COLUMNS_EXTENSIONS表
4. 实现TABLE_CONSTRAINTS_EXTENSIONS表
5. 实现VIEW_ROUTINE_USAGE表
6. 实现VIEW_TABLE_USAGE表
7. 实现KEYWORDS表
8. 实现其他系统表

### Phase 6: InnoDB表 - Buffer Pool（估计改动：~400行）

1. 实现INNODB_BUFFER_PAGE表
2. 实现INNODB_BUFFER_PAGE_LRU表
3. 实现INNODB_BUFFER_POOL_STATS表
4. 实现INNODB_CACHED_INDEXES表

### Phase 7: InnoDB表 - 元数据（估计改动：~500行）

1. 实现INNODB_COLUMNS表
2. 实现INNODB_DATAFILES表
3. 实现INNODB_FIELDS表
4. 实现INNODB_FOREIGN表
5. 实现INNODB_FOREIGN_COLS表
6. 实现INNODB_INDEXES表
7. 实现INNODB_TABLES表

### Phase 8: InnoDB表 - 压缩与全文（估计改动：~600行）

1. 实现6个压缩相关表（INNODB_CMP*）
2. 实现6个全文索引表（INNODB_FT_*）

### Phase 9: InnoDB表 - 表空间与事务（估计改动：~500行）

1. 实现INNODB_METRICS表
2. 实现INNODB_TABLESPACES表
3. 实现INNODB_TABLESPACES_BRIEF表
4. 实现INNODB_TRX表
5. 实现其他表空间相关表

---

## 七、测试计划

1. 为每个新表添加单元测试
2. 使用Navicat连接验证兼容性
3. 使用DBeaver连接验证兼容性
4. 使用MySQL Workbench连接验证兼容性
5. 运行标准information_schema查询测试套件