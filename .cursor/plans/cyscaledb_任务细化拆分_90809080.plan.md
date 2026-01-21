---
name: CyscaleDB 任务细化拆分
overview: 将计划文件中的83个大任务进一步细化为更小、可独立完成和验证的子任务单元，每个任务都有明确的输入、输出和验收标准
todos:
  - id: online_ddl_manager_class
    content: 创建 OnlineDdlManager 类框架
    status: completed
  - id: online_ddl_begin
    content: 实现 BeginOnlineDdl 方法
    status: completed
  - id: online_ddl_log_dml
    content: 实现 LogDmlChange 方法
    status: completed
  - id: online_ddl_commit_rollback
    content: 实现 CommitOnlineDdl 和 RollbackOnlineDdl 方法
    status: completed
  - id: config_model_class
    content: 创建 CyscaleDbConfiguration 类框架
    status: completed
  - id: config_bufferpool_props
    content: 添加 BufferPool 配置属性
    status: completed
  - id: config_json_serialization
    content: 实现 FromJson 和 ToJson 方法
    status: completed
  - id: config_file_io
    content: 实现 LoadFromFile 和 SaveToFile 方法
    status: completed
  - id: metrics_counter_class
    content: 创建 Counter 类
    status: completed
  - id: metrics_histogram_class
    content: 创建 Histogram 类
    status: completed
  - id: metrics_gauge_class
    content: 创建 Gauge 类
    status: completed
  - id: metrics_collector_singleton
    content: 创建 MetricsCollector 单例类框架
    status: completed
  - id: window_first_last_value
    content: 实现 ComputeFirstValue 和 ComputeLastValue 方法
    status: completed
  - id: window_ntile
    content: 实现 ComputeNtile 方法
    status: completed
  - id: window_cume_percent
    content: 实现 ComputeCumeDist 和 ComputePercentRank 方法
    status: completed
  - id: json_contains
    content: 实现 JsonContains 函数
    status: completed
  - id: json_length_keys
    content: 实现 JsonLength 和 JsonKeys 函数
    status: completed
  - id: json_search
    content: 实现 JsonSearch 函数
    status: completed
  - id: json_merge
    content: 实现 JsonMergePatch 和 JsonMergePreserve 函数
    status: completed
  - id: rwlock_catalog
    content: 修改 Catalog.cs 使用 ReaderWriterLockSlim
    status: completed
  - id: interval_tree_class
    content: 创建 IntervalTree 泛型类
    status: completed
  - id: buffer_pool_segment
    content: 创建 BufferPoolSegment 类并修改 BufferPool 使用分段锁
    status: completed
  - id: enum_type_definition
    content: 创建 EnumTypeDefinition 类并修改 ColumnDefinition 支持 ENUM
    status: completed
  - id: set_type_definition
    content: 创建 SetTypeDefinition 类并修改 ColumnDefinition 支持 SET
    status: completed
  - id: fulltext_index_class
    content: 创建 FullTextIndex 类和 ITokenizer 接口
    status: completed
  - id: fulltext_match_syntax
    content: 在 Parser 和 Executor 中实现 MATCH...AGAINST 语法
    status: completed
  - id: docs_update_all
    content: 更新所有文档（PROJECT_STATUS, CONFIGURATION, MONITORING, ONLINE_DDL）
    status: completed
  - id: tests_comprehensive
    content: 为所有新功能添加单元测试、集成测试和性能回归测试
    status: completed
---

# CyscaleDB 任务细化拆分方案

## 拆分原则

每个任务应满足以下条件：

1. **可独立完成**：不依赖其他未完成的任务
2. **可独立测试**：有明确的测试用例和验收标准
3. **可独立验证**：有明确的输入、输出和成功标准
4. **工作量适中**：每个任务预计 2-8 小时完成

## Phase 1.1: 在线 DDL 实现（细化为 28 个小任务）

### 1.1.1 在线 DDL 管理器框架（4 个任务）

#### 任务 1.1.1.1: 创建 OnlineDdlManager 类框架

- **输入**: 无
- **输出**: `src/CyscaleDB.Core/Storage/OnlineDdl/OnlineDdlManager.cs` 文件，包含类定义和基本字段
- **验收标准**: 
- 类可以编译通过
- 包含 `_changeLogs` 字典和 `_lock` 对象
- 包含基本的构造函数
- **测试**: 创建类实例，验证不为 null

#### 任务 1.1.1.2: 实现 BeginOnlineDdl 方法

- **输入**: databaseName, tableName, operation
- **输出**: bool 返回值，表示是否成功开始 DDL
- **验收标准**:
- 成功创建 DdlChangeLog 并添加到字典
- 如果已有 DDL 进行中，返回 false
- 线程安全
- **测试**: 
- 测试正常开始 DDL
- 测试重复开始返回 false
- 测试并发调用线程安全

#### 任务 1.1.1.3: 实现 LogDmlChange 方法

- **输入**: databaseName, tableName, DmlChange
- **输出**: void
- **验收标准**:
- 将 DML 变更添加到对应的 DdlChangeLog
- 如果没有 DDL 进行中，静默忽略
- 线程安全
- **测试**:
- 测试正常记录 DML 变更
- 测试无 DDL 时静默忽略
- 测试并发记录线程安全

#### 任务 1.1.1.4: 实现 CommitOnlineDdl 和 RollbackOnlineDdl 方法

- **输入**: databaseName, tableName
- **输出**: CommitOnlineDdl 返回 List<DmlChange>，RollbackOnlineDdl 返回 void
- **验收标准**:
- CommitOnlineDdl 返回所有记录的变更并移除日志
- RollbackOnlineDdl 移除日志但不返回变更
- 如果 DDL 不存在，CommitOnlineDdl 抛出异常
- **测试**:
- 测试正常提交和回滚
- 测试提交返回所有变更
- 测试无 DDL 时提交抛出异常

### 1.1.2 DdlChangeLog 和 DmlChange 数据结构（3 个任务）

#### 任务 1.1.2.1: 创建 DdlChangeLog 类

- **输入**: databaseName, tableName, operation
- **输出**: `DdlChangeLog.cs` 文件
- **验收标准**:
- 包含 DatabaseName, TableName, Operation 属性
- 包含 StartTime 时间戳
- 包含 AddChange 和 GetChanges 方法
- **测试**: 创建实例，验证属性正确

#### 任务 1.1.2.2: 创建 DmlChange 类

- **输入**: DmlChangeType, RowId, oldRowData, newRowData
- **输出**: `DmlChange.cs` 文件
- **验收标准**:
- 包含 Type, RowId, OldRowData, NewRowData 属性
- 提供静态工厂方法 CreateInsert, CreateUpdate, CreateDelete
- **测试**: 创建各种类型的 DmlChange，验证属性正确

#### 任务 1.1.2.3: 实现 DdlChangeLog 的线程安全操作

- **输入**: 多个并发的 AddChange 调用
- **输出**: 所有变更都被正确记录
- **验收标准**:
- AddChange 和 GetChanges 线程安全
- 使用锁保护内部列表
- **测试**: 并发添加变更，验证所有变更都被记录

### 1.1.3 ALTER TABLE 语法扩展（5 个任务）

#### 任务 1.1.3.1: 在 Statements.cs 中添加 AlterAlgorithm 枚举

- **输入**: 无
- **输出**: AlterAlgorithm 枚举（Default, Inplace, Copy）
- **验收标准**: 枚举定义正确，可以编译通过
- **测试**: 创建枚举值，验证值正确

#### 任务 1.1.3.2: 在 Statements.cs 中添加 AlterLockMode 枚举

- **输入**: 无
- **输出**: AlterLockMode 枚举（Default, None, Shared, Exclusive）
- **验收标准**: 枚举定义正确
- **测试**: 创建枚举值，验证值正确

#### 任务 1.1.3.3: 在 AlterTableStatement 中添加 Algorithm 和 Lock 属性

- **输入**: 无
- **输出**: AlterTableStatement 类包含可空属性 Algorithm 和 Lock
- **验收标准**: 属性类型正确，可以为 null
- **测试**: 创建 AlterTableStatement，设置属性，验证值正确

#### 任务 1.1.3.4: 在 Parser.cs 中解析 ALGORITHM 子句

- **输入**: SQL 语句 "ALTER TABLE t ALGORITHM=INPLACE"
- **输出**: AlterTableStatement 的 Algorithm 属性设置为 Inplace
- **验收标准**:
- 解析 ALGORITHM=INPLACE/COPY/DEFAULT
- 大小写不敏感
- 可选子句，不提供时 Algorithm 为 null
- **测试**:
- 测试解析 ALGORITHM=INPLACE
- 测试解析 ALGORITHM=COPY
- 测试不提供 ALGORITHM 时 Algorithm 为 null
- 测试大小写不敏感

#### 任务 1.1.3.5: 在 Parser.cs 中解析 LOCK 子句

- **输入**: SQL 语句 "ALTER TABLE t LOCK=NONE"
- **输出**: AlterTableStatement 的 Lock 属性设置为 None
- **验收标准**:
- 解析 LOCK=NONE/SHARED/EXCLUSIVE/DEFAULT
- 大小写不敏感
- 可选子句
- **测试**:
- 测试解析各种 LOCK 值
- 测试大小写不敏感
- 测试不提供 LOCK 时 Lock 为 null

### 1.1.4 Row 类延迟填充支持（3 个任务）

#### 任务 1.1.4.1: 在 Row.cs 中添加 _lazyColumns 字段

- **输入**: 无
- **输出**: Row 类包含 HashSet<int>? _lazyColumns 字段
- **验收标准**: 字段定义正确，可以为 null
- **测试**: 创建 Row，验证字段存在

#### 任务 1.1.4.2: 实现 GetValue 方法的延迟填充逻辑

- **输入**: columnName, 该列在 _lazyColumns 中
- **输出**: 返回列的默认值或 NULL
- **验收标准**:
- 如果列在 _lazyColumns 中，返回默认值
- 如果默认值为 null，返回 DataValue.Null
- 如果列不在 _lazyColumns 中，返回正常值
- **测试**:
- 测试延迟列返回默认值
- 测试非延迟列返回正常值
- 测试默认值为 null 的情况

#### 任务 1.1.4.3: 实现 SetLazyColumns 方法设置延迟列

- **输入**: HashSet<int> lazyColumnOrdinals
- **输出**: void，设置 _lazyColumns 字段
- **验收标准**: 正确设置 _lazyColumns 字段
- **测试**: 设置延迟列，验证 GetValue 返回默认值

### 1.1.5 ADD COLUMN 在线执行（5 个任务）

#### 任务 1.1.5.1: 在 Executor.cs 中实现 ExecuteAddColumnOnline 方法框架

- **输入**: AlterTableStatement（包含 AddColumnAction）
- **输出**: ExecutionResult
- **验收标准**:
- 方法签名正确
- 调用 BeginOnlineDdl
- 基本的 try-catch 结构
- **测试**: 调用方法，验证不抛出异常（即使功能未完成）

#### 任务 1.1.5.2: 实现元数据修改逻辑

- **输入**: tableName, ColumnDefinition
- **输出**: 表结构已更新，包含新列
- **验收标准**:
- 获取表结构
- 添加新列到结构
- 保存更新的结构
- **测试**: 执行 ADD COLUMN，验证表结构包含新列

#### 任务 1.1.5.3: 实现新行写入时包含新列

- **输入**: INSERT 语句到已添加新列的表
- **输出**: 新行包含新列的值（或默认值）
- **验收标准**:
- INSERT 时新列有值或使用默认值
- 旧行读取时通过延迟填充返回默认值
- **测试**:
- 插入新行，验证包含新列
- 读取旧行，验证延迟填充返回默认值

#### 任务 1.1.5.4: 实现 BackfillColumn 后台任务框架

- **输入**: tableName, ColumnDefinition
- **输出**: Task，后台逐步更新旧行
- **验收标准**:
- 创建后台任务
- 扫描表中的所有行
- 更新每行的新列值
- **测试**: 启动后台任务，验证旧行被更新

#### 任务 1.1.5.5: 实现 DDL 期间 DML 变更的应用

- **输入**: DDL 提交时的 DML 变更列表
- **输出**: 所有变更已应用到表
- **验收标准**:
- 获取 CommitOnlineDdl 返回的变更列表
- 逐个应用 INSERT/UPDATE/DELETE 变更
- 确保变更应用到正确的行
- **测试**:
- DDL 期间执行 INSERT，验证变更被应用
- DDL 期间执行 UPDATE，验证变更被应用
- DDL 期间执行 DELETE，验证变更被应用

### 1.1.6 ADD INDEX 在线构建（5 个任务）

#### 任务 1.1.6.1: 实现 CreateShadowIndex 方法

- **输入**: tableName, IndexDefinition
- **输出**: 影子索引路径或标识符
- **验收标准**:
- 创建新的索引结构（不替换现有索引）
- 返回索引标识符
- **测试**: 创建影子索引，验证索引存在且不影响现有索引

#### 任务 1.1.6.2: 实现 BuildIndexInBackground 方法框架

- **输入**: tableName, IndexDefinition
- **输出**: Task，后台构建索引
- **验收标准**:
- 创建后台任务
- 扫描表中的所有行
- 提取索引键值
- **测试**: 启动后台任务，验证索引键被提取

#### 任务 1.1.6.3: 实现索引键值插入逻辑

- **输入**: 行数据和索引定义
- **输出**: 索引条目被添加到影子索引
- **验收标准**:
- 从行中提取索引列的值
- 构建索引键
- 插入到影子索引
- **测试**: 插入索引条目，验证可以在影子索引中查找

#### 任务 1.1.6.4: 实现索引原子切换逻辑

- **输入**: 影子索引构建完成
- **输出**: 影子索引替换为正式索引
- **验收标准**:
- 原子性地替换索引
- 确保没有查询使用旧索引
- 删除影子索引
- **测试**: 切换索引，验证查询使用新索引

#### 任务 1.1.6.5: 实现 DDL 期间 DML 变更应用到索引

- **输入**: DDL 提交时的 DML 变更列表
- **输出**: 所有变更已应用到新索引
- **验收标准**:
- INSERT 变更：添加索引条目
- UPDATE 变更：更新索引条目
- DELETE 变更：删除索引条目
- **测试**:
- DDL 期间 INSERT，验证索引条目被添加
- DDL 期间 UPDATE，验证索引条目被更新
- DDL 期间 DELETE，验证索引条目被删除

### 1.1.7 测试（3 个任务）

#### 任务 1.1.7.1: 创建 OnlineDdlTests.cs 基础测试

- **输入**: 无
- **输出**: 测试文件，包含基本测试结构
- **验收标准**: 测试文件可以编译和运行
- **测试**: 运行空测试，验证通过

#### 任务 1.1.7.2: 添加 ADD COLUMN 在线执行测试

- **输入**: 测试用例
- **输出**: 测试方法，验证 ADD COLUMN 在线执行
- **验收标准**:
- 测试基本 ADD COLUMN 功能
- 测试并发 DML 不受影响
- 测试旧行延迟填充
- **测试**: 运行测试，验证通过

#### 任务 1.1.7.3: 添加 ADD INDEX 在线构建测试

- **输入**: 测试用例
- **输出**: 测试方法，验证 ADD INDEX 在线构建
- **验收标准**:
- 测试索引在线构建
- 测试并发 DML 变更被应用
- 测试索引查询正确
- **测试**: 运行测试，验证通过

## Phase 1.2: 配置系统（细化为 18 个小任务）

### 1.2.1 配置模型（6 个任务）

#### 任务 1.2.1.1: 创建 CyscaleDbConfiguration 类框架

- **输入**: 无
- **输出**: `src/CyscaleDB.Core/Common/Configuration.cs` 文件
- **验收标准**: 类定义正确，包含命名空间
- **测试**: 创建实例，验证不为 null

#### 任务 1.2.1.2: 添加 BufferPool 配置属性

- **输入**: 无
- **输出**: BufferPoolSizePages, BufferPoolYoungRatio 属性
- **验收标准**: 属性有默认值，类型正确
- **测试**: 创建配置，验证默认值正确

#### 任务 1.2.1.3: 添加 CTE、事务、锁配置属性

- **输入**: 无
- **输出**: RecursiveCteMaxIterations, DefaultIsolationLevel, LockWaitTimeoutMs 等属性
- **验收标准**: 所有属性有默认值
- **测试**: 创建配置，验证所有属性有值

#### 任务 1.2.1.4: 添加日志、检查点、WAL 配置属性

- **输入**: 无
- **输出**: MinimumLogLevel, SlowQueryThresholdMs, CheckpointIntervalSeconds 等属性
- **验收标准**: 所有属性有默认值
- **测试**: 创建配置，验证所有属性有值

#### 任务 1.2.1.5: 实现 FromJson 和 ToJson 方法

- **输入**: JSON 字符串或 CyscaleDbConfiguration 对象
- **输出**: CyscaleDbConfiguration 对象或 JSON 字符串
- **验收标准**:
- FromJson 正确解析 JSON
- ToJson 正确序列化对象
- 支持所有配置属性
- **测试**:
- 测试序列化和反序列化
- 测试部分属性缺失时使用默认值
- 测试无效 JSON 抛出异常

#### 任务 1.2.1.6: 实现 LoadFromFile 和 SaveToFile 方法

- **输入**: 文件路径
- **输出**: 从文件加载配置或保存配置到文件
- **验收标准**:
- LoadFromFile 读取文件并解析 JSON
- SaveToFile 序列化配置并写入文件
- 文件不存在时 LoadFromFile 返回默认配置或抛出异常
- **测试**:
- 测试保存和加载配置
- 测试文件不存在的情况
- 测试无效文件格式

### 1.2.2 配置集成（6 个任务）

#### 任务 1.2.2.1: 修改 BufferPool 构造函数接受配置

- **输入**: CyscaleDbConfiguration 对象
- **输出**: BufferPool 使用配置的值
- **验收标准**:
- 构造函数接受配置参数
- 使用配置的 BufferPoolSizePages 和 YoungRatio
- 保持向后兼容（可选参数）
- **测试**:
- 测试使用配置创建 BufferPool
- 验证容量和比例正确
- 测试向后兼容性

#### 任务 1.2.2.2: 修改 Executor 使用配置的 CTE 限制

- **输入**: CyscaleDbConfiguration 对象
- **输出**: Executor 使用配置的 RecursiveCteMaxIterations
- **验收标准**:
- Executor 接受配置参数
- 使用配置值替代硬编码值
- **测试**: 测试递归 CTE 使用配置的限制值

#### 任务 1.2.2.3: 修改 TransactionManager 使用配置

- **输入**: CyscaleDbConfiguration 对象
- **输出**: TransactionManager 使用配置的隔离级别和超时
- **验收标准**:
- 使用配置的 DefaultIsolationLevel
- 使用配置的 LockWaitTimeoutMs
- **测试**: 测试事务使用配置的值

#### 任务 1.2.2.4: 创建 SystemVariables 类框架

- **输入**: 无
- **输出**: `SystemVariables.cs` 文件
- **验收标准**: 类定义正确，包含配置引用
- **测试**: 创建实例，验证不为 null

#### 任务 1.2.2.5: 实现 SetGlobal 方法

- **输入**: variableName, value
- **输出**: void，修改全局配置
- **验收标准**:
- 支持修改可配置的变量
- 验证值的有效性
- 线程安全
- **测试**:
- 测试修改各种变量
- 测试无效值抛出异常
- 测试并发修改线程安全

#### 任务 1.2.2.6: 实现 SetSession 方法

- **输入**: variableName, value
- **输出**: void，修改会话配置
- **验收标准**:
- 支持会话级别的配置
- 会话结束时恢复全局配置
- **测试**: 测试会话配置不影响全局配置

### 1.2.3 测试（6 个任务）

#### 任务 1.2.3.1: 创建 ConfigurationTests.cs

- **输入**: 无
- **输出**: 测试文件
- **验收标准**: 测试文件可以编译
- **测试**: 运行空测试

#### 任务 1.2.3.2: 添加配置加载测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试从文件加载配置
- **测试**: 运行测试，验证通过

#### 任务 1.2.3.3: 添加配置保存测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试保存配置到文件
- **测试**: 运行测试，验证通过

#### 任务 1.2.3.4: 添加运行时修改测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试 SetGlobal 和 SetSession
- **测试**: 运行测试，验证通过

#### 任务 1.2.3.5: 添加配置验证测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试无效值被拒绝
- **测试**: 运行测试，验证通过

#### 任务 1.2.3.6: 添加配置集成测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试配置应用到各个组件
- **测试**: 运行测试，验证通过

## Phase 1.3: 性能监控（细化为 20 个小任务）

### 1.3.1 指标收集器基础类（6 个任务）

#### 任务 1.3.1.1: 创建 Counter 类

- **输入**: 无
- **输出**: `Counter.cs` 文件
- **验收标准**:
- 线程安全的计数器
- Increment 和 Value 方法
- **测试**: 测试并发递增，验证值正确

#### 任务 1.3.1.2: 创建 Histogram 类

- **输入**: 无
- **输出**: `Histogram.cs` 文件
- **验收标准**:
- Record 方法记录值
- P50, P95, P99 方法计算百分位数
- **测试**: 记录多个值，验证百分位数正确

#### 任务 1.3.1.3: 创建 Gauge 类

- **输入**: 无
- **输出**: `Gauge.cs` 文件
- **验收标准**:
- Set 和 Get 方法
- 线程安全
- **测试**: 测试设置和获取值

#### 任务 1.3.1.4: 创建 MetricsCollector 单例类框架

- **输入**: 无
- **输出**: `MetricsCollector.cs` 文件
- **验收标准**: 单例模式正确实现
- **测试**: 测试单例实例唯一

#### 任务 1.3.1.5: 添加查询指标属性

- **输入**: 无
- **输出**: QueriesExecuted, QueryExecutionTime, SlowQueries 属性
- **验收标准**: 所有属性类型正确
- **测试**: 访问属性，验证不为 null

#### 任务 1.3.1.6: 添加事务、锁、Buffer Pool、IO 指标属性

- **输入**: 无
- **输出**: 所有指标属性
- **验收标准**: 所有属性定义正确
- **测试**: 访问所有属性，验证不为 null

### 1.3.2 RecordQuery 方法实现（3 个任务）

#### 任务 1.3.2.1: 实现 RecordQuery 方法框架

- **输入**: sql, duration, plan
- **输出**: void，记录查询指标
- **验收标准**: 方法签名正确
- **测试**: 调用方法，验证不抛出异常

#### 任务 1.3.2.2: 实现指标更新逻辑

- **输入**: 查询信息
- **输出**: 更新 QueriesExecuted, QueryExecutionTime
- **验收标准**: 指标正确更新
- **测试**: 记录查询，验证指标更新

#### 任务 1.3.2.3: 实现慢查询检测逻辑

- **输入**: duration, threshold
- **输出**: 如果超过阈值，更新 SlowQueries
- **验收标准**: 慢查询被正确识别
- **测试**: 测试慢查询和正常查询

### 1.3.3 Executor 集成（2 个任务）

#### 任务 1.3.3.1: 在 Executor.Execute 中添加指标收集

- **输入**: Statement
- **输出**: 记录查询指标
- **验收标准**: 每个查询都被记录
- **测试**: 执行查询，验证指标更新

#### 任务 1.3.3.2: 实现执行计划收集

- **输入**: Statement, ExecutionResult
- **输出**: ExecutionPlan 对象
- **验收标准**: 包含 RowsExamined, RowsReturned, IndexesUsed
- **测试**: 执行查询，验证计划收集正确

### 1.3.4 慢查询日志（3 个任务）

#### 任务 1.3.4.1: 创建 SlowQueryLog 类框架

- **输入**: 无
- **输出**: `SlowQueryLog.cs` 文件
- **验收标准**: 类定义正确
- **测试**: 创建实例，验证不为 null

#### 任务 1.3.4.2: 实现日志文件写入功能

- **输入**: logFilePath
- **输出**: 日志文件写入功能
- **验收标准**: 可以写入日志文件
- **测试**: 写入日志，验证文件存在

#### 任务 1.3.4.3: 实现 WriteToLog 方法格式化

- **输入**: SlowQueryEntry
- **输出**: 格式化的日志条目
- **验收标准**: 格式类似 MySQL 慢查询日志
- **测试**: 写入日志，验证格式正确

### 1.3.5 SHOW STATUS 命令（3 个任务）

#### 任务 1.3.5.1: 在 Parser.cs 中添加 ParseShowStatus

- **输入**: SQL "SHOW STATUS"
- **输出**: ShowStatusStatement
- **验收标准**: 解析正确
- **测试**: 解析 SQL，验证语句正确

#### 任务 1.3.5.2: 在 Executor.cs 中实现 ExecuteShowStatus

- **输入**: ShowStatusStatement
- **输出**: ExecutionResult 包含所有指标
- **验收标准**: 返回所有性能指标
- **测试**: 执行 SHOW STATUS，验证返回指标

#### 任务 1.3.5.3: 格式化 SHOW STATUS 输出

- **输入**: 指标数据
- **输出**: 格式化的结果集
- **验收标准**: 格式类似 MySQL SHOW STATUS
- **测试**: 执行命令，验证输出格式

### 1.3.6 测试（3 个任务）

#### 任务 1.3.6.1: 创建 MetricsTests.cs

- **输入**: 无
- **输出**: 测试文件
- **验收标准**: 测试文件可以编译
- **测试**: 运行空测试

#### 任务 1.3.6.2: 添加指标收集测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试 Counter, Histogram, Gauge
- **测试**: 运行测试，验证通过

#### 任务 1.3.6.3: 添加慢查询日志和 SHOW STATUS 测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试慢查询日志和 SHOW STATUS
- **测试**: 运行测试，验证通过

## Phase 2.1: 窗口函数扩展（细化为 10 个小任务）

### 2.1.1 FIRST_VALUE / LAST_VALUE（2 个任务）

#### 任务 2.1.1.1: 实现 ComputeFirstValue 方法

- **输入**: WindowFunctionSpec, Partition
- **输出**: DataValue，分区第一行的值
- **验收标准**: 返回分区第一行的表达式值
- **测试**: 测试 FIRST_VALUE 函数

#### 任务 2.1.1.2: 实现 ComputeLastValue 方法

- **输入**: WindowFunctionSpec, Partition
- **输出**: DataValue，分区最后一行的值
- **验收标准**: 返回分区最后一行的表达式值
- **测试**: 测试 LAST_VALUE 函数

### 2.1.2 NTILE（1 个任务）

#### 任务 2.1.2.1: 实现 ComputeNtile 方法

- **输入**: WindowFunctionSpec, Partition, buckets
- **输出**: DataValue，桶编号（1-based）
- **验收标准**: 将分区分成 N 个桶，返回当前行所在桶
- **测试**: 测试 NTILE 函数，验证桶分配正确

### 2.1.3 CUME_DIST / PERCENT_RANK（2 个任务）

#### 任务 2.1.3.1: 实现 ComputeCumeDist 方法

- **输入**: WindowFunctionSpec, Partition
- **输出**: DataValue，累积分布值（0-1）
- **验收标准**: 计算 <= 当前行的行数 / 总行数
- **测试**: 测试 CUME_DIST 函数

#### 任务 2.1.3.2: 实现 ComputePercentRank 方法

- **输入**: WindowFunctionSpec, Partition
- **输出**: DataValue，百分比排名（0-1）
- **验收标准**: 计算 (rank - 1) / (总行数 - 1)
- **测试**: 测试 PERCENT_RANK 函数

### 2.1.4 NTH_VALUE（1 个任务）

#### 任务 2.1.4.1: 实现 ComputeNthValue 方法

- **输入**: WindowFunctionSpec, Partition, n
- **输出**: DataValue，第 N 行的值
- **验收标准**: 返回分区第 N 行的表达式值（1-based）
- **测试**: 测试 NTH_VALUE 函数

### 2.1.5 语法解析（2 个任务）

#### 任务 2.1.5.1: 在 Parser.cs 中添加新窗口函数解析

- **输入**: SQL 包含新窗口函数
- **输出**: WindowFunctionExpression
- **验收标准**: 解析所有新窗口函数
- **测试**: 解析各种窗口函数 SQL

#### 任务 2.1.5.2: 在 WindowFunctionType 枚举中添加新类型

- **输入**: 无
- **输出**: 枚举包含新函数类型
- **验收标准**: 枚举值正确
- **测试**: 使用枚举值，验证正确

### 2.1.6 测试（2 个任务）

#### 任务 2.1.6.1: 扩展 WindowFunctionTests.cs

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 为每个新函数添加测试
- **测试**: 运行测试，验证通过

#### 任务 2.1.6.2: 添加 MySQL 兼容性测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 结果与 MySQL 8.0 一致
- **测试**: 运行测试，对比 MySQL 结果

## Phase 2.2: JSON 函数扩展（细化为 10 个小任务）

### 2.2.1 JSON_CONTAINS（2 个任务）

#### 任务 2.2.1.1: 实现 JsonContains 核心逻辑

- **输入**: json, candidate, path
- **输出**: bool，是否包含
- **验收标准**: 正确判断 JSON 是否包含候选值
- **测试**: 测试各种包含场景

#### 任务 2.2.1.2: 在 Executor.cs 中集成 JSON_CONTAINS

- **输入**: FunctionCall
- **输出**: IExpressionEvaluator
- **验收标准**: 可以解析和执行 JSON_CONTAINS
- **测试**: 执行 JSON_CONTAINS 查询

### 2.2.2 JSON_LENGTH（1 个任务）

#### 任务 2.2.2.1: 实现 JsonLength 函数

- **输入**: json, path
- **输出**: int，长度
- **验收标准**: 返回数组长度、对象键数或 1（标量）
- **测试**: 测试各种 JSON 类型的长度

### 2.2.3 JSON_KEYS（1 个任务）

#### 任务 2.2.3.1: 实现 JsonKeys 函数

- **输入**: json, path
- **输出**: string，JSON 数组包含所有键
- **验收标准**: 返回对象的所有键作为 JSON 数组
- **测试**: 测试 JSON_KEYS 函数

### 2.2.4 JSON_SEARCH（2 个任务）

#### 任务 2.2.4.1: 实现 JsonSearch 核心逻辑

- **输入**: json, searchStr, path
- **输出**: string?，匹配路径或 null
- **验收标准**: 在 JSON 中搜索字符串，返回路径
- **测试**: 测试各种搜索场景

#### 任务 2.2.4.2: 在 Executor.cs 中集成 JSON_SEARCH

- **输入**: FunctionCall
- **输出**: IExpressionEvaluator
- **验收标准**: 可以解析和执行 JSON_SEARCH
- **测试**: 执行 JSON_SEARCH 查询

### 2.2.5 JSON_MERGE_PATCH / JSON_MERGE_PRESERVE（2 个任务）

#### 任务 2.2.5.1: 实现 JsonMergePatch 函数

- **输入**: json1, json2
- **输出**: string，合并后的 JSON
- **验收标准**: 实现 RFC 7396 JSON Merge Patch
- **测试**: 测试合并逻辑，包括 null 删除

#### 任务 2.2.5.2: 实现 JsonMergePreserve 函数

- **输入**: json1, json2
- **输出**: string，合并后的 JSON
- **验收标准**: 保留原有值的合并
- **测试**: 测试合并逻辑

### 2.2.6 测试（2 个任务）

#### 任务 2.2.6.1: 创建或扩展 JsonFunctionTests.cs

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 为每个新函数添加测试
- **测试**: 运行测试，验证通过

#### 任务 2.2.6.2: 添加 MySQL 兼容性测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 结果与 MySQL 8.0 一致
- **测试**: 运行测试，对比 MySQL 结果

## Phase 2.3: 并发性能优化（细化为 12 个小任务）

### 2.3.1 ReaderWriterLockSlim 优化（4 个任务）

#### 任务 2.3.1.1: 修改 Catalog.cs 使用 ReaderWriterLockSlim

- **输入**: Catalog.cs
- **输出**: 使用 ReaderWriterLockSlim 替代 object lock
- **验收标准**: 读操作使用读锁，写操作使用写锁
- **测试**: 测试并发读性能提升

#### 任务 2.3.1.2: 修改 ForeignKeyManager.cs 使用 ReaderWriterLockSlim

- **输入**: ForeignKeyManager.cs
- **输出**: 使用 ReaderWriterLockSlim
- **验收标准**: 读多写少场景性能提升
- **测试**: 测试并发访问性能

#### 任务 2.3.1.3: 修改 TransactionManager 部分操作使用读锁

- **输入**: TransactionManager.cs
- **输出**: 读取操作使用读锁
- **验收标准**: 读取事务信息不阻塞
- **测试**: 测试并发读取性能

#### 任务 2.3.1.4: 添加 ReaderWriterLockSlim 使用测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 验证并发性能提升
- **测试**: 运行性能测试

### 2.3.2 区间树优化（3 个任务）

#### 任务 2.3.2.1: 创建 IntervalTree 泛型类

- **输入**: 无
- **输出**: `IntervalTree.cs` 文件
- **验收标准**: 实现区间树数据结构，支持 O(log n) 查询
- **测试**: 测试区间查询性能

#### 任务 2.3.2.2: 实现区间插入和查询方法

- **输入**: 区间和值
- **输出**: 插入和查询功能
- **验收标准**: Insert 和 Query 方法正确实现
- **测试**: 测试插入和查询正确性

#### 任务 2.3.2.3: 修改 LockManager 使用 IntervalTree

- **输入**: LockManager.cs
- **输出**: 使用 IntervalTree 优化间隙锁查找
- **验收标准**: 间隙锁查找从 O(n) 降到 O(log n)
- **测试**: 测试性能提升

### 2.3.3 缓冲池分段锁（3 个任务）

#### 任务 2.3.3.1: 创建 BufferPoolSegment 类

- **输入**: 无
- **输出**: `BufferPoolSegment.cs` 文件
- **验收标准**: 实现分段缓冲池
- **测试**: 测试分段功能

#### 任务 2.3.3.2: 修改 BufferPool 使用分段锁

- **输入**: BufferPool.cs
- **输出**: 使用 16 个分段减少锁竞争
- **验收标准**: GetPage 使用对应分段的锁
- **测试**: 测试并发性能提升

#### 任务 2.3.3.3: 添加分段锁测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 验证并发性能提升
- **测试**: 运行性能测试

### 2.3.4 性能基准测试（2 个任务）

#### 任务 2.3.4.1: 扩展 ConcurrentTransactionTests.cs

- **输入**: 测试用例
- **输出**: 性能基准测试
- **验收标准**: 测试并发性能
- **测试**: 运行基准测试

#### 任务 2.3.4.2: 添加性能对比测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 对比优化前后的性能
- **测试**: 运行对比测试

## Phase 3.1: 新数据类型（细化为 12 个小任务）

### 3.1.1 ENUM 类型（5 个任务）

#### 任务 3.1.1.1: 创建 EnumTypeDefinition 类

- **输入**: 无
- **输出**: `EnumTypeDefinition.cs` 文件
- **验收标准**: 包含 Name, Values, Parse 方法
- **测试**: 创建 ENUM 类型定义

#### 任务 3.1.1.2: 修改 ColumnDefinition 支持 ENUM

- **输入**: ColumnDefinition.cs
- **输出**: 添加 EnumType 属性
- **验收标准**: 可以定义 ENUM 列
- **测试**: 创建 ENUM 列定义

#### 任务 3.1.1.3: 修改 DataValue 支持 ENUM

- **输入**: DataValue.cs
- **输出**: 支持 ENUM 类型存储
- **验收标准**: ENUM 值存储为整数索引
- **测试**: 创建和存储 ENUM 值

#### 任务 3.1.1.4: 修改 Parser 解析 ENUM 类型

- **输入**: SQL "CREATE TABLE t (c ENUM('a','b','c'))"
- **输出**: ColumnDefinition 包含 EnumType
- **验收标准**: 正确解析 ENUM 定义
- **测试**: 解析 ENUM 列定义

#### 任务 3.1.1.5: 修改存储层支持 ENUM 序列化

- **输入**: ENUM 值
- **输出**: 序列化和反序列化支持
- **验收标准**: ENUM 值可以正确存储和读取
- **测试**: 测试 ENUM 的 CRUD 操作

### 3.1.2 SET 类型（5 个任务）

#### 任务 3.1.2.1: 创建 SetTypeDefinition 类

- **输入**: 无
- **输出**: `SetTypeDefinition.cs` 文件
- **验收标准**: 包含 Name, Values, Parse 方法（位图）
- **测试**: 创建 SET 类型定义

#### 任务 3.1.2.2: 修改 ColumnDefinition 支持 SET

- **输入**: ColumnDefinition.cs
- **输出**: 添加 SetType 属性
- **验收标准**: 可以定义 SET 列
- **测试**: 创建 SET 列定义

#### 任务 3.1.2.3: 修改 DataValue 支持 SET

- **输入**: DataValue.cs
- **输出**: 支持 SET 类型存储（位图）
- **验收标准**: SET 值存储为位图
- **测试**: 创建和存储 SET 值

#### 任务 3.1.2.4: 修改 Parser 解析 SET 类型

- **输入**: SQL "CREATE TABLE t (c SET('a','b','c'))"
- **输出**: ColumnDefinition 包含 SetType
- **验收标准**: 正确解析 SET 定义
- **测试**: 解析 SET 列定义

#### 任务 3.1.2.5: 修改存储层支持 SET 序列化

- **输入**: SET 值
- **输出**: 序列化和反序列化支持
- **验收标准**: SET 值可以正确存储和读取
- **测试**: 测试 SET 的 CRUD 操作

### 3.1.3 测试（2 个任务）

#### 任务 3.1.3.1: 创建 EnumSetTypeTests.cs

- **输入**: 测试用例
- **输出**: 测试文件
- **验收标准**: 测试 ENUM 和 SET 的 CRUD 操作
- **测试**: 运行测试，验证通过

#### 任务 3.1.3.2: 添加类型验证和转换测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试无效值拒绝和类型转换
- **测试**: 运行测试，验证通过

## Phase 3.2: 全文索引（细化为 12 个小任务）

### 3.2.1 全文索引数据结构（4 个任务）

#### 任务 3.2.1.1: 创建 FullTextIndex 类框架

- **输入**: 无
- **输出**: `FullTextIndex.cs` 文件
- **验收标准**: 类定义正确，包含倒排索引字典
- **测试**: 创建实例，验证不为 null

#### 任务 3.2.1.2: 创建 ITokenizer 接口和 SimpleTokenizer

- **输入**: 无
- **输出**: `ITokenizer.cs` 和 `SimpleTokenizer.cs` 文件
- **验收标准**: 接口定义正确，实现基本分词
- **测试**: 测试分词功能

#### 任务 3.2.1.3: 实现 AddDocument 方法

- **输入**: documentId, text
- **输出**: void，文档添加到索引
- **验收标准**: 分词后添加到倒排索引
- **测试**: 添加文档，验证索引更新

#### 任务 3.2.1.4: 实现 Search 方法框架

- **输入**: query
- **输出**: List<DocumentReference>
- **验收标准**: 方法签名正确
- **测试**: 调用方法，验证不抛出异常

### 3.2.2 搜索功能（3 个任务）

#### 任务 3.2.2.1: 实现布尔搜索（AND, OR, NOT）

- **输入**: query 包含布尔操作符
- **输出**: 匹配的文档列表
- **验收标准**: 正确实现布尔搜索逻辑
- **测试**: 测试各种布尔查询

#### 任务 3.2.2.2: 实现相关性排序（TF-IDF）

- **输入**: 文档列表
- **输出**: 按相关性排序的文档列表
- **验收标准**: 计算 TF-IDF 分数并排序
- **测试**: 测试相关性排序正确

#### 任务 3.2.2.3: 实现 Search 方法完整逻辑

- **输入**: query
- **输出**: 排序后的文档列表
- **验收标准**: 结合布尔搜索和相关性排序
- **测试**: 测试完整搜索功能

### 3.2.3 MATCH...AGAINST 语法（3 个任务）

#### 任务 3.2.3.1: 在 Parser.cs 中添加 MatchExpression

- **输入**: SQL "MATCH (col1, col2) AGAINST ('text')"
- **输出**: MatchExpression
- **验收标准**: 正确解析 MATCH...AGAINST 语法
- **测试**: 解析各种 MATCH 表达式

#### 任务 3.2.3.2: 在 Statements.cs 中定义 MatchExpression

- **输入**: 无
- **输出**: MatchExpression 类定义
- **验收标准**: 包含 Columns, SearchText, Mode 属性
- **测试**: 创建 MatchExpression，验证属性

#### 任务 3.2.3.3: 在 Executor.cs 中实现 EvaluateMatchExpression

- **输入**: MatchExpression, Row
- **输出**: DataValue，相关性分数
- **验收标准**: 执行全文搜索并返回分数
- **测试**: 执行 MATCH 查询，验证结果

### 3.2.4 测试（2 个任务）

#### 任务 3.2.4.1: 创建 FullTextIndexTests.cs

- **输入**: 测试用例
- **输出**: 测试文件
- **验收标准**: 测试全文索引创建和基本搜索
- **测试**: 运行测试，验证通过

#### 任务 3.2.4.2: 添加相关性排序和性能测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试相关性排序和性能
- **测试**: 运行测试，验证通过

## Phase 4: 文档与测试（细化为 12 个小任务）

### 4.1 文档更新（4 个任务）

#### 任务 4.1.1: 更新 PROJECT_STATUS.md

- **输入**: 新完成的功能列表
- **输出**: 更新的文档
- **验收标准**: 标记所有新完成的功能
- **测试**: 检查文档完整性

#### 任务 4.1.2: 创建 CONFIGURATION.md

- **输入**: 配置系统信息
- **输出**: 配置文档
- **验收标准**: 包含配置文件格式、配置项详解、运行时修改、性能调优建议
- **测试**: 检查文档完整性

#### 任务 4.1.3: 创建 MONITORING.md

- **输入**: 性能监控信息
- **输出**: 监控文档
- **验收标准**: 包含指标说明、SHOW STATUS 命令、慢查询日志分析、性能调优建议
- **测试**: 检查文档完整性

#### 任务 4.1.4: 创建 ONLINE_DDL.md

- **输入**: 在线 DDL 信息
- **输出**: 在线 DDL 文档
- **验收标准**: 包含支持的操作、ALGORITHM/LOCK 选项、使用示例、性能影响、限制和注意事项
- **测试**: 检查文档完整性

### 4.2 测试完善（8 个任务）

#### 任务 4.2.1: 为在线 DDL 添加边界条件测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试边界条件和错误处理
- **测试**: 运行测试，验证通过

#### 任务 4.2.2: 为配置系统添加集成测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试配置与各组件集成
- **测试**: 运行测试，验证通过

#### 任务 4.2.3: 为性能监控添加压力测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试高并发下的指标收集
- **测试**: 运行测试，验证通过

#### 任务 4.2.4: 创建 PerformanceRegressionTests.cs

- **输入**: 测试用例
- **输出**: 测试文件
- **验收标准**: 包含性能回归测试
- **测试**: 运行测试，验证通过

#### 任务 4.2.5: 添加在线 DDL 性能回归测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试在线 DDL 期间 DML 性能
- **测试**: 运行测试，验证通过

#### 任务 4.2.6: 添加缓冲池并发性能回归测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试缓冲池并发性能
- **测试**: 运行测试，验证通过

#### 任务 4.2.7: 扩展 MySqlConnectorIntegrationTests.cs

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 测试新功能与 MySQL 8.0 兼容性
- **测试**: 运行测试，验证通过

#### 任务 4.2.8: 添加窗口函数和 JSON 函数兼容性测试

- **输入**: 测试用例
- **输出**: 测试方法
- **验收标准**: 结果与 MySQL 8.0 一致
- **测试**: 运行测试，对比 MySQL 结果

## 总结

原计划包含 83 个大任务，细化为 **147 个小任务**，每个任务都：

- 有明确的输入、输出和验收标准
- 可以独立完成和测试
- 工作量适中（2-8 小时）
- 形成完整的开发闭环

所有任务按照依赖关系组织，可以按照 Phase 顺序执行，也可以根据依赖关系并行执行部分任务。