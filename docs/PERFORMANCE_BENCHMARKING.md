# 性能基准测试指南

本文档说明如何运行性能基准测试，对比托管实现和 unsafe 实现的性能差异。

## 快速开始

### 运行所有基准测试

```bash
# 在 Release 模式下运行基准测试
dotnet run -c Release --project tests/CysRedis.Tests/CysRedis.Tests.csproj
```

### 使用 BenchmarkDotNet CLI

```bash
# 直接使用 BenchmarkDotNet
dotnet run -c Release --project tests/CysRedis.Tests/CysRedis.Tests.csproj -- --benchmark
```

## 基准测试内容

### 测试的数据结构

1. **RedisHash** - Hash 表操作
   - Set 操作（小/中/大规模）
   - Get 操作（中规模）
   - 混合操作（大规模：Set + Get + Delete）

2. **RedisSet** - 集合操作
   - Add 操作（小规模）
   - Contains 操作（中规模）
   - 混合操作（大规模：Add + Contains + Remove）

3. **RedisList** - 列表操作
   - Push 操作（小规模）
   - Pop 操作（中规模）
   - 混合操作（大规模：PushLeft + PushRight + Pop）

4. **RedisSortedSet** - 有序集合操作
   - Add 操作（小规模）
   - GetScore 操作（中规模）
   - 混合操作（大规模：Add + GetScore + IncrBy）

5. **Factory 模式** - 真实使用场景
   - 通过工厂创建数据结构的性能对比

### 测试规模

- **Small**: 100 个元素
- **Medium**: 1,000 个元素
- **Large**: 10,000 个元素

## 报告输出

基准测试完成后，会在以下位置生成报告：

### 报告位置

```
tests/CysRedis.Tests/BenchmarkDotNet.Artifacts/results/
```

### 报告格式

1. **HTML 报告** (`PerformanceComparisonBenchmarks-report.html`)
   - 交互式网页报告
   - 包含详细的性能对比图表
   - 支持排序和筛选

2. **Markdown 报告** (`PerformanceComparisonBenchmarks-report.md`)
   - 文本格式报告
   - 适合在文档中引用
   - 包含性能对比表格

3. **CSV 报告** (`PerformanceComparisonBenchmarks-report.csv`)
   - 电子表格格式
   - 适合进一步分析和可视化

## 报告解读

### 关键指标

1. **Mean (平均值)**: 操作的平均执行时间
2. **Error**: 统计误差范围
3. **StdDev (标准差)**: 性能波动程度
4. **Ratio**: 相对于基准（Managed）的性能比率
   - Ratio < 1.0: Unsafe 更快
   - Ratio > 1.0: Managed 更快
5. **Rank**: 性能排名（1 = 最快）
6. **Gen 0/Gen 1/Gen 2**: GC 代次分配次数
7. **Allocated**: 内存分配量

### 性能对比示例

```
| Method                    | Mean      | Error    | StdDev   | Ratio | Rank |
|--------------------------|-----------|----------|----------|-------|------|
| Hash_Set_Small_Managed   | 123.45 us | 2.34 us  | 3.21 us  | 1.00  | 2    |
| Hash_Set_Small_Unsafe    | 89.12 us  | 1.56 us  | 2.11 us  | 0.72  | 1    |
```

在这个例子中：
- Unsafe 实现比 Managed 实现快 **28%** (Ratio = 0.72)
- Unsafe 排名第 1（更快）

## 运行特定类别的测试

如果需要只测试特定数据结构，可以修改 `RunBenchmarks.cs` 中的代码：

```csharp
// 只运行 Hash 相关的基准测试
var summary = BenchmarkRunner.Run(typeof(PerformanceComparisonBenchmarks), config);
```

或者使用 BenchmarkDotNet 的过滤器：

```bash
dotnet run -c Release --project tests/CysRedis.Tests/CysRedis.Tests.csproj -- --filter "*Hash*"
```

## 性能优化建议

### 何时使用 Unsafe 实现

- **高吞吐量场景**: 需要处理大量请求
- **低延迟要求**: 对响应时间敏感
- **内存受限**: 需要减少 GC 压力
- **CPU 密集型**: 可以利用 SIMD 加速

### 何时使用 Managed 实现

- **开发/测试环境**: 更容易调试
- **安全性优先**: 避免内存安全问题
- **小规模数据**: 性能差异不明显
- **兼容性要求**: 某些环境不支持 unsafe 代码

## 注意事项

1. **运行环境**: 建议在 Release 模式下运行，关闭调试器
2. **系统负载**: 确保系统空闲，避免其他进程干扰
3. **多次运行**: 建议多次运行取平均值
4. **硬件差异**: 不同硬件配置结果可能不同
5. **JIT 预热**: BenchmarkDotNet 会自动进行预热，确保 JIT 优化生效

## 故障排除

### 编译错误

如果遇到编译错误，确保：
- `AllowUnsafeBlocks` 已启用
- BenchmarkDotNet 包已安装
- 所有依赖项已还原

### 运行时错误

如果基准测试失败：
- 检查是否有足够的系统资源
- 确保在 Release 模式下运行
- 查看详细错误日志

## 示例输出

运行基准测试后，控制台会显示类似以下内容：

```
===========================================
  CysRedis Performance Comparison
  Managed vs Unsafe Implementations
===========================================

// ... 基准测试执行过程 ...

=== Benchmark Summary ===
Total benchmarks: 24
Reports saved to: D:\Code\CyscaleDB\tests\CysRedis.Tests\BenchmarkDotNet.Artifacts\results\2026-01-21-12-00-00

Key files:
  - HTML Report: D:\Code\CyscaleDB\tests\CysRedis.Tests\BenchmarkDotNet.Artifacts\results\2026-01-21-12-00-00\PerformanceComparisonBenchmarks-report.html
  - Markdown Report: D:\Code\CyscaleDB\tests\CysRedis.Tests\BenchmarkDotNet.Artifacts\results\2026-01-21-12-00-00\PerformanceComparisonBenchmarks-report.md
  - CSV Report: D:\Code\CyscaleDB\tests\CysRedis.Tests\BenchmarkDotNet.Artifacts\results\2026-01-21-12-00-00\PerformanceComparisonBenchmarks-report.csv
```

## 进一步分析

### 使用 Excel/Google Sheets

1. 打开 CSV 报告
2. 创建性能对比图表
3. 分析不同规模下的性能趋势

### 自定义报告

可以修改 `PerformanceComparisonBenchmarks.cs` 中的配置：

```csharp
[SimpleJob(RuntimeMoniker.Net80, warmupCount: 5, iterationCount: 20)] // 增加迭代次数
[MemoryDiagnoser] // 启用内存分析
[Orderer(SummaryOrderPolicy.FastestToSlowest)] // 按速度排序
```

## 相关文档

- [Unsafe 优化文档](UNSAFE_OPTIMIZATIONS.md)
- [配置文档](CONFIGURATION.md)
- [项目状态](PROJECT_STATUS.md)
