# 性能基准测试快速指南

## 运行基准测试

### 方法 1: 直接运行（推荐）

```bash
cd tests/CysRedis.Tests
dotnet run -c Release
```

### 方法 2: 使用 BenchmarkDotNet CLI

```bash
dotnet run -c Release --project tests/CysRedis.Tests/CysRedis.Tests.csproj -- --benchmark
```

## 查看报告

基准测试完成后，报告会保存在：

```
tests/CysRedis.Tests/BenchmarkDotNet.Artifacts/results/[日期时间]/
```

### 报告文件

1. **PerformanceComparisonBenchmarks-report.html** - 交互式 HTML 报告（推荐）
2. **PerformanceComparisonBenchmarks-report.md** - Markdown 格式报告
3. **PerformanceComparisonBenchmarks-report.csv** - CSV 数据文件

## 报告解读

### 关键指标

- **Mean**: 平均执行时间（越小越好）
- **Ratio**: 相对于 Managed 实现的性能比率
  - Ratio < 1.0: Unsafe 更快
  - Ratio > 1.0: Managed 更快
- **Rank**: 性能排名（1 = 最快）
- **Allocated**: 内存分配量（越小越好）

### 性能提升计算

如果 Ratio = 0.75，表示 Unsafe 实现比 Managed 实现快：
- 速度提升 = (1 - 0.75) × 100% = 25%

## 测试场景

基准测试包含以下场景：

1. **Hash**: Set/Get/Delete 操作
2. **Set**: Add/Contains/Remove 操作
3. **List**: Push/Pop 操作
4. **SortedSet**: Add/GetScore/IncrBy 操作
5. **Factory**: 通过工厂创建的真实使用场景

每个场景都有 Small/Medium/Large 三种规模。

## 注意事项

- 建议在 Release 模式下运行
- 确保系统空闲，避免其他进程干扰
- 测试可能需要几分钟时间
- 首次运行会进行 JIT 编译，可能需要更长时间

## 更多信息

详细文档请参考：[PERFORMANCE_BENCHMARKING.md](../../../docs/PERFORMANCE_BENCHMARKING.md)
