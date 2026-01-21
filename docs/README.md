# CyscaleDB 文档目录

本目录包含 CyscaleDB 项目的核心文档。

## 文档列表

### CyscaleDB (MySQL 兼容数据库)

| 文档 | 说明 |
|------|------|
| [PROJECT_STATUS.md](./PROJECT_STATUS.md) | 项目进度状态，包含各阶段完成情况和未来规划 |
| [CAPABILITIES.md](./CAPABILITIES.md) | 功能能力清单，详细列出所有支持的 SQL 语法和特性 |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | 系统架构文档，描述各层设计和组件关系 |
| [CONNECTION.md](./CONNECTION.md) | 连接指南，包含服务器启动和客户端连接示例 |

### CysRedis (Redis 兼容服务器)

| 文档 | 说明 |
|------|------|
| [REDIS_GUIDE.md](./REDIS_GUIDE.md) | Redis 使用指南，包含启动命令、客户端连接和基本操作 |
| [REDIS_EXAMPLES.md](./REDIS_EXAMPLES.md) | Redis 命令示例，详细的使用案例 |
| [PERFORMANCE_BENCHMARKING.md](./PERFORMANCE_BENCHMARKING.md) | 性能基准测试指南，对比托管和 unsafe 实现 |
| [UNSAFE_OPTIMIZATIONS.md](./UNSAFE_OPTIMIZATIONS.md) | Unsafe 优化文档，高性能数据结构实现说明 |

## 使用指南

### 开始新需求前

1. 阅读 [PROJECT_STATUS.md](./PROJECT_STATUS.md) 了解当前项目状态
2. 阅读 [CAPABILITIES.md](./CAPABILITIES.md) 了解现有功能，避免重复实现
3. 阅读 [ARCHITECTURE.md](./ARCHITECTURE.md) 了解代码结构，确定修改位置

### 完成功能后

1. 更新 [PROJECT_STATUS.md](./PROJECT_STATUS.md) 中的进度
2. 更新 [CAPABILITIES.md](./CAPABILITIES.md) 添加新功能说明
3. 如有架构变更，更新 [ARCHITECTURE.md](./ARCHITECTURE.md)

## 快速链接

- **项目根目录**: [../](../)
- **源代码**: [../src/](../src/)
- **测试**: [../tests/](../tests/)
