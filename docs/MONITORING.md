# CyscaleDB 性能监控

## 概述

CyscaleDB 提供全面的性能监控系统，帮助识别和解决性能瓶颈。监控系统包括：
- 实时性能指标
- 慢查询日志
- SHOW STATUS 命令
- Buffer Pool 统计

## 性能指标

### 查询指标

| 指标名称 | 类型 | 说明 |
|---------|------|------|
| `queries_executed` | Counter | 执行的查询总数 |
| `query_execution_time_ms` | Histogram | 查询执行时间分布 |
| `slow_queries` | Counter | 慢查询总数 |
| `failed_queries` | Counter | 失败的查询总数 |

### 事务指标

| 指标名称 | 类型 | 说明 |
|---------|------|------|
| `transactions_started` | Counter | 开始的事务总数 |
| `transactions_committed` | Counter | 提交的事务总数 |
| `transactions_rolled_back` | Counter | 回滚的事务总数 |

### 锁指标

| 指标名称 | 类型 | 说明 |
|---------|------|------|
| `lock_waits` | Counter | 锁等待次数 |
| `lock_wait_time_ms` | Histogram | 锁等待时间分布 |
| `deadlocks` | Counter | 死锁次数 |
| `active_locks` | Gauge | 当前持有的锁数量 |

### Buffer Pool 指标

| 指标名称 | 类型 | 说明 |
|---------|------|------|
| `buffer_pool_used_pages` | Gauge | 使用的页数 |
| `buffer_pool_hits` | Counter | 缓存命中次数 |
| `buffer_pool_misses` | Counter | 缓存未命中次数 |
| `buffer_pool_hit_ratio` | Gauge | 缓存命中率 (0-1) |

### I/O 指标

| 指标名称 | 类型 | 说明 |
|---------|------|------|
| `pages_read` | Counter | 读取的页数 |
| `pages_written` | Counter | 写入的页数 |
| `io_read_time_ms` | Histogram | I/O 读取时间 |
| `io_write_time_ms` | Histogram | I/O 写入时间 |

## SHOW STATUS 命令

查看所有性能指标：

```sql
SHOW STATUS;
```

输出示例：
```
+-------------------------------+--------+
| Variable_name                 | Value  |
+-------------------------------+--------+
| queries_executed              | 15234  |
| slow_queries                  | 23     |
| transactions_committed        | 8901   |
| buffer_pool_hit_ratio         | 0.95   |
| deadlocks                     | 2      |
+-------------------------------+--------+
```

### 过滤指标

使用 LIKE 模式过滤：

```sql
-- 查看所有查询相关指标
SHOW STATUS LIKE 'queries%';

-- 查看 Buffer Pool 指标
SHOW STATUS LIKE 'buffer_pool%';

-- 查看事务指标
SHOW STATUS LIKE 'transactions%';

-- 查看锁指标
SHOW STATUS LIKE '%lock%';
```

### 重置指标

```sql
-- TODO: 实现重置功能
FLUSH STATUS;
```

## 慢查询日志

### 启用慢查询日志

在配置文件中：

```json
{
  "EnableSlowQueryLog": true,
  "SlowQueryThresholdMs": 1000,
  "SlowQueryLogPath": "cyscaledb-slow.log"
}
```

或运行时启用：

```sql
SET GLOBAL slow_query_log = ON;
SET GLOBAL slow_query_threshold_ms = 1000;
```

### 慢查询日志格式

日志采用 MySQL 兼容的格式：

```
# Time: 2026-01-21 14:30:45
# User@Host: root[root] @ [testdb]
# Query_time: 2.156789  Lock_time: 0.000000  Rows_sent: 1000  Rows_examined: 50000
# Indexes_used: idx_order_date
SET timestamp=1737471045;
SELECT * FROM orders WHERE order_date > '2025-01-01' ORDER BY amount DESC;

# Time: 2026-01-21 14:32:18
# User@Host: app[app] @ [production]
# Query_time: 5.234567  Lock_time: 0.000000  Rows_sent: 0  Rows_examined: 100000
SELECT COUNT(*) FROM large_table WHERE status = 'pending';
```

### 分析慢查询日志

使用工具分析：

```bash
# 查看最慢的 10 个查询
grep "Query_time" cyscaledb-slow.log | sort -t: -k2 -rn | head -10

# 统计慢查询模式
grep "SELECT" cyscaledb-slow.log | wc -l
grep "UPDATE" cyscaledb-slow.log | wc -l
```

### 轮转慢查询日志

```sql
-- TODO: 实现日志轮转
FLUSH SLOW LOGS;
```

手动轮转：

```bash
mv cyscaledb-slow.log cyscaledb-slow.log.20260121
# 数据库会自动创建新文件
```

## 监控 Dashboard

### 关键指标监控

**必须监控的指标**:

1. **QPS (Queries Per Second)**
   ```sql
   SHOW STATUS LIKE 'queries_executed';
   -- 计算: (current_value - previous_value) / time_interval
   ```

2. **缓存命中率**
   ```sql
   SHOW STATUS LIKE 'buffer_pool_hit_ratio';
   -- 目标: > 0.95 (95%)
   ```

3. **慢查询数量**
   ```sql
   SHOW STATUS LIKE 'slow_queries';
   -- 目标: < 总查询的 1%
   ```

4. **死锁数量**
   ```sql
   SHOW STATUS LIKE 'deadlocks';
   -- 目标: 尽可能接近 0
   ```

### 性能基线

建立性能基线以便比较：

```sql
-- 在系统正常负载时记录指标
SHOW STATUS;
```

保存输出作为基线，定期对比。

## 性能调优建议

### Buffer Pool 调优

**问题**: 缓存命中率低 (< 90%)

```sql
SHOW STATUS LIKE 'buffer_pool%';
```

**解决方案**:
- 增加 `BufferPoolSizePages`
- 调整 `BufferPoolYoungRatio`
- 分析查询是否有大表扫描

### 慢查询优化

**问题**: 慢查询过多

```sql
SHOW STATUS LIKE 'slow_queries';
```

**解决方案**:
1. 分析慢查询日志
2. 添加合适的索引
3. 优化查询语句
4. 考虑分区表

### 锁等待优化

**问题**: 锁等待时间过长

```sql
SHOW STATUS LIKE '%lock%';
```

**解决方案**:
- 减少事务持有锁的时间
- 优化索引减少锁冲突
- 使用合适的隔离级别
- 分析死锁原因

### I/O 优化

**问题**: I/O 操作过多

```sql
SHOW STATUS LIKE 'pages_%';
SHOW STATUS LIKE 'io_%';
```

**解决方案**:
- 增加 Buffer Pool 大小
- 启用预读
- 调整 checkpoint 频率
- 使用 SSD 存储

## 监控脚本示例

### Bash 监控脚本

```bash
#!/bin/bash
# monitor_cyscale.sh

while true; do
  echo "=== CyscaleDB Status $(date) ==="
  
  # QPS
  queries=$(mysql -u root -e "SHOW STATUS LIKE 'queries_executed'" | awk 'NR==2 {print $2}')
  echo "Total Queries: $queries"
  
  # Buffer Pool Hit Ratio
  hit_ratio=$(mysql -u root -e "SHOW STATUS LIKE 'buffer_pool_hit_ratio'" | awk 'NR==2 {print $2}')
  echo "Buffer Pool Hit Ratio: $hit_ratio"
  
  # Slow Queries
  slow=$(mysql -u root -e "SHOW STATUS LIKE 'slow_queries'" | awk 'NR==2 {print $2}')
  echo "Slow Queries: $slow"
  
  # Deadlocks
  deadlocks=$(mysql -u root -e "SHOW STATUS LIKE 'deadlocks'" | awk 'NR==2 {print $2}')
  echo "Deadlocks: $deadlocks"
  
  echo ""
  sleep 60
done
```

### PowerShell 监控脚本

```powershell
# monitor_cyscale.ps1

while ($true) {
    Write-Host "=== CyscaleDB Status $(Get-Date) ==="
    
    # 执行 SHOW STATUS 并解析输出
    $status = & mysql -u root -e "SHOW STATUS"
    
    # 提取关键指标
    $queries = ($status | Select-String "queries_executed").ToString().Split()[-1]
    Write-Host "Total Queries: $queries"
    
    $hitRatio = ($status | Select-String "buffer_pool_hit_ratio").ToString().Split()[-1]
    Write-Host "Buffer Pool Hit Ratio: $hitRatio"
    
    $slow = ($status | Select-String "slow_queries").ToString().Split()[-1]
    Write-Host "Slow Queries: $slow"
    
    Write-Host ""
    Start-Sleep -Seconds 60
}
```

## 告警设置

### 建议的告警阈值

| 指标 | 警告阈值 | 严重阈值 |
|------|---------|---------|
| Buffer Pool Hit Ratio | < 0.90 | < 0.80 |
| Slow Queries Ratio | > 0.01 | > 0.05 |
| Deadlocks per minute | > 1 | > 10 |
| Query Failure Rate | > 0.01 | > 0.05 |
| Lock Wait Time (P95) | > 1000ms | > 5000ms |

### 告警脚本示例

```bash
#!/bin/bash
# alert_check.sh

# 获取指标
hit_ratio=$(mysql -u root -e "SHOW STATUS LIKE 'buffer_pool_hit_ratio'" | awk 'NR==2 {print $2}')

# 检查阈值
if (( $(echo "$hit_ratio < 0.80" | bc -l) )); then
  echo "CRITICAL: Buffer pool hit ratio is $hit_ratio"
  # 发送告警（邮件/Slack/等）
  send_alert "Buffer pool hit ratio critical: $hit_ratio"
elif (( $(echo "$hit_ratio < 0.90" | bc -l) )); then
  echo "WARNING: Buffer pool hit ratio is $hit_ratio"
  send_alert "Buffer pool hit ratio warning: $hit_ratio"
fi
```

## 集成监控系统

### Prometheus

TODO: 实现 Prometheus exporter

### Grafana Dashboard

TODO: 提供 Grafana dashboard 模板

### 自定义指标导出

通过 SHOW STATUS 输出可以集成到任何监控系统：

```python
import subprocess
import json

def get_cyscale_metrics():
    result = subprocess.run(
        ['mysql', '-u', 'root', '-e', 'SHOW STATUS'],
        capture_output=True,
        text=True
    )
    
    metrics = {}
    for line in result.stdout.split('\n')[1:]:  # Skip header
        if line:
            parts = line.split()
            if len(parts) == 2:
                metrics[parts[0]] = parts[1]
    
    return metrics

# 定期调用并发送到监控系统
metrics = get_cyscale_metrics()
print(json.dumps(metrics, indent=2))
```

## 故障排查

### 性能突然下降

1. 检查慢查询日志
2. 查看 Buffer Pool 命中率
3. 检查是否有死锁
4. 查看系统资源使用（CPU/内存/磁盘）

### 查询超时

1. 检查锁等待时间
2. 分析是否有长事务
3. 检查索引使用情况
4. 查看是否有表扫描

### 内存使用过高

1. 检查 Buffer Pool 大小配置
2. 查看是否有内存泄漏
3. 检查并发连接数
4. 分析查询复杂度

## 相关文档

- [配置指南](CONFIGURATION.md)
- [在线 DDL](ONLINE_DDL.md)
- [项目状态](PROJECT_STATUS.md)
