---
name: Redis协议网络优化
overview: 为 CysRedis 添加心跳包机制、粘包处理优化，以及操作系统级别的 Socket 和内存优化，在保持 Redis 协议兼容性的前提下提升服务端性能。
todos:
  - id: socket-options
    content: 实现 Socket 系统级优化配置（TCP_NODELAY、Keep-Alive、缓冲区）
    status: completed
  - id: server-options
    content: 创建 RedisServerOptions 配置类，支持连接超时、最大客户端数等参数
    status: completed
  - id: pipe-reader
    content: 使用 System.IO.Pipelines 重构 RespReader，实现零拷贝粘包处理
    status: completed
  - id: pipe-writer
    content: 使用 System.IO.Pipelines 重构 RespWriter，实现批量刷新
    status: completed
  - id: client-tracking
    content: 在 RedisClient 添加活跃时间跟踪、IdleTime 属性
    status: completed
  - id: health-check
    content: 在 RedisServer 实现后台健康检查任务，清理超时连接
    status: completed
  - id: connection-limit
    content: 实现最大连接数限制，超限时返回标准错误
    status: completed
  - id: buffer-pool
    content: 添加 ArrayPool 内存池化，减少 GC 压力
    status: completed
  - id: valuetask-migration
    content: 将热路径方法从 Task 迁移到 ValueTask
    status: completed
---

# Redis 协议网络层优化方案

## 背景分析

当前 [`RedisServer.cs`](src/CysRedis.Core/Protocol/RedisServer.cs) 和 [`RedisClient.cs`](src/CysRedis.Core/Protocol/RedisClient.cs) 的实现存在以下可优化点：

1. 缺乏空闲连接检测和心跳机制
2. `RespReader` 使用传统的 Stream+byte[] 方式，未充分利用 System.IO.Pipelines
3. Socket 未进行系统级优化配置

---

## 一、心跳包机制（保持 Redis 协议兼容）

Redis 标准协议中使用 **PING/PONG** 作为心跳，我们保持这一标准，在服务端增加：

### 1.1 空闲连接检测

在 `RedisClient` 中添加：

```csharp
// 连接健康状态跟踪
public DateTime LastActivityAt { get; private set; }
public TimeSpan IdleTime => DateTime.UtcNow - LastActivityAt;
```

### 1.2 连接超时清理任务

在 `RedisServer` 中添加后台定时任务：

```csharp
// 定期检查并清理超时连接
private Task _clientHealthCheckTask;
private TimeSpan _clientIdleTimeout = TimeSpan.FromMinutes(5); // 可配置
private TimeSpan _healthCheckInterval = TimeSpan.FromSeconds(30);
```

- 遍历 `_clients` 字典，检测 `IdleTime > _clientIdleTimeout` 的连接
- 对超时连接优雅断开（发送 `+ERR connection timeout\r\n` 后关闭）
- 符合 Redis 的 `timeout` 配置行为

---

## 二、粘包处理优化（使用 System.IO.Pipelines）

### 2.1 重构 RespReader 使用 PipeReader

将 [`RespReader.cs`](src/CysRedis.Core/Protocol/RespReader.cs) 改为基于 `System.IO.Pipelines`：

```mermaid
flowchart LR
    Network["NetworkStream"] --> PipeReader["PipeReader"]
    PipeReader --> Parser["RESP Parser"]
    Parser --> |"完整命令"| Handler["命令处理"]
    Parser --> |"不完整数据"| PipeReader
```

核心变更：

```csharp
public class RespPipeReader
{
    private readonly PipeReader _pipeReader;
    
    public async ValueTask<RespValue?> ReadValueAsync(CancellationToken ct)
    {
        while (true)
        {
            var result = await _pipeReader.ReadAsync(ct);
            var buffer = result.Buffer;
            
            if (TryParseResp(buffer, out var value, out var consumed))
            {
                _pipeReader.AdvanceTo(consumed);
                return value;
            }
            
            if (result.IsCompleted)
                return null; // EOF
                
            _pipeReader.AdvanceTo(buffer.Start, buffer.End); // 继续读取
        }
    }
}
```

### 2.2 重构 RespWriter 使用 PipeWriter

将 [`RespWriter.cs`](src/CysRedis.Core/Protocol/RespWriter.cs) 改为基于 `PipeWriter`：

```csharp
public class RespPipeWriter
{
    private readonly PipeWriter _pipeWriter;
    
    public void WriteInteger(long value)
    {
        var span = _pipeWriter.GetSpan(24);
        // 直接写入 span，避免中间分配
        var written = FormatInteger(span, value);
        _pipeWriter.Advance(written);
    }
    
    public ValueTask FlushAsync(CancellationToken ct) 
        => _pipeWriter.FlushAsync(ct);
}
```

优势：

- 零拷贝：数据直接写入内核缓冲区
- 背压支持：自动处理写入速度不匹配
- 完整的粘包边界处理

---

## 三、操作系统级别优化

### 3.1 Socket 配置优化

在 `RedisServer.AcceptConnectionsAsync()` 中配置：

```csharp
private void ConfigureSocket(Socket socket)
{
    // 禁用 Nagle 算法 - 减少小包延迟
    socket.NoDelay = true;
    
    // 配置 TCP Keep-Alive
    socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 60);
    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 10);
    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 3);
    
    // 接收/发送缓冲区优化
    socket.ReceiveBufferSize = 64 * 1024;
    socket.SendBufferSize = 64 * 1024;
    
    // 允许地址重用
    socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
}
```

### 3.2 内存池化（ArrayPool）

在 [`Constants.cs`](src/CysRedis.Core/Common/Constants.cs) 添加共享池：

```csharp
public static class BufferPool
{
    public static readonly ArrayPool<byte> Shared = ArrayPool<byte>.Shared;
    
    // 大块数据使用独立池
    public static readonly ArrayPool<byte> Large = ArrayPool<byte>.Create(
        maxArrayLength: 1 * 1024 * 1024,  // 1MB
        maxArraysPerBucket: 16
    );
}
```

### 3.3 使用 ValueTask 减少堆分配

将热路径的 `Task<T>` 改为 `ValueTask<T>`：

```csharp
// Before
public async Task<string[]?> ReadCommandAsync(CancellationToken ct);

// After
public ValueTask<string[]?> ReadCommandAsync(CancellationToken ct);
```

---

## 四、RedisClient 连接管理增强

### 4.1 添加配置类

```csharp
public class RedisServerOptions
{
    public int Port { get; set; } = 6379;
    public string? DataDir { get; set; }
    
    // 连接管理
    public int MaxClients { get; set; } = 10000;
    public TimeSpan ClientIdleTimeout { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan HealthCheckInterval { get; set; } = TimeSpan.FromSeconds(30);
    
    // Socket 配置
    public bool TcpNoDelay { get; set; } = true;
    public bool TcpKeepAlive { get; set; } = true;
    public int ReceiveBufferSize { get; set; } = 64 * 1024;
    public int SendBufferSize { get; set; } = 64 * 1024;
}
```

### 4.2 连接限制

```csharp
// 在 AcceptConnectionsAsync 中
if (_clients.Count >= _options.MaxClients)
{
    // 写入错误后关闭
    await WriteError(tcpClient, "ERR max number of clients reached");
    tcpClient.Close();
    continue;
}
```

---

## 五、文件变更清单

| 文件 | 变更类型 | 说明 |

|------|---------|------|

| `Protocol/RespReader.cs` | 重构 | 改用 PipeReader，添加零拷贝解析 |

| `Protocol/RespWriter.cs` | 重构 | 改用 PipeWriter，添加批量刷新 |

| `Protocol/RedisServer.cs` | 增强 | Socket 优化、健康检查、连接限制 |

| `Protocol/RedisClient.cs` | 增强 | 活跃时间跟踪、连接状态管理 |

| `Common/Constants.cs` | 新增 | 配置常量、BufferPool |

| `Common/RedisServerOptions.cs` | 新增 | 服务端配置类 |

---

## 六、兼容性保证

所有优化保持 Redis RESP 协议完全兼容：

- 心跳使用标准 PING/PONG 命令
- 粘包处理基于 RESP 协议的 `\r\n` 边界
- 超时断开发送标准 ERR 响应
- 可通过 CONFIG 命令动态调整 timeout 等参数