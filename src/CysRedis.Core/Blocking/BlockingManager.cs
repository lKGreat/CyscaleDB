using System.Collections.Concurrent;
using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Blocking;

/// <summary>
/// 管理阻塞命令 (BLPOP, BRPOP, BZPOPMIN, BZPOPMAX等)
/// </summary>
public class BlockingManager
{
    private readonly ConcurrentDictionary<string, BlockingKeyInfo> _blockedKeys = new();
    private readonly ConcurrentDictionary<long, BlockedClient> _blockedClients = new();
    private readonly object _lock = new();

    /// <summary>
    /// 阻塞客户端在某个键上 (列表类型)
    /// </summary>
    public async Task<BlockingResult?> BlockOnListKeysAsync(
        RedisClient client,
        string[] keys,
        TimeSpan timeout,
        bool popLeft,
        CancellationToken cancellationToken)
    {
        var blockedClient = new BlockedClient
        {
            Client = client,
            BlockedAt = DateTime.UtcNow,
            Timeout = timeout,
            Keys = keys,
            OperationType = BlockOperationType.List,
            PopLeft = popLeft
        };

        // 标记客户端为阻塞状态
        client.Flags |= ClientFlags.Blocked;
        _blockedClients[client.Id] = blockedClient;

        // 为每个键注册阻塞客户端
        lock (_lock)
        {
            foreach (var key in keys)
            {
                var keyInfo = _blockedKeys.GetOrAdd(key, _ => new BlockingKeyInfo { Key = key });
                keyInfo.BlockedClients.Add(blockedClient);
            }
        }

        try
        {
            // 等待被唤醒或超时
            using var timeoutCts = new CancellationTokenSource(timeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            await blockedClient.WaitSignal.WaitAsync(linkedCts.Token).ConfigureAwait(false);

            // 被唤醒，返回结果
            return blockedClient.Result;
        }
        catch (OperationCanceledException)
        {
            // 超时或取消
            return null;
        }
        finally
        {
            // 清理阻塞状态
            UnblockClient(client.Id);
        }
    }

    /// <summary>
    /// 阻塞客户端在某个键上 (有序集合类型)
    /// </summary>
    public async Task<BlockingResult?> BlockOnSortedSetKeysAsync(
        RedisClient client,
        string[] keys,
        TimeSpan timeout,
        bool min,
        CancellationToken cancellationToken)
    {
        var blockedClient = new BlockedClient
        {
            Client = client,
            BlockedAt = DateTime.UtcNow,
            Timeout = timeout,
            Keys = keys,
            OperationType = BlockOperationType.SortedSet,
            PopMin = min
        };

        client.Flags |= ClientFlags.Blocked;
        _blockedClients[client.Id] = blockedClient;

        lock (_lock)
        {
            foreach (var key in keys)
            {
                var keyInfo = _blockedKeys.GetOrAdd(key, _ => new BlockingKeyInfo { Key = key });
                keyInfo.BlockedClients.Add(blockedClient);
            }
        }

        try
        {
            using var timeoutCts = new CancellationTokenSource(timeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            await blockedClient.WaitSignal.WaitAsync(linkedCts.Token).ConfigureAwait(false);
            return blockedClient.Result;
        }
        catch (OperationCanceledException)
        {
            return null;
        }
        finally
        {
            UnblockClient(client.Id);
        }
    }

    /// <summary>
    /// 当键上有新数据时，唤醒阻塞的客户端
    /// </summary>
    public void SignalKeyReady(string key)
    {
        if (!_blockedKeys.TryGetValue(key, out var keyInfo))
            return;

        lock (_lock)
        {
            // 移除第一个等待的客户端（FIFO）
            if (keyInfo.BlockedClients.Count > 0)
            {
                var blockedClient = keyInfo.BlockedClients[0];
                keyInfo.BlockedClients.RemoveAt(0);

                // 设置该客户端为 Unblocked 标记
                blockedClient.Client.Flags |= ClientFlags.Unblocked;

                // 通知客户端可以继续处理
                blockedClient.SignalKeyReady = key;
                blockedClient.WaitSignal.Release();
            }

            // 如果没有更多客户端等待，移除键信息
            if (keyInfo.BlockedClients.Count == 0)
            {
                _blockedKeys.TryRemove(key, out _);
            }
        }
    }

    /// <summary>
    /// 清理阻塞客户端
    /// </summary>
    private void UnblockClient(long clientId)
    {
        if (!_blockedClients.TryRemove(clientId, out var blockedClient))
            return;

        blockedClient.Client.Flags &= ~ClientFlags.Blocked;
        blockedClient.Client.Flags &= ~ClientFlags.Unblocked;

        // 从所有键的等待列表中移除
        lock (_lock)
        {
            foreach (var key in blockedClient.Keys)
            {
                if (_blockedKeys.TryGetValue(key, out var keyInfo))
                {
                    keyInfo.BlockedClients.Remove(blockedClient);
                    if (keyInfo.BlockedClients.Count == 0)
                    {
                        _blockedKeys.TryRemove(key, out _);
                    }
                }
            }
        }
    }

    /// <summary>
    /// 取消特定客户端的阻塞
    /// </summary>
    public void UnblockClientById(long clientId)
    {
        if (_blockedClients.TryGetValue(clientId, out var blockedClient))
        {
            blockedClient.WaitSignal.Release();
        }
    }

    /// <summary>
    /// 获取当前阻塞的客户端数量
    /// </summary>
    public int GetBlockedClientCount() => _blockedClients.Count;
}

/// <summary>
/// 键阻塞信息
/// </summary>
internal class BlockingKeyInfo
{
    public string Key { get; set; } = string.Empty;
    public List<BlockedClient> BlockedClients { get; } = new();
}

/// <summary>
/// 被阻塞的客户端信息
/// </summary>
internal class BlockedClient
{
    public RedisClient Client { get; set; } = null!;
    public DateTime BlockedAt { get; set; }
    public TimeSpan Timeout { get; set; }
    public string[] Keys { get; set; } = Array.Empty<string>();
    public BlockOperationType OperationType { get; set; }
    public bool PopLeft { get; set; }
    public bool PopMin { get; set; }
    public SemaphoreSlim WaitSignal { get; } = new(0, 1);
    public BlockingResult? Result { get; set; }
    public string? SignalKeyReady { get; set; }
}

/// <summary>
/// 阻塞操作类型
/// </summary>
internal enum BlockOperationType
{
    List,
    SortedSet
}

/// <summary>
/// 阻塞结果
/// </summary>
public class BlockingResult
{
    public string Key { get; set; } = string.Empty;
    public byte[]? Value { get; set; }
    public string? Member { get; set; }
    public double? Score { get; set; }
}
