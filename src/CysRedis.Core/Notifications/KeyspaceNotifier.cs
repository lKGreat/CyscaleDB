using CysRedis.Core.Common;
using CysRedis.Core.PubSub;

namespace CysRedis.Core.Notifications;

/// <summary>
/// Keyspace 事件通知管理器
/// </summary>
public class KeyspaceNotifier
{
    private readonly PubSubManager _pubSub;
    private KeyspaceEventFlags _notifyFlags;

    public KeyspaceNotifier(PubSubManager pubSub)
    {
        _pubSub = pubSub ?? throw new ArgumentNullException(nameof(pubSub));
        _notifyFlags = KeyspaceEventFlags.None;
    }

    /// <summary>
    /// 当前通知标志
    /// </summary>
    public KeyspaceEventFlags NotifyFlags
    {
        get => _notifyFlags;
        set => _notifyFlags = value;
    }

    /// <summary>
    /// 是否启用了通知
    /// </summary>
    public bool IsEnabled => _notifyFlags != KeyspaceEventFlags.None;

    /// <summary>
    /// 通知键空间事件 (针对特定键)
    /// </summary>
    public void NotifyKeyspaceEvent(int database, string key, string operation)
    {
        if (!ShouldNotify(operation))
            return;

        // __keyspace@<db>__:<key> <operation>
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.Keyspace))
        {
            var channel = $"__keyspace@{database}__:{key}";
            _ = _pubSub.PublishAsync(channel, operation, CancellationToken.None);
        }
    }

    /// <summary>
    /// 通知键事件 (针对特定操作)
    /// </summary>
    public void NotifyKeyeventEvent(int database, string key, string operation)
    {
        if (!ShouldNotify(operation))
            return;

        // __keyevent@<db>__:<operation> <key>
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.Keyevent))
        {
            var channel = $"__keyevent@{database}__:{operation}";
            _ = _pubSub.PublishAsync(channel, key, CancellationToken.None);
        }
    }

    /// <summary>
    /// 同时发送两种通知
    /// </summary>
    public void Notify(int database, string key, string operation)
    {
        NotifyKeyspaceEvent(database, key, operation);
        NotifyKeyeventEvent(database, key, operation);
    }

    /// <summary>
    /// 从配置字符串设置通知标志
    /// </summary>
    public void SetNotifyConfig(string config)
    {
        _notifyFlags = KeyspaceEventFlags.None;

        foreach (var c in config.ToLowerInvariant())
        {
            _notifyFlags |= c switch
            {
                'k' => KeyspaceEventFlags.Keyspace,
                'e' => KeyspaceEventFlags.Keyevent | KeyspaceEventFlags.Evicted,
                'g' => KeyspaceEventFlags.Generic,
                '$' => KeyspaceEventFlags.String,
                'l' => KeyspaceEventFlags.List,
                's' => KeyspaceEventFlags.Set,
                'h' => KeyspaceEventFlags.Hash,
                'z' => KeyspaceEventFlags.SortedSet,
                't' => KeyspaceEventFlags.Stream,
                'x' => KeyspaceEventFlags.Expired,
                'm' => KeyspaceEventFlags.Missed,
                'n' => KeyspaceEventFlags.New,
                'a' => KeyspaceEventFlags.All,
                _ => KeyspaceEventFlags.None
            };
        }
    }

    /// <summary>
    /// 获取当前配置字符串
    /// </summary>
    public string GetNotifyConfig()
    {
        if (_notifyFlags == KeyspaceEventFlags.None)
            return "";

        var config = "";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.Keyspace)) config += "K";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.Keyevent)) config += "E";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.Generic)) config += "g";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.String)) config += "$";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.List)) config += "l";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.Set)) config += "s";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.Hash)) config += "h";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.SortedSet)) config += "z";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.Stream)) config += "t";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.Expired)) config += "x";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.Evicted)) config += "e";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.Missed)) config += "m";
        if (_notifyFlags.HasFlag(KeyspaceEventFlags.New)) config += "n";

        return config;
    }

    /// <summary>
    /// 检查是否应该通知特定操作
    /// </summary>
    private bool ShouldNotify(string operation)
    {
        return operation.ToLowerInvariant() switch
        {
            // Generic commands
            "del" or "rename" or "move" or "copy" or "restore" => 
                _notifyFlags.HasFlag(KeyspaceEventFlags.Generic),
            
            // String commands
            "set" or "setex" or "setnx" or "append" or "incr" or "decr" => 
                _notifyFlags.HasFlag(KeyspaceEventFlags.String),
            
            // List commands
            "lpush" or "rpush" or "lpop" or "rpop" or "linsert" or "ltrim" => 
                _notifyFlags.HasFlag(KeyspaceEventFlags.List),
            
            // Set commands
            "sadd" or "srem" or "spop" => 
                _notifyFlags.HasFlag(KeyspaceEventFlags.Set),
            
            // Hash commands
            "hset" or "hdel" or "hincrby" => 
                _notifyFlags.HasFlag(KeyspaceEventFlags.Hash),
            
            // Sorted set commands
            "zadd" or "zrem" or "zincrby" => 
                _notifyFlags.HasFlag(KeyspaceEventFlags.SortedSet),
            
            // Stream commands
            "xadd" or "xtrim" or "xdel" => 
                _notifyFlags.HasFlag(KeyspaceEventFlags.Stream),
            
            // Expiration events
            "expire" or "expireat" or "expired" => 
                _notifyFlags.HasFlag(KeyspaceEventFlags.Expired),
            
            // Eviction events
            "evicted" => 
                _notifyFlags.HasFlag(KeyspaceEventFlags.Evicted),
            
            // New key events
            "new" => 
                _notifyFlags.HasFlag(KeyspaceEventFlags.New),
            
            _ => false
        };
    }
}

/// <summary>
/// Keyspace 事件标志
/// </summary>
[Flags]
public enum KeyspaceEventFlags
{
    None = 0,
    
    /// <summary>K: Keyspace events (__keyspace@<db>__:<key>)</summary>
    Keyspace = 1 << 0,
    
    /// <summary>E: Keyevent events (__keyevent@<db>__:<event>)</summary>
    Keyevent = 1 << 1,
    
    /// <summary>g: Generic commands (DEL, EXPIRE, RENAME)</summary>
    Generic = 1 << 2,
    
    /// <summary>$: String commands</summary>
    String = 1 << 3,
    
    /// <summary>l: List commands</summary>
    List = 1 << 4,
    
    /// <summary>s: Set commands</summary>
    Set = 1 << 5,
    
    /// <summary>h: Hash commands</summary>
    Hash = 1 << 6,
    
    /// <summary>z: Sorted set commands</summary>
    SortedSet = 1 << 7,
    
    /// <summary>t: Stream commands</summary>
    Stream = 1 << 8,
    
    /// <summary>x: Expired events</summary>
    Expired = 1 << 9,
    
    /// <summary>e: Evicted events</summary>
    Evicted = 1 << 10,
    
    /// <summary>m: Key miss events (not implemented)</summary>
    Missed = 1 << 11,
    
    /// <summary>n: New key events</summary>
    New = 1 << 12,
    
    /// <summary>A: Alias for all events</summary>
    All = Keyspace | Keyevent | Generic | String | List | Set | Hash | SortedSet | Stream | Expired | Evicted | New
}
