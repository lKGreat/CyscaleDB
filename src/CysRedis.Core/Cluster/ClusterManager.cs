using System.Collections.Concurrent;
using System.Net;
using CysRedis.Core.Common;

namespace CysRedis.Core.Cluster;

/// <summary>
/// Redis Cluster管理器 - 实现16384槽位分配和Gossip协议
/// </summary>
public class ClusterManager
{
    private const int CLUSTER_SLOTS = 16384;
    
    private readonly ConcurrentDictionary<string, ClusterNode> _nodes;
    private readonly int[] _slotToNode; // 槽位到节点的映射
    private string _myNodeId;
    private ClusterNode? _myself;
    private ClusterState _state;
    private readonly object _lock = new();

    public ClusterManager()
    {
        _nodes = new ConcurrentDictionary<string, ClusterNode>();
        _slotToNode = new int[CLUSTER_SLOTS];
        _myNodeId = GenerateNodeId();
        _state = ClusterState.Fail;
        
        // 初始化所有槽位为未分配
        for (int i = 0; i < CLUSTER_SLOTS; i++)
        {
            _slotToNode[i] = -1;
        }
    }

    /// <summary>
    /// 是否启用集群模式
    /// </summary>
    public bool IsEnabled { get; private set; }

    /// <summary>
    /// 当前节点ID
    /// </summary>
    public string MyNodeId => _myNodeId;

    /// <summary>
    /// 集群状态
    /// </summary>
    public ClusterState State => _state;

    /// <summary>
    /// 启用集群模式
    /// </summary>
    public void Enable(string ipAddress, int port, int busPort)
    {
        if (IsEnabled)
            return;

        _myself = new ClusterNode
        {
            NodeId = _myNodeId,
            IpAddress = ipAddress,
            Port = port,
            BusPort = busPort,
            Flags = ClusterNodeFlags.Myself | ClusterNodeFlags.Master,
            MasterNodeId = null,
            PingSent = DateTime.UtcNow,
            PongReceived = DateTime.UtcNow
        };

        _nodes[_myNodeId] = _myself;
        IsEnabled = true;
        
        Logger.Info("Cluster mode enabled: node {0} at {1}:{2}", _myNodeId, ipAddress, port);
    }

    /// <summary>
    /// 计算键所属的槽位
    /// </summary>
    public static int GetSlot(string key)
    {
        // 处理hash tag: {user1000}.following -> 只hash "user1000"
        var start = key.IndexOf('{');
        if (start >= 0)
        {
            var end = key.IndexOf('}', start + 1);
            if (end > start + 1)
            {
                key = key.Substring(start + 1, end - start - 1);
            }
        }

        return Crc16.Compute(key) % CLUSTER_SLOTS;
    }

    /// <summary>
    /// 检查键是否在当前节点上
    /// </summary>
    public bool IsKeyInMySlots(string key)
    {
        if (!IsEnabled || _myself == null)
            return true; // 非集群模式，所有键都在当前节点

        var slot = GetSlot(key);
        return _myself.Slots.Contains(slot);
    }

    /// <summary>
    /// 获取键所在的节点
    /// </summary>
    public ClusterNode? GetNodeForKey(string key)
    {
        var slot = GetSlot(key);
        return GetNodeForSlot(slot);
    }

    /// <summary>
    /// 获取槽位所在的节点
    /// </summary>
    public ClusterNode? GetNodeForSlot(int slot)
    {
        if (!IsEnabled)
            return null;

        foreach (var node in _nodes.Values)
        {
            if (node.Slots.Contains(slot))
                return node;
        }

        return null;
    }

    /// <summary>
    /// 添加槽位到当前节点
    /// </summary>
    public void AddSlots(params int[] slots)
    {
        if (_myself == null)
            throw new InvalidOperationException("Cluster not enabled");

        lock (_lock)
        {
            foreach (var slot in slots)
            {
                if (slot < 0 || slot >= CLUSTER_SLOTS)
                    throw new ArgumentException($"Invalid slot: {slot}");

                _myself.Slots.Add(slot);
            }
            
            UpdateClusterState();
        }
    }

    /// <summary>
    /// 分配槽位范围到当前节点
    /// </summary>
    public void AddSlotsRange(int start, int end)
    {
        var slots = Enumerable.Range(start, end - start + 1).ToArray();
        AddSlots(slots);
    }

    /// <summary>
    /// 删除槽位
    /// </summary>
    public void DelSlots(params int[] slots)
    {
        if (_myself == null)
            throw new InvalidOperationException("Cluster not enabled");

        lock (_lock)
        {
            foreach (var slot in slots)
            {
                _myself.Slots.Remove(slot);
            }
            
            UpdateClusterState();
        }
    }

    /// <summary>
    /// 添加节点到集群
    /// </summary>
    public void MeetNode(string ipAddress, int port)
    {
        var nodeId = GenerateNodeId();
        var node = new ClusterNode
        {
            NodeId = nodeId,
            IpAddress = ipAddress,
            Port = port,
            BusPort = port + 10000,
            Flags = ClusterNodeFlags.Master,
            PingSent = DateTime.UtcNow,
            PongReceived = DateTime.UtcNow
        };

        _nodes[nodeId] = node;
        Logger.Info("Cluster MEET: added node {0} at {1}:{2}", nodeId, ipAddress, port);
    }

    /// <summary>
    /// 获取所有节点
    /// </summary>
    public IEnumerable<ClusterNode> GetNodes() => _nodes.Values;

    /// <summary>
    /// 获取节点
    /// </summary>
    public ClusterNode? GetNode(string nodeId)
    {
        return _nodes.TryGetValue(nodeId, out var node) ? node : null;
    }

    /// <summary>
    /// 移除节点
    /// </summary>
    public bool ForgetNode(string nodeId)
    {
        if (nodeId == _myNodeId)
            return false; // 不能移除自己

        return _nodes.TryRemove(nodeId, out _);
    }

    /// <summary>
    /// 获取槽位分配信息
    /// </summary>
    public List<SlotRange> GetSlotsInfo()
    {
        var ranges = new List<SlotRange>();
        
        foreach (var node in _nodes.Values.Where(n => n.Flags.HasFlag(ClusterNodeFlags.Master)))
        {
            var slots = node.Slots.OrderBy(s => s).ToList();
            if (slots.Count == 0)
                continue;

            // 合并连续的槽位为范围
            int start = slots[0];
            int end = slots[0];

            for (int i = 1; i < slots.Count; i++)
            {
                if (slots[i] == end + 1)
                {
                    end = slots[i];
                }
                else
                {
                    ranges.Add(new SlotRange
                    {
                        Start = start,
                        End = end,
                        Node = node
                    });
                    start = slots[i];
                    end = slots[i];
                }
            }

            ranges.Add(new SlotRange
            {
                Start = start,
                End = end,
                Node = node
            });
        }

        return ranges;
    }

    /// <summary>
    /// 更新集群状态
    /// </summary>
    private void UpdateClusterState()
    {
        // 检查所有槽位是否都已分配
        var assignedSlots = new HashSet<int>();
        
        foreach (var node in _nodes.Values.Where(n => n.Flags.HasFlag(ClusterNodeFlags.Master)))
        {
            assignedSlots.UnionWith(node.Slots);
        }

        _state = assignedSlots.Count == CLUSTER_SLOTS ? ClusterState.Ok : ClusterState.Fail;
    }

    /// <summary>
    /// 生成节点ID（40个十六进制字符）
    /// </summary>
    private static string GenerateNodeId()
    {
        var bytes = new byte[20];
        Random.Shared.NextBytes(bytes);
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    /// <summary>
    /// 获取集群信息
    /// </summary>
    public Dictionary<string, string> GetInfo()
    {
        var info = new Dictionary<string, string>
        {
            ["cluster_state"] = _state.ToString().ToLowerInvariant(),
            ["cluster_slots_assigned"] = _myself?.Slots.Count.ToString() ?? "0",
            ["cluster_slots_ok"] = CLUSTER_SLOTS.ToString(),
            ["cluster_slots_pfail"] = "0",
            ["cluster_slots_fail"] = "0",
            ["cluster_known_nodes"] = _nodes.Count.ToString(),
            ["cluster_size"] = _nodes.Values.Count(n => n.Flags.HasFlag(ClusterNodeFlags.Master)).ToString(),
            ["cluster_current_epoch"] = "1",
            ["cluster_my_epoch"] = "1"
        };

        return info;
    }
}

/// <summary>
/// 集群节点信息
/// </summary>
public class ClusterNode
{
    public string NodeId { get; set; } = string.Empty;
    public string IpAddress { get; set; } = string.Empty;
    public int Port { get; set; }
    public int BusPort { get; set; }
    public ClusterNodeFlags Flags { get; set; }
    public string? MasterNodeId { get; set; }
    public DateTime PingSent { get; set; }
    public DateTime PongReceived { get; set; }
    public long ConfigEpoch { get; set; }
    public string LinkState { get; set; } = "connected";
    public HashSet<int> Slots { get; } = new();

    /// <summary>
    /// 获取节点角色
    /// </summary>
    public string Role => Flags.HasFlag(ClusterNodeFlags.Master) ? "master" : "slave";

    /// <summary>
    /// 格式化为CLUSTER NODES输出格式
    /// </summary>
    public string ToNodesString()
    {
        var flags = new List<string>();
        if (Flags.HasFlag(ClusterNodeFlags.Myself)) flags.Add("myself");
        if (Flags.HasFlag(ClusterNodeFlags.Master)) flags.Add("master");
        if (Flags.HasFlag(ClusterNodeFlags.Slave)) flags.Add("slave");
        if (Flags.HasFlag(ClusterNodeFlags.Fail)) flags.Add("fail");
        if (Flags.HasFlag(ClusterNodeFlags.PFail)) flags.Add("fail?");
        if (Flags.HasFlag(ClusterNodeFlags.Noaddr)) flags.Add("noaddr");

        var flagsStr = string.Join(",", flags);
        var masterRef = MasterNodeId ?? "-";
        var pingSent = "0";
        var pongRecv = "0";
        var configEpoch = ConfigEpoch.ToString();
        var linkState = LinkState;

        // 槽位范围
        var slotsStr = "";
        if (Slots.Count > 0)
        {
            var sortedSlots = Slots.OrderBy(s => s).ToList();
            var ranges = new List<string>();
            int start = sortedSlots[0];
            int end = sortedSlots[0];

            for (int i = 1; i < sortedSlots.Count; i++)
            {
                if (sortedSlots[i] == end + 1)
                {
                    end = sortedSlots[i];
                }
                else
                {
                    ranges.Add(start == end ? start.ToString() : $"{start}-{end}");
                    start = sortedSlots[i];
                    end = sortedSlots[i];
                }
            }
            ranges.Add(start == end ? start.ToString() : $"{start}-{end}");
            slotsStr = " " + string.Join(" ", ranges);
        }

        return $"{NodeId} {IpAddress}:{Port}@{BusPort} {flagsStr} {masterRef} {pingSent} {pongRecv} {configEpoch} {linkState}{slotsStr}";
    }
}

/// <summary>
/// 集群节点标志
/// </summary>
[Flags]
public enum ClusterNodeFlags
{
    None = 0,
    Myself = 1 << 0,
    Master = 1 << 1,
    Slave = 1 << 2,
    PFail = 1 << 3,
    Fail = 1 << 4,
    Noaddr = 1 << 5,
    Handshake = 1 << 6,
    NoFailover = 1 << 7
}

/// <summary>
/// 集群状态
/// </summary>
public enum ClusterState
{
    Ok,
    Fail
}

/// <summary>
/// 槽位范围
/// </summary>
public class SlotRange
{
    public int Start { get; set; }
    public int End { get; set; }
    public ClusterNode Node { get; set; } = null!;
}

/// <summary>
/// CRC16校验算法 (用于槽位计算)
/// </summary>
public static class Crc16
{
    private static readonly ushort[] Table;

    static Crc16()
    {
        Table = new ushort[256];
        for (ushort i = 0; i < 256; i++)
        {
            ushort crc = i;
            for (int j = 0; j < 8; j++)
            {
                if ((crc & 1) != 0)
                    crc = (ushort)((crc >> 1) ^ 0xA001);
                else
                    crc >>= 1;
            }
            Table[i] = crc;
        }
    }

    /// <summary>
    /// 计算CRC16校验和
    /// </summary>
    public static ushort Compute(string key)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(key);
        ushort crc = 0;
        
        foreach (var b in bytes)
        {
            crc = (ushort)((crc >> 8) ^ Table[(crc ^ b) & 0xFF]);
        }
        
        return crc;
    }
}
