using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using CysRedis.Core.Unsafe.Common;

namespace CysRedis.Core.Unsafe.DataStructures;

/// <summary>
/// High-performance skip list using unmanaged memory and pointer operations.
/// Optimized for cache locality and minimal allocations.
/// </summary>
public unsafe sealed class UnsafeSkipList : IDisposable
{
    private const int MaxLevel = 32;
    private const double Probability = 0.25;
    
    private SkipListNode* _head;
    private SkipListNode* _tail;
    private int _level;
    private int _count;
    private Random _random = new();
    
    // Node pool for efficient allocation
    private void* _nodePool;
    private int _nodePoolSize;
    private int _nodePoolCapacity;
    private const int NodePoolChunkSize = 1024; // Allocate nodes in chunks
    
    /// <summary>
    /// Number of elements in the skip list.
    /// </summary>
    public int Count => _count;
    
    /// <summary>
    /// Creates a new skip list.
    /// </summary>
    public UnsafeSkipList()
    {
        _head = AllocateNode(MaxLevel);
        _level = 1;
        _count = 0;
        _tail = null;
        
        // Initialize head node levels
        for (int i = 0; i < MaxLevel; i++)
        {
            _head->Levels[i].Forward = null;
            _head->Levels[i].Span = 0;
        }
    }
    
    /// <summary>
    /// Inserts or updates an element.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Insert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        var update = stackalloc SkipListNode*[MaxLevel];
        var rank = stackalloc int[MaxLevel];
        var current = _head;
        
        // Find the position to insert
        for (int i = _level - 1; i >= 0; i--)
        {
            rank[i] = i == _level - 1 ? 0 : rank[i + 1];
            SkipListLevel* currentLevels = current->Levels;
            
            while (currentLevels[i].Forward != null &&
                   CompareKeys(currentLevels[i].Forward->Key, currentLevels[i].Forward->KeyLength, key) < 0)
            {
                rank[i] += currentLevels[i].Span;
                current = currentLevels[i].Forward;
                currentLevels = current->Levels;
            }
            update[i] = current;
        }
        
        // Check if key already exists
        SkipListLevel* currentLevels0 = current->Levels;
        current = currentLevels0[0].Forward;
        if (current != null && CompareKeys(current->Key, current->KeyLength, key) == 0)
        {
            // Update existing
            UpdateValue(current, value);
            return false;
        }
        
        // Generate random level
        int level = RandomLevel();
        if (level > _level)
        {
            for (int i = _level; i < level; i++)
            {
                rank[i] = 0;
                update[i] = _head;
                SkipListLevel* headLevels = _head->Levels;
                headLevels[i].Span = _count;
            }
            _level = level;
        }
        
        // Create new node
        var newNode = AllocateNode(level);
        CopyKey(newNode, key);
        CopyValue(newNode, value);
        
        // Access levels through pointer arithmetic
        SkipListLevel* newNodeLevels = newNode->Levels;
        for (int i = 0; i < level; i++)
        {
            SkipListLevel* updateLevels = update[i]->Levels;
            newNodeLevels[i].Forward = updateLevels[i].Forward;
            updateLevels[i].Forward = newNode;
            
            newNodeLevels[i].Span = updateLevels[i].Span - (rank[0] - rank[i]);
            updateLevels[i].Span = rank[0] - rank[i] + 1;
        }
        
        // Increment span for untouched levels
        for (int i = level; i < _level; i++)
        {
            SkipListLevel* updateLevels = update[i]->Levels;
            updateLevels[i].Span++;
        }
        
        // Set backward pointer
        newNode->Backward = update[0] == _head ? null : update[0];
        SkipListLevel* newNodeLevels0 = newNode->Levels;
        if (newNodeLevels0[0].Forward != null)
            newNodeLevels0[0].Forward->Backward = newNode;
        else
            _tail = newNode;
        
        _count++;
        return true;
    }
    
    /// <summary>
    /// Removes an element by key.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Remove(ReadOnlySpan<byte> key)
    {
        var update = stackalloc SkipListNode*[MaxLevel];
        var current = _head;
        
        for (int i = _level - 1; i >= 0; i--)
        {
            SkipListLevel* currentLevels = current->Levels;
            while (currentLevels[i].Forward != null &&
                   CompareKeys(currentLevels[i].Forward->Key, currentLevels[i].Forward->KeyLength, key) < 0)
            {
                current = currentLevels[i].Forward;
                currentLevels = current->Levels;
            }
            update[i] = current;
        }
        
        SkipListLevel* currentLevels0 = current->Levels;
        current = currentLevels0[0].Forward;
        if (current == null || CompareKeys(current->Key, current->KeyLength, key) != 0)
            return false;
        
        // Delete node
        DeleteNode(current, update);
        return true;
    }
    
    /// <summary>
    /// Finds an element by key.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Find(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        var current = _head;
        
        for (int i = _level - 1; i >= 0; i--)
        {
            SkipListLevel* currentLevels = current->Levels;
            while (currentLevels[i].Forward != null &&
                   CompareKeys(currentLevels[i].Forward->Key, currentLevels[i].Forward->KeyLength, key) < 0)
            {
                current = currentLevels[i].Forward;
                currentLevels = current->Levels;
            }
        }
        
        SkipListLevel* currentLevels0 = current->Levels;
        current = currentLevels0[0].Forward;
        if (current != null && CompareKeys(current->Key, current->KeyLength, key) == 0)
        {
            value = new ReadOnlySpan<byte>(current->Value, current->ValueLength);
            return true;
        }
        
        value = default;
        return false;
    }
    
    /// <summary>
    /// Gets the rank of a key (0-based).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long GetRank(ReadOnlySpan<byte> key)
    {
        long rank = 0;
        var current = _head;
        
        for (int i = _level - 1; i >= 0; i--)
        {
            SkipListLevel* currentLevels = current->Levels;
            while (currentLevels[i].Forward != null &&
                   CompareKeys(currentLevels[i].Forward->Key, currentLevels[i].Forward->KeyLength, key) <= 0)
            {
                rank += currentLevels[i].Span;
                current = currentLevels[i].Forward;
                currentLevels = current->Levels;
            }
            
            if (current != _head && CompareKeys(current->Key, current->KeyLength, key) == 0)
                return rank - 1;
        }
        
        return -1; // Not found
    }
    
    /// <summary>
    /// Gets element by rank (0-based).
    /// </summary>
    public bool GetByRank(long rank, out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        if (rank < 0 || rank >= _count)
        {
            key = default;
            value = default;
            return false;
        }
        
        long traversed = 0;
        var current = _head;
        
        for (int i = _level - 1; i >= 0; i--)
        {
            SkipListLevel* currentLevels = current->Levels;
            while (currentLevels[i].Forward != null &&
                   traversed + currentLevels[i].Span <= rank + 1)
            {
                traversed += currentLevels[i].Span;
                current = currentLevels[i].Forward;
                currentLevels = current->Levels;
            }
            
            if (traversed == rank + 1)
            {
                key = new ReadOnlySpan<byte>(current->Key, current->KeyLength);
                value = new ReadOnlySpan<byte>(current->Value, current->ValueLength);
                return true;
            }
        }
        
        key = default;
        value = default;
        return false;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int CompareKeys(byte* key1, int len1, ReadOnlySpan<byte> key2)
    {
        int minLen = len1 < key2.Length ? len1 : key2.Length;
        
        fixed (byte* ptr2 = key2)
        {
            for (int i = 0; i < minLen; i++)
            {
                int cmp = key1[i] - ptr2[i];
                if (cmp != 0) return cmp;
            }
        }
        
        return len1 - key2.Length;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CopyKey(SkipListNode* node, ReadOnlySpan<byte> key)
    {
        int len = key.Length < 256 ? key.Length : 255;
        node->KeyLength = (ushort)len;
        
        fixed (byte* src = key)
        {
            SimdHelpers.CopyMemory(src, node->Key, len);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CopyValue(SkipListNode* node, ReadOnlySpan<byte> value)
    {
        int len = value.Length < 512 ? value.Length : 511;
        node->ValueLength = (ushort)len;
        
        fixed (byte* src = value)
        {
            SimdHelpers.CopyMemory(src, node->Value, len);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void UpdateValue(SkipListNode* node, ReadOnlySpan<byte> value)
    {
        CopyValue(node, value);
    }
    
    private void DeleteNode(SkipListNode* node, SkipListNode** update)
    {
        SkipListLevel* nodeLevels = node->Levels;
        for (int i = 0; i < _level; i++)
        {
            SkipListLevel* updateLevels = update[i]->Levels;
            if (updateLevels[i].Forward == node)
            {
                updateLevels[i].Span += nodeLevels[i].Span - 1;
                updateLevels[i].Forward = nodeLevels[i].Forward;
            }
            else
            {
                updateLevels[i].Span--;
            }
        }
        
        if (nodeLevels[0].Forward != null)
            nodeLevels[0].Forward->Backward = node->Backward;
        else
            _tail = node->Backward;
        
        SkipListLevel* headLevels = _head->Levels;
        while (_level > 1 && headLevels[_level - 1].Forward == null)
            _level--;
        
        _count--;
        FreeNode(node);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int RandomLevel()
    {
        int level = 1;
        while (_random.NextDouble() < Probability && level < MaxLevel)
            level++;
        return level;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private SkipListNode* AllocateNode(int level)
    {
        // Allocate node with fixed size (max 32 levels)
        void* ptr = UnsafeMemoryManager.AlignedAlloc((nuint)sizeof(SkipListNode), SimdHelpers.CacheLineSize, zeroInit: true);
        
        var node = (SkipListNode*)ptr;
        node->Level = (byte)level;
        node->KeyLength = 0;
        node->ValueLength = 0;
        node->Backward = null;
        
        return node;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void FreeNode(SkipListNode* node)
    {
        if (node != null)
        {
            UnsafeMemoryManager.AlignedFree(node);
        }
    }
    
    public void Dispose()
    {
        // Free all nodes
        if (_head != null)
        {
            SkipListLevel* headLevels = _head->Levels;
            var current = headLevels[0].Forward;
            while (current != null)
            {
                SkipListLevel* currentLevels = current->Levels;
                var next = currentLevels[0].Forward;
                FreeNode(current);
                current = next;
            }
        }
        
        FreeNode(_head);
        _head = null;
        _tail = null;
        _count = 0;
    }
}

/// <summary>
/// Skip list node structure.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct SkipListNode
{
    public fixed byte Key[256];
    public fixed byte Value[512];
    public ushort KeyLength;
    public ushort ValueLength;
    public byte Level;
    public SkipListNode* Backward;
    public SkipListLevel Levels0;
    public SkipListLevel Levels1;
    public SkipListLevel Levels2;
    public SkipListLevel Levels3;
    public SkipListLevel Levels4;
    public SkipListLevel Levels5;
    public SkipListLevel Levels6;
    public SkipListLevel Levels7;
    public SkipListLevel Levels8;
    public SkipListLevel Levels9;
    public SkipListLevel Levels10;
    public SkipListLevel Levels11;
    public SkipListLevel Levels12;
    public SkipListLevel Levels13;
    public SkipListLevel Levels14;
    public SkipListLevel Levels15;
    public SkipListLevel Levels16;
    public SkipListLevel Levels17;
    public SkipListLevel Levels18;
    public SkipListLevel Levels19;
    public SkipListLevel Levels20;
    public SkipListLevel Levels21;
    public SkipListLevel Levels22;
    public SkipListLevel Levels23;
    public SkipListLevel Levels24;
    public SkipListLevel Levels25;
    public SkipListLevel Levels26;
    public SkipListLevel Levels27;
    public SkipListLevel Levels28;
    public SkipListLevel Levels29;
    public SkipListLevel Levels30;
    public SkipListLevel Levels31;
    
    public SkipListLevel* Levels
    {
        get
        {
            fixed (SkipListLevel* ptr = &Levels0)
            {
                return ptr;
            }
        }
    }
}

/// <summary>
/// Skip list level structure.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct SkipListLevel
{
    public SkipListNode* Forward;
    public int Span;
}
