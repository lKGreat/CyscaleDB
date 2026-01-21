using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using CysRedis.Core.Unsafe.Common;
using CysRedis.Core.Unsafe.DataStructures;

namespace CysRedis.Core.Unsafe.DataStructures;

/// <summary>
/// High-performance Redis list using quicklist structure (ziplist nodes in a doubly-linked list).
/// </summary>
public unsafe sealed class UnsafeRedisList : IDisposable
{
    private const int ZiplistMaxSize = 8192; // Max bytes per ziplist node
    
    private QuickListNode* _head;
    private QuickListNode* _tail;
    private int _count;
    
    /// <summary>
    /// Number of elements in the list.
    /// </summary>
    public int Count => _count;
    
    /// <summary>
    /// Creates a new list.
    /// </summary>
    public UnsafeRedisList()
    {
        _head = null;
        _tail = null;
        _count = 0;
    }
    
    /// <summary>
    /// Pushes an element to the left (beginning).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void PushLeft(ReadOnlySpan<byte> value)
    {
        if (_head == null)
        {
            CreateFirstNode(value);
            return;
        }
        
        // Try to add to head ziplist
        if (_head->Ziplist.TotalBytes + EstimateEntrySize(value) <= ZiplistMaxSize)
        {
            _head->Ziplist.Append(value);
            _head->Count++;
            _count++;
        }
        else
        {
            // Create new node
            var newNode = AllocateNode();
            newNode->Ziplist.Append(value);
            newNode->Count = 1;
            newNode->Next = _head;
            _head->Prev = newNode;
            _head = newNode;
            _count++;
        }
    }
    
    /// <summary>
    /// Pushes an element to the right (end).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void PushRight(ReadOnlySpan<byte> value)
    {
        if (_tail == null)
        {
            CreateFirstNode(value);
            return;
        }
        
        // Try to add to tail ziplist
        if (_tail->Ziplist.TotalBytes + EstimateEntrySize(value) <= ZiplistMaxSize)
        {
            _tail->Ziplist.Append(value);
            _tail->Count++;
            _count++;
        }
        else
        {
            // Create new node
            var newNode = AllocateNode();
            newNode->Ziplist.Append(value);
            newNode->Count = 1;
            newNode->Prev = _tail;
            _tail->Next = newNode;
            _tail = newNode;
            _count++;
        }
    }
    
    /// <summary>
    /// Pops an element from the left.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool PopLeft(out ReadOnlySpan<byte> value)
    {
        if (_head == null || _count == 0)
        {
            value = default;
            return false;
        }
        
        // Get first entry from head ziplist
        if (_head->Count > 0)
        {
            var entry = _head->Ziplist.GetAt(0);
            value = entry.GetBytes();
            // Remove from ziplist (simplified - would need listpack delete)
            _head->Count--;
            _count--;
            
            if (_head->Count == 0)
            {
                // Remove empty node
                var next = _head->Next;
                FreeNode(_head);
                _head = next;
                if (_head != null)
                    _head->Prev = null;
                else
                    _tail = null;
            }
            
            return true;
        }
        
        value = default;
        return false;
    }
    
    /// <summary>
    /// Pops an element from the right.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool PopRight(out ReadOnlySpan<byte> value)
    {
        if (_tail == null || _count == 0)
        {
            value = default;
            return false;
        }
        
        // Get last entry from tail ziplist
        if (_tail->Count > 0)
        {
            var entry = _tail->Ziplist.GetAt(_tail->Count - 1);
            value = entry.GetBytes();
            // Remove from ziplist (simplified)
            _tail->Count--;
            _count--;
            
            if (_tail->Count == 0)
            {
                // Remove empty node
                var prev = _tail->Prev;
                FreeNode(_tail);
                _tail = prev;
                if (_tail != null)
                    _tail->Next = null;
                else
                    _head = null;
            }
            
            return true;
        }
        
        value = default;
        return false;
    }
    
    private void CreateFirstNode(ReadOnlySpan<byte> value)
    {
        var node = AllocateNode();
        node->Ziplist.Append(value);
        node->Count = 1;
        node->Prev = null;
        node->Next = null;
        _head = node;
        _tail = node;
        _count = 1;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int EstimateEntrySize(ReadOnlySpan<byte> value)
    {
        // Rough estimate: encoding overhead + data length
        return value.Length + 10;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private QuickListNode* AllocateNode()
    {
        var node = (QuickListNode*)UnsafeMemoryManager.Alloc((nuint)sizeof(QuickListNode));
        node->Ziplist = new UnsafeListpack(256);
        node->Count = 0;
        node->Prev = null;
        node->Next = null;
        return node;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void FreeNode(QuickListNode* node)
    {
        if (node != null)
        {
            node->Ziplist.Dispose();
            UnsafeMemoryManager.Free(node);
        }
    }
    
    public void Dispose()
    {
        var current = _head;
        while (current != null)
        {
            var next = current->Next;
            FreeNode(current);
            current = next;
        }
        _head = null;
        _tail = null;
        _count = 0;
    }
}

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct QuickListNode
{
    public QuickListNode* Prev;
    public QuickListNode* Next;
    public UnsafeListpack Ziplist;
    public int Count;
    public int Encoding; // 0=RAW, 1=LZF
}
