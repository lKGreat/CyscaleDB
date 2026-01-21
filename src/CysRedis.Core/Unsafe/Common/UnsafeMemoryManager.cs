using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using CysRedis.Core.Unsafe.Common;

namespace CysRedis.Core.Unsafe.Common;

/// <summary>
/// Unified memory manager for all unmanaged memory allocations.
/// Provides memory pooling, statistics, and leak detection.
/// </summary>
public static unsafe class UnsafeMemoryManager
{
    private static readonly ConcurrentDictionary<IntPtr, AllocationInfo> _allocations = new();
    private static long _totalAllocated;
    private static long _totalFreed;
    private static int _allocationCount;
    private static bool _leakDetectionEnabled;

    /// <summary>
    /// Allocation information for tracking.
    /// </summary>
    public struct AllocationInfo
    {
        public nuint Size;
        public string? StackTrace;
        public DateTime Timestamp;
    }

    /// <summary>
    /// Enables or disables leak detection (debug mode only).
    /// </summary>
    public static bool LeakDetectionEnabled
    {
        get => _leakDetectionEnabled;
        set => _leakDetectionEnabled = value;
    }

    /// <summary>
    /// Gets total allocated memory in bytes.
    /// </summary>
    public static long TotalAllocated => _totalAllocated;

    /// <summary>
    /// Gets total freed memory in bytes.
    /// </summary>
    public static long TotalFreed => _totalFreed;

    /// <summary>
    /// Gets current allocation count.
    /// </summary>
    public static int AllocationCount => _allocationCount;

    /// <summary>
    /// Gets current memory usage (allocated - freed).
    /// </summary>
    public static long CurrentUsage => _totalAllocated - _totalFreed;

    /// <summary>
    /// Allocates unmanaged memory with tracking.
    /// </summary>
    public static void* Alloc(nuint size, bool zeroInit = false)
    {
        void* ptr = zeroInit ? NativeMemory.AllocZeroed(size) : NativeMemory.Alloc(size);
        
        Interlocked.Add(ref _totalAllocated, (long)size);
        Interlocked.Increment(ref _allocationCount);

        if (_leakDetectionEnabled)
        {
            _allocations.TryAdd((IntPtr)ptr, new AllocationInfo
            {
                Size = size,
                StackTrace = Environment.StackTrace,
                Timestamp = DateTime.UtcNow
            });
        }

        return ptr;
    }

    /// <summary>
    /// Allocates aligned unmanaged memory with tracking.
    /// </summary>
    public static void* AlignedAlloc(nuint size, nuint alignment, bool zeroInit = false)
    {
        void* ptr = NativeMemory.AlignedAlloc(size, alignment);
        if (zeroInit)
        {
            NativeMemory.Clear(ptr, size);
        }
        
        Interlocked.Add(ref _totalAllocated, (long)size);
        Interlocked.Increment(ref _allocationCount);

        if (_leakDetectionEnabled)
        {
            _allocations.TryAdd((IntPtr)ptr, new AllocationInfo
            {
                Size = size,
                StackTrace = Environment.StackTrace,
                Timestamp = DateTime.UtcNow
            });
        }

        return ptr;
    }

    /// <summary>
    /// Frees unmanaged memory with tracking.
    /// </summary>
    public static void Free(void* ptr)
    {
        if (ptr == null) return;

        nuint size = 0;
        if (_leakDetectionEnabled && _allocations.TryRemove((IntPtr)ptr, out var info))
        {
            size = info.Size;
        }

        NativeMemory.Free(ptr);
        
        if (size > 0)
        {
            Interlocked.Add(ref _totalFreed, (long)size);
            Interlocked.Decrement(ref _allocationCount);
        }
    }

    /// <summary>
    /// Frees aligned unmanaged memory with tracking.
    /// </summary>
    public static void AlignedFree(void* ptr)
    {
        if (ptr == null) return;

        nuint size = 0;
        if (_leakDetectionEnabled && _allocations.TryRemove((IntPtr)ptr, out var info))
        {
            size = info.Size;
        }

        NativeMemory.AlignedFree(ptr);
        
        if (size > 0)
        {
            Interlocked.Add(ref _totalFreed, (long)size);
            Interlocked.Decrement(ref _allocationCount);
        }
    }

    /// <summary>
    /// Reallocates memory (allocates new, copies, frees old).
    /// </summary>
    public static void* Realloc(void* ptr, nuint newSize, nuint oldSize)
    {
        void* newPtr = NativeMemory.Alloc(newSize);
        
        if (ptr != null && oldSize > 0)
        {
            nuint copySize = oldSize < newSize ? oldSize : newSize;
            Buffer.MemoryCopy(ptr, newPtr, newSize, copySize);
            Free(ptr);
        }

        Interlocked.Add(ref _totalAllocated, (long)newSize);
        Interlocked.Add(ref _totalFreed, (long)oldSize);

        if (_leakDetectionEnabled && newPtr != null)
        {
            _allocations.TryAdd((IntPtr)newPtr, new AllocationInfo
            {
                Size = newSize,
                StackTrace = Environment.StackTrace,
                Timestamp = DateTime.UtcNow
            });
        }

        return newPtr;
    }

    /// <summary>
    /// Gets all current allocations (for leak detection).
    /// </summary>
    public static Dictionary<IntPtr, AllocationInfo> GetAllocations()
    {
        return new Dictionary<IntPtr, AllocationInfo>(_allocations);
    }

    /// <summary>
    /// Clears all tracking information.
    /// </summary>
    public static void ClearTracking()
    {
        _allocations.Clear();
        Interlocked.Exchange(ref _totalAllocated, 0);
        Interlocked.Exchange(ref _totalFreed, 0);
        Interlocked.Exchange(ref _allocationCount, 0);
    }

    /// <summary>
    /// Validates that all memory has been freed (debug only).
    /// </summary>
    public static void ValidateNoLeaks()
    {
        if (!_leakDetectionEnabled) return;

        if (_allocations.Count > 0)
        {
            var leaks = string.Join("\n", _allocations.Select(kvp => 
                $"Leak: {kvp.Value.Size} bytes at {kvp.Key:X16}\n{kvp.Value.StackTrace}"));
            
            throw new InvalidOperationException($"Memory leaks detected:\n{leaks}");
        }
    }
}
