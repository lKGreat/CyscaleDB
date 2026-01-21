using System.Runtime.InteropServices;

namespace CysRedis.Core.Unsafe.Common;

/// <summary>
/// Safe handle for unmanaged memory to prevent leaks.
/// </summary>
public sealed unsafe class UnmanagedMemoryHandle : SafeHandle
{
    private readonly nuint _size;

    /// <summary>
    /// Creates a new unmanaged memory handle.
    /// </summary>
    public UnmanagedMemoryHandle(nuint size, bool zeroInit = false) : base(IntPtr.Zero, true)
    {
        _size = size;
        if (size == 0)
        {
            SetHandle(IntPtr.Zero);
            return;
        }

        void* ptr = NativeMemory.Alloc(size);
        if (zeroInit)
        {
            NativeMemory.Clear(ptr, size);
        }

        SetHandle((IntPtr)ptr);
    }

    /// <summary>
    /// Gets the pointer to the unmanaged memory.
    /// </summary>
    public void* Pointer => handle.ToPointer();

    /// <summary>
    /// Gets the size of the allocated memory.
    /// </summary>
    public nuint Size => _size;

    /// <summary>
    /// Gets whether the handle is invalid.
    /// </summary>
    public override bool IsInvalid => handle == IntPtr.Zero;

    /// <summary>
    /// Releases the unmanaged memory.
    /// </summary>
    protected override bool ReleaseHandle()
    {
        if (handle != IntPtr.Zero)
        {
            NativeMemory.Free(handle.ToPointer());
            SetHandle(IntPtr.Zero);
        }
        return true;
    }

    /// <summary>
    /// Creates a handle from an existing pointer (for wrapping).
    /// </summary>
    public static UnmanagedMemoryHandle FromPointer(void* ptr, nuint size, bool ownsHandle = true)
    {
        var handle = new UnmanagedMemoryHandle(0);
        handle.SetHandle((IntPtr)ptr);
        return handle;
    }
}

/// <summary>
/// Safe handle for aligned unmanaged memory.
/// </summary>
public sealed unsafe class AlignedMemoryHandle : SafeHandle
{
    private readonly nuint _size;
    private readonly nuint _alignment;

    /// <summary>
    /// Creates a new aligned memory handle.
    /// </summary>
    public AlignedMemoryHandle(nuint size, nuint alignment, bool zeroInit = false) : base(IntPtr.Zero, true)
    {
        _size = size;
        _alignment = alignment;
        if (size == 0)
        {
            SetHandle(IntPtr.Zero);
            return;
        }

        void* ptr = NativeMemory.AlignedAlloc(size, alignment);
        if (zeroInit)
        {
            NativeMemory.Clear(ptr, size);
        }

        SetHandle((IntPtr)ptr);
    }

    /// <summary>
    /// Gets the pointer to the aligned memory.
    /// </summary>
    public void* Pointer => handle.ToPointer();

    /// <summary>
    /// Gets the size of the allocated memory.
    /// </summary>
    public nuint Size => _size;

    /// <summary>
    /// Gets the alignment of the memory.
    /// </summary>
    public nuint Alignment => _alignment;

    /// <summary>
    /// Gets whether the handle is invalid.
    /// </summary>
    public override bool IsInvalid => handle == IntPtr.Zero;

    /// <summary>
    /// Releases the aligned memory.
    /// </summary>
    protected override bool ReleaseHandle()
    {
        if (handle != IntPtr.Zero)
        {
            NativeMemory.AlignedFree(handle.ToPointer());
            SetHandle(IntPtr.Zero);
        }
        return true;
    }
}
