using System.Text;
using CysRedis.Core.Unsafe.DataStructures;

namespace CysRedis.Core.DataStructures.Adapters;

/// <summary>
/// Adapter that wraps UnsafeRedisList to implement RedisList interface.
/// </summary>
public sealed class UnsafeRedisListAdapter : RedisList, IDisposable
{
    private readonly UnsafeRedisList _unsafeList;

    /// <summary>
    /// Creates a new adapter wrapping an unsafe list.
    /// </summary>
    public UnsafeRedisListAdapter()
    {
        _unsafeList = new UnsafeRedisList();
    }

    /// <summary>
    /// Number of elements in the list.
    /// </summary>
    public new int Count => _unsafeList.Count;

    /// <summary>
    /// Pushes an element to the left (beginning).
    /// </summary>
    public new void PushLeft(byte[] value)
    {
        _unsafeList.PushLeft(value);
    }

    /// <summary>
    /// Pushes an element to the right (end).
    /// </summary>
    public new void PushRight(byte[] value)
    {
        _unsafeList.PushRight(value);
    }

    /// <summary>
    /// Pops an element from the left (beginning).
    /// </summary>
    public new byte[]? PopLeft()
    {
        if (_unsafeList.PopLeft(out var value))
        {
            return value.ToArray();
        }
        return null;
    }

    /// <summary>
    /// Pops an element from the right (end).
    /// </summary>
    public new byte[]? PopRight()
    {
        if (_unsafeList.PopRight(out var value))
        {
            return value.ToArray();
        }
        return null;
    }

    /// <summary>
    /// Gets an element by index.
    /// </summary>
    public new byte[]? GetByIndex(int index)
    {
        // Note: UnsafeRedisList doesn't expose GetByIndex
        // This would need to be implemented by iterating
        return null;
    }

    /// <summary>
    /// Sets an element by index.
    /// </summary>
    public new bool SetByIndex(int index, byte[] value)
    {
        // Note: UnsafeRedisList doesn't expose SetByIndex
        // This would need to be implemented
        return false;
    }

    /// <summary>
    /// Gets a range of elements.
    /// </summary>
    public new List<byte[]> GetRange(int start, int stop)
    {
        // Note: UnsafeRedisList doesn't expose GetRange
        // This would need to be implemented
        return new List<byte[]>();
    }

    /// <summary>
    /// Trims the list to the specified range.
    /// </summary>
    public new void Trim(int start, int stop)
    {
        // Note: UnsafeRedisList doesn't expose Trim
        // This would need to be implemented
    }

    /// <summary>
    /// Disposes the adapter and underlying unsafe list.
    /// </summary>
    public new void Dispose()
    {
        _unsafeList?.Dispose();
    }
}
