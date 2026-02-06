using System.Collections.Concurrent;

namespace CyscaleDB.Core.Common;

/// <summary>
/// Global memory budget manager that tracks and limits memory usage across all query operators.
/// Prevents OOM by enforcing per-session and per-operator memory quotas.
/// When an operator exceeds its budget, it should switch to spill-to-disk mode.
/// </summary>
public sealed class MemoryBudgetManager
{
    private long _totalBudgetBytes;
    private long _allocatedBytes;
    private readonly ConcurrentDictionary<string, long> _operatorAllocations = new();
    private readonly object _lock = new();

    /// <summary>
    /// Default sort buffer size (256 MB).
    /// </summary>
    public const long DefaultSortBufferSize = 256 * 1024 * 1024;

    /// <summary>
    /// Default join buffer size (256 MB).
    /// </summary>
    public const long DefaultJoinBufferSize = 256 * 1024 * 1024;

    /// <summary>
    /// Default total execution memory budget (2 GB).
    /// </summary>
    public const long DefaultTotalBudget = 2L * 1024 * 1024 * 1024;

    /// <summary>
    /// Gets the total memory budget in bytes.
    /// </summary>
    public long TotalBudgetBytes => _totalBudgetBytes;

    /// <summary>
    /// Gets the currently allocated bytes across all operators.
    /// </summary>
    public long AllocatedBytes => Interlocked.Read(ref _allocatedBytes);

    /// <summary>
    /// Gets the remaining available bytes.
    /// </summary>
    public long AvailableBytes => _totalBudgetBytes - AllocatedBytes;

    /// <summary>
    /// Creates a new MemoryBudgetManager with the specified total budget.
    /// </summary>
    /// <param name="totalBudgetBytes">Total memory budget in bytes. Defaults to 2 GB.</param>
    public MemoryBudgetManager(long totalBudgetBytes = DefaultTotalBudget)
    {
        _totalBudgetBytes = totalBudgetBytes;
    }

    /// <summary>
    /// Tries to allocate memory for an operator. Returns true if the allocation succeeds.
    /// If the global budget is exceeded, returns false and the operator should spill to disk.
    /// </summary>
    /// <param name="operatorId">Unique identifier for the operator requesting memory.</param>
    /// <param name="bytes">Number of bytes to allocate.</param>
    /// <returns>True if allocation succeeded, false if budget exceeded.</returns>
    public bool TryAllocate(string operatorId, long bytes)
    {
        if (bytes <= 0) return true;

        lock (_lock)
        {
            if (_allocatedBytes + bytes > _totalBudgetBytes)
                return false;

            _allocatedBytes += bytes;
            _operatorAllocations.AddOrUpdate(operatorId, bytes, (_, existing) => existing + bytes);
            return true;
        }
    }

    /// <summary>
    /// Releases memory previously allocated by an operator.
    /// </summary>
    /// <param name="operatorId">Unique identifier for the operator releasing memory.</param>
    /// <param name="bytes">Number of bytes to release.</param>
    public void Release(string operatorId, long bytes)
    {
        if (bytes <= 0) return;

        lock (_lock)
        {
            _allocatedBytes = Math.Max(0, _allocatedBytes - bytes);

            if (_operatorAllocations.TryGetValue(operatorId, out var current))
            {
                var newValue = Math.Max(0, current - bytes);
                if (newValue == 0)
                    _operatorAllocations.TryRemove(operatorId, out _);
                else
                    _operatorAllocations[operatorId] = newValue;
            }
        }
    }

    /// <summary>
    /// Releases all memory allocated by an operator.
    /// </summary>
    /// <param name="operatorId">Unique identifier for the operator.</param>
    public void ReleaseAll(string operatorId)
    {
        lock (_lock)
        {
            if (_operatorAllocations.TryRemove(operatorId, out var allocated))
            {
                _allocatedBytes = Math.Max(0, _allocatedBytes - allocated);
            }
        }
    }

    /// <summary>
    /// Gets the amount of memory allocated by a specific operator.
    /// </summary>
    public long GetOperatorAllocation(string operatorId)
    {
        return _operatorAllocations.TryGetValue(operatorId, out var value) ? value : 0;
    }

    /// <summary>
    /// Updates the total budget (e.g., in response to SET GLOBAL max_execution_memory).
    /// </summary>
    public void SetTotalBudget(long newBudgetBytes)
    {
        Interlocked.Exchange(ref _totalBudgetBytes, Math.Max(0, newBudgetBytes));
    }

    /// <summary>
    /// Gets the singleton instance. Used as default for operators that don't have a session-specific manager.
    /// </summary>
    public static MemoryBudgetManager Default { get; } = new();
}
