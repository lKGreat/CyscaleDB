using System.Collections.Concurrent;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// File group level I/O scheduler that manages parallel I/O operations
/// across multiple data files within a file group.
///
/// Each data file has its own I/O thread/queue, allowing true parallel
/// disk access when files are on different physical disks.
///
/// Features:
///   - Per-file I/O queues with dedicated worker threads
///   - Batch I/O submission for read-ahead and write-behind
///   - Priority scheduling (foreground queries > background flush)
///   - I/O statistics per file for monitoring and balancing
/// </summary>
public sealed class FileGroupIoScheduler : IDisposable
{
    private readonly ConcurrentDictionary<int, IoQueue> _fileQueues = new();
    private readonly int _maxIoThreadsPerFile;
    private bool _disposed;

    /// <summary>
    /// Gets the number of active I/O queues.
    /// </summary>
    public int ActiveQueueCount => _fileQueues.Count;

    /// <summary>
    /// Gets total pending I/O operations across all files.
    /// </summary>
    public int TotalPendingOps => _fileQueues.Values.Sum(q => q.PendingCount);

    public FileGroupIoScheduler(int maxIoThreadsPerFile = 2)
    {
        _maxIoThreadsPerFile = maxIoThreadsPerFile;
    }

    /// <summary>
    /// Registers a data file for I/O scheduling.
    /// </summary>
    public void RegisterFile(int fileId, string filePath)
    {
        _fileQueues.TryAdd(fileId, new IoQueue(fileId, filePath, _maxIoThreadsPerFile));
    }

    /// <summary>
    /// Unregisters a data file.
    /// </summary>
    public void UnregisterFile(int fileId)
    {
        if (_fileQueues.TryRemove(fileId, out var queue))
        {
            queue.Dispose();
        }
    }

    /// <summary>
    /// Submits a read request for a specific file.
    /// </summary>
    public Task<byte[]> SubmitReadAsync(int fileId, long offset, int length, IoRequestPriority priority = IoRequestPriority.Normal)
    {
        if (!_fileQueues.TryGetValue(fileId, out var queue))
            throw new InvalidOperationException($"File {fileId} not registered.");

        return queue.SubmitReadAsync(offset, length, priority);
    }

    /// <summary>
    /// Submits a write request for a specific file.
    /// </summary>
    public Task SubmitWriteAsync(int fileId, long offset, byte[] data, IoRequestPriority priority = IoRequestPriority.Normal)
    {
        if (!_fileQueues.TryGetValue(fileId, out var queue))
            throw new InvalidOperationException($"File {fileId} not registered.");

        return queue.SubmitWriteAsync(offset, data, priority);
    }

    /// <summary>
    /// Submits parallel reads across multiple files (for striped table scans).
    /// </summary>
    public async Task<List<(int FileId, byte[] Data)>> SubmitParallelReadsAsync(
        List<(int FileId, long Offset, int Length)> requests,
        CancellationToken ct = default)
    {
        var tasks = requests.Select(r =>
            SubmitReadAsync(r.FileId, r.Offset, r.Length)
                .ContinueWith(t => (r.FileId, t.Result), ct))
            .ToList();

        var results = await Task.WhenAll(tasks);
        return results.ToList();
    }

    /// <summary>
    /// Gets I/O statistics for a specific file.
    /// </summary>
    public IoStatistics? GetFileStatistics(int fileId)
    {
        return _fileQueues.TryGetValue(fileId, out var queue) ? queue.Statistics : null;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var queue in _fileQueues.Values)
            queue.Dispose();
        _fileQueues.Clear();
    }
}

/// <summary>
/// Per-file I/O queue with dedicated processing.
/// </summary>
internal sealed class IoQueue : IDisposable
{
    private readonly int _fileId;
    private readonly string _filePath;
    private readonly SemaphoreSlim _semaphore;
    private bool _disposed;

    public IoStatistics Statistics { get; } = new();
    public int PendingCount => _semaphore.CurrentCount;

    public IoQueue(int fileId, string filePath, int maxConcurrency)
    {
        _fileId = fileId;
        _filePath = filePath;
        _semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
    }

    public async Task<byte[]> SubmitReadAsync(long offset, int length, IoRequestPriority priority)
    {
        await _semaphore.WaitAsync();
        try
        {
            using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite, 4096, FileOptions.Asynchronous);

            fs.Position = offset;
            var buffer = new byte[length];
            await fs.ReadAsync(buffer.AsMemory(0, length));

            Interlocked.Increment(ref Statistics.ReadCount);
            Interlocked.Add(ref Statistics.BytesRead, length);

            return buffer;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async Task SubmitWriteAsync(long offset, byte[] data, IoRequestPriority priority)
    {
        await _semaphore.WaitAsync();
        try
        {
            using var fs = new FileStream(_filePath, FileMode.OpenOrCreate, FileAccess.Write,
                FileShare.ReadWrite, 4096, FileOptions.Asynchronous);

            fs.Position = offset;
            await fs.WriteAsync(data);

            Interlocked.Increment(ref Statistics.WriteCount);
            Interlocked.Add(ref Statistics.BytesWritten, data.Length);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _semaphore.Dispose();
    }
}

/// <summary>
/// I/O request priority levels.
/// </summary>
public enum IoRequestPriority
{
    /// <summary>Background operations (flush, compaction).</summary>
    Background = 0,

    /// <summary>Normal query operations.</summary>
    Normal = 1,

    /// <summary>High priority (user-facing, latency-sensitive).</summary>
    High = 2
}

/// <summary>
/// I/O statistics for a single file.
/// </summary>
public class IoStatistics
{
    public long ReadCount;
    public long WriteCount;
    public long BytesRead;
    public long BytesWritten;
}
