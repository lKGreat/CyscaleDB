namespace CyscaleDB.Core.Common;

/// <summary>
/// Storage engine configuration options.
/// Provides comprehensive configuration for I/O, caching, and flush strategies.
/// </summary>
public class StorageOptions
{
    /// <summary>
    /// Root directory for data files.
    /// </summary>
    public string DataDirectory { get; set; } = "./data";

    #region Buffer Pool Configuration

    /// <summary>
    /// Number of pages to cache in the buffer pool.
    /// </summary>
    public int BufferPoolSizePages { get; set; } = Constants.DefaultBufferPoolSize;

    /// <summary>
    /// Percentage of buffer pool reserved for old region (0-100).
    /// Similar to InnoDB's innodb_old_blocks_pct.
    /// </summary>
    public int OldBlockPercentage { get; set; } = 37;

    /// <summary>
    /// Time in milliseconds a page must stay in old region before promotion.
    /// Similar to InnoDB's innodb_old_blocks_time.
    /// </summary>
    public int OldBlockTimeMs { get; set; } = 1000;

    #endregion

    #region I/O Configuration

    /// <summary>
    /// Use O_DIRECT for bypassing OS page cache (Linux only).
    /// Can improve performance for database workloads.
    /// </summary>
    public bool UseDirectIo { get; set; } = false;

    /// <summary>
    /// Number of pages to read ahead during sequential access.
    /// Set to 0 to disable read-ahead.
    /// </summary>
    public int ReadAheadPages { get; set; } = 8;

    /// <summary>
    /// Enable asynchronous I/O operations.
    /// </summary>
    public bool UseAsyncIo { get; set; } = true;

    /// <summary>
    /// Number of I/O threads for background operations.
    /// </summary>
    public int IoThreads { get; set; } = 4;

    #endregion

    #region Flush Configuration

    /// <summary>
    /// Flush mode for ensuring data durability.
    /// </summary>
    public FlushMode FlushMode { get; set; } = FlushMode.FSync;

    /// <summary>
    /// Interval in milliseconds for periodic flush operations.
    /// </summary>
    public int FlushIntervalMs { get; set; } = 1000;

    /// <summary>
    /// Maximum number of dirty pages before triggering a flush.
    /// </summary>
    public int MaxDirtyPages { get; set; } = 100;

    /// <summary>
    /// Percentage of dirty pages that triggers background flushing.
    /// </summary>
    public int DirtyPageFlushThreshold { get; set; } = 75;

    #endregion

    #region Doublewrite Configuration

    /// <summary>
    /// Enable doublewrite buffer for crash safety.
    /// </summary>
    public bool EnableDoublewrite { get; set; } = true;

    /// <summary>
    /// Size of doublewrite buffer in pages.
    /// </summary>
    public int DoublewriteBufferSize { get; set; } = 128;

    #endregion

    /// <summary>
    /// Gets the default storage options.
    /// </summary>
    public static StorageOptions Default => new();

    /// <summary>
    /// Gets options optimized for SSD storage.
    /// </summary>
    public static StorageOptions Ssd => new()
    {
        UseDirectIo = true,
        ReadAheadPages = 4,  // SSDs have lower sequential advantage
        IoThreads = 8,       // SSDs handle parallel I/O better
        FlushMode = FlushMode.FDataSync,
        FlushIntervalMs = 500
    };

    /// <summary>
    /// Gets options optimized for HDD storage.
    /// </summary>
    public static StorageOptions Hdd => new()
    {
        UseDirectIo = false,
        ReadAheadPages = 16,  // HDDs benefit more from sequential access
        IoThreads = 2,        // Limited by seek time
        FlushMode = FlushMode.FSync,
        FlushIntervalMs = 2000
    };

    /// <summary>
    /// Gets options for maximum durability.
    /// </summary>
    public static StorageOptions Durable => new()
    {
        FlushMode = FlushMode.FSync,
        FlushIntervalMs = 100,
        EnableDoublewrite = true,
        MaxDirtyPages = 50
    };
}

/// <summary>
/// Flush mode for data durability.
/// </summary>
public enum FlushMode
{
    /// <summary>
    /// No explicit flush - relies on OS to flush when needed.
    /// Fastest but least durable.
    /// </summary>
    None,

    /// <summary>
    /// Use fsync() to flush data and metadata.
    /// Most durable option.
    /// </summary>
    FSync,

    /// <summary>
    /// Use fdatasync() to flush data only (not metadata).
    /// Good balance of performance and durability.
    /// </summary>
    FDataSync
}
