using System.IO.Compression;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Transactions;

/// <summary>
/// Handles archiving (compression) and cleanup of old WAL files.
/// </summary>
public sealed class WalArchiver : IDisposable
{
    private readonly string _dataDirectory;
    private readonly WalLog _walLog;
    private readonly Logger _logger;
    private Timer? _archiveTimer;
    private bool _disposed;

    /// <summary>
    /// Creates a new WalArchiver.
    /// </summary>
    public WalArchiver(string dataDirectory, WalLog walLog)
    {
        _dataDirectory = dataDirectory ?? throw new ArgumentNullException(nameof(dataDirectory));
        _walLog = walLog ?? throw new ArgumentNullException(nameof(walLog));
        _logger = LogManager.Default.GetLogger<WalArchiver>();

        // Subscribe to rotation events
        _walLog.LogRotated += OnLogRotated;
    }

    /// <summary>
    /// Starts periodic archiving.
    /// </summary>
    public void StartPeriodicArchiving(TimeSpan interval)
    {
        _archiveTimer = new Timer(
            _ => ArchiveOldLogs(),
            null,
            interval,
            interval);

        _logger.Info("Started periodic WAL archiving every {0}", interval);
    }

    /// <summary>
    /// Stops periodic archiving.
    /// </summary>
    public void StopPeriodicArchiving()
    {
        _archiveTimer?.Dispose();
        _archiveTimer = null;
    }

    /// <summary>
    /// Archives (compresses) old log files.
    /// </summary>
    public void ArchiveOldLogs()
    {
        try
        {
            var rotatedFiles = _walLog.GetRotatedLogFiles().ToList();

            // Keep the most recent files uncompressed, compress older ones
            foreach (var file in rotatedFiles.OrderBy(f => f).SkipLast(Constants.MaxWalFiles))
            {
                if (!file.EndsWith(".gz") && File.Exists(file))
                {
                    CompressFile(file);
                }
            }

            _logger.Debug("Archived old WAL logs");
        }
        catch (Exception ex)
        {
            _logger.Error("Error archiving WAL logs: {0}", ex.Message);
        }
    }

    /// <summary>
    /// Purges archived logs older than the retention period.
    /// </summary>
    public void PurgeOldArchives(int retentionDays = Constants.WalArchiveRetentionDays)
    {
        try
        {
            var cutoffDate = DateTime.UtcNow.AddDays(-retentionDays);
            var pattern = $"cyscaledb{Constants.WalFileExtension}.*";
            var directory = _dataDirectory;

            if (!Directory.Exists(directory))
                return;

            foreach (var file in Directory.GetFiles(directory, pattern))
            {
                var fileInfo = new FileInfo(file);
                if (fileInfo.LastWriteTimeUtc < cutoffDate)
                {
                    File.Delete(file);
                    _logger.Info("Purged old archive: {0}", file);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.Error("Error purging old archives: {0}", ex.Message);
        }
    }

    /// <summary>
    /// Compresses a file using gzip.
    /// </summary>
    public void CompressFile(string filePath)
    {
        var compressedPath = filePath + ".gz";

        try
        {
            using (var originalStream = File.OpenRead(filePath))
            using (var compressedStream = File.Create(compressedPath))
            using (var gzipStream = new GZipStream(compressedStream, CompressionLevel.Optimal))
            {
                originalStream.CopyTo(gzipStream);
            }

            // Delete original file after successful compression
            File.Delete(filePath);

            _logger.Debug("Compressed WAL file: {0} -> {1}", filePath, compressedPath);
        }
        catch (Exception ex)
        {
            _logger.Error("Error compressing file {0}: {1}", filePath, ex.Message);
            
            // Clean up partial compressed file if it exists
            if (File.Exists(compressedPath))
            {
                try { File.Delete(compressedPath); } catch { }
            }
        }
    }

    /// <summary>
    /// Decompresses an archived file.
    /// </summary>
    public string? DecompressFile(string compressedPath)
    {
        if (!compressedPath.EndsWith(".gz"))
            return compressedPath;

        var decompressedPath = compressedPath[..^3]; // Remove .gz

        try
        {
            using (var compressedStream = File.OpenRead(compressedPath))
            using (var gzipStream = new GZipStream(compressedStream, CompressionMode.Decompress))
            using (var decompressedStream = File.Create(decompressedPath))
            {
                gzipStream.CopyTo(decompressedStream);
            }

            _logger.Debug("Decompressed WAL file: {0} -> {1}", compressedPath, decompressedPath);
            return decompressedPath;
        }
        catch (Exception ex)
        {
            _logger.Error("Error decompressing file {0}: {1}", compressedPath, ex.Message);
            return null;
        }
    }

    /// <summary>
    /// Gets all archived (compressed) log files.
    /// </summary>
    public IEnumerable<string> GetArchivedFiles()
    {
        var pattern = $"cyscaledb{Constants.WalFileExtension}.*.gz";
        var directory = _dataDirectory;

        if (!Directory.Exists(directory))
            yield break;

        foreach (var file in Directory.GetFiles(directory, pattern))
        {
            yield return file;
        }
    }

    /// <summary>
    /// Restores archives up to the given LSN for recovery.
    /// </summary>
    public List<string> RestoreArchivesForRecovery(long upToLsn)
    {
        var restoredFiles = new List<string>();
        var archives = GetArchivedFiles().OrderBy(f => f).ToList();

        foreach (var archive in archives)
        {
            var decompressed = DecompressFile(archive);
            if (decompressed != null)
            {
                restoredFiles.Add(decompressed);
            }
        }

        return restoredFiles;
    }

    private void OnLogRotated(object? sender, WalRotatedEventArgs e)
    {
        // Optionally trigger archiving when log is rotated
        Task.Run(() => ArchiveOldLogs());
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        StopPeriodicArchiving();
        _walLog.LogRotated -= OnLogRotated;
    }
}
