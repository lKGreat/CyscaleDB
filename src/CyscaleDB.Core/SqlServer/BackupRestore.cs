using System.IO.Compression;
using System.Text;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.SqlServer;

/// <summary>
/// Implements SQL Server-compatible backup and restore operations.
/// Supports BACKUP DATABASE ... TO DISK = '...' and RESTORE DATABASE ... FROM DISK = '...'
///
/// Backup format (.bak):
///   - Header: Magic + Version + Metadata (JSON)
///   - Data: Compressed archive of database files
///   - Tail: Checksum
///
/// This enables SSMS backup/restore wizard functionality.
/// </summary>
public sealed class BackupRestore
{
    private readonly Catalog _catalog;
    private readonly Logger _logger;

    private const string BackupMagic = "CYSCALE_BAK";
    private const int BackupVersion = 1;

    public BackupRestore(Catalog catalog)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _logger = LogManager.Default.GetLogger<BackupRestore>();
    }

    /// <summary>
    /// Performs a full database backup to a .bak file.
    /// BACKUP DATABASE [dbname] TO DISK = 'path'
    /// </summary>
    public BackupResult BackupDatabase(string databaseName, string backupPath, string? description = null)
    {
        var db = _catalog.GetDatabase(databaseName);
        if (db == null)
            throw new CyscaleException($"Database '{databaseName}' not found.");

        var startTime = DateTime.UtcNow;
        long totalBytes = 0;
        int fileCount = 0;

        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(backupPath) ?? ".");

            using var fileStream = new FileStream(backupPath, FileMode.Create, FileAccess.Write);
            using var writer = new BinaryWriter(fileStream);

            // Write header
            writer.Write(Encoding.ASCII.GetBytes(BackupMagic));
            writer.Write(BackupVersion);
            writer.Write(databaseName);
            writer.Write(DateTime.UtcNow.Ticks);
            writer.Write(description ?? $"Full backup of {databaseName}");

            // Collect table schemas as metadata
            var tableNames = new List<string>();
            foreach (var table in db.Tables)
            {
                tableNames.Add(table.TableName);
            }
            writer.Write(tableNames.Count);
            foreach (var name in tableNames)
            {
                writer.Write(name);
            }

            // Write database data directory contents as compressed archive
            var dbDir = db.DataDirectory;
            if (Directory.Exists(dbDir))
            {
                var files = Directory.GetFiles(dbDir, "*", SearchOption.AllDirectories);
                writer.Write(files.Length);

                foreach (var file in files)
                {
                    var relativePath = Path.GetRelativePath(dbDir, file);
                    var fileData = File.ReadAllBytes(file);

                    // Compress each file
                    byte[] compressed;
                    using (var ms = new MemoryStream())
                    {
                        using (var gzip = new GZipStream(ms, CompressionLevel.Fastest))
                        {
                            gzip.Write(fileData);
                        }
                        compressed = ms.ToArray();
                    }

                    writer.Write(relativePath);
                    writer.Write(fileData.Length);      // Original size
                    writer.Write(compressed.Length);     // Compressed size
                    writer.Write(compressed);

                    totalBytes += fileData.Length;
                    fileCount++;
                }
            }
            else
            {
                writer.Write(0); // No files
            }

            // Write checksum
            writer.Write(ComputeChecksum(totalBytes, fileCount));

            _logger.Info("Backup completed: {0} files, {1} bytes → {2}",
                fileCount, totalBytes, backupPath);

            return new BackupResult
            {
                Success = true,
                DatabaseName = databaseName,
                BackupPath = backupPath,
                FileCount = fileCount,
                TotalBytes = totalBytes,
                Duration = DateTime.UtcNow - startTime,
                Message = $"Database '{databaseName}' backed up successfully to '{backupPath}'."
            };
        }
        catch (Exception ex)
        {
            _logger.Error("Backup failed: {0}", ex.Message);
            return new BackupResult
            {
                Success = false,
                DatabaseName = databaseName,
                BackupPath = backupPath,
                Message = $"Backup failed: {ex.Message}"
            };
        }
    }

    /// <summary>
    /// Restores a database from a .bak file.
    /// RESTORE DATABASE [dbname] FROM DISK = 'path'
    /// </summary>
    public RestoreResult RestoreDatabase(string databaseName, string backupPath, string? targetDirectory = null)
    {
        if (!File.Exists(backupPath))
            throw new CyscaleException($"Backup file not found: '{backupPath}'");

        var startTime = DateTime.UtcNow;
        int fileCount = 0;
        long totalBytes = 0;

        try
        {
            using var fileStream = new FileStream(backupPath, FileMode.Open, FileAccess.Read);
            using var reader = new BinaryReader(fileStream);

            // Read and verify header
            var magic = Encoding.ASCII.GetString(reader.ReadBytes(BackupMagic.Length));
            if (magic != BackupMagic)
                throw new CyscaleException("Invalid backup file format.");

            var version = reader.ReadInt32();
            if (version > BackupVersion)
                throw new CyscaleException($"Unsupported backup version: {version}");

            var originalDbName = reader.ReadString();
            var backupTime = new DateTime(reader.ReadInt64());
            var description = reader.ReadString();

            // Read table metadata
            var tableCount = reader.ReadInt32();
            var tableNames = new List<string>();
            for (int i = 0; i < tableCount; i++)
            {
                tableNames.Add(reader.ReadString());
            }

            // Create or get database
            var db = _catalog.GetDatabase(databaseName);
            if (db == null)
            {
                db = _catalog.CreateDatabase(databaseName);
            }

            var restoreDir = targetDirectory ?? db.DataDirectory;
            Directory.CreateDirectory(restoreDir);

            // Read and restore files
            var numFiles = reader.ReadInt32();
            for (int i = 0; i < numFiles; i++)
            {
                var relativePath = reader.ReadString();
                var originalSize = reader.ReadInt32();
                var compressedSize = reader.ReadInt32();
                var compressedData = reader.ReadBytes(compressedSize);

                // Decompress
                byte[] decompressed;
                using (var ms = new MemoryStream(compressedData))
                using (var gzip = new GZipStream(ms, CompressionMode.Decompress))
                using (var output = new MemoryStream())
                {
                    gzip.CopyTo(output);
                    decompressed = output.ToArray();
                }

                var targetPath = Path.Combine(restoreDir, relativePath);
                Directory.CreateDirectory(Path.GetDirectoryName(targetPath)!);
                File.WriteAllBytes(targetPath, decompressed);

                fileCount++;
                totalBytes += decompressed.Length;
            }

            _logger.Info("Restore completed: {0} files, {1} bytes from {2}",
                fileCount, totalBytes, backupPath);

            return new RestoreResult
            {
                Success = true,
                DatabaseName = databaseName,
                OriginalDatabaseName = originalDbName,
                BackupPath = backupPath,
                FileCount = fileCount,
                TotalBytes = totalBytes,
                BackupTime = backupTime,
                Description = description,
                Duration = DateTime.UtcNow - startTime,
                Message = $"Database '{databaseName}' restored successfully from '{backupPath}'."
            };
        }
        catch (Exception ex)
        {
            _logger.Error("Restore failed: {0}", ex.Message);
            return new RestoreResult
            {
                Success = false,
                DatabaseName = databaseName,
                BackupPath = backupPath,
                Message = $"Restore failed: {ex.Message}"
            };
        }
    }

    /// <summary>
    /// Lists backup information from a .bak file without restoring.
    /// RESTORE HEADERONLY FROM DISK = 'path'
    /// </summary>
    public BackupInfo? GetBackupInfo(string backupPath)
    {
        if (!File.Exists(backupPath)) return null;

        using var fileStream = new FileStream(backupPath, FileMode.Open, FileAccess.Read);
        using var reader = new BinaryReader(fileStream);

        var magic = Encoding.ASCII.GetString(reader.ReadBytes(BackupMagic.Length));
        if (magic != BackupMagic) return null;

        var version = reader.ReadInt32();
        var databaseName = reader.ReadString();
        var backupTime = new DateTime(reader.ReadInt64());
        var description = reader.ReadString();

        return new BackupInfo
        {
            DatabaseName = databaseName,
            BackupTime = backupTime,
            Description = description,
            Version = version,
            FileSizeBytes = fileStream.Length
        };
    }

    private static int ComputeChecksum(long totalBytes, int fileCount)
    {
        return HashCode.Combine(totalBytes, fileCount, BackupMagic);
    }
}

/// <summary>
/// Result of a backup operation.
/// </summary>
public class BackupResult
{
    public bool Success { get; set; }
    public string DatabaseName { get; set; } = "";
    public string BackupPath { get; set; } = "";
    public int FileCount { get; set; }
    public long TotalBytes { get; set; }
    public TimeSpan Duration { get; set; }
    public string Message { get; set; } = "";
}

/// <summary>
/// Result of a restore operation.
/// </summary>
public class RestoreResult
{
    public bool Success { get; set; }
    public string DatabaseName { get; set; } = "";
    public string OriginalDatabaseName { get; set; } = "";
    public string BackupPath { get; set; } = "";
    public int FileCount { get; set; }
    public long TotalBytes { get; set; }
    public DateTime BackupTime { get; set; }
    public string Description { get; set; } = "";
    public TimeSpan Duration { get; set; }
    public string Message { get; set; } = "";
}

/// <summary>
/// Metadata from a backup file header.
/// </summary>
public class BackupInfo
{
    public string DatabaseName { get; set; } = "";
    public DateTime BackupTime { get; set; }
    public string Description { get; set; } = "";
    public int Version { get; set; }
    public long FileSizeBytes { get; set; }
}
