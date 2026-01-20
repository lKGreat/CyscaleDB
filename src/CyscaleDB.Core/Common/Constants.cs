namespace CyscaleDB.Core.Common;

/// <summary>
/// Global constants used throughout CyscaleDB.
/// </summary>
public static class Constants
{
    /// <summary>
    /// The default page size in bytes (4KB).
    /// </summary>
    public const int PageSize = 4096;

    /// <summary>
    /// The size of a page header in bytes.
    /// </summary>
    public const int PageHeaderSize = 16;

    /// <summary>
    /// The size of a slot entry in the slot directory.
    /// Each slot is 4 bytes: 2 bytes offset + 2 bytes length.
    /// </summary>
    public const int SlotSize = 4;

    /// <summary>
    /// The default buffer pool size (number of pages).
    /// </summary>
    public const int DefaultBufferPoolSize = 1024;

    /// <summary>
    /// The maximum length of a table name.
    /// </summary>
    public const int MaxTableNameLength = 64;

    /// <summary>
    /// The maximum length of a column name.
    /// </summary>
    public const int MaxColumnNameLength = 64;

    /// <summary>
    /// The maximum length of a database name.
    /// </summary>
    public const int MaxDatabaseNameLength = 64;

    /// <summary>
    /// The default VARCHAR length when not specified.
    /// </summary>
    public const int DefaultVarCharLength = 255;

    /// <summary>
    /// The maximum VARCHAR length.
    /// </summary>
    public const int MaxVarCharLength = 65535;

    /// <summary>
    /// The default server port (MySQL-compatible).
    /// </summary>
    public const int DefaultPort = 3306;

    /// <summary>
    /// The file extension for data files.
    /// </summary>
    public const string DataFileExtension = ".cdb";

    /// <summary>
    /// The file extension for WAL files.
    /// </summary>
    public const string WalFileExtension = ".wal";

    /// <summary>
    /// The name of the catalog metadata file.
    /// </summary>
    public const string CatalogFileName = "catalog.meta";

    /// <summary>
    /// The name of the system database.
    /// </summary>
    public const string SystemDatabaseName = "cyscale_system";

    /// <summary>
    /// The default database name.
    /// </summary>
    public const string DefaultDatabaseName = "default";

    /// <summary>
    /// Server version string for MySQL protocol.
    /// </summary>
    public const string ServerVersion = "8.0.0-CyscaleDB";

    /// <summary>
    /// Maximum number of columns per table.
    /// </summary>
    public const int MaxColumnsPerTable = 1024;

    /// <summary>
    /// Maximum row size in bytes.
    /// </summary>
    public const int MaxRowSize = PageSize - PageHeaderSize - SlotSize;

    #region Index Constants

    /// <summary>
    /// The file extension for B-Tree index files.
    /// </summary>
    public const string IndexFileExtension = ".idx";

    /// <summary>
    /// The file extension for Hash index files.
    /// </summary>
    public const string HashIndexExtension = ".hash";

    /// <summary>
    /// Maximum number of keys per B-Tree internal node.
    /// </summary>
    public const int BTreeOrder = 128;

    /// <summary>
    /// Maximum number of keys per B-Tree leaf node.
    /// </summary>
    public const int BTreeLeafCapacity = 256;

    /// <summary>
    /// Maximum length of an index name.
    /// </summary>
    public const int MaxIndexNameLength = 64;

    /// <summary>
    /// Initial directory size for extendible hash index.
    /// </summary>
    public const int HashInitialDirectorySize = 4;

    /// <summary>
    /// Maximum bucket size for hash index.
    /// </summary>
    public const int HashBucketCapacity = 64;

    #endregion

    #region WAL and Checkpoint Constants

    /// <summary>
    /// The name of the checkpoint metadata file.
    /// </summary>
    public const string CheckpointFileName = "checkpoint.meta";

    /// <summary>
    /// Maximum WAL file size before rotation (16MB).
    /// </summary>
    public const long MaxWalFileSize = 16 * 1024 * 1024;

    /// <summary>
    /// Maximum number of WAL files to keep before archiving.
    /// </summary>
    public const int MaxWalFiles = 10;

    /// <summary>
    /// Checkpoint interval in seconds (5 minutes).
    /// </summary>
    public const int CheckpointIntervalSeconds = 300;

    /// <summary>
    /// Default retention days for archived WAL files.
    /// </summary>
    public const int WalArchiveRetentionDays = 7;

    #endregion

    #region View Constants

    /// <summary>
    /// Maximum length of a view name.
    /// </summary>
    public const int MaxViewNameLength = 64;

    /// <summary>
    /// Maximum length of view definition SQL.
    /// </summary>
    public const int MaxViewDefinitionLength = 65535;

    #endregion
}
