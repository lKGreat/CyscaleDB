namespace CysRedis.Core.Common;

/// <summary>
/// Global constants for CysRedis.
/// </summary>
public static class Constants
{
    /// <summary>
    /// Server version string.
    /// </summary>
    public const string ServerVersion = "1.0.0";

    /// <summary>
    /// Redis protocol version.
    /// </summary>
    public const string RedisVersion = "7.2.0";

    /// <summary>
    /// Default server port.
    /// </summary>
    public const int DefaultPort = 6379;

    /// <summary>
    /// Default number of databases.
    /// </summary>
    public const int DefaultDatabaseCount = 16;

    /// <summary>
    /// RESP protocol line ending.
    /// </summary>
    public const string CRLF = "\r\n";

    /// <summary>
    /// Default buffer size for network I/O.
    /// </summary>
    public const int DefaultBufferSize = 16 * 1024;

    /// <summary>
    /// Maximum inline command length.
    /// </summary>
    public const int MaxInlineLength = 64 * 1024;

    /// <summary>
    /// Maximum bulk string length.
    /// </summary>
    public const int MaxBulkLength = 512 * 1024 * 1024;

    /// <summary>
    /// Maximum number of arguments in a command.
    /// </summary>
    public const int MaxArguments = 1024 * 1024;
}
