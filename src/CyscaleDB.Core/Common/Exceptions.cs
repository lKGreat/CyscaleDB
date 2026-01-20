namespace CyscaleDB.Core.Common;

/// <summary>
/// Base exception for all CyscaleDB errors.
/// </summary>
public class CyscaleException : Exception
{
    /// <summary>
    /// The error code associated with this exception.
    /// </summary>
    public ErrorCode ErrorCode { get; }

    public CyscaleException(string message, ErrorCode errorCode = ErrorCode.Unknown)
        : base(message)
    {
        ErrorCode = errorCode;
    }

    public CyscaleException(string message, Exception innerException, ErrorCode errorCode = ErrorCode.Unknown)
        : base(message, innerException)
    {
        ErrorCode = errorCode;
    }
}

/// <summary>
/// Exception thrown for SQL syntax errors.
/// </summary>
public class SqlSyntaxException : CyscaleException
{
    /// <summary>
    /// The position in the SQL string where the error occurred.
    /// </summary>
    public int Position { get; }

    /// <summary>
    /// The line number where the error occurred.
    /// </summary>
    public int Line { get; }

    /// <summary>
    /// The column number where the error occurred.
    /// </summary>
    public int Column { get; }

    public SqlSyntaxException(string message, int position = -1, int line = -1, int column = -1)
        : base(message, ErrorCode.SyntaxError)
    {
        Position = position;
        Line = line;
        Column = column;
    }
}

/// <summary>
/// Exception thrown when a table is not found.
/// </summary>
public class TableNotFoundException : CyscaleException
{
    public string TableName { get; }

    public TableNotFoundException(string tableName)
        : base($"Table '{tableName}' does not exist", ErrorCode.TableNotFound)
    {
        TableName = tableName;
    }
}

/// <summary>
/// Exception thrown when a table already exists.
/// </summary>
public class TableExistsException : CyscaleException
{
    public string TableName { get; }

    public TableExistsException(string tableName)
        : base($"Table '{tableName}' already exists", ErrorCode.TableExists)
    {
        TableName = tableName;
    }
}

/// <summary>
/// Exception thrown when a database is not found.
/// </summary>
public class DatabaseNotFoundException : CyscaleException
{
    public string DatabaseName { get; }

    public DatabaseNotFoundException(string databaseName)
        : base($"Database '{databaseName}' does not exist", ErrorCode.DatabaseNotFound)
    {
        DatabaseName = databaseName;
    }
}

/// <summary>
/// Exception thrown when a database already exists.
/// </summary>
public class DatabaseExistsException : CyscaleException
{
    public string DatabaseName { get; }

    public DatabaseExistsException(string databaseName)
        : base($"Database '{databaseName}' already exists", ErrorCode.DatabaseExists)
    {
        DatabaseName = databaseName;
    }
}

/// <summary>
/// Exception thrown when a column is not found.
/// </summary>
public class ColumnNotFoundException : CyscaleException
{
    public string ColumnName { get; }
    public string? TableName { get; }

    public ColumnNotFoundException(string columnName, string? tableName = null)
        : base(tableName != null
            ? $"Column '{columnName}' not found in table '{tableName}'"
            : $"Column '{columnName}' not found",
            ErrorCode.ColumnNotFound)
    {
        ColumnName = columnName;
        TableName = tableName;
    }
}

/// <summary>
/// Exception thrown when a view is not found.
/// </summary>
public class ViewNotFoundException : CyscaleException
{
    public string ViewName { get; }

    public ViewNotFoundException(string viewName)
        : base($"View '{viewName}' does not exist", ErrorCode.ViewNotFound)
    {
        ViewName = viewName;
    }
}

/// <summary>
/// Exception thrown when a view already exists.
/// </summary>
public class ViewExistsException : CyscaleException
{
    public string ViewName { get; }

    public ViewExistsException(string viewName)
        : base($"View '{viewName}' already exists", ErrorCode.ViewExists)
    {
        ViewName = viewName;
    }
}

/// <summary>
/// Exception thrown when an index is not found.
/// </summary>
public class IndexNotFoundException : CyscaleException
{
    public string IndexName { get; }

    public IndexNotFoundException(string indexName)
        : base($"Index '{indexName}' does not exist", ErrorCode.IndexNotFound)
    {
        IndexName = indexName;
    }
}

/// <summary>
/// Exception thrown when an index already exists.
/// </summary>
public class IndexExistsException : CyscaleException
{
    public string IndexName { get; }

    public IndexExistsException(string indexName)
        : base($"Index '{indexName}' already exists", ErrorCode.IndexExists)
    {
        IndexName = indexName;
    }
}

/// <summary>
/// Exception thrown for data type conversion errors.
/// </summary>
public class TypeConversionException : CyscaleException
{
    public DataType SourceType { get; }
    public DataType TargetType { get; }

    public TypeConversionException(DataType sourceType, DataType targetType)
        : base($"Cannot convert from {sourceType} to {targetType}", ErrorCode.TypeMismatch)
    {
        SourceType = sourceType;
        TargetType = targetType;
    }
}

/// <summary>
/// Exception thrown for constraint violations.
/// </summary>
public class ConstraintViolationException : CyscaleException
{
    public string ConstraintName { get; }

    public ConstraintViolationException(string message, string constraintName = "")
        : base(message, ErrorCode.ConstraintViolation)
    {
        ConstraintName = constraintName;
    }
}

/// <summary>
/// Exception thrown for transaction errors.
/// </summary>
public class TransactionException : CyscaleException
{
    public long TransactionId { get; }

    public TransactionException(string message, long transactionId = 0, ErrorCode errorCode = ErrorCode.TransactionError)
        : base(message, errorCode)
    {
        TransactionId = transactionId;
    }
}

/// <summary>
/// Exception thrown when a deadlock is detected.
/// </summary>
public class DeadlockException : TransactionException
{
    public DeadlockException(long transactionId)
        : base($"Deadlock detected for transaction {transactionId}", transactionId, ErrorCode.Deadlock)
    {
    }
}

/// <summary>
/// Exception thrown when a lock timeout occurs.
/// </summary>
public class LockTimeoutException : TransactionException
{
    public LockTimeoutException(long transactionId, string resourceName)
        : base($"Lock timeout on resource '{resourceName}' for transaction {transactionId}",
            transactionId, ErrorCode.LockTimeout)
    {
    }
}

/// <summary>
/// Exception thrown for storage-related errors.
/// </summary>
public class StorageException : CyscaleException
{
    public StorageException(string message, ErrorCode errorCode = ErrorCode.StorageError)
        : base(message, errorCode)
    {
    }

    public StorageException(string message, Exception innerException)
        : base(message, innerException, ErrorCode.StorageError)
    {
    }
}

/// <summary>
/// Exception thrown when a page is corrupted.
/// </summary>
public class PageCorruptedException : StorageException
{
    public int PageId { get; }

    public PageCorruptedException(int pageId)
        : base($"Page {pageId} is corrupted", ErrorCode.PageCorrupted)
    {
        PageId = pageId;
    }
}

/// <summary>
/// Error codes for CyscaleDB exceptions.
/// These are compatible with MySQL error code ranges.
/// </summary>
public enum ErrorCode
{
    // General errors (1000-1099)
    Unknown = 1000,
    InternalError = 1001,
    OutOfMemory = 1002,
    
    // Syntax errors (1100-1199)
    SyntaxError = 1100,
    ParseError = 1101,
    InvalidToken = 1102,
    
    // Database/Table errors (1200-1299)
    DatabaseNotFound = 1200,
    DatabaseExists = 1201,
    TableNotFound = 1202,
    TableExists = 1203,
    ColumnNotFound = 1204,
    ViewNotFound = 1205,
    ViewExists = 1206,
    IndexNotFound = 1207,
    IndexExists = 1208,
    
    // Data errors (1300-1399)
    TypeMismatch = 1300,
    DataTruncated = 1301,
    NullConstraint = 1302,
    ConstraintViolation = 1303,
    DuplicateKey = 1304,
    
    // Transaction errors (1400-1499)
    TransactionError = 1400,
    Deadlock = 1401,
    LockTimeout = 1402,
    TransactionNotStarted = 1403,
    TransactionAlreadyStarted = 1404,
    
    // Storage errors (1500-1599)
    StorageError = 1500,
    PageCorrupted = 1501,
    FileNotFound = 1502,
    DiskFull = 1503,
    
    // Protocol errors (1600-1699)
    ProtocolError = 1600,
    AuthenticationFailed = 1601,
    ConnectionClosed = 1602,
}
