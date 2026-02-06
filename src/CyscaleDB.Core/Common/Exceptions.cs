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

    /// <summary>
    /// The SQL STATE code (5-char ANSI standard, e.g. "42S02").
    /// </summary>
    public string SqlState { get; }

    public CyscaleException(string message, ErrorCode errorCode = ErrorCode.Unknown)
        : base(message)
    {
        ErrorCode = errorCode;
        SqlState = MySqlErrorMapper.GetSqlState(errorCode);
    }

    public CyscaleException(string message, Exception innerException, ErrorCode errorCode = ErrorCode.Unknown)
        : base(message, innerException)
    {
        ErrorCode = errorCode;
        SqlState = MySqlErrorMapper.GetSqlState(errorCode);
    }
}

/// <summary>
/// Maps CyscaleDB error codes to standard MySQL error codes and SQL STATE values.
/// </summary>
public static class MySqlErrorMapper
{
    /// <summary>
    /// Gets the standard MySQL error code for a CyscaleDB ErrorCode.
    /// </summary>
    public static int GetMySqlErrorCode(ErrorCode errorCode)
    {
        return errorCode switch
        {
            ErrorCode.SyntaxError or ErrorCode.ParseError or ErrorCode.InvalidToken => 1064,
            ErrorCode.DatabaseNotFound => 1049,
            ErrorCode.DatabaseExists => 1007,
            ErrorCode.TableNotFound => 1146,
            ErrorCode.TableExists => 1050,
            ErrorCode.ColumnNotFound => 1054,
            ErrorCode.ViewNotFound => 1146,
            ErrorCode.ViewExists => 1050,
            ErrorCode.IndexNotFound => 1091,
            ErrorCode.IndexExists => 1061,
            ErrorCode.ProcedureNotFound => 1305,
            ErrorCode.ProcedureExists => 1304,
            ErrorCode.TriggerNotFound => 1360,
            ErrorCode.TriggerExists => 1359,
            ErrorCode.EventNotFound => 1539,
            ErrorCode.EventExists => 1537,
            ErrorCode.UserNotFound => 1396,
            ErrorCode.AccessDenied => 1045,
            ErrorCode.UserAlreadyExists => 1396,
            ErrorCode.TypeMismatch => 1366,
            ErrorCode.DataTruncated => 1265,
            ErrorCode.NullConstraint => 1048,
            ErrorCode.ConstraintViolation => 1451,
            ErrorCode.DuplicateKey => 1062,
            ErrorCode.TransactionError => 1399,
            ErrorCode.Deadlock => 1213,
            ErrorCode.LockTimeout => 1205,
            ErrorCode.TransactionNotStarted => 1399,
            ErrorCode.TransactionAlreadyStarted => 1399,
            ErrorCode.StorageError => 1030,
            ErrorCode.PageCorrupted => 1030,
            ErrorCode.AuthenticationFailed => 1045,
            _ => 1105 // HY000 Unknown error
        };
    }

    /// <summary>
    /// Gets the SQL STATE for a CyscaleDB ErrorCode.
    /// </summary>
    public static string GetSqlState(ErrorCode errorCode)
    {
        return errorCode switch
        {
            ErrorCode.SyntaxError or ErrorCode.ParseError or ErrorCode.InvalidToken => "42000",
            ErrorCode.DatabaseNotFound => "42000",
            ErrorCode.DatabaseExists => "HY000",
            ErrorCode.TableNotFound => "42S02",
            ErrorCode.TableExists => "42S01",
            ErrorCode.ColumnNotFound => "42S22",
            ErrorCode.ViewNotFound => "42S02",
            ErrorCode.ViewExists => "42S01",
            ErrorCode.IndexNotFound => "42000",
            ErrorCode.IndexExists => "42000",
            ErrorCode.ProcedureNotFound => "42000",
            ErrorCode.ProcedureExists => "42000",
            ErrorCode.UserNotFound => "HY000",
            ErrorCode.AccessDenied => "28000",
            ErrorCode.UserAlreadyExists => "HY000",
            ErrorCode.TypeMismatch => "HY000",
            ErrorCode.DataTruncated => "01000",
            ErrorCode.NullConstraint => "23000",
            ErrorCode.ConstraintViolation => "23000",
            ErrorCode.DuplicateKey => "23000",
            ErrorCode.Deadlock => "40001",
            ErrorCode.LockTimeout => "HY000",
            ErrorCode.TransactionError or ErrorCode.TransactionNotStarted or ErrorCode.TransactionAlreadyStarted => "HY000",
            ErrorCode.AuthenticationFailed => "28000",
            _ => "HY000"
        };
    }

    /// <summary>
    /// Gets both MySQL error code and SQL STATE for an exception.
    /// </summary>
    public static (int mysqlErrorCode, string sqlState) Map(Exception ex)
    {
        if (ex is CyscaleException cex)
        {
            return (GetMySqlErrorCode(cex.ErrorCode), cex.SqlState);
        }
        if (ex is SqlSyntaxException)
        {
            return (1064, "42000");
        }
        // Generic fallback
        return (1105, "HY000");
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
    public string? DatabaseName { get; }

    public TableNotFoundException(string tableName)
        : base($"Table '{tableName}' does not exist", ErrorCode.TableNotFound)
    {
        TableName = tableName;
    }

    public TableNotFoundException(string tableName, string databaseName)
        : base($"Table '{databaseName}.{tableName}' does not exist", ErrorCode.TableNotFound)
    {
        TableName = tableName;
        DatabaseName = databaseName;
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
    /// <summary>
    /// The database name where the lock was requested.
    /// </summary>
    public string? DatabaseName { get; }

    /// <summary>
    /// The table name where the lock was requested.
    /// </summary>
    public string? TableName { get; }

    public LockTimeoutException(long transactionId, string resourceName)
        : base($"Lock timeout on resource '{resourceName}' for transaction {transactionId}",
            transactionId, ErrorCode.LockTimeout)
    {
    }

    public LockTimeoutException(string message, string databaseName, string tableName)
        : base(message, 0, ErrorCode.LockTimeout)
    {
        DatabaseName = databaseName;
        TableName = tableName;
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
    ProcedureNotFound = 1209,
    ProcedureExists = 1210,
    TriggerNotFound = 1211,
    TriggerExists = 1212,
    EventNotFound = 1213,
    EventExists = 1214,
    
    // Auth errors (1250-1299)
    UserNotFound = 1250,
    AccessDenied = 1251,
    UserAlreadyExists = 1252,

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
