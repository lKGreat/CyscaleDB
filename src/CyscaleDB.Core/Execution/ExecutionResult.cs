using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution;

/// <summary>
/// Represents the result of executing a SQL statement.
/// </summary>
public class ExecutionResult
{
    /// <summary>
    /// The type of result.
    /// </summary>
    public ResultType Type { get; }

    /// <summary>
    /// The affected row count (for INSERT, UPDATE, DELETE).
    /// </summary>
    public long AffectedRows { get; }

    /// <summary>
    /// The last inserted auto-increment ID.
    /// </summary>
    public long LastInsertId { get; }

    /// <summary>
    /// The result set (for SELECT queries).
    /// </summary>
    public ResultSet? ResultSet { get; }

    /// <summary>
    /// A message (for utility statements).
    /// </summary>
    public string? Message { get; }

    /// <summary>
    /// Creates a result for a successful query.
    /// </summary>
    public static ExecutionResult Query(ResultSet resultSet)
    {
        return new ExecutionResult(ResultType.Query, 0, 0, resultSet, null);
    }

    /// <summary>
    /// Creates a result for a modification statement.
    /// </summary>
    public static ExecutionResult Modification(long affectedRows, long lastInsertId = 0)
    {
        return new ExecutionResult(ResultType.Modification, affectedRows, lastInsertId, null, null);
    }

    /// <summary>
    /// Creates a result for a DDL statement.
    /// </summary>
    public static ExecutionResult Ddl(string message)
    {
        return new ExecutionResult(ResultType.Ddl, 0, 0, null, message);
    }

    /// <summary>
    /// Creates an empty result.
    /// </summary>
    public static ExecutionResult Empty()
    {
        return new ExecutionResult(ResultType.Empty, 0, 0, null, null);
    }

    private ExecutionResult(ResultType type, long affectedRows, long lastInsertId, ResultSet? resultSet, string? message)
    {
        Type = type;
        AffectedRows = affectedRows;
        LastInsertId = lastInsertId;
        ResultSet = resultSet;
        Message = message;
    }
}

/// <summary>
/// Types of execution results.
/// </summary>
public enum ResultType
{
    /// <summary>
    /// A query result set.
    /// </summary>
    Query,

    /// <summary>
    /// A modification (INSERT, UPDATE, DELETE) result.
    /// </summary>
    Modification,

    /// <summary>
    /// A DDL statement result.
    /// </summary>
    Ddl,

    /// <summary>
    /// An empty result.
    /// </summary>
    Empty
}

/// <summary>
/// Represents a result set from a query.
/// </summary>
public class ResultSet
{
    /// <summary>
    /// The column definitions.
    /// </summary>
    public List<ResultColumn> Columns { get; } = [];

    /// <summary>
    /// The rows in the result set.
    /// </summary>
    public List<DataValue[]> Rows { get; } = [];

    /// <summary>
    /// The number of columns.
    /// </summary>
    public int ColumnCount => Columns.Count;

    /// <summary>
    /// The number of rows.
    /// </summary>
    public int RowCount => Rows.Count;

    /// <summary>
    /// Creates a result set from an operator.
    /// </summary>
    public static ResultSet FromOperator(IOperator op)
    {
        var result = new ResultSet();

        // Add columns
        foreach (var col in op.Schema.Columns)
        {
            result.Columns.Add(new ResultColumn
            {
                Name = col.Name,
                DataType = col.DataType
            });
        }

        // Add rows
        op.Open();
        try
        {
            Row? row;
            while ((row = op.Next()) != null)
            {
                var values = new DataValue[row.Values.Length];
                Array.Copy(row.Values, values, row.Values.Length);
                result.Rows.Add(values);
            }
        }
        finally
        {
            op.Close();
        }

        return result;
    }

    /// <summary>
    /// Creates a result set from a table schema (with empty rows).
    /// </summary>
    public static ResultSet FromSchema(TableSchema schema)
    {
        var result = new ResultSet();

        foreach (var col in schema.Columns)
        {
            result.Columns.Add(new ResultColumn
            {
                Name = col.Name,
                DataType = col.DataType,
                TableName = schema.TableName,
                DatabaseName = schema.DatabaseName
            });
        }

        return result;
    }
}

/// <summary>
/// Represents a column in a result set.
/// </summary>
public class ResultColumn
{
    /// <summary>
    /// The column name.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// The data type.
    /// </summary>
    public DataType DataType { get; set; }

    /// <summary>
    /// The table name (optional).
    /// </summary>
    public string? TableName { get; set; }

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }
}
