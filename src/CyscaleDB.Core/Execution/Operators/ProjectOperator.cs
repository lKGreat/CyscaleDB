using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution.Expressions;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Projects (selects) specific columns from the input.
/// </summary>
public sealed class ProjectOperator : OperatorBase
{
    private readonly IOperator _input;
    private readonly List<ProjectionColumn> _projections;
    private readonly TableSchema _outputSchema;

    public override TableSchema Schema => _outputSchema;

    public ProjectOperator(IOperator input, List<ProjectionColumn> projections, string databaseName, string tableName)
    {
        _input = input ?? throw new ArgumentNullException(nameof(input));
        _projections = projections ?? throw new ArgumentNullException(nameof(projections));

        // Build output schema
        var columns = new List<ColumnDefinition>();
        for (int i = 0; i < projections.Count; i++)
        {
            var proj = projections[i];
            var column = new ColumnDefinition(
                proj.OutputName,
                proj.DataType,
                isNullable: true)
            {
                OrdinalPosition = i
            };
            columns.Add(column);
        }

        _outputSchema = new TableSchema(0, databaseName, tableName, columns);
    }

    public override void Open()
    {
        base.Open();
        _input.Open();
    }

    public override Row? Next()
    {
        var inputRow = _input.Next();
        if (inputRow == null)
            return null;

        var values = new DataValue[_projections.Count];
        for (int i = 0; i < _projections.Count; i++)
        {
            values[i] = _projections[i].Expression.Evaluate(inputRow);
        }

        return new Row(_outputSchema, values);
    }

    public override void Close()
    {
        _input.Close();
        base.Close();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _input.Dispose();
        }
        base.Dispose(disposing);
    }
}

/// <summary>
/// Represents a column projection.
/// </summary>
public class ProjectionColumn
{
    /// <summary>
    /// The expression to evaluate for this column.
    /// </summary>
    public IExpressionEvaluator Expression { get; }

    /// <summary>
    /// The output column name (or alias).
    /// </summary>
    public string OutputName { get; }

    /// <summary>
    /// The output data type.
    /// </summary>
    public DataType DataType { get; }

    public ProjectionColumn(IExpressionEvaluator expression, string outputName, DataType dataType)
    {
        Expression = expression;
        OutputName = outputName;
        DataType = dataType;
    }
}
