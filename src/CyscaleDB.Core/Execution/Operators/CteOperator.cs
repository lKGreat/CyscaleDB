using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Operator that reads from a materialized CTE (Common Table Expression) result.
/// CTEs are pre-computed and stored as ResultSets, this operator iterates over those results.
/// </summary>
public sealed class CteOperator : IOperator
{
    private readonly ResultSet _resultSet;
    private readonly string _alias;
    private readonly TableSchema _schema;
    private int _currentRow;
    private bool _isOpen;

    /// <summary>
    /// Gets the schema of the CTE result.
    /// </summary>
    public TableSchema Schema => _schema;

    /// <summary>
    /// Gets the alias for this CTE reference.
    /// </summary>
    public string Alias => _alias;

    /// <summary>
    /// Creates a new CTE operator that reads from a materialized CTE result.
    /// </summary>
    /// <param name="resultSet">The materialized CTE result set</param>
    /// <param name="alias">The alias for this CTE reference</param>
    public CteOperator(ResultSet resultSet, string alias)
    {
        _resultSet = resultSet ?? throw new ArgumentNullException(nameof(resultSet));
        _alias = alias ?? throw new ArgumentNullException(nameof(alias));
        
        // Build schema from result set columns
        var columns = resultSet.Columns.Select((col, i) => 
            new ColumnDefinition(col.Name, col.DataType, 255))
            .ToList();
        
        _schema = new TableSchema(0, "cyscaledb", alias, columns);
        _currentRow = -1;
    }

    /// <inheritdoc/>
    public void Open()
    {
        if (_isOpen)
            throw new InvalidOperationException("Operator is already open");

        _currentRow = -1;
        _isOpen = true;
    }

    /// <inheritdoc/>
    public Row? Next()
    {
        if (!_isOpen)
            throw new InvalidOperationException("Operator is not open");

        _currentRow++;
        
        if (_currentRow >= _resultSet.RowCount)
            return null;

        // Convert result set row to Row object
        var values = _resultSet.Rows[_currentRow];
        return new Row(_schema, values);
    }

    /// <inheritdoc/>
    public void Close()
    {
        _currentRow = -1;
        _isOpen = false;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Close();
    }
}
