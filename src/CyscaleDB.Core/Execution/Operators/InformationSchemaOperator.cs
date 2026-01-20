using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Operator that reads from an information_schema virtual table.
/// Returns pre-computed rows from the InformationSchemaProvider.
/// </summary>
public sealed class InformationSchemaOperator : OperatorBase
{
    private readonly TableSchema _schema;
    private readonly List<DataValue[]> _rows;
    private int _currentRowIndex;
    private readonly string? _alias;

    public override TableSchema Schema => _schema;

    public InformationSchemaOperator(TableSchema schema, List<DataValue[]> rows, string? alias = null)
    {
        _rows = rows ?? throw new ArgumentNullException(nameof(rows));
        _alias = alias;
        
        // Create schema with alias if provided
        if (!string.IsNullOrEmpty(alias))
        {
            _schema = new TableSchema(
                schema.TableId,
                schema.DatabaseName,
                alias,
                schema.Columns.ToList()
            );
        }
        else
        {
            _schema = schema;
        }
    }

    public override void Open()
    {
        base.Open();
        _currentRowIndex = 0;
    }

    public override Row? Next()
    {
        if (_currentRowIndex >= _rows.Count)
            return null;

        var values = _rows[_currentRowIndex++];
        return new Row(_schema, values);
    }

    public override void Close()
    {
        _currentRowIndex = 0;
        base.Close();
    }
}
