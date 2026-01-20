using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// DUAL operator that produces a single row with no columns.
/// Used for SELECT without FROM (e.g., SELECT 1, SELECT NOW()).
/// </summary>
public sealed class DualOperator : OperatorBase
{
    private readonly TableSchema _schema;
    private bool _emitted;

    public override TableSchema Schema => _schema;

    public DualOperator(string databaseName = "")
    {
        _schema = new TableSchema(0, databaseName, "DUAL", new List<ColumnDefinition>());
    }

    public override void Open()
    {
        base.Open();
        _emitted = false;
    }

    public override Row? Next()
    {
        if (_emitted)
            return null;

        _emitted = true;
        return new Row(_schema, Array.Empty<DataValue>());
    }

    public override void Close()
    {
        _emitted = false;
        base.Close();
    }
}
