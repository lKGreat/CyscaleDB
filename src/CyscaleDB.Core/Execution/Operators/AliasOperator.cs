using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Wraps an operator and provides an aliased schema.
/// Used for subqueries with aliases in FROM clauses.
/// </summary>
public sealed class AliasOperator : OperatorBase
{
    private readonly IOperator _input;
    private readonly TableSchema _aliasedSchema;

    public override TableSchema Schema => _aliasedSchema;

    public AliasOperator(IOperator input, string alias)
    {
        _input = input ?? throw new ArgumentNullException(nameof(input));
        if (string.IsNullOrEmpty(alias))
            throw new ArgumentNullException(nameof(alias));

        // Create new schema with the alias as the table name
        var inputSchema = input.Schema;
        var columns = inputSchema.Columns.Select((col, idx) => new ColumnDefinition(
            col.Name,
            col.DataType,
            col.MaxLength,
            col.Precision,
            col.Scale,
            col.IsNullable,
            col.IsPrimaryKey,
            col.IsAutoIncrement,
            col.DefaultValue)
        {
            OrdinalPosition = idx
        }).ToList();

        _aliasedSchema = new TableSchema(
            inputSchema.TableId,
            inputSchema.DatabaseName,
            alias,
            columns
        );
    }

    public override void Open()
    {
        base.Open();
        _input.Open();
    }

    public override Row? Next()
    {
        var row = _input.Next();
        if (row == null)
            return null;

        // Return row with aliased schema
        return new Row(_aliasedSchema, row.Values);
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
