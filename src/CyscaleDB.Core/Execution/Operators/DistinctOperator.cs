using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// DISTINCT operator that removes duplicate rows.
/// </summary>
public sealed class DistinctOperator : OperatorBase
{
    private readonly IOperator _input;
    private readonly HashSet<string> _seenRows;

    public override TableSchema Schema => _input.Schema;

    public DistinctOperator(IOperator input)
    {
        _input = input ?? throw new ArgumentNullException(nameof(input));
        _seenRows = new HashSet<string>();
    }

    public override void Open()
    {
        base.Open();
        _input.Open();
        _seenRows.Clear();
    }

    public override Row? Next()
    {
        while (true)
        {
            var row = _input.Next();
            if (row == null)
                return null;

            // Create a hash key from all values
            var key = CreateRowKey(row);
            if (_seenRows.Add(key))
            {
                return row;
            }
            // Row is duplicate, skip it
        }
    }

    public override void Close()
    {
        _input.Close();
        _seenRows.Clear();
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

    private static string CreateRowKey(Row row)
    {
        var parts = new string[row.Values.Length];
        for (int i = 0; i < row.Values.Length; i++)
        {
            var val = row.Values[i];
            parts[i] = val.IsNull ? "\0NULL\0" : ValueToString(val);
        }
        return string.Join("\x1F", parts); // Unit separator
    }

    private static string ValueToString(DataValue val)
    {
        return val.Type switch
        {
            DataType.Int => val.AsInt().ToString(),
            DataType.BigInt => val.AsBigInt().ToString(),
            DataType.SmallInt => val.AsSmallInt().ToString(),
            DataType.TinyInt => val.AsTinyInt().ToString(),
            DataType.Float => val.AsFloat().ToString("G9"),
            DataType.Double => val.AsDouble().ToString("G17"),
            DataType.Decimal => val.AsDecimal().ToString("G"),
            DataType.VarChar or DataType.Char or DataType.Text => val.AsString(),
            DataType.DateTime => val.AsDateTime().ToString("O"),
            DataType.Date => val.AsDate().ToString("yyyy-MM-dd"),
            DataType.Time => val.AsTime().ToString(@"hh\:mm\:ss"),
            DataType.Boolean => val.AsBoolean() ? "T" : "F",
            _ => val.GetRawValue()?.ToString() ?? ""
        };
    }
}
