using CyscaleDB.Core.Execution.Expressions;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Filters rows based on a predicate.
/// </summary>
public sealed class FilterOperator : OperatorBase
{
    private readonly IOperator _input;
    private readonly IExpressionEvaluator _predicate;

    public override TableSchema Schema => _input.Schema;

    public FilterOperator(IOperator input, IExpressionEvaluator predicate)
    {
        _input = input ?? throw new ArgumentNullException(nameof(input));
        _predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
    }

    public override void Open()
    {
        base.Open();
        _input.Open();
    }

    public override Row? Next()
    {
        while (true)
        {
            var row = _input.Next();
            if (row == null)
                return null;

            var result = _predicate.Evaluate(row);
            if (result.Type == Common.DataType.Boolean && result.AsBoolean())
            {
                return row;
            }
        }
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
