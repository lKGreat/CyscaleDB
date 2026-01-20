using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Limits the number of rows returned.
/// </summary>
public sealed class LimitOperator : OperatorBase
{
    private readonly IOperator _input;
    private readonly int _limit;
    private readonly int _offset;
    private int _skipped;
    private int _returned;

    public override TableSchema Schema => _input.Schema;

    public LimitOperator(IOperator input, int limit, int offset = 0)
    {
        _input = input ?? throw new ArgumentNullException(nameof(input));
        _limit = limit;
        _offset = offset;
    }

    public override void Open()
    {
        base.Open();
        _input.Open();
        _skipped = 0;
        _returned = 0;
    }

    public override Row? Next()
    {
        // Skip offset rows
        while (_skipped < _offset)
        {
            var row = _input.Next();
            if (row == null)
                return null;
            _skipped++;
        }

        // Return up to limit rows
        if (_returned >= _limit)
            return null;

        var result = _input.Next();
        if (result != null)
        {
            _returned++;
        }
        return result;
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
