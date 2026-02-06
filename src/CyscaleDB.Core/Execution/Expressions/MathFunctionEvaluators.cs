using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Expressions;

/// <summary>
/// Generic evaluator for single-argument math functions (ABS, CEIL, FLOOR, SQRT, etc.).
/// </summary>
internal sealed class MathUnaryEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    private readonly Func<double, double> _func;

    public MathUnaryEvaluator(IExpressionEvaluator arg, Func<double, double> func)
    {
        _arg = arg;
        _func = func;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        var result = _func(val.ToDouble());
        return DataValue.FromDouble(result);
    }
}

/// <summary>
/// Generic evaluator for two-argument math functions (POW, ATAN2, MOD, ROUND(x,d), TRUNCATE, etc.).
/// </summary>
internal sealed class MathBinaryEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg1;
    private readonly IExpressionEvaluator _arg2;
    private readonly Func<double, double, double> _func;

    public MathBinaryEvaluator(IExpressionEvaluator arg1, IExpressionEvaluator arg2, Func<double, double, double> func)
    {
        _arg1 = arg1;
        _arg2 = arg2;
        _func = func;
    }

    public DataValue Evaluate(Row row)
    {
        var v1 = _arg1.Evaluate(row);
        var v2 = _arg2.Evaluate(row);
        if (v1.IsNull || v2.IsNull) return DataValue.Null;
        var result = _func(v1.ToDouble(), v2.ToDouble());
        return DataValue.FromDouble(result);
    }
}

/// <summary>
/// ROUND function: ROUND(x) or ROUND(x, d)
/// </summary>
internal sealed class RoundEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _value;
    private readonly IExpressionEvaluator? _decimals;

    public RoundEvaluator(IExpressionEvaluator value, IExpressionEvaluator? decimals)
    {
        _value = value;
        _decimals = decimals;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _value.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        var d = val.ToDouble();
        int dec = 0;
        if (_decimals != null)
        {
            var decVal = _decimals.Evaluate(row);
            if (!decVal.IsNull) dec = (int)decVal.ToLong();
        }
        var result = Math.Round(d, Math.Max(0, Math.Min(dec, 15)), MidpointRounding.AwayFromZero);
        return DataValue.FromDouble(result);
    }
}

/// <summary>
/// TRUNCATE function: TRUNCATE(x, d) - truncates to d decimal places
/// </summary>
internal sealed class TruncateEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _value;
    private readonly IExpressionEvaluator _decimals;

    public TruncateEvaluator(IExpressionEvaluator value, IExpressionEvaluator decimals)
    {
        _value = value;
        _decimals = decimals;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _value.Evaluate(row);
        var decVal = _decimals.Evaluate(row);
        if (val.IsNull || decVal.IsNull) return DataValue.Null;
        var d = val.ToDouble();
        int dec = (int)decVal.ToLong();
        var factor = Math.Pow(10, dec);
        var result = Math.Truncate(d * factor) / factor;
        return DataValue.FromDouble(result);
    }
}

/// <summary>
/// RAND function: RAND() or RAND(seed)
/// </summary>
internal sealed class RandEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator? _seed;
    private static readonly Random SharedRandom = new();

    public RandEvaluator(IExpressionEvaluator? seed)
    {
        _seed = seed;
    }

    public DataValue Evaluate(Row row)
    {
        if (_seed != null)
        {
            var seedVal = _seed.Evaluate(row);
            if (!seedVal.IsNull)
            {
                var rng = new Random((int)seedVal.ToLong());
                return DataValue.FromDouble(rng.NextDouble());
            }
        }
        return DataValue.FromDouble(SharedRandom.NextDouble());
    }
}

/// <summary>
/// CONV function: CONV(n, from_base, to_base)
/// </summary>
internal sealed class ConvEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _num;
    private readonly IExpressionEvaluator _fromBase;
    private readonly IExpressionEvaluator _toBase;

    public ConvEvaluator(IExpressionEvaluator num, IExpressionEvaluator fromBase, IExpressionEvaluator toBase)
    {
        _num = num;
        _fromBase = fromBase;
        _toBase = toBase;
    }

    public DataValue Evaluate(Row row)
    {
        var numVal = _num.Evaluate(row);
        var fromVal = _fromBase.Evaluate(row);
        var toVal = _toBase.Evaluate(row);
        if (numVal.IsNull || fromVal.IsNull || toVal.IsNull) return DataValue.Null;

        try
        {
            int fromBase = (int)fromVal.ToLong();
            int toBase = (int)toVal.ToLong();
            if (fromBase < 2 || fromBase > 36 || toBase < 2 || toBase > 36)
                return DataValue.Null;

            var numStr = numVal.IsNull ? "0" : numVal.AsString().Trim();
            bool negative = numStr.StartsWith('-');
            if (negative) numStr = numStr[1..];

            long value = Convert.ToInt64(numStr, fromBase);
            string result = ConvertToBase(Math.Abs(value), toBase);
            if (negative) result = "-" + result;
            return DataValue.FromVarChar(result.ToUpperInvariant());
        }
        catch
        {
            return DataValue.Null;
        }
    }

    private static string ConvertToBase(long value, int toBase)
    {
        if (value == 0) return "0";
        const string chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        var result = new System.Text.StringBuilder();
        while (value > 0)
        {
            result.Insert(0, chars[(int)(value % toBase)]);
            value /= toBase;
        }
        return result.ToString();
    }
}

/// <summary>
/// CRC32 function: CRC32(expr)
/// </summary>
internal sealed class Crc32Evaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;

    public Crc32Evaluator(IExpressionEvaluator arg)
    {
        _arg = arg;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        var bytes = System.Text.Encoding.UTF8.GetBytes(val.AsString());
        uint crc = ComputeCrc32(bytes);
        return DataValue.FromBigInt(crc);
    }

    private static uint ComputeCrc32(byte[] data)
    {
        uint crc = 0xFFFFFFFF;
        foreach (var b in data)
        {
            crc ^= b;
            for (int i = 0; i < 8; i++)
                crc = (crc >> 1) ^ ((crc & 1) != 0 ? 0xEDB88320u : 0);
        }
        return ~crc;
    }
}

/// <summary>
/// SIGN function: returns -1, 0, or 1
/// </summary>
internal sealed class SignEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;

    public SignEvaluator(IExpressionEvaluator arg)
    {
        _arg = arg;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        var d = val.ToDouble();
        return DataValue.FromInt(Math.Sign(d));
    }
}
