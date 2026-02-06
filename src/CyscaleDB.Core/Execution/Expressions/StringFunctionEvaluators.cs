using System.Globalization;
using System.Text;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Expressions;

/// <summary>
/// SUBSTRING/SUBSTR/MID: SUBSTRING(str, pos) or SUBSTRING(str, pos, len)
/// </summary>
internal sealed class SubstringEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator _pos;
    private readonly IExpressionEvaluator? _len;

    public SubstringEvaluator(IExpressionEvaluator str, IExpressionEvaluator pos, IExpressionEvaluator? len)
    {
        _str = str;
        _pos = pos;
        _len = len;
    }

    public DataValue Evaluate(Row row)
    {
        var strVal = _str.Evaluate(row);
        var posVal = _pos.Evaluate(row);
        if (strVal.IsNull || posVal.IsNull) return DataValue.Null;

        var s = strVal.AsString();
        int pos = (int)posVal.ToLong();
        // MySQL: 1-based, negative means from end
        if (pos > 0) pos--; // convert to 0-based
        else if (pos < 0) pos = s.Length + pos;
        else return DataValue.FromVarChar("");

        if (pos < 0) pos = 0;
        if (pos >= s.Length) return DataValue.FromVarChar("");

        if (_len != null)
        {
            var lenVal = _len.Evaluate(row);
            if (lenVal.IsNull) return DataValue.Null;
            int len = (int)lenVal.ToLong();
            if (len < 0) return DataValue.FromVarChar("");
            int actualLen = Math.Min(len, s.Length - pos);
            return DataValue.FromVarChar(s.Substring(pos, actualLen));
        }
        return DataValue.FromVarChar(s[pos..]);
    }
}

/// <summary>
/// LEFT(str, len) - returns leftmost len characters
/// </summary>
internal sealed class LeftEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator _len;

    public LeftEvaluator(IExpressionEvaluator str, IExpressionEvaluator len) { _str = str; _len = len; }

    public DataValue Evaluate(Row row)
    {
        var s = _str.Evaluate(row);
        var l = _len.Evaluate(row);
        if (s.IsNull || l.IsNull) return DataValue.Null;
        var str = s.AsString();
        int len = Math.Max(0, (int)l.ToLong());
        return DataValue.FromVarChar(str[..Math.Min(len, str.Length)]);
    }
}

/// <summary>
/// RIGHT(str, len) - returns rightmost len characters
/// </summary>
internal sealed class RightEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator _len;

    public RightEvaluator(IExpressionEvaluator str, IExpressionEvaluator len) { _str = str; _len = len; }

    public DataValue Evaluate(Row row)
    {
        var s = _str.Evaluate(row);
        var l = _len.Evaluate(row);
        if (s.IsNull || l.IsNull) return DataValue.Null;
        var str = s.AsString();
        int len = Math.Max(0, (int)l.ToLong());
        int start = Math.Max(0, str.Length - len);
        return DataValue.FromVarChar(str[start..]);
    }
}

/// <summary>
/// TRIM([{BOTH|LEADING|TRAILING} [remstr] FROM] str) / TRIM(str) / LTRIM(str) / RTRIM(str)
/// </summary>
internal sealed class TrimEvaluator : IExpressionEvaluator
{
    public enum TrimMode { Both, Leading, Trailing }

    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator? _trimChar;
    private readonly TrimMode _mode;

    public TrimEvaluator(IExpressionEvaluator str, IExpressionEvaluator? trimChar, TrimMode mode)
    {
        _str = str;
        _trimChar = trimChar;
        _mode = mode;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _str.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        var s = val.AsString();

        char[]? chars = null;
        if (_trimChar != null)
        {
            var tc = _trimChar.Evaluate(row);
            if (!tc.IsNull)
            {
                var tcStr = tc.AsString();
                if (tcStr.Length > 0) chars = tcStr.ToCharArray();
            }
        }

        return _mode switch
        {
            TrimMode.Leading => DataValue.FromVarChar(chars != null ? s.TrimStart(chars) : s.TrimStart()),
            TrimMode.Trailing => DataValue.FromVarChar(chars != null ? s.TrimEnd(chars) : s.TrimEnd()),
            _ => DataValue.FromVarChar(chars != null ? s.Trim(chars) : s.Trim())
        };
    }
}

/// <summary>
/// LPAD(str, len, padstr) / RPAD(str, len, padstr)
/// </summary>
internal sealed class PadEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator _len;
    private readonly IExpressionEvaluator _padStr;
    private readonly bool _leftPad;

    public PadEvaluator(IExpressionEvaluator str, IExpressionEvaluator len, IExpressionEvaluator padStr, bool leftPad)
    {
        _str = str; _len = len; _padStr = padStr; _leftPad = leftPad;
    }

    public DataValue Evaluate(Row row)
    {
        var sVal = _str.Evaluate(row);
        var lVal = _len.Evaluate(row);
        var pVal = _padStr.Evaluate(row);
        if (sVal.IsNull || lVal.IsNull || pVal.IsNull) return DataValue.Null;

        var s = sVal.AsString();
        int targetLen = (int)lVal.ToLong();
        var pad = pVal.AsString();

        if (targetLen < 0) return DataValue.Null;
        if (targetLen <= s.Length) return DataValue.FromVarChar(s[..targetLen]);
        if (pad.Length == 0) return DataValue.Null;

        var sb = new StringBuilder(targetLen);
        int needed = targetLen - s.Length;

        if (_leftPad)
        {
            while (sb.Length < needed) sb.Append(pad);
            sb.Length = needed;
            sb.Append(s);
        }
        else
        {
            sb.Append(s);
            while (sb.Length < targetLen) sb.Append(pad);
            sb.Length = targetLen;
        }
        return DataValue.FromVarChar(sb.ToString());
    }
}

/// <summary>
/// REPLACE(str, from_str, to_str)
/// </summary>
internal sealed class ReplaceEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator _from;
    private readonly IExpressionEvaluator _to;

    public ReplaceEvaluator(IExpressionEvaluator str, IExpressionEvaluator from, IExpressionEvaluator to)
    {
        _str = str; _from = from; _to = to;
    }

    public DataValue Evaluate(Row row)
    {
        var s = _str.Evaluate(row);
        var f = _from.Evaluate(row);
        var t = _to.Evaluate(row);
        if (s.IsNull || f.IsNull || t.IsNull) return DataValue.Null;
        return DataValue.FromVarChar(s.AsString().Replace(f.AsString(), t.AsString()));
    }
}

/// <summary>
/// LOCATE(substr, str [, pos]) / POSITION(substr IN str) / INSTR(str, substr)
/// </summary>
internal sealed class LocateEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _substr;
    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator? _pos;

    public LocateEvaluator(IExpressionEvaluator substr, IExpressionEvaluator str, IExpressionEvaluator? pos)
    {
        _substr = substr; _str = str; _pos = pos;
    }

    public DataValue Evaluate(Row row)
    {
        var sub = _substr.Evaluate(row);
        var s = _str.Evaluate(row);
        if (sub.IsNull || s.IsNull) return DataValue.Null;

        int startPos = 0;
        if (_pos != null)
        {
            var p = _pos.Evaluate(row);
            if (!p.IsNull) startPos = Math.Max(0, (int)p.ToLong() - 1);
        }

        var str = s.AsString();
        var substr = sub.AsString();
        if (startPos >= str.Length) return DataValue.FromInt(0);

        int idx = str.IndexOf(substr, startPos, StringComparison.Ordinal);
        return DataValue.FromInt(idx >= 0 ? idx + 1 : 0); // 1-based
    }
}

/// <summary>
/// INSERT(str, pos, len, newstr)
/// </summary>
internal sealed class InsertStringEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator _pos;
    private readonly IExpressionEvaluator _len;
    private readonly IExpressionEvaluator _newStr;

    public InsertStringEvaluator(IExpressionEvaluator str, IExpressionEvaluator pos, IExpressionEvaluator len, IExpressionEvaluator newStr)
    {
        _str = str; _pos = pos; _len = len; _newStr = newStr;
    }

    public DataValue Evaluate(Row row)
    {
        var sVal = _str.Evaluate(row);
        var pVal = _pos.Evaluate(row);
        var lVal = _len.Evaluate(row);
        var nVal = _newStr.Evaluate(row);
        if (sVal.IsNull || pVal.IsNull || lVal.IsNull || nVal.IsNull) return DataValue.Null;

        var s = sVal.AsString();
        int pos = (int)pVal.ToLong() - 1; // 1-based to 0-based
        int len = (int)lVal.ToLong();

        if (pos < 0 || pos > s.Length) return DataValue.FromVarChar(s);
        int end = Math.Min(pos + len, s.Length);
        return DataValue.FromVarChar(string.Concat(s.AsSpan(0, pos), nVal.AsString(), s.AsSpan(end)));
    }
}

/// <summary>
/// REPEAT(str, count)
/// </summary>
internal sealed class RepeatEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator _count;

    public RepeatEvaluator(IExpressionEvaluator str, IExpressionEvaluator count) { _str = str; _count = count; }

    public DataValue Evaluate(Row row)
    {
        var s = _str.Evaluate(row);
        var c = _count.Evaluate(row);
        if (s.IsNull || c.IsNull) return DataValue.Null;
        int count = Math.Max(0, (int)c.ToLong());
        if (count == 0) return DataValue.FromVarChar("");
        var str = s.AsString();
        var sb = new StringBuilder(str.Length * count);
        for (int i = 0; i < count; i++) sb.Append(str);
        return DataValue.FromVarChar(sb.ToString());
    }
}

/// <summary>
/// REVERSE(str)
/// </summary>
internal sealed class ReverseEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    public ReverseEvaluator(IExpressionEvaluator str) { _str = str; }

    public DataValue Evaluate(Row row)
    {
        var val = _str.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        var chars = val.AsString().ToCharArray();
        Array.Reverse(chars);
        return DataValue.FromVarChar(new string(chars));
    }
}

/// <summary>
/// SPACE(n) - returns a string of n spaces
/// </summary>
internal sealed class SpaceEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _n;
    public SpaceEvaluator(IExpressionEvaluator n) { _n = n; }

    public DataValue Evaluate(Row row)
    {
        var val = _n.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        int n = Math.Max(0, (int)val.ToLong());
        return DataValue.FromVarChar(new string(' ', n));
    }
}

/// <summary>
/// FORMAT(x, d [, locale]) - formats number with d decimal places and commas
/// </summary>
internal sealed class FormatEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _num;
    private readonly IExpressionEvaluator _dec;

    public FormatEvaluator(IExpressionEvaluator num, IExpressionEvaluator dec) { _num = num; _dec = dec; }

    public DataValue Evaluate(Row row)
    {
        var n = _num.Evaluate(row);
        var d = _dec.Evaluate(row);
        if (n.IsNull || d.IsNull) return DataValue.Null;
        int dec = Math.Max(0, (int)d.ToLong());
        var result = n.ToDouble().ToString("N" + dec, CultureInfo.InvariantCulture);
        return DataValue.FromVarChar(result);
    }
}

/// <summary>
/// ASCII(str) - returns numeric value of leftmost character
/// </summary>
internal sealed class AsciiEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    public AsciiEvaluator(IExpressionEvaluator str) { _str = str; }

    public DataValue Evaluate(Row row)
    {
        var val = _str.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        var s = val.AsString();
        return DataValue.FromInt(s.Length > 0 ? (int)s[0] : 0);
    }
}

/// <summary>
/// ORD(str) - returns code for leftmost (multibyte) character
/// </summary>
internal sealed class OrdEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    public OrdEvaluator(IExpressionEvaluator str) { _str = str; }

    public DataValue Evaluate(Row row)
    {
        var val = _str.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        var s = val.AsString();
        if (s.Length == 0) return DataValue.FromInt(0);
        var bytes = Encoding.UTF8.GetBytes(s[..1]);
        long result = 0;
        foreach (var b in bytes)
            result = (result << 8) | b;
        return DataValue.FromBigInt(result);
    }
}

/// <summary>
/// CHAR(n1, n2, ...) - returns string from character codes
/// </summary>
internal sealed class CharFunctionEvaluator : IExpressionEvaluator
{
    private readonly List<IExpressionEvaluator> _args;
    public CharFunctionEvaluator(List<IExpressionEvaluator> args) { _args = args; }

    public DataValue Evaluate(Row row)
    {
        var sb = new StringBuilder();
        foreach (var arg in _args)
        {
            var val = arg.Evaluate(row);
            if (val.IsNull) continue;
            sb.Append((char)(int)val.ToLong());
        }
        return DataValue.FromVarChar(sb.ToString());
    }
}

/// <summary>
/// HEX(str_or_num) - hexadecimal representation
/// </summary>
internal sealed class HexEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public HexEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        if (val.Type is DataType.Int or DataType.BigInt or DataType.TinyInt or DataType.SmallInt or DataType.Float or DataType.Double or DataType.Decimal)
            return DataValue.FromVarChar(((long)val.ToDouble()).ToString("X"));
        // String: hex encode each byte
        var bytes = Encoding.UTF8.GetBytes(val.AsString());
        return DataValue.FromVarChar(Convert.ToHexString(bytes));
    }
}

/// <summary>
/// UNHEX(str) - inverse of HEX()
/// </summary>
internal sealed class UnhexEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public UnhexEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            var hex = val.AsString();
            if (hex.Length % 2 != 0) hex = "0" + hex;
            var bytes = Convert.FromHexString(hex);
            return DataValue.FromVarChar(Encoding.UTF8.GetString(bytes));
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// BIN(n) - binary string representation
/// </summary>
internal sealed class BinEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public BinEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        return DataValue.FromVarChar(Convert.ToString((long)val.ToDouble(), 2));
    }
}

/// <summary>
/// OCT(n) - octal string representation
/// </summary>
internal sealed class OctEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public OctEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        return DataValue.FromVarChar(Convert.ToString((long)val.ToDouble(), 8));
    }
}

/// <summary>
/// FROM_BASE64(str) / TO_BASE64(str)
/// </summary>
internal sealed class Base64Evaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    private readonly bool _encode;

    public Base64Evaluator(IExpressionEvaluator arg, bool encode) { _arg = arg; _encode = encode; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            if (_encode)
                return DataValue.FromVarChar(Convert.ToBase64String(Encoding.UTF8.GetBytes(val.AsString())));
            else
                return DataValue.FromVarChar(Encoding.UTF8.GetString(Convert.FromBase64String(val.AsString())));
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// CONCAT_WS(separator, str1, str2, ...)
/// </summary>
internal sealed class ConcatWsEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _separator;
    private readonly List<IExpressionEvaluator> _args;

    public ConcatWsEvaluator(IExpressionEvaluator separator, List<IExpressionEvaluator> args)
    {
        _separator = separator;
        _args = args;
    }

    public DataValue Evaluate(Row row)
    {
        var sep = _separator.Evaluate(row);
        if (sep.IsNull) return DataValue.Null;
        var sepStr = sep.AsString();
        var parts = new List<string>();
        foreach (var arg in _args)
        {
            var val = arg.Evaluate(row);
            if (!val.IsNull) parts.Add(val.AsString()); // CONCAT_WS skips NULL
        }
        return DataValue.FromVarChar(string.Join(sepStr, parts));
    }
}

/// <summary>
/// STRCMP(str1, str2) - returns 0, -1, or 1
/// </summary>
internal sealed class StrcmpEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _s1;
    private readonly IExpressionEvaluator _s2;

    public StrcmpEvaluator(IExpressionEvaluator s1, IExpressionEvaluator s2) { _s1 = s1; _s2 = s2; }

    public DataValue Evaluate(Row row)
    {
        var v1 = _s1.Evaluate(row);
        var v2 = _s2.Evaluate(row);
        if (v1.IsNull || v2.IsNull) return DataValue.Null;
        return DataValue.FromInt(Math.Sign(string.Compare(v1.AsString(), v2.AsString(), StringComparison.Ordinal)));
    }
}

/// <summary>
/// SOUNDEX(str) - returns soundex string
/// </summary>
internal sealed class SoundexEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    public SoundexEvaluator(IExpressionEvaluator str) { _str = str; }

    public DataValue Evaluate(Row row)
    {
        var val = _str.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        return DataValue.FromVarChar(ComputeSoundex(val.AsString()));
    }

    private static string ComputeSoundex(string s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        var result = new StringBuilder();
        result.Append(char.ToUpper(s[0]));
        char lastCode = GetSoundexCode(s[0]);
        for (int i = 1; i < s.Length && result.Length < 4; i++)
        {
            char code = GetSoundexCode(s[i]);
            if (code != '0' && code != lastCode)
            {
                result.Append(code);
                lastCode = code;
            }
        }
        while (result.Length < 4) result.Append('0');
        return result.ToString();
    }

    private static char GetSoundexCode(char c)
    {
        return char.ToUpper(c) switch
        {
            'B' or 'F' or 'P' or 'V' => '1',
            'C' or 'G' or 'J' or 'K' or 'Q' or 'S' or 'X' or 'Z' => '2',
            'D' or 'T' => '3',
            'L' => '4',
            'M' or 'N' => '5',
            'R' => '6',
            _ => '0'
        };
    }
}

/// <summary>
/// SUBSTRING_INDEX(str, delim, count)
/// </summary>
internal sealed class SubstringIndexEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator _delim;
    private readonly IExpressionEvaluator _count;

    public SubstringIndexEvaluator(IExpressionEvaluator str, IExpressionEvaluator delim, IExpressionEvaluator count)
    {
        _str = str; _delim = delim; _count = count;
    }

    public DataValue Evaluate(Row row)
    {
        var sVal = _str.Evaluate(row);
        var dVal = _delim.Evaluate(row);
        var cVal = _count.Evaluate(row);
        if (sVal.IsNull || dVal.IsNull || cVal.IsNull) return DataValue.Null;

        var s = sVal.AsString();
        var d = dVal.AsString();
        int count = (int)cVal.ToLong();

        if (d.Length == 0) return DataValue.FromVarChar("");

        var parts = s.Split(d);
        if (count > 0)
        {
            count = Math.Min(count, parts.Length);
            return DataValue.FromVarChar(string.Join(d, parts.Take(count)));
        }
        else
        {
            count = Math.Min(-count, parts.Length);
            return DataValue.FromVarChar(string.Join(d, parts.Skip(parts.Length - count)));
        }
    }
}

/// <summary>
/// ELT(n, str1, str2, ...) - returns the nth string
/// </summary>
internal sealed class EltEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _n;
    private readonly List<IExpressionEvaluator> _strings;

    public EltEvaluator(IExpressionEvaluator n, List<IExpressionEvaluator> strings)
    {
        _n = n; _strings = strings;
    }

    public DataValue Evaluate(Row row)
    {
        var nVal = _n.Evaluate(row);
        if (nVal.IsNull) return DataValue.Null;
        int idx = (int)nVal.ToLong() - 1; // 1-based
        if (idx < 0 || idx >= _strings.Count) return DataValue.Null;
        return _strings[idx].Evaluate(row);
    }
}

/// <summary>
/// FIND_IN_SET(str, strlist) - returns position in comma-separated list
/// </summary>
internal sealed class FindInSetEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator _strList;

    public FindInSetEvaluator(IExpressionEvaluator str, IExpressionEvaluator strList)
    {
        _str = str; _strList = strList;
    }

    public DataValue Evaluate(Row row)
    {
        var s = _str.Evaluate(row);
        var l = _strList.Evaluate(row);
        if (s.IsNull || l.IsNull) return DataValue.Null;
        var search = s.AsString();
        var parts = l.AsString().Split(',');
        for (int i = 0; i < parts.Length; i++)
        {
            if (parts[i].Equals(search, StringComparison.Ordinal))
                return DataValue.FromInt(i + 1);
        }
        return DataValue.FromInt(0);
    }
}

/// <summary>
/// EXPORT_SET(bits, on, off [, separator [, number_of_bits]])
/// </summary>
internal sealed class ExportSetEvaluator : IExpressionEvaluator
{
    private readonly List<IExpressionEvaluator> _args;
    public ExportSetEvaluator(List<IExpressionEvaluator> args) { _args = args; }

    public DataValue Evaluate(Row row)
    {
        if (_args.Count < 3) return DataValue.Null;
        var bitsVal = _args[0].Evaluate(row);
        var onVal = _args[1].Evaluate(row);
        var offVal = _args[2].Evaluate(row);
        if (bitsVal.IsNull || onVal.IsNull || offVal.IsNull) return DataValue.Null;

        long bits = bitsVal.ToLong();
        var on = onVal.AsString();
        var off = offVal.AsString();
        var sep = _args.Count > 3 ? _args[3].Evaluate(row).AsString() : ",";
        int numBits = _args.Count > 4 ? (int)_args[4].Evaluate(row).ToLong() : 64;
        numBits = Math.Min(numBits, 64);

        var parts = new string[numBits];
        for (int i = 0; i < numBits; i++)
            parts[i] = (bits & (1L << i)) != 0 ? on : off;
        return DataValue.FromVarChar(string.Join(sep, parts));
    }
}

/// <summary>
/// MAKE_SET(bits, str1, str2, ...) - returns set of comma-separated strings
/// </summary>
internal sealed class MakeSetEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _bits;
    private readonly List<IExpressionEvaluator> _strings;

    public MakeSetEvaluator(IExpressionEvaluator bits, List<IExpressionEvaluator> strings)
    {
        _bits = bits; _strings = strings;
    }

    public DataValue Evaluate(Row row)
    {
        var bitsVal = _bits.Evaluate(row);
        if (bitsVal.IsNull) return DataValue.Null;
        long bits = bitsVal.ToLong();
        var parts = new List<string>();
        for (int i = 0; i < _strings.Count && i < 64; i++)
        {
            if ((bits & (1L << i)) != 0)
            {
                var val = _strings[i].Evaluate(row);
                if (!val.IsNull) parts.Add(val.AsString());
            }
        }
        return DataValue.FromVarChar(string.Join(",", parts));
    }
}

/// <summary>
/// QUOTE(str) - escapes string for SQL
/// </summary>
internal sealed class QuoteEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    public QuoteEvaluator(IExpressionEvaluator str) { _str = str; }

    public DataValue Evaluate(Row row)
    {
        var val = _str.Evaluate(row);
        if (val.IsNull) return DataValue.FromVarChar("NULL");
        var s = val.AsString()
            .Replace("\\", "\\\\")
            .Replace("'", "\\'")
            .Replace("\0", "\\0")
            .Replace("\n", "\\n")
            .Replace("\r", "\\r")
            .Replace("\x1a", "\\Z");
        return DataValue.FromVarChar($"'{s}'");
    }
}

/// <summary>
/// BIT_LENGTH(str) - returns length in bits
/// </summary>
internal sealed class BitLengthEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    public BitLengthEvaluator(IExpressionEvaluator str) { _str = str; }

    public DataValue Evaluate(Row row)
    {
        var val = _str.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        return DataValue.FromBigInt(Encoding.UTF8.GetByteCount(val.AsString()) * 8L);
    }
}

/// <summary>
/// WEIGHT_STRING(str) - returns weight string for sorting (stub: returns bytes)
/// </summary>
internal sealed class WeightStringEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    public WeightStringEvaluator(IExpressionEvaluator str) { _str = str; }

    public DataValue Evaluate(Row row)
    {
        var val = _str.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        // Simplified: return hex of bytes
        var bytes = Encoding.UTF8.GetBytes(val.AsString());
        return DataValue.FromBlob(bytes);
    }
}
