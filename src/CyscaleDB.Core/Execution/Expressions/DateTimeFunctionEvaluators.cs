using System.Globalization;
using System.Text;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Expressions;

/// <summary>
/// Generic single-arg date part extractor: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, etc.
/// </summary>
internal sealed class DatePartEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    private readonly Func<DateTime, int> _extractor;

    public DatePartEvaluator(IExpressionEvaluator arg, Func<DateTime, int> extractor)
    {
        _arg = arg;
        _extractor = extractor;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            var dt = val.ToDateTime();
            return DataValue.FromInt(_extractor(dt));
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// DAYNAME(date) - returns name of weekday
/// </summary>
internal sealed class DayNameEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public DayNameEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try { return DataValue.FromVarChar(val.ToDateTime().ToString("dddd", CultureInfo.InvariantCulture)); }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// MONTHNAME(date) - returns name of month
/// </summary>
internal sealed class MonthNameEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public MonthNameEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try { return DataValue.FromVarChar(val.ToDateTime().ToString("MMMM", CultureInfo.InvariantCulture)); }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// DATE(expr) - extracts date part
/// </summary>
internal sealed class DateExtractEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public DateExtractEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try { return DataValue.FromDate(DateOnly.FromDateTime(val.ToDateTime())); }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// TIME(expr) - extracts time part
/// </summary>
internal sealed class TimeExtractEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public TimeExtractEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try { return DataValue.FromTime(TimeOnly.FromDateTime(val.ToDateTime())); }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// DATE_ADD/ADDDATE, DATE_SUB/SUBDATE: adds/subtracts interval
/// For simplicity, we support: DATE_ADD(date, INTERVAL n unit)
/// Called as DATE_ADD(date, n) where the unit is passed via the function name variant.
/// </summary>
internal sealed class DateAddEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _date;
    private readonly IExpressionEvaluator _interval;
    private readonly string _unit;
    private readonly bool _subtract;

    public DateAddEvaluator(IExpressionEvaluator date, IExpressionEvaluator interval, string unit, bool subtract)
    {
        _date = date;
        _interval = interval;
        _unit = unit.ToUpperInvariant();
        _subtract = subtract;
    }

    public DataValue Evaluate(Row row)
    {
        var dateVal = _date.Evaluate(row);
        var intVal = _interval.Evaluate(row);
        if (dateVal.IsNull || intVal.IsNull) return DataValue.Null;

        try
        {
            var dt = dateVal.ToDateTime();
            int n = (int)intVal.ToLong();
            if (_subtract) n = -n;

            dt = _unit switch
            {
                "MICROSECOND" => dt.AddTicks(n * 10L),
                "SECOND" => dt.AddSeconds(n),
                "MINUTE" => dt.AddMinutes(n),
                "HOUR" => dt.AddHours(n),
                "DAY" => dt.AddDays(n),
                "WEEK" => dt.AddDays(n * 7),
                "MONTH" => dt.AddMonths(n),
                "QUARTER" => dt.AddMonths(n * 3),
                "YEAR" => dt.AddYears(n),
                _ => dt.AddDays(n) // default to days
            };

            return DataValue.FromDateTime(dt);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// DATEDIFF(date1, date2) - returns days between dates
/// </summary>
internal sealed class DateDiffEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _date1;
    private readonly IExpressionEvaluator _date2;

    public DateDiffEvaluator(IExpressionEvaluator date1, IExpressionEvaluator date2)
    {
        _date1 = date1; _date2 = date2;
    }

    public DataValue Evaluate(Row row)
    {
        var v1 = _date1.Evaluate(row);
        var v2 = _date2.Evaluate(row);
        if (v1.IsNull || v2.IsNull) return DataValue.Null;
        try
        {
            var d1 = v1.ToDateTime().Date;
            var d2 = v2.ToDateTime().Date;
            return DataValue.FromInt((int)(d1 - d2).TotalDays);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// TIMEDIFF(time1, time2) - returns time difference
/// </summary>
internal sealed class TimeDiffEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _t1;
    private readonly IExpressionEvaluator _t2;

    public TimeDiffEvaluator(IExpressionEvaluator t1, IExpressionEvaluator t2) { _t1 = t1; _t2 = t2; }

    public DataValue Evaluate(Row row)
    {
        var v1 = _t1.Evaluate(row);
        var v2 = _t2.Evaluate(row);
        if (v1.IsNull || v2.IsNull) return DataValue.Null;
        try
        {
            var dt1 = v1.ToDateTime();
            var dt2 = v2.ToDateTime();
            var diff = dt1 - dt2;
            var sign = diff < TimeSpan.Zero ? "-" : "";
            diff = diff.Duration();
            return DataValue.FromVarChar($"{sign}{(int)diff.TotalHours:00}:{diff.Minutes:00}:{diff.Seconds:00}");
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// TIMESTAMPDIFF(unit, dt1, dt2) - difference in specified units
/// </summary>
internal sealed class TimestampDiffEvaluator : IExpressionEvaluator
{
    private readonly string _unit;
    private readonly IExpressionEvaluator _dt1;
    private readonly IExpressionEvaluator _dt2;

    public TimestampDiffEvaluator(string unit, IExpressionEvaluator dt1, IExpressionEvaluator dt2)
    {
        _unit = unit.ToUpperInvariant(); _dt1 = dt1; _dt2 = dt2;
    }

    public DataValue Evaluate(Row row)
    {
        var v1 = _dt1.Evaluate(row);
        var v2 = _dt2.Evaluate(row);
        if (v1.IsNull || v2.IsNull) return DataValue.Null;
        try
        {
            var d1 = v1.ToDateTime();
            var d2 = v2.ToDateTime();
            long result = _unit switch
            {
                "MICROSECOND" => (long)(d2 - d1).TotalMilliseconds * 1000,
                "SECOND" => (long)(d2 - d1).TotalSeconds,
                "MINUTE" => (long)(d2 - d1).TotalMinutes,
                "HOUR" => (long)(d2 - d1).TotalHours,
                "DAY" => (long)(d2 - d1).TotalDays,
                "WEEK" => (long)(d2 - d1).TotalDays / 7,
                "MONTH" => (d2.Year - d1.Year) * 12 + d2.Month - d1.Month,
                "QUARTER" => ((d2.Year - d1.Year) * 12 + d2.Month - d1.Month) / 3,
                "YEAR" => d2.Year - d1.Year,
                _ => (long)(d2 - d1).TotalDays
            };
            return DataValue.FromBigInt(result);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// TIMESTAMPADD(unit, interval, datetime)
/// </summary>
internal sealed class TimestampAddEvaluator : IExpressionEvaluator
{
    private readonly string _unit;
    private readonly IExpressionEvaluator _interval;
    private readonly IExpressionEvaluator _datetime;

    public TimestampAddEvaluator(string unit, IExpressionEvaluator interval, IExpressionEvaluator datetime)
    {
        _unit = unit.ToUpperInvariant(); _interval = interval; _datetime = datetime;
    }

    public DataValue Evaluate(Row row)
    {
        var iVal = _interval.Evaluate(row);
        var dtVal = _datetime.Evaluate(row);
        if (iVal.IsNull || dtVal.IsNull) return DataValue.Null;
        try
        {
            var dt = dtVal.ToDateTime();
            int n = (int)iVal.ToLong();
            dt = _unit switch
            {
                "MICROSECOND" => dt.AddTicks(n * 10L),
                "SECOND" => dt.AddSeconds(n),
                "MINUTE" => dt.AddMinutes(n),
                "HOUR" => dt.AddHours(n),
                "DAY" => dt.AddDays(n),
                "WEEK" => dt.AddDays(n * 7),
                "MONTH" => dt.AddMonths(n),
                "QUARTER" => dt.AddMonths(n * 3),
                "YEAR" => dt.AddYears(n),
                _ => dt.AddDays(n)
            };
            return DataValue.FromDateTime(dt);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// DATE_FORMAT(date, format) - formats date with MySQL format specifiers
/// </summary>
internal sealed class DateFormatEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _date;
    private readonly IExpressionEvaluator _format;

    public DateFormatEvaluator(IExpressionEvaluator date, IExpressionEvaluator format)
    {
        _date = date; _format = format;
    }

    public DataValue Evaluate(Row row)
    {
        var dVal = _date.Evaluate(row);
        var fVal = _format.Evaluate(row);
        if (dVal.IsNull || fVal.IsNull) return DataValue.Null;
        try
        {
            var dt = dVal.ToDateTime();
            var fmt = fVal.AsString();
            return DataValue.FromVarChar(FormatMySqlDate(dt, fmt));
        }
        catch { return DataValue.Null; }
    }

    internal static string FormatMySqlDate(DateTime dt, string fmt)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < fmt.Length; i++)
        {
            if (fmt[i] == '%' && i + 1 < fmt.Length)
            {
                i++;
                sb.Append(fmt[i] switch
                {
                    'Y' => dt.Year.ToString("D4"),
                    'y' => (dt.Year % 100).ToString("D2"),
                    'm' => dt.Month.ToString("D2"),
                    'c' => dt.Month.ToString(),
                    'd' => dt.Day.ToString("D2"),
                    'e' => dt.Day.ToString(),
                    'H' => dt.Hour.ToString("D2"),
                    'k' => dt.Hour.ToString(),
                    'h' or 'I' => ((dt.Hour % 12 == 0 ? 12 : dt.Hour % 12)).ToString("D2"),
                    'l' => (dt.Hour % 12 == 0 ? 12 : dt.Hour % 12).ToString(),
                    'i' => dt.Minute.ToString("D2"),
                    's' or 'S' => dt.Second.ToString("D2"),
                    'f' => (dt.Millisecond * 1000).ToString("D6"),
                    'p' => dt.Hour < 12 ? "AM" : "PM",
                    'r' => dt.ToString("hh:mm:ss tt", CultureInfo.InvariantCulture),
                    'T' => dt.ToString("HH:mm:ss", CultureInfo.InvariantCulture),
                    'D' => dt.Day.ToString() + GetOrdinalSuffix(dt.Day),
                    'W' => dt.ToString("dddd", CultureInfo.InvariantCulture),
                    'a' => dt.ToString("ddd", CultureInfo.InvariantCulture),
                    'M' => dt.ToString("MMMM", CultureInfo.InvariantCulture),
                    'b' => dt.ToString("MMM", CultureInfo.InvariantCulture),
                    'j' => dt.DayOfYear.ToString("D3"),
                    'w' => ((int)dt.DayOfWeek).ToString(),
                    'U' => CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(dt, CalendarWeekRule.FirstDay, DayOfWeek.Sunday).ToString("D2"),
                    'u' => CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(dt, CalendarWeekRule.FirstDay, DayOfWeek.Monday).ToString("D2"),
                    'V' => CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(dt, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Sunday).ToString("D2"),
                    'v' => CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(dt, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday).ToString("D2"),
                    '%' => "%",
                    _ => $"%{fmt[i]}"
                });
            }
            else
            {
                sb.Append(fmt[i]);
            }
        }
        return sb.ToString();
    }

    private static string GetOrdinalSuffix(int day)
    {
        if (day >= 11 && day <= 13) return "th";
        return (day % 10) switch { 1 => "st", 2 => "nd", 3 => "rd", _ => "th" };
    }
}

/// <summary>
/// TIME_FORMAT(time, format) - formats time with MySQL format specifiers
/// </summary>
internal sealed class TimeFormatEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _time;
    private readonly IExpressionEvaluator _format;

    public TimeFormatEvaluator(IExpressionEvaluator time, IExpressionEvaluator format)
    {
        _time = time; _format = format;
    }

    public DataValue Evaluate(Row row)
    {
        var tVal = _time.Evaluate(row);
        var fVal = _format.Evaluate(row);
        if (tVal.IsNull || fVal.IsNull) return DataValue.Null;
        try
        {
            var dt = tVal.ToDateTime();
            return DataValue.FromVarChar(DateFormatEvaluator.FormatMySqlDate(dt, fVal.AsString()));
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// STR_TO_DATE(str, format) - parses date string using MySQL format
/// </summary>
internal sealed class StrToDateEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator _format;

    public StrToDateEvaluator(IExpressionEvaluator str, IExpressionEvaluator format)
    {
        _str = str; _format = format;
    }

    public DataValue Evaluate(Row row)
    {
        var sVal = _str.Evaluate(row);
        var fVal = _format.Evaluate(row);
        if (sVal.IsNull || fVal.IsNull) return DataValue.Null;
        try
        {
            // Convert MySQL format to .NET format
            var mysqlFmt = fVal.AsString();
            var netFmt = ConvertMySqlFormatToNet(mysqlFmt);
            if (DateTime.TryParseExact(sVal.AsString(), netFmt, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                return DataValue.FromDateTime(dt);
            if (DateTime.TryParse(sVal.AsString(), CultureInfo.InvariantCulture, DateTimeStyles.None, out dt))
                return DataValue.FromDateTime(dt);
            return DataValue.Null;
        }
        catch { return DataValue.Null; }
    }

    private static string ConvertMySqlFormatToNet(string mysqlFmt)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < mysqlFmt.Length; i++)
        {
            if (mysqlFmt[i] == '%' && i + 1 < mysqlFmt.Length)
            {
                i++;
                sb.Append(mysqlFmt[i] switch
                {
                    'Y' => "yyyy",
                    'y' => "yy",
                    'm' => "MM",
                    'c' => "M",
                    'd' => "dd",
                    'e' => "d",
                    'H' => "HH",
                    'k' => "H",
                    'h' or 'I' => "hh",
                    'l' => "h",
                    'i' => "mm",
                    's' or 'S' => "ss",
                    'f' => "ffffff",
                    'p' => "tt",
                    'M' => "MMMM",
                    'b' => "MMM",
                    'W' => "dddd",
                    'a' => "ddd",
                    'T' => "HH:mm:ss",
                    _ => mysqlFmt[i].ToString()
                });
            }
            else
            {
                sb.Append(mysqlFmt[i]);
            }
        }
        return sb.ToString();
    }
}

/// <summary>
/// UNIX_TIMESTAMP() or UNIX_TIMESTAMP(date)
/// </summary>
internal sealed class UnixTimestampEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator? _date;
    public UnixTimestampEvaluator(IExpressionEvaluator? date) { _date = date; }

    public DataValue Evaluate(Row row)
    {
        DateTime dt;
        if (_date != null)
        {
            var val = _date.Evaluate(row);
            if (val.IsNull) return DataValue.Null;
            try { dt = val.ToDateTime(); }
            catch { return DataValue.Null; }
        }
        else
        {
            dt = DateTime.UtcNow;
        }
        return DataValue.FromBigInt(new DateTimeOffset(dt).ToUnixTimeSeconds());
    }
}

/// <summary>
/// FROM_UNIXTIME(unix_timestamp [, format])
/// </summary>
internal sealed class FromUnixTimeEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _ts;
    private readonly IExpressionEvaluator? _format;

    public FromUnixTimeEvaluator(IExpressionEvaluator ts, IExpressionEvaluator? format)
    {
        _ts = ts; _format = format;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _ts.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            var dt = DateTimeOffset.FromUnixTimeSeconds(val.ToLong()).LocalDateTime;
            if (_format != null)
            {
                var fVal = _format.Evaluate(row);
                if (!fVal.IsNull)
                    return DataValue.FromVarChar(DateFormatEvaluator.FormatMySqlDate(dt, fVal.AsString()));
            }
            return DataValue.FromDateTime(dt);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// FROM_DAYS(n) - converts day number to date
/// </summary>
internal sealed class FromDaysEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _n;
    public FromDaysEvaluator(IExpressionEvaluator n) { _n = n; }

    public DataValue Evaluate(Row row)
    {
        var val = _n.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            long days = val.ToLong();
            // MySQL day 1 = 0001-01-01
            var dt = new DateTime(1, 1, 1).AddDays(days - 1);
            return DataValue.FromDate(DateOnly.FromDateTime(dt));
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// TO_DAYS(date) - converts date to day number
/// </summary>
internal sealed class ToDaysEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _date;
    public ToDaysEvaluator(IExpressionEvaluator date) { _date = date; }

    public DataValue Evaluate(Row row)
    {
        var val = _date.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            var dt = val.ToDateTime();
            return DataValue.FromBigInt((long)(dt - new DateTime(1, 1, 1)).TotalDays + 1);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// TO_SECONDS(date) - returns seconds since year 0
/// </summary>
internal sealed class ToSecondsEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _date;
    public ToSecondsEvaluator(IExpressionEvaluator date) { _date = date; }

    public DataValue Evaluate(Row row)
    {
        var val = _date.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            var dt = val.ToDateTime();
            long days = (long)(dt - new DateTime(1, 1, 1)).TotalDays + 366; // MySQL offset for year 0
            long seconds = days * 86400 + dt.Hour * 3600 + dt.Minute * 60 + dt.Second;
            return DataValue.FromBigInt(seconds);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// SEC_TO_TIME(seconds) - converts seconds to time
/// </summary>
internal sealed class SecToTimeEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _sec;
    public SecToTimeEvaluator(IExpressionEvaluator sec) { _sec = sec; }

    public DataValue Evaluate(Row row)
    {
        var val = _sec.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        long sec = val.ToLong();
        var sign = sec < 0 ? "-" : "";
        sec = Math.Abs(sec);
        long h = sec / 3600;
        long m = (sec % 3600) / 60;
        long s = sec % 60;
        return DataValue.FromVarChar($"{sign}{h:00}:{m:00}:{s:00}");
    }
}

/// <summary>
/// TIME_TO_SEC(time) - converts time to seconds
/// </summary>
internal sealed class TimeToSecEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _time;
    public TimeToSecEvaluator(IExpressionEvaluator time) { _time = time; }

    public DataValue Evaluate(Row row)
    {
        var val = _time.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            if (val.Type == DataType.Time)
            {
                var t = val.AsTime();
                return DataValue.FromBigInt(t.Hour * 3600L + t.Minute * 60L + t.Second);
            }
            var dt = val.ToDateTime();
            return DataValue.FromBigInt(dt.Hour * 3600L + dt.Minute * 60L + dt.Second);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// MAKEDATE(year, dayofyear) - creates date from year and day of year
/// </summary>
internal sealed class MakeDateEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _year;
    private readonly IExpressionEvaluator _day;

    public MakeDateEvaluator(IExpressionEvaluator year, IExpressionEvaluator day) { _year = year; _day = day; }

    public DataValue Evaluate(Row row)
    {
        var y = _year.Evaluate(row);
        var d = _day.Evaluate(row);
        if (y.IsNull || d.IsNull) return DataValue.Null;
        try
        {
            int year = (int)y.ToLong();
            int day = (int)d.ToLong();
            if (day <= 0) return DataValue.Null;
            var dt = new DateTime(year, 1, 1).AddDays(day - 1);
            return DataValue.FromDate(DateOnly.FromDateTime(dt));
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// MAKETIME(hour, minute, second) - creates time
/// </summary>
internal sealed class MakeTimeEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _h;
    private readonly IExpressionEvaluator _m;
    private readonly IExpressionEvaluator _s;

    public MakeTimeEvaluator(IExpressionEvaluator h, IExpressionEvaluator m, IExpressionEvaluator s)
    {
        _h = h; _m = m; _s = s;
    }

    public DataValue Evaluate(Row row)
    {
        var hVal = _h.Evaluate(row);
        var mVal = _m.Evaluate(row);
        var sVal = _s.Evaluate(row);
        if (hVal.IsNull || mVal.IsNull || sVal.IsNull) return DataValue.Null;
        try
        {
            int h = (int)hVal.ToLong();
            int m = (int)mVal.ToLong();
            int s = (int)sVal.ToLong();
            return DataValue.FromTime(new TimeOnly(Math.Clamp(h, 0, 23), Math.Clamp(m, 0, 59), Math.Clamp(s, 0, 59)));
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// LAST_DAY(date) - returns last day of month
/// </summary>
internal sealed class LastDayEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _date;
    public LastDayEvaluator(IExpressionEvaluator date) { _date = date; }

    public DataValue Evaluate(Row row)
    {
        var val = _date.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            var dt = val.ToDateTime();
            var lastDay = new DateTime(dt.Year, dt.Month, DateTime.DaysInMonth(dt.Year, dt.Month));
            return DataValue.FromDate(DateOnly.FromDateTime(lastDay));
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// ADDTIME(datetime, time) / SUBTIME(datetime, time)
/// </summary>
internal sealed class AddTimeEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _datetime;
    private readonly IExpressionEvaluator _time;
    private readonly bool _subtract;

    public AddTimeEvaluator(IExpressionEvaluator datetime, IExpressionEvaluator time, bool subtract)
    {
        _datetime = datetime; _time = time; _subtract = subtract;
    }

    public DataValue Evaluate(Row row)
    {
        var dtVal = _datetime.Evaluate(row);
        var tVal = _time.Evaluate(row);
        if (dtVal.IsNull || tVal.IsNull) return DataValue.Null;
        try
        {
            var dt = dtVal.ToDateTime();
            TimeSpan ts;
            if (tVal.Type == DataType.Time)
                ts = tVal.AsTime().ToTimeSpan();
            else
            {
                // Try to parse as time string "HH:MM:SS"
                var s = tVal.AsString();
                if (!TimeSpan.TryParse(s, out ts))
                    ts = TimeSpan.FromSeconds(tVal.ToDouble());
            }
            dt = _subtract ? dt - ts : dt + ts;
            return DataValue.FromDateTime(dt);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// PERIOD_ADD(period, n) - adds n months to period (YYYYMM or YYMM format)
/// </summary>
internal sealed class PeriodAddEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _period;
    private readonly IExpressionEvaluator _n;

    public PeriodAddEvaluator(IExpressionEvaluator period, IExpressionEvaluator n) { _period = period; _n = n; }

    public DataValue Evaluate(Row row)
    {
        var pVal = _period.Evaluate(row);
        var nVal = _n.Evaluate(row);
        if (pVal.IsNull || nVal.IsNull) return DataValue.Null;
        try
        {
            long p = pVal.ToLong();
            int n = (int)nVal.ToLong();
            int year = (int)(p / 100);
            int month = (int)(p % 100);
            if (year < 100) year += (year < 70) ? 2000 : 1900;
            int totalMonths = year * 12 + month - 1 + n;
            year = totalMonths / 12;
            month = totalMonths % 12 + 1;
            return DataValue.FromBigInt(year * 100L + month);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// PERIOD_DIFF(period1, period2) - returns months between periods
/// </summary>
internal sealed class PeriodDiffEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _p1;
    private readonly IExpressionEvaluator _p2;

    public PeriodDiffEvaluator(IExpressionEvaluator p1, IExpressionEvaluator p2) { _p1 = p1; _p2 = p2; }

    public DataValue Evaluate(Row row)
    {
        var v1 = _p1.Evaluate(row);
        var v2 = _p2.Evaluate(row);
        if (v1.IsNull || v2.IsNull) return DataValue.Null;
        try
        {
            long p1 = v1.ToLong(), p2 = v2.ToLong();
            int y1 = (int)(p1 / 100), m1 = (int)(p1 % 100);
            int y2 = (int)(p2 / 100), m2 = (int)(p2 % 100);
            if (y1 < 100) y1 += (y1 < 70) ? 2000 : 1900;
            if (y2 < 100) y2 += (y2 < 70) ? 2000 : 1900;
            return DataValue.FromInt((y1 * 12 + m1) - (y2 * 12 + m2));
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// CONVERT_TZ(dt, from_tz, to_tz) - converts datetime between time zones (simplified)
/// </summary>
internal sealed class ConvertTzEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _dt;
    private readonly IExpressionEvaluator _from;
    private readonly IExpressionEvaluator _to;

    public ConvertTzEvaluator(IExpressionEvaluator dt, IExpressionEvaluator from, IExpressionEvaluator to)
    {
        _dt = dt; _from = from; _to = to;
    }

    public DataValue Evaluate(Row row)
    {
        var dtVal = _dt.Evaluate(row);
        var fromVal = _from.Evaluate(row);
        var toVal = _to.Evaluate(row);
        if (dtVal.IsNull || fromVal.IsNull || toVal.IsNull) return DataValue.Null;
        try
        {
            var dt = dtVal.ToDateTime();
            var fromOff = ParseOffset(fromVal.AsString());
            var toOff = ParseOffset(toVal.AsString());
            var utc = dt - fromOff;
            return DataValue.FromDateTime(utc + toOff);
        }
        catch { return DataValue.Null; }
    }

    private static TimeSpan ParseOffset(string tz)
    {
        tz = tz.Trim();
        if (tz.Equals("SYSTEM", StringComparison.OrdinalIgnoreCase))
            return TimeZoneInfo.Local.BaseUtcOffset;
        if (tz.StartsWith('+') || tz.StartsWith('-'))
        {
            if (TimeSpan.TryParse(tz, out var ts)) return ts;
        }
        // Try timezone name
        try
        {
            var tzi = TimeZoneInfo.FindSystemTimeZoneById(tz);
            return tzi.BaseUtcOffset;
        }
        catch { return TimeSpan.Zero; }
    }
}

/// <summary>
/// GET_FORMAT(type, standard) - returns format string
/// </summary>
internal sealed class GetFormatEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _type;
    private readonly IExpressionEvaluator _standard;

    public GetFormatEvaluator(IExpressionEvaluator type, IExpressionEvaluator standard)
    {
        _type = type; _standard = standard;
    }

    public DataValue Evaluate(Row row)
    {
        var t = _type.Evaluate(row);
        var s = _standard.Evaluate(row);
        if (t.IsNull || s.IsNull) return DataValue.Null;
        var type = t.AsString().ToUpperInvariant();
        var std = s.AsString().ToUpperInvariant();

        return DataValue.FromVarChar((type, std) switch
        {
            ("DATE", "USA") => "%m.%d.%Y",
            ("DATE", "JIS" or "ISO") => "%Y-%m-%d",
            ("DATE", "EUR") => "%d.%m.%Y",
            ("DATE", "INTERNAL") => "%Y%m%d",
            ("DATETIME", "USA") => "%Y-%m-%d %H.%i.%s",
            ("DATETIME", "JIS" or "ISO") => "%Y-%m-%d %H:%i:%s",
            ("DATETIME", "EUR") => "%Y-%m-%d %H.%i.%s",
            ("DATETIME", "INTERNAL") => "%Y%m%d%H%i%s",
            ("TIME", "USA") => "%h:%i:%s %p",
            ("TIME", "JIS" or "ISO") => "%H:%i:%s",
            ("TIME", "EUR") => "%H.%i.%s",
            ("TIME", "INTERNAL") => "%H%i%s",
            _ => "%Y-%m-%d %H:%i:%s"
        });
    }
}

/// <summary>
/// EXTRACT(unit FROM date) - extracts part from date
/// </summary>
internal sealed class ExtractEvaluator : IExpressionEvaluator
{
    private readonly string _unit;
    private readonly IExpressionEvaluator _date;

    public ExtractEvaluator(string unit, IExpressionEvaluator date)
    {
        _unit = unit.ToUpperInvariant();
        _date = date;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _date.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            var dt = val.ToDateTime();
            long result = _unit switch
            {
                "MICROSECOND" => dt.Millisecond * 1000,
                "SECOND" => dt.Second,
                "MINUTE" => dt.Minute,
                "HOUR" => dt.Hour,
                "DAY" => dt.Day,
                "WEEK" => CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(dt, CalendarWeekRule.FirstDay, DayOfWeek.Sunday),
                "MONTH" => dt.Month,
                "QUARTER" => (dt.Month - 1) / 3 + 1,
                "YEAR" => dt.Year,
                "YEAR_MONTH" => dt.Year * 100 + dt.Month,
                "DAY_HOUR" => dt.Day * 100 + dt.Hour,
                "DAY_MINUTE" => dt.Day * 10000 + dt.Hour * 100 + dt.Minute,
                "DAY_SECOND" => dt.Day * 1000000L + dt.Hour * 10000 + dt.Minute * 100 + dt.Second,
                "HOUR_MINUTE" => dt.Hour * 100 + dt.Minute,
                "HOUR_SECOND" => dt.Hour * 10000 + dt.Minute * 100 + dt.Second,
                "MINUTE_SECOND" => dt.Minute * 100 + dt.Second,
                _ => 0
            };
            return DataValue.FromBigInt(result);
        }
        catch { return DataValue.Null; }
    }
}
