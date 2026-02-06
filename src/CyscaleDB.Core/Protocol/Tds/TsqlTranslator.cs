using System.Text;
using System.Text.RegularExpressions;

namespace CyscaleDB.Core.Protocol.Tds;

/// <summary>
/// Translates T-SQL (SQL Server dialect) to MySQL-compatible SQL understood by CyscaleDB.
/// Handles common syntax differences between the two dialects.
///
/// Key transformations:
///   - [identifier] → `identifier` (bracket to backtick quoting)
///   - TOP N → LIMIT N (moved to end of SELECT)
///   - GETDATE() → NOW()
///   - ISNULL(a,b) → IFNULL(a,b)
///   - @@VERSION → VERSION()
///   - @@SERVERNAME → 'CyscaleDB'
///   - @@SPID → CONNECTION_ID()
///   - N'string' → 'string' (Unicode string prefix)
///   - dbo.table → table (strip schema prefix)
///   - EXEC sp_* → translated to SHOW/SELECT equivalents
///   - SET NOCOUNT ON/OFF → stripped
///   - GO batch separator → stripped
/// </summary>
public static partial class TsqlTranslator
{
    /// <summary>
    /// Translates a T-SQL statement to MySQL-compatible SQL.
    /// </summary>
    public static string Translate(string tsql)
    {
        if (string.IsNullOrWhiteSpace(tsql))
            return tsql;

        var sql = tsql.Trim();

        // Strip GO batch separator
        sql = StripGo(sql);

        // Strip SET NOCOUNT ON/OFF
        sql = StripSetNocount(sql);

        // Handle system stored procedures
        var spResult = TranslateSystemProcedure(sql);
        if (spResult != null)
            return spResult;

        // Handle @@variables
        sql = TranslateSystemVariables(sql);

        // Handle SERVERPROPERTY()
        sql = TranslateServerProperty(sql);

        // Replace bracket identifiers with backtick
        sql = ReplaceBracketIdentifiers(sql);

        // Strip N prefix from string literals
        sql = StripNPrefix(sql);

        // Strip schema prefix (dbo.)
        sql = StripSchemaPrefix(sql);

        // Handle TOP N → LIMIT N
        sql = TranslateTop(sql);

        // Function translations
        sql = TranslateFunctions(sql);

        // Handle USE [database]
        sql = TranslateUse(sql);

        return sql.Trim();
    }

    #region Transformations

    private static string StripGo(string sql)
    {
        // Remove standalone GO statements
        return Regex.Replace(sql, @"(?m)^\s*GO\s*$", "", RegexOptions.IgnoreCase).Trim();
    }

    private static string StripSetNocount(string sql)
    {
        return Regex.Replace(sql, @"SET\s+NOCOUNT\s+(ON|OFF)\s*;?\s*",
            "", RegexOptions.IgnoreCase).Trim();
    }

    private static string ReplaceBracketIdentifiers(string sql)
    {
        var sb = new StringBuilder(sql.Length);
        bool inString = false;
        char stringChar = '\0';

        for (int i = 0; i < sql.Length; i++)
        {
            var c = sql[i];

            if (inString)
            {
                sb.Append(c);
                if (c == stringChar)
                {
                    // Check for escaped quote
                    if (i + 1 < sql.Length && sql[i + 1] == stringChar)
                    {
                        sb.Append(sql[++i]);
                    }
                    else
                    {
                        inString = false;
                    }
                }
            }
            else if (c == '\'' || c == '"')
            {
                inString = true;
                stringChar = c;
                sb.Append(c);
            }
            else if (c == '[')
            {
                // Find matching ]
                var endIdx = sql.IndexOf(']', i + 1);
                if (endIdx > i)
                {
                    var identifier = sql[(i + 1)..endIdx];
                    sb.Append('`');
                    sb.Append(identifier);
                    sb.Append('`');
                    i = endIdx;
                }
                else
                {
                    sb.Append(c);
                }
            }
            else
            {
                sb.Append(c);
            }
        }

        return sb.ToString();
    }

    private static string StripNPrefix(string sql)
    {
        // Replace N'string' with 'string' (Unicode string literal prefix)
        return Regex.Replace(sql, @"N'", "'", RegexOptions.None);
    }

    private static string StripSchemaPrefix(string sql)
    {
        // Remove dbo. and sys. schema prefixes (but not in string literals)
        sql = Regex.Replace(sql, @"\bdbo\.", "", RegexOptions.IgnoreCase);
        return sql;
    }

    private static string TranslateTop(string sql)
    {
        // SELECT TOP N ... → SELECT ... LIMIT N
        var match = Regex.Match(sql,
            @"SELECT\s+(TOP\s+(\d+)\s+)(.*)",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);

        if (match.Success)
        {
            var n = match.Groups[2].Value;
            var rest = match.Groups[3].Value;
            sql = $"SELECT {rest} LIMIT {n}";
        }

        return sql;
    }

    private static string TranslateFunctions(string sql)
    {
        // GETDATE() → NOW()
        sql = Regex.Replace(sql, @"\bGETDATE\s*\(\s*\)", "NOW()", RegexOptions.IgnoreCase);

        // SYSDATETIME() → NOW(6)
        sql = Regex.Replace(sql, @"\bSYSDATETIME\s*\(\s*\)", "NOW(6)", RegexOptions.IgnoreCase);

        // ISNULL(a, b) → IFNULL(a, b)
        sql = Regex.Replace(sql, @"\bISNULL\s*\(", "IFNULL(", RegexOptions.IgnoreCase);

        // LEN(s) → LENGTH(s)
        sql = Regex.Replace(sql, @"\bLEN\s*\(", "LENGTH(", RegexOptions.IgnoreCase);

        // CHARINDEX(sub, str) → LOCATE(sub, str)
        sql = Regex.Replace(sql, @"\bCHARINDEX\s*\(", "LOCATE(", RegexOptions.IgnoreCase);

        // NEWID() → UUID()
        sql = Regex.Replace(sql, @"\bNEWID\s*\(\s*\)", "UUID()", RegexOptions.IgnoreCase);

        // CONVERT(type, expr) → CAST(expr AS type) -- simplified
        // This is complex; skip for now

        return sql;
    }

    private static string TranslateSystemVariables(string sql)
    {
        // @@VERSION → VERSION()
        sql = Regex.Replace(sql, @"@@VERSION\b",
            "'Microsoft SQL Server 2022 (CyscaleDB) - 16.0.1000.0'", RegexOptions.IgnoreCase);

        // @@SERVERNAME → 'CyscaleDB'
        sql = Regex.Replace(sql, @"@@SERVERNAME\b", "'CyscaleDB'", RegexOptions.IgnoreCase);

        // @@SPID → CONNECTION_ID()
        sql = Regex.Replace(sql, @"@@SPID\b", "CONNECTION_ID()", RegexOptions.IgnoreCase);

        // @@ROWCOUNT → ROW_COUNT() (not exactly equivalent but close)
        sql = Regex.Replace(sql, @"@@ROWCOUNT\b", "ROW_COUNT()", RegexOptions.IgnoreCase);

        // @@TRANCOUNT → 0 (simplified)
        sql = Regex.Replace(sql, @"@@TRANCOUNT\b", "0", RegexOptions.IgnoreCase);

        // @@IDENTITY → LAST_INSERT_ID()
        sql = Regex.Replace(sql, @"@@IDENTITY\b", "LAST_INSERT_ID()", RegexOptions.IgnoreCase);

        // @@ERROR → 0
        sql = Regex.Replace(sql, @"@@ERROR\b", "0", RegexOptions.IgnoreCase);

        // @@LANGUAGE → 'us_english'
        sql = Regex.Replace(sql, @"@@LANGUAGE\b", "'us_english'", RegexOptions.IgnoreCase);

        // @@MAX_PRECISION → 38
        sql = Regex.Replace(sql, @"@@MAX_PRECISION\b", "38", RegexOptions.IgnoreCase);

        return sql;
    }

    private static string TranslateServerProperty(string sql)
    {
        // SERVERPROPERTY('ProductVersion') → '16.0.1000.0'
        sql = Regex.Replace(sql, @"SERVERPROPERTY\s*\(\s*'ProductVersion'\s*\)",
            "'16.0.1000.0'", RegexOptions.IgnoreCase);
        sql = Regex.Replace(sql, @"SERVERPROPERTY\s*\(\s*'ProductLevel'\s*\)",
            "'RTM'", RegexOptions.IgnoreCase);
        sql = Regex.Replace(sql, @"SERVERPROPERTY\s*\(\s*'Edition'\s*\)",
            "'Developer Edition (64-bit)'", RegexOptions.IgnoreCase);
        sql = Regex.Replace(sql, @"SERVERPROPERTY\s*\(\s*'ServerName'\s*\)",
            "'CyscaleDB'", RegexOptions.IgnoreCase);
        sql = Regex.Replace(sql, @"SERVERPROPERTY\s*\(\s*'InstanceName'\s*\)",
            "'CYSCALEDB'", RegexOptions.IgnoreCase);
        sql = Regex.Replace(sql, @"SERVERPROPERTY\s*\(\s*'MachineName'\s*\)",
            "'CyscaleDB'", RegexOptions.IgnoreCase);
        sql = Regex.Replace(sql, @"SERVERPROPERTY\s*\(\s*'Collation'\s*\)",
            "'SQL_Latin1_General_CP1_CI_AS'", RegexOptions.IgnoreCase);
        sql = Regex.Replace(sql, @"SERVERPROPERTY\s*\(\s*'IsIntegratedSecurityOnly'\s*\)",
            "0", RegexOptions.IgnoreCase);
        sql = Regex.Replace(sql, @"SERVERPROPERTY\s*\(\s*'EngineEdition'\s*\)",
            "3", RegexOptions.IgnoreCase); // 3 = Enterprise
        // Generic fallback for unknown properties
        sql = Regex.Replace(sql, @"SERVERPROPERTY\s*\(\s*'[^']*'\s*\)",
            "NULL", RegexOptions.IgnoreCase);
        return sql;
    }

    private static string TranslateUse(string sql)
    {
        // USE [database] → USE `database`
        // Already handled by bracket replacement, but also handle without brackets
        var match = Regex.Match(sql, @"^\s*USE\s+(\w+)\s*;?\s*$", RegexOptions.IgnoreCase);
        if (match.Success)
        {
            var db = match.Groups[1].Value;
            return $"USE `{db}`";
        }
        return sql;
    }

    #endregion

    #region System Stored Procedures

    private static string? TranslateSystemProcedure(string sql)
    {
        var trimmed = sql.TrimStart();

        // EXEC sp_databases → SHOW DATABASES
        if (Regex.IsMatch(trimmed, @"^(EXEC|EXECUTE)\s+sp_databases", RegexOptions.IgnoreCase))
            return "SHOW DATABASES";

        // EXEC sp_tables → SHOW TABLES
        if (Regex.IsMatch(trimmed, @"^(EXEC|EXECUTE)\s+sp_tables", RegexOptions.IgnoreCase))
            return "SHOW TABLES";

        // EXEC sp_columns @table_name = 'xxx' → DESCRIBE xxx
        var spColumnsMatch = Regex.Match(trimmed,
            @"^(EXEC|EXECUTE)\s+sp_columns\s+@table_name\s*=\s*'([^']+)'",
            RegexOptions.IgnoreCase);
        if (spColumnsMatch.Success)
            return $"DESCRIBE `{spColumnsMatch.Groups[2].Value}`";

        // EXEC sp_helpdb → SHOW DATABASES
        if (Regex.IsMatch(trimmed, @"^(EXEC|EXECUTE)\s+sp_helpdb", RegexOptions.IgnoreCase))
            return "SHOW DATABASES";

        // EXEC sp_who → SHOW PROCESSLIST
        if (Regex.IsMatch(trimmed, @"^(EXEC|EXECUTE)\s+sp_who", RegexOptions.IgnoreCase))
            return "SHOW PROCESSLIST";

        // EXEC sp_help 'table' → DESCRIBE table
        var spHelpMatch = Regex.Match(trimmed,
            @"^(EXEC|EXECUTE)\s+sp_help\s+'([^']+)'",
            RegexOptions.IgnoreCase);
        if (spHelpMatch.Success)
            return $"DESCRIBE `{spHelpMatch.Groups[2].Value}`";

        return null;
    }

    #endregion
}
