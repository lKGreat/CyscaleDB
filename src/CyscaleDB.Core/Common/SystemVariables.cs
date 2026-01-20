namespace CyscaleDB.Core.Common;

/// <summary>
/// Manages MySQL system variables for session and global scope.
/// </summary>
public sealed class SystemVariables
{
    private readonly Dictionary<string, object?> _sessionVariables;
    private static readonly Dictionary<string, object?> _globalVariables;

    /// <summary>
    /// Default system variable values (MySQL compatible).
    /// </summary>
    private static readonly Dictionary<string, object?> DefaultVariables = new(StringComparer.OrdinalIgnoreCase)
    {
        // Server information
        ["version"] = Constants.ServerVersion,
        ["version_comment"] = "CyscaleDB Server",
        ["version_compile_os"] = Environment.OSVersion.Platform.ToString(),
        ["version_compile_machine"] = Environment.Is64BitOperatingSystem ? "x86_64" : "x86",

        // Character set and collation
        ["character_set_client"] = "utf8mb4",
        ["character_set_connection"] = "utf8mb4",
        ["character_set_database"] = "utf8mb4",
        ["character_set_results"] = "utf8mb4",
        ["character_set_server"] = "utf8mb4",
        ["character_set_system"] = "utf8mb4",
        ["collation_connection"] = "utf8mb4_general_ci",
        ["collation_database"] = "utf8mb4_general_ci",
        ["collation_server"] = "utf8mb4_general_ci",

        // Connection settings
        ["autocommit"] = 1,
        ["auto_increment_increment"] = 1,
        ["auto_increment_offset"] = 1,
        ["connect_timeout"] = 10,
        ["interactive_timeout"] = 28800,
        ["wait_timeout"] = 28800,
        ["net_read_timeout"] = 30,
        ["net_write_timeout"] = 60,

        // SQL mode
        ["sql_mode"] = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",

        // Transaction
        ["transaction_isolation"] = "REPEATABLE-READ",
        ["tx_isolation"] = "REPEATABLE-READ",
        ["transaction_read_only"] = 0,
        ["tx_read_only"] = 0,

        // General settings
        ["max_allowed_packet"] = 67108864, // 64MB
        ["max_connections"] = 151,
        ["lower_case_table_names"] = 0,
        ["time_zone"] = "SYSTEM",
        ["system_time_zone"] = TimeZoneInfo.Local.StandardName,

        // Performance
        ["query_cache_size"] = 0,
        ["query_cache_type"] = "OFF",

        // Features
        ["have_ssl"] = "DISABLED",
        ["have_openssl"] = "DISABLED",
        ["have_query_cache"] = "NO",
        ["have_compress"] = "YES",

        // Protocol
        ["protocol_version"] = 10,
        ["hostname"] = Environment.MachineName,
        ["port"] = Constants.DefaultPort,

        // Compatibility
        ["sql_auto_is_null"] = 0,
        ["sql_big_selects"] = 1,
        ["sql_buffer_result"] = 0,
        ["sql_log_bin"] = 0,
        ["sql_notes"] = 1,
        ["sql_quote_show_create"] = 1,
        ["sql_safe_updates"] = 0,
        ["sql_select_limit"] = 18446744073709551615UL,
        ["sql_warnings"] = 0,

        // License
        ["license"] = "MIT",
    };

    static SystemVariables()
    {
        _globalVariables = new Dictionary<string, object?>(DefaultVariables, StringComparer.OrdinalIgnoreCase);
    }

    public SystemVariables()
    {
        _sessionVariables = new Dictionary<string, object?>(DefaultVariables, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Gets a session variable value.
    /// </summary>
    public object? GetSession(string name)
    {
        return _sessionVariables.TryGetValue(name, out var value) ? value : null;
    }

    /// <summary>
    /// Sets a session variable value.
    /// </summary>
    public void SetSession(string name, object? value)
    {
        _sessionVariables[name] = value;
    }

    /// <summary>
    /// Gets a global variable value.
    /// </summary>
    public static object? GetGlobal(string name)
    {
        return _globalVariables.TryGetValue(name, out var value) ? value : null;
    }

    /// <summary>
    /// Sets a global variable value.
    /// </summary>
    public static void SetGlobal(string name, object? value)
    {
        _globalVariables[name] = value;
    }

    /// <summary>
    /// Gets a variable value with the specified scope.
    /// </summary>
    public object? Get(string name, bool isGlobal = false)
    {
        return isGlobal ? GetGlobal(name) : GetSession(name);
    }

    /// <summary>
    /// Sets a variable value with the specified scope.
    /// </summary>
    public void Set(string name, object? value, bool isGlobal = false)
    {
        if (isGlobal)
            SetGlobal(name, value);
        else
            SetSession(name, value);
    }

    /// <summary>
    /// Gets all session variables.
    /// </summary>
    public IEnumerable<KeyValuePair<string, object?>> GetAllSession()
    {
        return _sessionVariables;
    }

    /// <summary>
    /// Gets all global variables.
    /// </summary>
    public static IEnumerable<KeyValuePair<string, object?>> GetAllGlobal()
    {
        return _globalVariables;
    }

    /// <summary>
    /// Converts a variable value to a DataValue.
    /// </summary>
    public static DataValue ToDataValue(object? value)
    {
        return value switch
        {
            null => DataValue.Null,
            string s => DataValue.FromVarChar(s),
            int i => DataValue.FromInt(i),
            long l => DataValue.FromBigInt(l),
            ulong ul => DataValue.FromBigInt((long)ul),
            bool b => DataValue.FromInt(b ? 1 : 0),
            double d => DataValue.FromDouble(d),
            _ => DataValue.FromVarChar(value.ToString() ?? "")
        };
    }
}
