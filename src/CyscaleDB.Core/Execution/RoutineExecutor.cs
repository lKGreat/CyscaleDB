using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution;

/// <summary>
/// Manages cursor state for stored procedure execution.
/// </summary>
public sealed class CursorManager
{
    private readonly Dictionary<string, CursorState> _cursors = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Declares a cursor with a result set.
    /// </summary>
    public void DeclareCursor(string name, ResultSet resultSet)
    {
        _cursors[name] = new CursorState(resultSet);
    }

    /// <summary>
    /// Opens a cursor.
    /// </summary>
    public void OpenCursor(string name)
    {
        if (!_cursors.TryGetValue(name, out var cursor))
            throw new CyscaleException($"Cursor '{name}' is not declared");
        cursor.Open();
    }

    /// <summary>
    /// Fetches the next row from a cursor.
    /// </summary>
    /// <returns>The row values, or null if no more rows.</returns>
    public DataValue[]? FetchCursor(string name)
    {
        if (!_cursors.TryGetValue(name, out var cursor))
            throw new CyscaleException($"Cursor '{name}' is not declared");
        return cursor.Fetch();
    }

    /// <summary>
    /// Closes a cursor.
    /// </summary>
    public void CloseCursor(string name)
    {
        if (!_cursors.TryGetValue(name, out var cursor))
            throw new CyscaleException($"Cursor '{name}' is not declared");
        cursor.Close();
    }

    /// <summary>
    /// Checks if a cursor exists.
    /// </summary>
    public bool HasCursor(string name) => _cursors.ContainsKey(name);

    /// <summary>
    /// Clears all cursors.
    /// </summary>
    public void ClearAll() => _cursors.Clear();
}

/// <summary>
/// State of a single cursor.
/// </summary>
internal sealed class CursorState
{
    private readonly ResultSet _resultSet;
    private int _currentIndex;
    private bool _isOpen;

    public CursorState(ResultSet resultSet)
    {
        _resultSet = resultSet;
        _currentIndex = 0;
        _isOpen = false;
    }

    public void Open()
    {
        if (_isOpen)
            throw new CyscaleException("Cursor is already open");
        _isOpen = true;
        _currentIndex = 0;
    }

    public DataValue[]? Fetch()
    {
        if (!_isOpen)
            throw new CyscaleException("Cursor is not open");
        if (_currentIndex >= _resultSet.Rows.Count)
            return null;
        return _resultSet.Rows[_currentIndex++];
    }

    public void Close()
    {
        if (!_isOpen)
            throw new CyscaleException("Cursor is not open");
        _isOpen = false;
    }
}

/// <summary>
/// Represents a SIGNAL/RESIGNAL condition for error handling in stored routines.
/// </summary>
public sealed class SignalException : CyscaleException
{
    public string SqlState { get; }
    public string? MessageText { get; }
    public int? MysqlErrno { get; }

    public SignalException(string sqlState, string? messageText = null, int? mysqlErrno = null)
        : base(messageText ?? $"Unhandled user-defined exception with SQLSTATE '{sqlState}'")
    {
        SqlState = sqlState;
        MessageText = messageText;
        MysqlErrno = mysqlErrno;
    }
}

/// <summary>
/// Condition handler types for stored routine error handling.
/// </summary>
public enum HandlerType
{
    Continue,
    Exit,
    Undo
}

/// <summary>
/// Represents a declared condition handler in a stored routine.
/// </summary>
public sealed class ConditionHandler
{
    public HandlerType Type { get; }
    public List<string> Conditions { get; }
    public List<Parsing.Ast.Statement> HandlerStatements { get; }

    public ConditionHandler(HandlerType type, List<string> conditions, List<Parsing.Ast.Statement> handlerStatements)
    {
        Type = type;
        Conditions = conditions;
        HandlerStatements = handlerStatements;
    }

    /// <summary>
    /// Checks if this handler matches the given SQLSTATE or condition name.
    /// </summary>
    public bool Matches(string sqlState, int? mysqlErrno = null)
    {
        foreach (var condition in Conditions)
        {
            if (condition.Equals("SQLEXCEPTION", StringComparison.OrdinalIgnoreCase))
            {
                // Matches any SQLSTATE that starts with something other than 00 or 01 or 02
                if (!sqlState.StartsWith("00") && !sqlState.StartsWith("01") && !sqlState.StartsWith("02"))
                    return true;
            }
            else if (condition.Equals("SQLWARNING", StringComparison.OrdinalIgnoreCase))
            {
                if (sqlState.StartsWith("01"))
                    return true;
            }
            else if (condition.Equals("NOT FOUND", StringComparison.OrdinalIgnoreCase))
            {
                if (sqlState.StartsWith("02"))
                    return true;
            }
            else if (condition.Equals(sqlState, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }
        return false;
    }
}

/// <summary>
/// Manages the event scheduler for CREATE EVENT support.
/// </summary>
public sealed class EventScheduler : IDisposable
{
    private readonly Dictionary<string, (Timer Timer, string EventName, string DatabaseName)> _timers = new();
    private readonly Func<string, string, ExecutionResult>? _executeCallback;
    private bool _enabled;

    public EventScheduler(Func<string, string, ExecutionResult>? executeCallback = null)
    {
        _executeCallback = executeCallback;
        _enabled = false;
    }

    public bool IsEnabled => _enabled;

    public void Enable() => _enabled = true;
    public void Disable() => _enabled = false;

    /// <summary>
    /// Schedules a recurring event.
    /// </summary>
    public void ScheduleEvent(string database, string eventName, TimeSpan interval, string eventBody)
    {
        CancelEvent(eventName);

        if (!_enabled) return;

        var timer = new Timer(_ =>
        {
            try
            {
                _executeCallback?.Invoke(database, eventBody);
            }
            catch (Exception)
            {
                // Swallow event execution errors
            }
        }, null, interval, interval);

        _timers[eventName] = (timer, eventName, database);
    }

    /// <summary>
    /// Cancels a scheduled event.
    /// </summary>
    public void CancelEvent(string eventName)
    {
        if (_timers.TryGetValue(eventName, out var entry))
        {
            entry.Timer.Dispose();
            _timers.Remove(eventName);
        }
    }

    public void Dispose()
    {
        foreach (var (timer, _, _) in _timers.Values)
        {
            timer.Dispose();
        }
        _timers.Clear();
    }
}
