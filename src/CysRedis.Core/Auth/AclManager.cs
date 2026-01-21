using System.Collections.Concurrent;
using CysRedis.Core.Common;

namespace CysRedis.Core.Auth;

/// <summary>
/// Manages Access Control List (ACL) for user authentication and authorization.
/// </summary>
public class AclManager
{
    private readonly ConcurrentDictionary<string, AclUser> _users;
    private const string DefaultUserName = "default";

    public AclManager()
    {
        _users = new ConcurrentDictionary<string, AclUser>(StringComparer.Ordinal);
        
        // Create default user with full access
        var defaultUser = new AclUser(DefaultUserName)
        {
            Enabled = true,
            NoPassword = true
        };
        defaultUser.AllowAllCommands();
        defaultUser.AllowAllKeys();
        _users[DefaultUserName] = defaultUser;
    }

    /// <summary>
    /// Authenticates a user with password.
    /// </summary>
    public AclUser? Authenticate(string? username, string? password)
    {
        username ??= DefaultUserName;

        if (!_users.TryGetValue(username, out var user))
            return null;

        if (!user.Enabled)
            return null;

        if (user.NoPassword)
            return user;

        if (password != null && user.CheckPassword(password))
            return user;

        return null;
    }

    /// <summary>
    /// Gets a user by name.
    /// </summary>
    public AclUser? GetUser(string username)
    {
        return _users.TryGetValue(username, out var user) ? user : null;
    }

    /// <summary>
    /// Creates or updates a user.
    /// </summary>
    public void SetUser(string username, Action<AclUser> configure)
    {
        var user = _users.GetOrAdd(username, name => new AclUser(name));
        configure(user);
    }

    /// <summary>
    /// Deletes a user.
    /// </summary>
    public bool DeleteUser(string username)
    {
        if (username == DefaultUserName)
            return false; // Cannot delete default user
        return _users.TryRemove(username, out _);
    }

    /// <summary>
    /// Gets all usernames.
    /// </summary>
    public IEnumerable<string> GetUserNames() => _users.Keys;

    /// <summary>
    /// Gets all users.
    /// </summary>
    public IEnumerable<AclUser> GetUsers() => _users.Values;

    /// <summary>
    /// Checks if a user can execute a command on a key.
    /// </summary>
    public bool CanExecute(AclUser user, string command, string? key)
    {
        if (!user.Enabled)
            return false;

        if (!user.CanExecuteCommand(command))
            return false;

        if (key != null && !user.CanAccessKey(key))
            return false;

        return true;
    }
}

/// <summary>
/// Represents an ACL user.
/// </summary>
public class AclUser
{
    public string Name { get; }
    public bool Enabled { get; set; }
    public bool NoPassword { get; set; }

    private readonly HashSet<string> _passwords = new();
    private readonly HashSet<string> _allowedCommands = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _disallowedCommands = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<string> _keyPatterns = new();
    private bool _allCommands;
    private bool _allKeys;

    public AclUser(string name)
    {
        Name = name;
        Enabled = false;
        NoPassword = false;
    }

    /// <summary>
    /// Adds a password for the user.
    /// </summary>
    public void AddPassword(string password)
    {
        var hash = HashPassword(password);
        _passwords.Add(hash);
        NoPassword = false;
    }

    /// <summary>
    /// Removes a password.
    /// </summary>
    public void RemovePassword(string password)
    {
        var hash = HashPassword(password);
        _passwords.Remove(hash);
    }

    /// <summary>
    /// Checks if password is valid.
    /// </summary>
    public bool CheckPassword(string password)
    {
        if (NoPassword) return true;
        var hash = HashPassword(password);
        return _passwords.Contains(hash);
    }

    /// <summary>
    /// Allows all commands.
    /// </summary>
    public void AllowAllCommands()
    {
        _allCommands = true;
        _disallowedCommands.Clear();
    }

    /// <summary>
    /// Allows specific commands.
    /// </summary>
    public void AllowCommand(string command)
    {
        _allowedCommands.Add(command);
    }

    /// <summary>
    /// Disallows specific commands.
    /// </summary>
    public void DisallowCommand(string command)
    {
        _disallowedCommands.Add(command);
    }

    /// <summary>
    /// Allows all keys.
    /// </summary>
    public void AllowAllKeys()
    {
        _allKeys = true;
        _keyPatterns.Clear();
    }

    /// <summary>
    /// Adds a key pattern.
    /// </summary>
    public void AddKeyPattern(string pattern)
    {
        _keyPatterns.Add(pattern);
    }

    /// <summary>
    /// Checks if user can execute command.
    /// </summary>
    public bool CanExecuteCommand(string command)
    {
        if (_disallowedCommands.Contains(command))
            return false;

        if (_allCommands)
            return true;

        return _allowedCommands.Contains(command);
    }

    /// <summary>
    /// Checks if user can access key.
    /// </summary>
    public bool CanAccessKey(string key)
    {
        if (_allKeys)
            return true;

        foreach (var pattern in _keyPatterns)
        {
            if (MatchPattern(pattern, key))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Gets ACL flags for INFO.
    /// </summary>
    public IEnumerable<string> GetFlags()
    {
        var flags = new List<string>();
        if (Enabled) flags.Add("on");
        else flags.Add("off");
        if (NoPassword) flags.Add("nopass");
        if (_allCommands) flags.Add("allcommands");
        if (_allKeys) flags.Add("allkeys");
        return flags;
    }

    private static string HashPassword(string password)
    {
        using var sha256 = System.Security.Cryptography.SHA256.Create();
        var hash = sha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(password));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    private static bool MatchPattern(string pattern, string value)
    {
        if (pattern == "*") return true;
        // Simple glob matching
        return System.Text.RegularExpressions.Regex.IsMatch(
            value,
            "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
                .Replace("\\*", ".*")
                .Replace("\\?", ".") + "$");
    }
}
