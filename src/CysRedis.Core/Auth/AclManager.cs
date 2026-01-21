using System.Collections.Concurrent;
using System.Text;
using CysRedis.Core.Common;

namespace CysRedis.Core.Auth;

/// <summary>
/// Manages Access Control List (ACL) for user authentication and authorization.
/// </summary>
public class AclManager
{
    private readonly ConcurrentDictionary<string, AclUser> _users;
    private readonly List<AclLogEntry> _aclLog;
    private readonly object _logLock = new();
    private readonly int _maxLogEntries = 128;
    private const string DefaultUserName = "default";
    private string? _aclFilePath;

    public AclManager()
    {
        _users = new ConcurrentDictionary<string, AclUser>(StringComparer.Ordinal);
        _aclLog = new List<AclLogEntry>();
        
        // Create default user with full access
        var defaultUser = new AclUser(DefaultUserName)
        {
            Enabled = true,
            NoPassword = true
        };
        defaultUser.AllowAllCommands();
        defaultUser.AllowAllKeys();
        defaultUser.AllowAllChannels();
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
        {
            LogAuthFailure(user.Name, command, "User disabled");
            return false;
        }

        if (!user.CanExecuteCommand(command))
        {
            LogAuthFailure(user.Name, command, $"Command '{command}' not allowed");
            return false;
        }

        if (key != null && !user.CanAccessKey(key))
        {
            LogAuthFailure(user.Name, command, $"Key '{key}' not accessible");
            return false;
        }

        return true;
    }

    /// <summary>
    /// Checks if a user can access a Pub/Sub channel.
    /// </summary>
    public bool CanAccessChannel(AclUser user, string channel)
    {
        if (!user.Enabled)
            return false;

        return user.CanAccessChannel(channel);
    }

    /// <summary>
    /// Logs an ACL authentication failure.
    /// </summary>
    private void LogAuthFailure(string username, string command, string reason)
    {
        lock (_logLock)
        {
            var entry = new AclLogEntry
            {
                Timestamp = DateTime.UtcNow,
                Username = username,
                Command = command,
                Reason = reason
            };
            
            _aclLog.Add(entry);
            
            // 保持日志大小限制
            if (_aclLog.Count > _maxLogEntries)
            {
                _aclLog.RemoveAt(0);
            }
        }
    }

    /// <summary>
    /// Gets ACL log entries.
    /// </summary>
    public List<AclLogEntry> GetLog(int? count = null)
    {
        lock (_logLock)
        {
            var entries = _aclLog.ToList();
            if (count.HasValue && count.Value < entries.Count)
            {
                entries = entries.TakeLast(count.Value).ToList();
            }
            return entries;
        }
    }

    /// <summary>
    /// Clears the ACL log.
    /// </summary>
    public void ResetLog()
    {
        lock (_logLock)
        {
            _aclLog.Clear();
        }
    }

    /// <summary>
    /// Saves ACL configuration to file.
    /// </summary>
    public void Save(string? filePath = null)
    {
        filePath ??= _aclFilePath ?? throw new InvalidOperationException("ACL file path not set");
        _aclFilePath = filePath;

        var sb = new StringBuilder();
        
        foreach (var user in _users.Values)
        {
            sb.Append("user ").Append(user.Name);
            
            if (user.Enabled)
                sb.Append(" on");
            else
                sb.Append(" off");
            
            if (user.NoPassword)
                sb.Append(" nopass");
            
            foreach (var password in user.GetPasswordHashes())
            {
                sb.Append(" >").Append(password);
            }
            
            if (user.AllCommands)
                sb.Append(" +@all");
            else
            {
                foreach (var cmd in user.GetAllowedCommands())
                {
                    sb.Append(" +").Append(cmd);
                }
            }
            
            if (user.AllKeys)
                sb.Append(" ~*");
            else
            {
                foreach (var pattern in user.GetKeyPatterns())
                {
                    sb.Append(" ~").Append(pattern);
                }
            }
            
            if (user.AllChannels)
                sb.Append(" &*");
            else
            {
                foreach (var pattern in user.GetChannelPatterns())
                {
                    sb.Append(" &").Append(pattern);
                }
            }
            
            sb.AppendLine();
        }
        
        File.WriteAllText(filePath, sb.ToString());
        Logger.Info("ACL configuration saved to {0}", filePath);
    }

    /// <summary>
    /// Loads ACL configuration from file.
    /// </summary>
    public void Load(string? filePath = null)
    {
        filePath ??= _aclFilePath ?? throw new InvalidOperationException("ACL file path not set");
        _aclFilePath = filePath;

        if (!File.Exists(filePath))
        {
            Logger.Warning("ACL file not found: {0}", filePath);
            return;
        }

        var lines = File.ReadAllLines(filePath);
        
        foreach (var line in lines)
        {
            var trimmed = line.Trim();
            if (string.IsNullOrEmpty(trimmed) || trimmed.StartsWith('#'))
                continue;

            ParseAclRule(trimmed);
        }
        
        Logger.Info("ACL configuration loaded from {0}", filePath);
    }

    /// <summary>
    /// Parses an ACL rule string.
    /// </summary>
    private void ParseAclRule(string rule)
    {
        var parts = rule.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2 || !parts[0].Equals("user", StringComparison.OrdinalIgnoreCase))
            return;

        var username = parts[1];
        SetUser(username, user =>
        {
            for (int i = 2; i < parts.Length; i++)
            {
                var part = parts[i];
                
                if (part == "on")
                    user.Enabled = true;
                else if (part == "off")
                    user.Enabled = false;
                else if (part == "nopass")
                    user.NoPassword = true;
                else if (part.StartsWith('>'))
                    user.AddPasswordHash(part[1..]);
                else if (part == "+@all")
                    user.AllowAllCommands();
                else if (part.StartsWith('+'))
                    user.AllowCommand(part[1..]);
                else if (part.StartsWith('-'))
                    user.DisallowCommand(part[1..]);
                else if (part == "~*")
                    user.AllowAllKeys();
                else if (part.StartsWith('~'))
                    user.AddKeyPattern(part[1..]);
                else if (part == "&*")
                    user.AllowAllChannels();
                else if (part.StartsWith('&'))
                    user.AddChannelPattern(part[1..]);
            }
        });
    }
}

/// <summary>
/// ACL log entry.
/// </summary>
public class AclLogEntry
{
    public DateTime Timestamp { get; set; }
    public string Username { get; set; } = string.Empty;
    public string Command { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
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
    private readonly List<string> _channelPatterns = new();
    private bool _allCommands;
    private bool _allKeys;
    private bool _allChannels;

    public bool AllCommands => _allCommands;
    public bool AllKeys => _allKeys;
    public bool AllChannels => _allChannels;

    public AclUser(string name)
    {
        Name = name;
        Enabled = false;
        NoPassword = false;
        _allChannels = false;
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
    /// Adds a pre-hashed password.
    /// </summary>
    public void AddPasswordHash(string hash)
    {
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
    /// Gets password hashes for persistence.
    /// </summary>
    public IEnumerable<string> GetPasswordHashes() => _passwords;

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
    /// Gets key patterns for persistence.
    /// </summary>
    public IEnumerable<string> GetKeyPatterns() => _keyPatterns;

    /// <summary>
    /// Gets allowed commands for persistence.
    /// </summary>
    public IEnumerable<string> GetAllowedCommands() => _allowedCommands;

    /// <summary>
    /// Allows all channels.
    /// </summary>
    public void AllowAllChannels()
    {
        _allChannels = true;
        _channelPatterns.Clear();
    }

    /// <summary>
    /// Adds a channel pattern.
    /// </summary>
    public void AddChannelPattern(string pattern)
    {
        _channelPatterns.Add(pattern);
    }

    /// <summary>
    /// Gets channel patterns for persistence.
    /// </summary>
    public IEnumerable<string> GetChannelPatterns() => _channelPatterns;

    /// <summary>
    /// Checks if user can access channel.
    /// </summary>
    public bool CanAccessChannel(string channel)
    {
        if (_allChannels)
            return true;

        foreach (var pattern in _channelPatterns)
        {
            if (MatchPattern(pattern, channel))
                return true;
        }

        return false;
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
