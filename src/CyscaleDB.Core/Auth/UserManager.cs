using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Auth;

/// <summary>
/// Manages user accounts, roles, and authentication for CyscaleDB.
/// Supports persistence to disk and MySQL 8.4 compatible privilege model.
/// </summary>
public sealed class UserManager
{
    private readonly Dictionary<string, UserInfo> _users;
    private readonly Dictionary<string, RoleInfo> _roles;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Logger _logger;
    private string? _persistPath;
    private static UserManager? _instance;

    /// <summary>
    /// Gets the singleton instance of the UserManager.
    /// </summary>
    public static UserManager Instance => _instance ??= new UserManager();

    private UserManager()
    {
        _users = new Dictionary<string, UserInfo>(StringComparer.OrdinalIgnoreCase);
        _roles = new Dictionary<string, RoleInfo>(StringComparer.OrdinalIgnoreCase);
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<UserManager>();

        // Create default root user with no password
        CreateUser("root", "", "localhost");
        CreateUser("root", "", "%"); // Allow root from any host
    }

    /// <summary>
    /// Sets the persistence path for saving/loading user data.
    /// </summary>
    public void SetPersistPath(string dataDir)
    {
        _persistPath = Path.Combine(dataDir, "mysql_users.json");
        LoadFromDisk();
    }

    /// <summary>
    /// Creates a new user account.
    /// </summary>
    public void CreateUser(string username, string password, string host = "%",
        string authPlugin = "caching_sha2_password", bool ifNotExists = false)
    {
        _lock.EnterWriteLock();
        try
        {
            var key = GetUserKey(username, host);
            if (_users.ContainsKey(key))
            {
                if (ifNotExists) return;
                throw new CyscaleException($"User '{username}'@'{host}' already exists", ErrorCode.UserAlreadyExists);
            }
            var passwordHash = password.Length > 0 ? HashPassword(password) : null;
            _users[key] = new UserInfo(username, host, passwordHash)
            {
                AuthPlugin = authPlugin,
                AccountLocked = false,
                PasswordExpired = false,
                MaxConnections = 0,
                MaxQueriesPerHour = 0,
                MaxUpdatesPerHour = 0,
                MaxUserConnections = 0
            };
            _logger.Info("Created user '{0}'@'{1}'", username, host);
            SaveToDisk();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Alters a user account. Supports changing password, locking, resource limits.
    /// </summary>
    public void AlterUser(string username, string host, string? newPassword = null,
        bool? locked = null, bool? passwordExpired = null,
        int? maxConnections = null, int? maxQueriesPerHour = null)
    {
        _lock.EnterWriteLock();
        try
        {
            var key = GetUserKey(username, host);
            if (!_users.TryGetValue(key, out var user))
                throw new CyscaleException($"User '{username}'@'{host}' does not exist", ErrorCode.UserNotFound);

            if (newPassword != null)
            {
                var passwordHash = newPassword.Length > 0 ? HashPassword(newPassword) : null;
                _users[key] = user with { PasswordHash = passwordHash };
                user = _users[key];
            }
            if (locked.HasValue) user.AccountLocked = locked.Value;
            if (passwordExpired.HasValue) user.PasswordExpired = passwordExpired.Value;
            if (maxConnections.HasValue) user.MaxConnections = maxConnections.Value;
            if (maxQueriesPerHour.HasValue) user.MaxQueriesPerHour = maxQueriesPerHour.Value;

            _logger.Info("Altered user '{0}'@'{1}'", username, host);
            SaveToDisk();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Sets or changes a user's password.
    /// </summary>
    public void SetPassword(string username, string password, string host = "%")
    {
        _lock.EnterWriteLock();
        try
        {
            var key = GetUserKey(username, host);
            if (_users.TryGetValue(key, out var user))
            {
                var passwordHash = password.Length > 0 ? HashPassword(password) : null;
                _users[key] = user with { PasswordHash = passwordHash };
                _logger.Info("Password changed for user '{0}'@'{1}'", username, host);
                SaveToDisk();
            }
            else
            {
                throw new CyscaleException($"User '{username}'@'{host}' does not exist", ErrorCode.UserNotFound);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Drops a user account.
    /// </summary>
    public void DropUser(string username, string host = "%", bool ifExists = false)
    {
        _lock.EnterWriteLock();
        try
        {
            var key = GetUserKey(username, host);
            if (_users.Remove(key))
            {
                _logger.Info("Dropped user '{0}'@'{1}'", username, host);
                SaveToDisk();
            }
            else if (!ifExists)
            {
                throw new CyscaleException($"User '{username}'@'{host}' does not exist", ErrorCode.UserNotFound);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Validates a user's credentials using mysql_native_password protocol.
    /// </summary>
    public bool ValidatePassword(string username, byte[] clientAuthResponse, byte[] salt, string clientHost)
    {
        _lock.EnterReadLock();
        try
        {
            var user = FindUser(username, clientHost);
            if (user == null)
            {
                _logger.Warning("Authentication failed: user '{0}'@'{1}' not found", username, clientHost);
                return false;
            }

            if (user.AccountLocked)
            {
                _logger.Warning("Authentication failed: account '{0}'@'{1}' is locked", username, clientHost);
                return false;
            }

            // If no password is set, accept empty auth response
            if (user.PasswordHash == null)
            {
                _logger.Debug("User '{0}'@'{1}' has no password, accepting empty auth", username, user.Host);
                return clientAuthResponse.Length == 0 || IsEmptyAuthResponse(clientAuthResponse);
            }

            // Verify the client's auth response
            var expectedResponse = ComputeAuthResponse(user.PasswordHash, salt);
            var isValid = clientAuthResponse.Length == expectedResponse.Length &&
                         CryptographicOperations.FixedTimeEquals(clientAuthResponse, expectedResponse);

            if (!isValid)
            {
                _logger.Warning("Authentication failed: invalid password for '{0}'@'{1}'", username, clientHost);
            }
            else
            {
                _logger.Debug("Authentication successful for '{0}'@'{1}'", username, user.Host);
            }

            return isValid;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Checks if a user exists.
    /// </summary>
    public bool UserExists(string username, string host = "%")
    {
        _lock.EnterReadLock();
        try
        {
            return FindUser(username, host) != null;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Gets all users.
    /// </summary>
    public IEnumerable<(string username, string host)> GetAllUsers()
    {
        _lock.EnterReadLock();
        try
        {
            return _users.Values.Select(u => (u.Username, u.Host)).ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Gets user info for display (e.g., SHOW GRANTS).
    /// </summary>
    public UserInfo? GetUser(string username, string host)
    {
        _lock.EnterReadLock();
        try
        {
            return FindUser(username, host);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Finds a user by username and host, with wildcard support.
    /// </summary>
    private UserInfo? FindUser(string username, string clientHost)
    {
        // Try exact host match first
        var key = GetUserKey(username, clientHost);
        if (_users.TryGetValue(key, out var user))
            return user;

        // Try localhost variations
        if (clientHost == "::1" || clientHost == "127.0.0.1" || clientHost.StartsWith("localhost"))
        {
            foreach (var localhostVariant in new[] { "localhost", "127.0.0.1", "::1" })
            {
                key = GetUserKey(username, localhostVariant);
                if (_users.TryGetValue(key, out user))
                    return user;
            }
        }

        // Try wildcard host
        key = GetUserKey(username, "%");
        return _users.TryGetValue(key, out user) ? user : null;
    }

    private static string GetUserKey(string username, string host)
    {
        return $"{username.ToLowerInvariant()}@{host.ToLowerInvariant()}";
    }

    /// <summary>
    /// Computes the mysql_native_password hash: SHA1(password).
    /// </summary>
    private static byte[] HashPassword(string password)
    {
        var passwordBytes = Encoding.UTF8.GetBytes(password);
        return SHA1.HashData(passwordBytes);
    }

    /// <summary>
    /// Computes the expected auth response for mysql_native_password protocol.
    /// Protocol: XOR(SHA1(password), SHA1(salt + SHA1(SHA1(password))))
    /// </summary>
    private static byte[] ComputeAuthResponse(byte[] passwordHash, byte[] salt)
    {
        var doubleHash = SHA1.HashData(passwordHash);
        var combined = new byte[salt.Length + doubleHash.Length];
        Array.Copy(salt, 0, combined, 0, salt.Length);
        Array.Copy(doubleHash, 0, combined, salt.Length, doubleHash.Length);
        var scramble = SHA1.HashData(combined);
        var response = new byte[20];
        for (int i = 0; i < 20; i++)
        {
            response[i] = (byte)(passwordHash[i] ^ scramble[i]);
        }
        return response;
    }

    private static bool IsEmptyAuthResponse(byte[] response)
    {
        if (response.Length == 0) return true;
        foreach (var b in response)
        {
            if (b != 0) return false;
        }
        return true;
    }

    #region Role Management

    /// <summary>
    /// Creates a role.
    /// </summary>
    public void CreateRole(string roleName, bool ifNotExists = false)
    {
        _lock.EnterWriteLock();
        try
        {
            if (_roles.ContainsKey(roleName))
            {
                if (ifNotExists) return;
                throw new CyscaleException($"Role '{roleName}' already exists");
            }
            _roles[roleName] = new RoleInfo(roleName);
            _logger.Info("Created role '{0}'", roleName);
            SaveToDisk();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Drops a role.
    /// </summary>
    public void DropRole(string roleName, bool ifExists = false)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_roles.Remove(roleName) && !ifExists)
                throw new CyscaleException($"Role '{roleName}' does not exist");
            
            // Remove role from all users
            foreach (var user in _users.Values)
            {
                user.Roles.Remove(roleName);
            }
            _logger.Info("Dropped role '{0}'", roleName);
            SaveToDisk();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Grants a role to a user.
    /// </summary>
    public void GrantRole(string roleName, string username, string host)
    {
        _lock.EnterWriteLock();
        try
        {
            var user = FindUser(username, host)
                ?? throw new CyscaleException($"User '{username}'@'{host}' does not exist", ErrorCode.UserNotFound);
            if (!_roles.ContainsKey(roleName))
                throw new CyscaleException($"Role '{roleName}' does not exist");
            user.Roles.Add(roleName);
            _logger.Info("Granted role '{0}' to '{1}'@'{2}'", roleName, username, host);
            SaveToDisk();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Revokes a role from a user.
    /// </summary>
    public void RevokeRole(string roleName, string username, string host)
    {
        _lock.EnterWriteLock();
        try
        {
            var user = FindUser(username, host)
                ?? throw new CyscaleException($"User '{username}'@'{host}' does not exist", ErrorCode.UserNotFound);
            user.Roles.Remove(roleName);
            _logger.Info("Revoked role '{0}' from '{1}'@'{2}'", roleName, username, host);
            SaveToDisk();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Grants a privilege to a role.
    /// </summary>
    public void GrantPrivilegeToRole(string roleName, PrivilegeType privilege, string? database = null, string? table = null)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_roles.TryGetValue(roleName, out var role))
                throw new CyscaleException($"Role '{roleName}' does not exist");
            role.Privileges.Add(new UserPrivilege(privilege, database, table));
            SaveToDisk();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Gets all roles.
    /// </summary>
    public IEnumerable<string> GetAllRoles()
    {
        _lock.EnterReadLock();
        try
        {
            return _roles.Keys.ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    #endregion

    #region Privilege Management

    /// <summary>
    /// Grants a privilege to a user.
    /// </summary>
    public void GrantPrivilege(string username, string host, PrivilegeType privilege, string? database = null, string? table = null)
    {
        _lock.EnterWriteLock();
        try
        {
            var user = FindUser(username, host)
                ?? throw new CyscaleException($"User '{username}'@'{host}' does not exist", ErrorCode.UserNotFound);
            user.Privileges.Add(new UserPrivilege(privilege, database, table));
            _logger.Info("Granted {0} to '{1}'@'{2}' on {3}.{4}",
                privilege, username, host, database ?? "*", table ?? "*");
            SaveToDisk();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Revokes a privilege from a user.
    /// </summary>
    public void RevokePrivilege(string username, string host, PrivilegeType privilege, string? database = null, string? table = null)
    {
        _lock.EnterWriteLock();
        try
        {
            var user = FindUser(username, host)
                ?? throw new CyscaleException($"User '{username}'@'{host}' does not exist", ErrorCode.UserNotFound);
            var priv = new UserPrivilege(privilege, database, table);
            user.Privileges.Remove(priv);
            _logger.Info("Revoked {0} from '{1}'@'{2}' on {3}.{4}",
                privilege, username, host, database ?? "*", table ?? "*");
            SaveToDisk();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Checks if a user has a specific privilege, including role-inherited privileges.
    /// </summary>
    public bool HasPrivilege(string username, string host, PrivilegeType privilege, string? database = null, string? table = null)
    {
        _lock.EnterReadLock();
        try
        {
            var user = FindUser(username, host);
            if (user == null) return false;

            // Root user has all privileges
            if (user.Username.Equals("root", StringComparison.OrdinalIgnoreCase))
                return true;

            // Check direct user privileges
            foreach (var p in user.Privileges)
            {
                if (p.Covers(privilege, database, table))
                    return true;
            }

            // Check role-inherited privileges
            foreach (var roleName in user.Roles)
            {
                if (_roles.TryGetValue(roleName, out var role))
                {
                    foreach (var p in role.Privileges)
                    {
                        if (p.Covers(privilege, database, table))
                            return true;
                    }
                }
            }

            return false;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Gets all privileges for a user (including from roles).
    /// </summary>
    public IEnumerable<UserPrivilege> GetPrivileges(string username, string host)
    {
        _lock.EnterReadLock();
        try
        {
            var user = FindUser(username, host);
            if (user == null) return [];
            
            var allPrivs = new List<UserPrivilege>(user.Privileges);
            
            // Include role privileges
            foreach (var roleName in user.Roles)
            {
                if (_roles.TryGetValue(roleName, out var role))
                    allPrivs.AddRange(role.Privileges);
            }
            
            return allPrivs;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Parses a privilege string to PrivilegeType.
    /// </summary>
    public static PrivilegeType ParsePrivilege(string privilegeStr)
    {
        return privilegeStr.ToUpperInvariant() switch
        {
            "ALL" or "ALL PRIVILEGES" => PrivilegeType.All,
            "SELECT" => PrivilegeType.Select,
            "INSERT" => PrivilegeType.Insert,
            "UPDATE" => PrivilegeType.Update,
            "DELETE" => PrivilegeType.Delete,
            "CREATE" => PrivilegeType.Create,
            "DROP" => PrivilegeType.Drop,
            "ALTER" => PrivilegeType.Alter,
            "INDEX" => PrivilegeType.Index,
            "CREATE VIEW" => PrivilegeType.CreateView,
            "SHOW VIEW" => PrivilegeType.ShowView,
            "CREATE ROUTINE" => PrivilegeType.CreateRoutine,
            "ALTER ROUTINE" => PrivilegeType.AlterRoutine,
            "EXECUTE" => PrivilegeType.Execute,
            "TRIGGER" => PrivilegeType.Trigger,
            "EVENT" => PrivilegeType.Event,
            "GRANT OPTION" => PrivilegeType.Grant,
            "REFERENCES" => PrivilegeType.References,
            "RELOAD" => PrivilegeType.Reload,
            "SHUTDOWN" => PrivilegeType.Shutdown,
            "PROCESS" => PrivilegeType.Process,
            "FILE" => PrivilegeType.File,
            "SHOW DATABASES" => PrivilegeType.ShowDatabases,
            "SUPER" => PrivilegeType.Super,
            "REPLICATION SLAVE" => PrivilegeType.ReplicationSlave,
            "REPLICATION CLIENT" => PrivilegeType.ReplicationClient,
            "CREATE TEMPORARY TABLES" => PrivilegeType.CreateTemporaryTables,
            "LOCK TABLES" => PrivilegeType.LockTables,
            "CREATE USER" => PrivilegeType.CreateUser,
            "CREATE ROLE" => PrivilegeType.CreateRole,
            "DROP ROLE" => PrivilegeType.DropRole,
            "USAGE" => PrivilegeType.Usage,
            _ => throw new CyscaleException($"Unknown privilege: {privilegeStr}")
        };
    }

    #endregion

    #region Persistence

    private void SaveToDisk()
    {
        if (_persistPath == null) return;
        try
        {
            var data = new UserPersistenceData
            {
                Users = _users.Values.Select(u => new UserPersistenceEntry
                {
                    Username = u.Username,
                    Host = u.Host,
                    PasswordHashBase64 = u.PasswordHash != null ? Convert.ToBase64String(u.PasswordHash) : null,
                    AuthPlugin = u.AuthPlugin,
                    AccountLocked = u.AccountLocked,
                    PasswordExpired = u.PasswordExpired,
                    MaxConnections = u.MaxConnections,
                    MaxQueriesPerHour = u.MaxQueriesPerHour,
                    Roles = u.Roles.ToList(),
                    Privileges = u.Privileges.Select(p => new PrivilegePersistenceEntry
                    {
                        Type = p.Type.ToString(),
                        Database = p.DatabaseName,
                        Table = p.TableName
                    }).ToList()
                }).ToList(),
                Roles = _roles.Values.Select(r => new RolePersistenceEntry
                {
                    Name = r.Name,
                    Privileges = r.Privileges.Select(p => new PrivilegePersistenceEntry
                    {
                        Type = p.Type.ToString(),
                        Database = p.DatabaseName,
                        Table = p.TableName
                    }).ToList()
                }).ToList()
            };

            var json = JsonSerializer.Serialize(data, new JsonSerializerOptions { WriteIndented = true });
            var dir = Path.GetDirectoryName(_persistPath);
            if (dir != null && !Directory.Exists(dir))
                Directory.CreateDirectory(dir);
            File.WriteAllText(_persistPath, json);
        }
        catch (Exception ex)
        {
            _logger.Warning("Failed to save user data: {0}", ex.Message);
        }
    }

    private void LoadFromDisk()
    {
        if (_persistPath == null || !File.Exists(_persistPath)) return;
        try
        {
            var json = File.ReadAllText(_persistPath);
            var data = JsonSerializer.Deserialize<UserPersistenceData>(json);
            if (data == null) return;

            _lock.EnterWriteLock();
            try
            {
                _users.Clear();
                _roles.Clear();

                foreach (var entry in data.Users)
                {
                    var passwordHash = entry.PasswordHashBase64 != null
                        ? Convert.FromBase64String(entry.PasswordHashBase64) : null;
                    var key = GetUserKey(entry.Username, entry.Host);
                    var user = new UserInfo(entry.Username, entry.Host, passwordHash)
                    {
                        AuthPlugin = entry.AuthPlugin ?? "caching_sha2_password",
                        AccountLocked = entry.AccountLocked,
                        PasswordExpired = entry.PasswordExpired,
                        MaxConnections = entry.MaxConnections,
                        MaxQueriesPerHour = entry.MaxQueriesPerHour,
                    };
                    foreach (var r in entry.Roles ?? [])
                        user.Roles.Add(r);
                    foreach (var p in entry.Privileges ?? [])
                    {
                        if (Enum.TryParse<PrivilegeType>(p.Type, true, out var pt))
                            user.Privileges.Add(new UserPrivilege(pt, p.Database, p.Table));
                    }
                    _users[key] = user;
                }

                foreach (var entry in data.Roles ?? [])
                {
                    var role = new RoleInfo(entry.Name);
                    foreach (var p in entry.Privileges ?? [])
                    {
                        if (Enum.TryParse<PrivilegeType>(p.Type, true, out var pt))
                            role.Privileges.Add(new UserPrivilege(pt, p.Database, p.Table));
                    }
                    _roles[entry.Name] = role;
                }

                _logger.Info("Loaded {0} users and {1} roles from disk", _users.Count, _roles.Count);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
        catch (Exception ex)
        {
            _logger.Warning("Failed to load user data: {0}", ex.Message);
        }
    }

    // Persistence DTOs
    private class UserPersistenceData
    {
        public List<UserPersistenceEntry> Users { get; set; } = [];
        public List<RolePersistenceEntry> Roles { get; set; } = [];
    }

    private class UserPersistenceEntry
    {
        public string Username { get; set; } = "";
        public string Host { get; set; } = "%";
        public string? PasswordHashBase64 { get; set; }
        public string? AuthPlugin { get; set; }
        public bool AccountLocked { get; set; }
        public bool PasswordExpired { get; set; }
        public int MaxConnections { get; set; }
        public int MaxQueriesPerHour { get; set; }
        public List<string> Roles { get; set; } = [];
        public List<PrivilegePersistenceEntry> Privileges { get; set; } = [];
    }

    private class RolePersistenceEntry
    {
        public string Name { get; set; } = "";
        public List<PrivilegePersistenceEntry> Privileges { get; set; } = [];
    }

    private class PrivilegePersistenceEntry
    {
        public string Type { get; set; } = "";
        public string? Database { get; set; }
        public string? Table { get; set; }
    }

    #endregion
}

/// <summary>
/// Represents a user account.
/// </summary>
public record UserInfo(string Username, string Host, byte[]? PasswordHash)
{
    /// <summary>
    /// The privileges granted to this user.
    /// </summary>
    public HashSet<UserPrivilege> Privileges { get; init; } = [];

    /// <summary>
    /// Roles assigned to this user.
    /// </summary>
    public HashSet<string> Roles { get; init; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Authentication plugin (mysql_native_password, caching_sha2_password, etc.).
    /// </summary>
    public string AuthPlugin { get; set; } = "caching_sha2_password";

    /// <summary>
    /// Whether the account is locked.
    /// </summary>
    public bool AccountLocked { get; set; }

    /// <summary>
    /// Whether the password is expired.
    /// </summary>
    public bool PasswordExpired { get; set; }

    /// <summary>
    /// Max simultaneous connections (0 = unlimited).
    /// </summary>
    public int MaxConnections { get; set; }

    /// <summary>
    /// Max queries per hour (0 = unlimited).
    /// </summary>
    public int MaxQueriesPerHour { get; set; }

    /// <summary>
    /// Max updates per hour (0 = unlimited).
    /// </summary>
    public int MaxUpdatesPerHour { get; set; }

    /// <summary>
    /// Max user connections (0 = unlimited).
    /// </summary>
    public int MaxUserConnections { get; set; }
}

/// <summary>
/// Represents a role.
/// </summary>
public class RoleInfo
{
    public string Name { get; }
    public HashSet<UserPrivilege> Privileges { get; } = [];

    public RoleInfo(string name)
    {
        Name = name;
    }
}

/// <summary>
/// Represents a privilege granted to a user or role.
/// </summary>
public record UserPrivilege(
    PrivilegeType Type,
    string? DatabaseName = null,
    string? TableName = null
)
{
    /// <summary>
    /// Checks if this privilege covers the specified target.
    /// </summary>
    public bool Covers(PrivilegeType requiredType, string? database, string? table)
    {
        if (Type != PrivilegeType.All && Type != requiredType)
            return false;
        if (DatabaseName != null && !string.Equals(DatabaseName, database, StringComparison.OrdinalIgnoreCase))
            return false;
        if (TableName != null && !string.Equals(TableName, table, StringComparison.OrdinalIgnoreCase))
            return false;
        return true;
    }
}

/// <summary>
/// Types of privileges that can be granted (MySQL 8.4 compatible).
/// </summary>
public enum PrivilegeType
{
    All,
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Index,
    CreateView,
    ShowView,
    CreateRoutine,
    AlterRoutine,
    Execute,
    Trigger,
    Event,
    Grant,
    References,
    Reload,
    Shutdown,
    Process,
    File,
    ShowDatabases,
    Super,
    ReplicationSlave,
    ReplicationClient,
    CreateTemporaryTables,
    LockTables,
    CreateUser,
    CreateRole,
    DropRole,
    Usage
}
