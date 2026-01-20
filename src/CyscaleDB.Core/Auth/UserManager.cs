using System.Security.Cryptography;
using System.Text;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Auth;

/// <summary>
/// Manages user accounts and authentication for CyscaleDB.
/// </summary>
public sealed class UserManager
{
    private readonly Dictionary<string, UserInfo> _users;
    private readonly ReaderWriterLockSlim _lock;
    private readonly Logger _logger;
    private static UserManager? _instance;

    /// <summary>
    /// Gets the singleton instance of the UserManager.
    /// </summary>
    public static UserManager Instance => _instance ??= new UserManager();

    private UserManager()
    {
        _users = new Dictionary<string, UserInfo>(StringComparer.OrdinalIgnoreCase);
        _lock = new ReaderWriterLockSlim();
        _logger = LogManager.Default.GetLogger<UserManager>();

        // Create default root user with no password
        CreateUser("root", "", "localhost");
        CreateUser("root", "", "%"); // Allow root from any host
    }

    /// <summary>
    /// Creates a new user account.
    /// </summary>
    public void CreateUser(string username, string password, string host = "%")
    {
        _lock.EnterWriteLock();
        try
        {
            var key = GetUserKey(username, host);
            var passwordHash = password.Length > 0 ? HashPassword(password) : null;
            _users[key] = new UserInfo(username, host, passwordHash);
            _logger.Info("Created user '{0}'@'{1}'", username, host);
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
    public void DropUser(string username, string host = "%")
    {
        _lock.EnterWriteLock();
        try
        {
            var key = GetUserKey(username, host);
            if (_users.Remove(key))
            {
                _logger.Info("Dropped user '{0}'@'{1}'", username, host);
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
    /// <param name="username">The username</param>
    /// <param name="clientAuthResponse">The client's auth response (20 bytes)</param>
    /// <param name="salt">The server's challenge (20 bytes)</param>
    /// <param name="clientHost">The client's host address</param>
    /// <returns>True if authentication succeeds</returns>
    public bool ValidatePassword(string username, byte[] clientAuthResponse, byte[] salt, string clientHost)
    {
        _lock.EnterReadLock();
        try
        {
            // Try exact host match first, then wildcard
            var user = FindUser(username, clientHost);
            if (user == null)
            {
                _logger.Warning("Authentication failed: user '{0}'@'{1}' not found", username, clientHost);
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
    /// This is stored as a hex string in MySQL, but we store the raw bytes.
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
        // SHA1(SHA1(password))
        var doubleHash = SHA1.HashData(passwordHash);

        // salt + SHA1(SHA1(password))
        var combined = new byte[salt.Length + doubleHash.Length];
        Array.Copy(salt, 0, combined, 0, salt.Length);
        Array.Copy(doubleHash, 0, combined, salt.Length, doubleHash.Length);

        // SHA1(salt + SHA1(SHA1(password)))
        var scramble = SHA1.HashData(combined);

        // XOR with SHA1(password)
        var response = new byte[20];
        for (int i = 0; i < 20; i++)
        {
            response[i] = (byte)(passwordHash[i] ^ scramble[i]);
        }

        return response;
    }

    /// <summary>
    /// Checks if an auth response is effectively empty (all zeros or very short).
    /// </summary>
    private static bool IsEmptyAuthResponse(byte[] response)
    {
        if (response.Length == 0)
            return true;

        // Check if all bytes are zero
        foreach (var b in response)
        {
            if (b != 0)
                return false;
        }
        return true;
    }
}

/// <summary>
/// Represents a user account.
/// </summary>
public record UserInfo(string Username, string Host, byte[]? PasswordHash);
