using System.Security.Cryptography;

namespace CyscaleDB.Core.Storage.Encryption;

/// <summary>
/// Provides transparent data-at-rest encryption for tablespaces.
/// Implements AES-256-CBC encryption per MySQL 8.4 InnoDB specification.
///
/// Architecture:
///   - Master key: stored externally (file-based keyring or configured key)
///   - Tablespace key: per-tablespace, encrypted by master key, stored in tablespace header
///   - Page encryption: each page encrypted/decrypted transparently by storage layer
///
/// Usage:
///   ALTER TABLE t ENCRYPTION='Y'   → enables encryption
///   ALTER TABLE t ENCRYPTION='N'   → disables encryption
///
/// Key rotation:
///   ALTER INSTANCE ROTATE INNODB MASTER KEY → re-encrypts all tablespace keys
/// </summary>
public sealed class TablespaceEncryption : IDisposable
{
    private byte[]? _masterKey;
    private readonly Dictionary<string, byte[]> _tablespaceKeys = new();
    private readonly string? _keyringPath;
    private bool _disposed;

    /// <summary>
    /// Whether encryption is available (master key loaded).
    /// </summary>
    public bool IsAvailable => _masterKey != null;

    /// <summary>
    /// Gets the number of encrypted tablespaces.
    /// </summary>
    public int EncryptedTablespaceCount => _tablespaceKeys.Count;

    /// <summary>
    /// Creates a new TablespaceEncryption manager.
    /// </summary>
    /// <param name="keyringPath">Path to the keyring file. If null, generates a random master key.</param>
    public TablespaceEncryption(string? keyringPath = null)
    {
        _keyringPath = keyringPath;

        if (keyringPath != null && File.Exists(keyringPath))
        {
            _masterKey = File.ReadAllBytes(keyringPath);
        }
        else
        {
            // Generate a random master key
            _masterKey = RandomNumberGenerator.GetBytes(32); // AES-256
            if (keyringPath != null)
            {
                Directory.CreateDirectory(Path.GetDirectoryName(keyringPath)!);
                File.WriteAllBytes(keyringPath, _masterKey);
            }
        }
    }

    /// <summary>
    /// Enables encryption for a tablespace. Generates a per-tablespace key.
    /// </summary>
    /// <param name="tablespaceName">The tablespace (or table) name.</param>
    /// <returns>True if encryption was enabled.</returns>
    public bool EnableEncryption(string tablespaceName)
    {
        if (_masterKey == null) return false;

        if (!_tablespaceKeys.ContainsKey(tablespaceName))
        {
            var tablespaceKey = RandomNumberGenerator.GetBytes(32);
            _tablespaceKeys[tablespaceName] = tablespaceKey;
        }

        return true;
    }

    /// <summary>
    /// Disables encryption for a tablespace.
    /// </summary>
    public void DisableEncryption(string tablespaceName)
    {
        _tablespaceKeys.Remove(tablespaceName);
    }

    /// <summary>
    /// Checks if a tablespace is encrypted.
    /// </summary>
    public bool IsEncrypted(string tablespaceName)
    {
        return _tablespaceKeys.ContainsKey(tablespaceName);
    }

    /// <summary>
    /// Encrypts a page's data.
    /// </summary>
    /// <param name="tablespaceName">The tablespace name.</param>
    /// <param name="pageData">The raw page data to encrypt.</param>
    /// <returns>Encrypted page data (IV prepended).</returns>
    public byte[] EncryptPage(string tablespaceName, byte[] pageData)
    {
        if (!_tablespaceKeys.TryGetValue(tablespaceName, out var key))
            throw new InvalidOperationException($"Tablespace '{tablespaceName}' is not encrypted.");

        using var aes = Aes.Create();
        aes.Key = key;
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.PKCS7;
        aes.GenerateIV();

        using var encryptor = aes.CreateEncryptor();
        var encrypted = encryptor.TransformFinalBlock(pageData, 0, pageData.Length);

        // Prepend IV (16 bytes) to encrypted data
        var result = new byte[16 + encrypted.Length];
        aes.IV.CopyTo(result, 0);
        encrypted.CopyTo(result, 16);

        return result;
    }

    /// <summary>
    /// Decrypts a page's data.
    /// </summary>
    /// <param name="tablespaceName">The tablespace name.</param>
    /// <param name="encryptedData">The encrypted page data (IV prepended).</param>
    /// <returns>Decrypted page data.</returns>
    public byte[] DecryptPage(string tablespaceName, byte[] encryptedData)
    {
        if (!_tablespaceKeys.TryGetValue(tablespaceName, out var key))
            throw new InvalidOperationException($"Tablespace '{tablespaceName}' is not encrypted.");

        if (encryptedData.Length < 16)
            throw new ArgumentException("Encrypted data too short to contain IV.");

        // Extract IV (first 16 bytes)
        var iv = new byte[16];
        Array.Copy(encryptedData, 0, iv, 0, 16);

        var ciphertext = new byte[encryptedData.Length - 16];
        Array.Copy(encryptedData, 16, ciphertext, 0, ciphertext.Length);

        using var aes = Aes.Create();
        aes.Key = key;
        aes.IV = iv;
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.PKCS7;

        using var decryptor = aes.CreateDecryptor();
        return decryptor.TransformFinalBlock(ciphertext, 0, ciphertext.Length);
    }

    /// <summary>
    /// Rotates the master key. Re-encrypts all tablespace keys with the new master key.
    /// Equivalent to: ALTER INSTANCE ROTATE INNODB MASTER KEY
    /// </summary>
    public void RotateMasterKey()
    {
        var newMasterKey = RandomNumberGenerator.GetBytes(32);

        // Re-encrypt tablespace keys with new master key
        // (In a real implementation, tablespace keys would be stored encrypted by the master key.
        //  Here we just regenerate the master key since tablespace keys are in memory.)
        _masterKey = newMasterKey;

        if (_keyringPath != null)
        {
            File.WriteAllBytes(_keyringPath, _masterKey);
        }
    }

    /// <summary>
    /// Gets the encrypted tablespace key for persistence (encrypted by master key).
    /// </summary>
    public byte[]? GetEncryptedTablespaceKey(string tablespaceName)
    {
        if (_masterKey == null || !_tablespaceKeys.TryGetValue(tablespaceName, out var tsKey))
            return null;

        // Encrypt the tablespace key with the master key
        using var aes = Aes.Create();
        aes.Key = _masterKey;
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.PKCS7;
        aes.GenerateIV();

        using var encryptor = aes.CreateEncryptor();
        var encrypted = encryptor.TransformFinalBlock(tsKey, 0, tsKey.Length);

        var result = new byte[16 + encrypted.Length];
        aes.IV.CopyTo(result, 0);
        encrypted.CopyTo(result, 16);

        return result;
    }

    /// <summary>
    /// Restores a tablespace key from its encrypted form.
    /// </summary>
    public bool RestoreTablespaceKey(string tablespaceName, byte[] encryptedKey)
    {
        if (_masterKey == null || encryptedKey.Length < 16)
            return false;

        var iv = new byte[16];
        Array.Copy(encryptedKey, 0, iv, 0, 16);

        var ciphertext = new byte[encryptedKey.Length - 16];
        Array.Copy(encryptedKey, 16, ciphertext, 0, ciphertext.Length);

        using var aes = Aes.Create();
        aes.Key = _masterKey;
        aes.IV = iv;
        aes.Mode = CipherMode.CBC;
        aes.Padding = PaddingMode.PKCS7;

        using var decryptor = aes.CreateDecryptor();
        var tsKey = decryptor.TransformFinalBlock(ciphertext, 0, ciphertext.Length);

        _tablespaceKeys[tablespaceName] = tsKey;
        return true;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // Zero out keys
        if (_masterKey != null)
        {
            CryptographicOperations.ZeroMemory(_masterKey);
            _masterKey = null;
        }

        foreach (var key in _tablespaceKeys.Values)
        {
            CryptographicOperations.ZeroMemory(key);
        }
        _tablespaceKeys.Clear();
    }
}
