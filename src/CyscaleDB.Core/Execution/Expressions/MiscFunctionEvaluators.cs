using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Expressions;

#region Encryption / Hash Functions

/// <summary>
/// MD5(str) - calculates MD5 hash
/// </summary>
internal sealed class Md5Evaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public Md5Evaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        var hash = MD5.HashData(Encoding.UTF8.GetBytes(val.AsString()));
        return DataValue.FromVarChar(Convert.ToHexString(hash).ToLowerInvariant());
    }
}

/// <summary>
/// SHA1/SHA(str) - calculates SHA-1 hash
/// </summary>
internal sealed class Sha1Evaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public Sha1Evaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        var hash = SHA1.HashData(Encoding.UTF8.GetBytes(val.AsString()));
        return DataValue.FromVarChar(Convert.ToHexString(hash).ToLowerInvariant());
    }
}

/// <summary>
/// SHA2(str, hash_length) - calculates SHA-2 hash
/// </summary>
internal sealed class Sha2Evaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _str;
    private readonly IExpressionEvaluator _len;

    public Sha2Evaluator(IExpressionEvaluator str, IExpressionEvaluator len) { _str = str; _len = len; }

    public DataValue Evaluate(Row row)
    {
        var s = _str.Evaluate(row);
        var l = _len.Evaluate(row);
        if (s.IsNull || l.IsNull) return DataValue.Null;
        var bytes = Encoding.UTF8.GetBytes(s.AsString());
        int hashLen = (int)l.ToLong();
        byte[] hash = hashLen switch
        {
            224 => SHA256.HashData(bytes)[..28], // Approximate SHA-224
            256 or 0 => SHA256.HashData(bytes),
            384 => SHA384.HashData(bytes),
            512 => SHA512.HashData(bytes),
            _ => SHA256.HashData(bytes)
        };
        return DataValue.FromVarChar(Convert.ToHexString(hash).ToLowerInvariant());
    }
}

/// <summary>
/// AES_ENCRYPT(str, key_str) / AES_DECRYPT(crypt_str, key_str)
/// </summary>
internal sealed class AesEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _data;
    private readonly IExpressionEvaluator _key;
    private readonly bool _encrypt;

    public AesEvaluator(IExpressionEvaluator data, IExpressionEvaluator key, bool encrypt)
    {
        _data = data; _key = key; _encrypt = encrypt;
    }

    public DataValue Evaluate(Row row)
    {
        var d = _data.Evaluate(row);
        var k = _key.Evaluate(row);
        if (d.IsNull || k.IsNull) return DataValue.Null;
        try
        {
            using var aes = Aes.Create();
            aes.Mode = CipherMode.ECB;
            aes.Padding = PaddingMode.PKCS7;
            // MySQL uses 128-bit key, padded with zeros
            var keyBytes = new byte[16];
            var rawKey = Encoding.UTF8.GetBytes(k.AsString());
            for (int i = 0; i < rawKey.Length; i++)
                keyBytes[i % 16] ^= rawKey[i];
            aes.Key = keyBytes;

            if (_encrypt)
            {
                using var enc = aes.CreateEncryptor();
                var plainBytes = Encoding.UTF8.GetBytes(d.AsString());
                var encrypted = enc.TransformFinalBlock(plainBytes, 0, plainBytes.Length);
                return DataValue.FromVarChar(Convert.ToBase64String(encrypted));
            }
            else
            {
                using var dec = aes.CreateDecryptor();
                var cipherBytes = Convert.FromBase64String(d.AsString());
                var decrypted = dec.TransformFinalBlock(cipherBytes, 0, cipherBytes.Length);
                return DataValue.FromVarChar(Encoding.UTF8.GetString(decrypted));
            }
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// COMPRESS(str) / UNCOMPRESS(str) - GZip compression
/// </summary>
internal sealed class CompressEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    private readonly bool _compress;

    public CompressEvaluator(IExpressionEvaluator arg, bool compress) { _arg = arg; _compress = compress; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            if (_compress)
            {
                var bytes = Encoding.UTF8.GetBytes(val.AsString());
                using var ms = new System.IO.MemoryStream();
                using (var gz = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Compress))
                    gz.Write(bytes, 0, bytes.Length);
                return DataValue.FromVarChar(Convert.ToBase64String(ms.ToArray()));
            }
            else
            {
                var compressed = Convert.FromBase64String(val.AsString());
                using var ms = new System.IO.MemoryStream(compressed);
                using var gz = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress);
                using var output = new System.IO.MemoryStream();
                gz.CopyTo(output);
                return DataValue.FromVarChar(Encoding.UTF8.GetString(output.ToArray()));
            }
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// RANDOM_BYTES(len) - returns len random bytes
/// </summary>
internal sealed class RandomBytesEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _len;
    public RandomBytesEvaluator(IExpressionEvaluator len) { _len = len; }

    public DataValue Evaluate(Row row)
    {
        var val = _len.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        int len = Math.Clamp((int)val.ToLong(), 1, 1024);
        var bytes = RandomNumberGenerator.GetBytes(len);
        return DataValue.FromBlob(bytes);
    }
}

#endregion

#region UUID Functions

/// <summary>
/// UUID() - generates UUID string
/// </summary>
internal sealed class UuidEvaluator : IExpressionEvaluator
{
    public DataValue Evaluate(Row row) => DataValue.FromVarChar(Guid.NewGuid().ToString());
}

/// <summary>
/// UUID_SHORT() - generates short UUID (64-bit)
/// </summary>
internal sealed class UuidShortEvaluator : IExpressionEvaluator
{
    private static long _counter = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() << 16;

    public DataValue Evaluate(Row row) => DataValue.FromBigInt(Interlocked.Increment(ref _counter));
}

/// <summary>
/// UUID_TO_BIN(uuid [, swap_flag]) - converts UUID string to binary
/// </summary>
internal sealed class UuidToBinEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _uuid;
    private readonly IExpressionEvaluator? _swap;

    public UuidToBinEvaluator(IExpressionEvaluator uuid, IExpressionEvaluator? swap) { _uuid = uuid; _swap = swap; }

    public DataValue Evaluate(Row row)
    {
        var val = _uuid.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            var guid = Guid.Parse(val.AsString());
            var bytes = guid.ToByteArray();
            bool swap = false;
            if (_swap != null)
            {
                var sv = _swap.Evaluate(row);
                swap = !sv.IsNull && sv.ToLong() != 0;
            }
            if (swap)
            {
                // Swap time-low and time-hi for better indexing
                var swapped = new byte[16];
                Array.Copy(bytes, 6, swapped, 0, 2);  // time-hi
                Array.Copy(bytes, 4, swapped, 2, 2);  // time-mid
                Array.Copy(bytes, 0, swapped, 4, 4);  // time-low
                Array.Copy(bytes, 8, swapped, 8, 8);  // rest
                bytes = swapped;
            }
            return DataValue.FromBlob(bytes);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// BIN_TO_UUID(bin [, swap_flag]) - converts binary to UUID string
/// </summary>
internal sealed class BinToUuidEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _bin;
    private readonly IExpressionEvaluator? _swap;

    public BinToUuidEvaluator(IExpressionEvaluator bin, IExpressionEvaluator? swap) { _bin = bin; _swap = swap; }

    public DataValue Evaluate(Row row)
    {
        var val = _bin.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            var bytes = val.AsBlob();
            if (bytes.Length != 16) return DataValue.Null;
            bool swap = false;
            if (_swap != null)
            {
                var sv = _swap.Evaluate(row);
                swap = !sv.IsNull && sv.ToLong() != 0;
            }
            if (swap)
            {
                var unswapped = new byte[16];
                Array.Copy(bytes, 4, unswapped, 0, 4);  // time-low
                Array.Copy(bytes, 2, unswapped, 4, 2);  // time-mid
                Array.Copy(bytes, 0, unswapped, 6, 2);  // time-hi
                Array.Copy(bytes, 8, unswapped, 8, 8);  // rest
                bytes = unswapped;
            }
            return DataValue.FromVarChar(new Guid(bytes).ToString());
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// IS_UUID(str) - returns 1 if valid UUID format
/// </summary>
internal sealed class IsUuidEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public IsUuidEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        return DataValue.FromInt(Guid.TryParse(val.AsString(), out _) ? 1 : 0);
    }
}

#endregion

#region Locking Functions

/// <summary>
/// Named lock manager - shared across sessions
/// </summary>
internal static class NamedLockManager
{
    private static readonly ConcurrentDictionary<string, (long ConnectionId, DateTime AcquiredAt)> _locks = new();

    public static bool GetLock(string name, long connectionId, int timeoutSeconds)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
        while (DateTime.UtcNow < deadline)
        {
            if (_locks.TryAdd(name, (connectionId, DateTime.UtcNow)))
                return true;
            if (_locks.TryGetValue(name, out var existing) && existing.ConnectionId == connectionId)
                return true; // already held by same connection
            Thread.Sleep(100);
        }
        return false;
    }

    public static bool ReleaseLock(string name, long connectionId)
    {
        if (_locks.TryGetValue(name, out var existing) && existing.ConnectionId == connectionId)
            return _locks.TryRemove(name, out _);
        return false;
    }

    public static int ReleaseAllLocks(long connectionId)
    {
        int count = 0;
        foreach (var kvp in _locks)
        {
            if (kvp.Value.ConnectionId == connectionId && _locks.TryRemove(kvp.Key, out _))
                count++;
        }
        return count;
    }

    public static bool IsFreeLock(string name) => !_locks.ContainsKey(name);

    public static long? IsUsedLock(string name) =>
        _locks.TryGetValue(name, out var v) ? v.ConnectionId : null;
}

/// <summary>
/// GET_LOCK(name, timeout)
/// </summary>
internal sealed class GetLockEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _name;
    private readonly IExpressionEvaluator _timeout;

    public GetLockEvaluator(IExpressionEvaluator name, IExpressionEvaluator timeout) { _name = name; _timeout = timeout; }

    public DataValue Evaluate(Row row)
    {
        var n = _name.Evaluate(row);
        var t = _timeout.Evaluate(row);
        if (n.IsNull) return DataValue.Null;
        int timeout = t.IsNull ? 0 : (int)t.ToLong();
        return DataValue.FromInt(NamedLockManager.GetLock(n.AsString(), 1, timeout) ? 1 : 0);
    }
}

/// <summary>
/// RELEASE_LOCK(name)
/// </summary>
internal sealed class ReleaseLockEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _name;
    public ReleaseLockEvaluator(IExpressionEvaluator name) { _name = name; }

    public DataValue Evaluate(Row row)
    {
        var n = _name.Evaluate(row);
        if (n.IsNull) return DataValue.Null;
        return DataValue.FromInt(NamedLockManager.ReleaseLock(n.AsString(), 1) ? 1 : 0);
    }
}

/// <summary>
/// RELEASE_ALL_LOCKS()
/// </summary>
internal sealed class ReleaseAllLocksEvaluator : IExpressionEvaluator
{
    public DataValue Evaluate(Row row) => DataValue.FromInt(NamedLockManager.ReleaseAllLocks(1));
}

/// <summary>
/// IS_FREE_LOCK(name)
/// </summary>
internal sealed class IsFreeLockEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _name;
    public IsFreeLockEvaluator(IExpressionEvaluator name) { _name = name; }

    public DataValue Evaluate(Row row)
    {
        var n = _name.Evaluate(row);
        if (n.IsNull) return DataValue.Null;
        return DataValue.FromInt(NamedLockManager.IsFreeLock(n.AsString()) ? 1 : 0);
    }
}

/// <summary>
/// IS_USED_LOCK(name) - returns connection_id or NULL
/// </summary>
internal sealed class IsUsedLockEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _name;
    public IsUsedLockEvaluator(IExpressionEvaluator name) { _name = name; }

    public DataValue Evaluate(Row row)
    {
        var n = _name.Evaluate(row);
        if (n.IsNull) return DataValue.Null;
        var connId = NamedLockManager.IsUsedLock(n.AsString());
        return connId.HasValue ? DataValue.FromBigInt(connId.Value) : DataValue.Null;
    }
}

#endregion

#region Regex Functions

/// <summary>
/// REGEXP_LIKE(expr, pat [, match_type])
/// </summary>
internal sealed class RegexpLikeEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _expr;
    private readonly IExpressionEvaluator _pattern;
    private readonly IExpressionEvaluator? _matchType;

    public RegexpLikeEvaluator(IExpressionEvaluator expr, IExpressionEvaluator pattern, IExpressionEvaluator? matchType)
    {
        _expr = expr; _pattern = pattern; _matchType = matchType;
    }

    public DataValue Evaluate(Row row)
    {
        var e = _expr.Evaluate(row);
        var p = _pattern.Evaluate(row);
        if (e.IsNull || p.IsNull) return DataValue.Null;
        var opts = GetOptions(row);
        try { return DataValue.FromInt(Regex.IsMatch(e.AsString(), p.AsString(), opts) ? 1 : 0); }
        catch { return DataValue.Null; }
    }

    private RegexOptions GetOptions(Row row)
    {
        var opts = RegexOptions.None;
        if (_matchType != null)
        {
            var mt = _matchType.Evaluate(row);
            if (!mt.IsNull)
            {
                var s = mt.AsString();
                if (s.Contains('i')) opts |= RegexOptions.IgnoreCase;
                if (s.Contains('m')) opts |= RegexOptions.Multiline;
                if (s.Contains('n')) opts |= RegexOptions.Singleline;
            }
        }
        return opts;
    }
}

/// <summary>
/// REGEXP_INSTR(expr, pat [, pos [, occurrence [, return_option [, match_type]]]])
/// </summary>
internal sealed class RegexpInstrEvaluator : IExpressionEvaluator
{
    private readonly List<IExpressionEvaluator> _args;

    public RegexpInstrEvaluator(List<IExpressionEvaluator> args) { _args = args; }

    public DataValue Evaluate(Row row)
    {
        if (_args.Count < 2) return DataValue.Null;
        var e = _args[0].Evaluate(row);
        var p = _args[1].Evaluate(row);
        if (e.IsNull || p.IsNull) return DataValue.Null;

        int pos = _args.Count > 2 ? (int)_args[2].Evaluate(row).ToLong() : 1;
        int occurrence = _args.Count > 3 ? (int)_args[3].Evaluate(row).ToLong() : 1;
        int returnOption = _args.Count > 4 ? (int)_args[4].Evaluate(row).ToLong() : 0;

        try
        {
            var str = e.AsString();
            var matches = Regex.Matches(str[(pos - 1)..], p.AsString());
            if (occurrence <= 0 || occurrence > matches.Count) return DataValue.FromInt(0);
            var match = matches[occurrence - 1];
            int result = match.Index + pos;
            if (returnOption == 1) result += match.Length;
            return DataValue.FromInt(result);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// REGEXP_REPLACE(expr, pat, repl [, pos [, occurrence [, match_type]]])
/// </summary>
internal sealed class RegexpReplaceEvaluator : IExpressionEvaluator
{
    private readonly List<IExpressionEvaluator> _args;

    public RegexpReplaceEvaluator(List<IExpressionEvaluator> args) { _args = args; }

    public DataValue Evaluate(Row row)
    {
        if (_args.Count < 3) return DataValue.Null;
        var e = _args[0].Evaluate(row);
        var p = _args[1].Evaluate(row);
        var r = _args[2].Evaluate(row);
        if (e.IsNull || p.IsNull || r.IsNull) return DataValue.Null;

        try
        {
            var str = e.AsString();
            var pattern = p.AsString();
            var replacement = r.AsString();
            int pos = _args.Count > 3 ? Math.Max(1, (int)_args[3].Evaluate(row).ToLong()) : 1;
            int occurrence = _args.Count > 4 ? (int)_args[4].Evaluate(row).ToLong() : 0;

            if (pos > 1)
            {
                var prefix = str[..(pos - 1)];
                str = str[(pos - 1)..];
                if (occurrence == 0)
                    return DataValue.FromVarChar(prefix + Regex.Replace(str, pattern, replacement));
                else
                    return DataValue.FromVarChar(prefix + ReplaceNth(str, pattern, replacement, occurrence));
            }

            if (occurrence == 0)
                return DataValue.FromVarChar(Regex.Replace(str, pattern, replacement));
            return DataValue.FromVarChar(ReplaceNth(str, pattern, replacement, occurrence));
        }
        catch { return DataValue.Null; }
    }

    private static string ReplaceNth(string input, string pattern, string replacement, int n)
    {
        int count = 0;
        return Regex.Replace(input, pattern, m => ++count == n ? replacement : m.Value);
    }
}

/// <summary>
/// REGEXP_SUBSTR(expr, pat [, pos [, occurrence [, match_type]]])
/// </summary>
internal sealed class RegexpSubstrEvaluator : IExpressionEvaluator
{
    private readonly List<IExpressionEvaluator> _args;

    public RegexpSubstrEvaluator(List<IExpressionEvaluator> args) { _args = args; }

    public DataValue Evaluate(Row row)
    {
        if (_args.Count < 2) return DataValue.Null;
        var e = _args[0].Evaluate(row);
        var p = _args[1].Evaluate(row);
        if (e.IsNull || p.IsNull) return DataValue.Null;

        int pos = _args.Count > 2 ? Math.Max(1, (int)_args[2].Evaluate(row).ToLong()) : 1;
        int occurrence = _args.Count > 3 ? Math.Max(1, (int)_args[3].Evaluate(row).ToLong()) : 1;

        try
        {
            var str = e.AsString()[(pos - 1)..];
            var matches = Regex.Matches(str, p.AsString());
            if (occurrence > matches.Count) return DataValue.Null;
            return DataValue.FromVarChar(matches[occurrence - 1].Value);
        }
        catch { return DataValue.Null; }
    }
}

#endregion

#region Network Functions

/// <summary>
/// INET_ATON(expr) - returns numeric IPv4 address
/// </summary>
internal sealed class InetAtonEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public InetAtonEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            var ip = IPAddress.Parse(val.AsString());
            var bytes = ip.GetAddressBytes();
            if (bytes.Length != 4) return DataValue.Null;
            return DataValue.FromBigInt(((long)bytes[0] << 24) | ((long)bytes[1] << 16) | ((long)bytes[2] << 8) | bytes[3]);
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// INET_NTOA(expr) - returns IPv4 address from numeric
/// </summary>
internal sealed class InetNtoaEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public InetNtoaEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            long n = val.ToLong();
            return DataValue.FromVarChar($"{(n >> 24) & 0xFF}.{(n >> 16) & 0xFF}.{(n >> 8) & 0xFF}.{n & 0xFF}");
        }
        catch { return DataValue.Null; }
    }
}

#endregion

#region Miscellaneous Functions

/// <summary>
/// SLEEP(seconds) - pauses for specified seconds, returns 0
/// </summary>
internal sealed class SleepEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _seconds;
    public SleepEvaluator(IExpressionEvaluator seconds) { _seconds = seconds; }

    public DataValue Evaluate(Row row)
    {
        var val = _seconds.Evaluate(row);
        if (val.IsNull) return DataValue.FromInt(0);
        var ms = (int)(val.ToDouble() * 1000);
        if (ms > 0) Thread.Sleep(Math.Min(ms, 300000)); // max 5 minutes
        return DataValue.FromInt(0);
    }
}

/// <summary>
/// BENCHMARK(count, expr) - executes expression count times, returns 0
/// </summary>
internal sealed class BenchmarkEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _count;
    private readonly IExpressionEvaluator _expr;

    public BenchmarkEvaluator(IExpressionEvaluator count, IExpressionEvaluator expr) { _count = count; _expr = expr; }

    public DataValue Evaluate(Row row)
    {
        var c = _count.Evaluate(row);
        if (c.IsNull) return DataValue.FromInt(0);
        int count = Math.Min((int)c.ToLong(), 1000000); // safety limit
        for (int i = 0; i < count; i++)
            _expr.Evaluate(row);
        return DataValue.FromInt(0);
    }
}

/// <summary>
/// ANY_VALUE(expr) - suppresses ONLY_FULL_GROUP_BY rejection
/// </summary>
internal sealed class AnyValueEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _expr;
    public AnyValueEvaluator(IExpressionEvaluator expr) { _expr = expr; }
    public DataValue Evaluate(Row row) => _expr.Evaluate(row);
}

/// <summary>
/// BIT_COUNT(n) - returns number of set bits
/// </summary>
internal sealed class BitCountEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public BitCountEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        return DataValue.FromInt(System.Numerics.BitOperations.PopCount((ulong)val.ToLong()));
    }
}

/// <summary>
/// NULLIF(expr1, expr2) - returns NULL if expr1 = expr2, else expr1
/// </summary>
internal sealed class NullIfEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _expr1;
    private readonly IExpressionEvaluator _expr2;

    public NullIfEvaluator(IExpressionEvaluator expr1, IExpressionEvaluator expr2) { _expr1 = expr1; _expr2 = expr2; }

    public DataValue Evaluate(Row row)
    {
        var v1 = _expr1.Evaluate(row);
        var v2 = _expr2.Evaluate(row);
        return v1 == v2 ? DataValue.Null : v1;
    }
}

/// <summary>
/// GREATEST(val1, val2, ...) / LEAST(val1, val2, ...)
/// </summary>
internal sealed class GreatestLeastEvaluator : IExpressionEvaluator
{
    private readonly List<IExpressionEvaluator> _args;
    private readonly bool _greatest;

    public GreatestLeastEvaluator(List<IExpressionEvaluator> args, bool greatest)
    {
        _args = args; _greatest = greatest;
    }

    public DataValue Evaluate(Row row)
    {
        DataValue? result = null;
        foreach (var arg in _args)
        {
            var val = arg.Evaluate(row);
            if (val.IsNull) return DataValue.Null; // MySQL returns NULL if any arg is NULL
            if (result == null)
                result = val;
            else
            {
                int cmp = val.CompareTo(result.Value);
                if (_greatest ? cmp > 0 : cmp < 0)
                    result = val;
            }
        }
        return result ?? DataValue.Null;
    }
}

/// <summary>
/// CAST(expr AS type) / CONVERT(expr, type)
/// </summary>
internal sealed class CastEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _expr;
    private readonly DataType _targetType;

    public CastEvaluator(IExpressionEvaluator expr, DataType targetType) { _expr = expr; _targetType = targetType; }

    public DataValue Evaluate(Row row)
    {
        var val = _expr.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            return _targetType switch
            {
                DataType.Int => DataValue.FromInt((int)val.ToLong()),
                DataType.BigInt => DataValue.FromBigInt(val.ToLong()),
                DataType.Float => DataValue.FromFloat((float)val.ToDouble()),
                DataType.Double => DataValue.FromDouble(val.ToDouble()),
                DataType.Decimal => DataValue.FromDecimal((decimal)val.ToDouble()),
                DataType.VarChar or DataType.Char or DataType.Text => DataValue.FromVarChar(val.AsString()),
                DataType.DateTime => DataValue.FromDateTime(val.ToDateTime()),
                DataType.Date => DataValue.FromDate(DateOnly.FromDateTime(val.ToDateTime())),
                DataType.Time => DataValue.FromTime(TimeOnly.FromDateTime(val.ToDateTime())),
                _ => DataValue.FromVarChar(val.AsString())
            };
        }
        catch { return DataValue.Null; }
    }
}

/// <summary>
/// UNCOMPRESSED_LENGTH(compressed_str)
/// </summary>
internal sealed class UncompressedLengthEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public UncompressedLengthEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        try
        {
            var compressed = Convert.FromBase64String(val.AsString());
            using var ms = new System.IO.MemoryStream(compressed);
            using var gz = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress);
            using var output = new System.IO.MemoryStream();
            gz.CopyTo(output);
            return DataValue.FromBigInt(output.Length);
        }
        catch { return DataValue.FromBigInt(0); }
    }
}

/// <summary>
/// CHARSET(str) - returns character set name
/// </summary>
internal sealed class CharsetFuncEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public CharsetFuncEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.FromVarChar("binary");
        return DataValue.FromVarChar("utf8mb4");
    }
}

/// <summary>
/// COERCIBILITY(str) - returns coercibility value
/// </summary>
internal sealed class CoercibilityEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public CoercibilityEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row) => DataValue.FromInt(2); // Default coercibility
}

/// <summary>
/// COLLATION(str) - returns collation name
/// </summary>
internal sealed class CollationFuncEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public CollationFuncEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        var val = _arg.Evaluate(row);
        if (val.IsNull) return DataValue.FromVarChar("binary");
        return DataValue.FromVarChar("utf8mb4_general_ci");
    }
}

/// <summary>
/// GROUPING(expr) - returns 1 for super-aggregate row, 0 otherwise
/// </summary>
internal sealed class GroupingEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public GroupingEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row)
    {
        // Simplified: always return 0 (no ROLLUP support yet)
        return DataValue.FromInt(0);
    }
}

/// <summary>
/// VALUES(col) - for INSERT ... ON DUPLICATE KEY UPDATE contexts
/// </summary>
internal sealed class ValuesEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;
    public ValuesEvaluator(IExpressionEvaluator arg) { _arg = arg; }

    public DataValue Evaluate(Row row) => _arg.Evaluate(row);
}

/// <summary>
/// INTERVAL(n, n1, n2, ...) - returns index where n fits
/// </summary>
internal sealed class IntervalEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _n;
    private readonly List<IExpressionEvaluator> _list;

    public IntervalEvaluator(IExpressionEvaluator n, List<IExpressionEvaluator> list) { _n = n; _list = list; }

    public DataValue Evaluate(Row row)
    {
        var nVal = _n.Evaluate(row);
        if (nVal.IsNull) return DataValue.FromInt(-1);
        var d = nVal.ToDouble();
        for (int i = 0; i < _list.Count; i++)
        {
            var v = _list[i].Evaluate(row);
            if (v.IsNull) continue;
            if (d < v.ToDouble()) return DataValue.FromInt(i);
        }
        return DataValue.FromInt(_list.Count);
    }
}

#endregion
