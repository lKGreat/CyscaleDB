using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using CysRedis.Core.Common;

namespace CysRedis.Core.Scripting;

/// <summary>
/// Manages Lua scripts and their execution.
/// Note: This is a simplified implementation. Full Lua support would require
/// integrating a Lua interpreter like MoonSharp or NLua.
/// </summary>
public class ScriptManager
{
    private readonly ConcurrentDictionary<string, string> _scripts;

    public ScriptManager()
    {
        _scripts = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Loads a script and returns its SHA1 hash.
    /// </summary>
    public string Load(string script)
    {
        var sha1 = ComputeSha1(script);
        _scripts[sha1] = script;
        return sha1;
    }

    /// <summary>
    /// Gets a script by its SHA1 hash.
    /// </summary>
    public string? GetScript(string sha1)
    {
        return _scripts.TryGetValue(sha1, out var script) ? script : null;
    }

    /// <summary>
    /// Checks if scripts exist.
    /// </summary>
    public bool[] Exists(string[] sha1s)
    {
        var results = new bool[sha1s.Length];
        for (int i = 0; i < sha1s.Length; i++)
        {
            results[i] = _scripts.ContainsKey(sha1s[i]);
        }
        return results;
    }

    /// <summary>
    /// Flushes all loaded scripts.
    /// </summary>
    public void Flush()
    {
        _scripts.Clear();
    }

    /// <summary>
    /// Computes SHA1 hash of a script.
    /// </summary>
    public static string ComputeSha1(string script)
    {
        var bytes = Encoding.UTF8.GetBytes(script);
        var hash = SHA1.HashData(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    /// <summary>
    /// Executes a Lua script.
    /// Note: This is a placeholder. Real implementation would use a Lua interpreter.
    /// </summary>
    public object? Execute(string script, string[] keys, string[] args, 
        Func<string[], object?> redisCall)
    {
        // Simplified: Only support basic return values
        // A full implementation would integrate MoonSharp or NLua
        
        // For now, just return nil - users would need to add a proper Lua engine
        Logger.Warning("Lua scripting is not fully implemented. Script: {0}", 
            script.Length > 50 ? script[..50] + "..." : script);
        
        return null;
    }
}
