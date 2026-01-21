using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using CysRedis.Core.Common;
using MoonSharp.Interpreter;

namespace CysRedis.Core.Scripting;

/// <summary>
/// Manages Lua scripts and their execution using MoonSharp.
/// </summary>
public class ScriptManager
{
    private readonly ConcurrentDictionary<string, string> _scripts;
    private readonly ConcurrentDictionary<string, Script> _compiledScripts;

    public ScriptManager()
    {
        _scripts = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        _compiledScripts = new ConcurrentDictionary<string, Script>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Loads a script and returns its SHA1 hash.
    /// </summary>
    public string Load(string script)
    {
        var sha1 = ComputeSha1(script);
        _scripts[sha1] = script;
        
        // 预编译脚本
        try
        {
            var luaScript = new Script();
            luaScript.DoString(script);
            _compiledScripts[sha1] = luaScript;
        }
        catch (Exception ex)
        {
            Logger.Warning("Failed to precompile script {0}: {1}", sha1, ex.Message);
        }
        
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
        _compiledScripts.Clear();
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
    /// </summary>
    public object? Execute(
        string script, 
        string[] keys, 
        string[] args,
        Func<string[], Task<object?>> redisCall,
        Func<string[], Task<object?>> redisPcall,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // 创建Lua环境
            var luaScript = new Script();
            
            // 设置全局变量 KEYS 和 ARGV
            var keysTable = new Table(luaScript);
            for (int i = 0; i < keys.Length; i++)
            {
                keysTable[i + 1] = keys[i];
            }
            luaScript.Globals["KEYS"] = keysTable;
            
            var argvTable = new Table(luaScript);
            for (int i = 0; i < args.Length; i++)
            {
                argvTable[i + 1] = args[i];
            }
            luaScript.Globals["ARGV"] = argvTable;
            
            // 注册 redis.call 函数
            luaScript.Globals["redis"] = new RedisLuaApi(redisCall, redisPcall);
            
            // 执行脚本
            var result = luaScript.DoString(script);
            
            // 转换结果
            return ConvertLuaResult(result);
        }
        catch (ScriptRuntimeException ex)
        {
            throw new RedisException($"ERR Error running script: {ex.DecoratedMessage}");
        }
        catch (Exception ex)
        {
            throw new RedisException($"ERR Error running script: {ex.Message}");
        }
    }

    /// <summary>
    /// Executes a loaded script by SHA1.
    /// </summary>
    public object? ExecuteSha(
        string sha1,
        string[] keys,
        string[] args,
        Func<string[], Task<object?>> redisCall,
        Func<string[], Task<object?>> redisPcall,
        CancellationToken cancellationToken = default)
    {
        var script = GetScript(sha1);
        if (script == null)
            throw new RedisException("NOSCRIPT No matching script. Please use EVAL.");
        
        return Execute(script, keys, args, redisCall, redisPcall, cancellationToken);
    }

    /// <summary>
    /// Converts MoonSharp DynValue to .NET object.
    /// </summary>
    private static object? ConvertLuaResult(DynValue value)
    {
        return value.Type switch
        {
            DataType.Nil or DataType.Void => null,
            DataType.Boolean => value.Boolean,
            DataType.Number => value.Number,
            DataType.String => value.String,
            DataType.Table => ConvertLuaTable(value.Table),
            _ => value.ToObject()
        };
    }

    /// <summary>
    /// Converts Lua table to array or dictionary.
    /// </summary>
    private static object? ConvertLuaTable(Table table)
    {
        // Check if it's an array (sequential numeric keys starting from 1)
        var keys = table.Keys.ToList();
        if (keys.All(k => k.Type == DataType.Number))
        {
            var numKeys = keys.Select(k => (int)k.Number).OrderBy(k => k).ToList();
            if (numKeys.Count > 0 && numKeys[0] == 1 && numKeys.SequenceEqual(Enumerable.Range(1, numKeys.Count)))
            {
                // It's an array
                var array = new object?[numKeys.Count];
                for (int i = 0; i < numKeys.Count; i++)
                {
                    var element = table.Get(i + 1);
                    array[i] = ConvertLuaResult(element);
                }
                return array;
            }
        }

        // It's a dictionary
        var dict = new Dictionary<string, object?>();
        foreach (var pair in table.Pairs)
        {
            var key = pair.Key.Type == DataType.String 
                ? pair.Key.String 
                : pair.Key.ToObject()?.ToString() ?? "";
            dict[key] = ConvertLuaResult(pair.Value);
        }
        return dict;
    }
}

/// <summary>
/// Redis Lua API implementation for redis.call and redis.pcall.
/// </summary>
public class RedisLuaApi
{
    private readonly Func<string[], Task<object?>> _call;
    private readonly Func<string[], Task<object?>> _pcall;

    public RedisLuaApi(Func<string[], Task<object?>> call, Func<string[], Task<object?>> pcall)
    {
        _call = call;
        _pcall = pcall;
    }

    /// <summary>
    /// redis.call() - 执行Redis命令，失败时抛出异常
    /// </summary>
    public object? call(params DynValue[] args)
    {
        var cmdArgs = args.Select(a => a.CastToString()).ToArray();
        try
        {
            var result = _call(cmdArgs).GetAwaiter().GetResult();
            return result;
        }
        catch (RedisException ex)
        {
            throw new ScriptRuntimeException(ex.Message);
        }
    }

    /// <summary>
    /// redis.pcall() - 执行Redis命令，失败时返回错误
    /// </summary>
    public object? pcall(params DynValue[] args)
    {
        var cmdArgs = args.Select(a => a.CastToString()).ToArray();
        try
        {
            var result = _pcall(cmdArgs).GetAwaiter().GetResult();
            return result;
        }
        catch (RedisException ex)
        {
            // 返回错误表
            return new { err = ex.Message };
        }
    }

    /// <summary>
    /// redis.status_reply() - 创建状态回复
    /// </summary>
    public object status_reply(DynValue status)
    {
        return new { ok = status.CastToString() };
    }

    /// <summary>
    /// redis.error_reply() - 创建错误回复
    /// </summary>
    public object error_reply(DynValue error)
    {
        return new { err = error.CastToString() };
    }

    /// <summary>
    /// redis.log() - 记录日志
    /// </summary>
    public void log(DynValue level, DynValue message)
    {
        var levelInt = (int)(level.Number);
        var messageStr = message.CastToString();
        
        switch (levelInt)
        {
            case 0: // LOG_DEBUG
                Logger.Debug("Lua: {0}", messageStr);
                break;
            case 1: // LOG_VERBOSE
                Logger.Info("Lua: {0}", messageStr);
                break;
            case 2: // LOG_NOTICE
                Logger.Info("Lua: {0}", messageStr);
                break;
            case 3: // LOG_WARNING
                Logger.Warning("Lua: {0}", messageStr);
                break;
        }
    }
}
