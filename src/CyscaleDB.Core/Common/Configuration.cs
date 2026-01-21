using System.Text.Json;
using System.Text.Json.Serialization;
using CyscaleDB.Core.Transactions;

namespace CyscaleDB.Core.Common;

/// <summary>
/// Configuration for CyscaleDB database engine.
/// Provides centralized management of all configurable parameters.
/// </summary>
public sealed class CyscaleDbConfiguration
{
    // Buffer Pool Configuration
    public int BufferPoolSizePages { get; set; } = 1024;  // Default 16MB (16KB per page)
    public double BufferPoolYoungRatio { get; set; } = 5.0 / 8.0;  // 62.5% young region
    
    // CTE Configuration
    public int RecursiveCteMaxIterations { get; set; } = 1000;
    
    // Transaction Configuration
    public IsolationLevel DefaultIsolationLevel { get; set; } = IsolationLevel.RepeatableRead;
    
    // Lock Configuration
    public int LockWaitTimeoutMs { get; set; } = 5000;  // 5 seconds
    public int DeadlockCheckIntervalMs { get; set; } = 1000;  // 1 second
    
    // Logging Configuration
    public LogLevel MinimumLogLevel { get; set; } = LogLevel.Info;
    public bool EnableSlowQueryLog { get; set; } = false;
    public int SlowQueryThresholdMs { get; set; } = 1000;  // 1 second
    public string SlowQueryLogPath { get; set; } = "cyscaledb-slow.log";
    
    // Checkpoint Configuration
    public int CheckpointIntervalSeconds { get; set; } = 300;  // 5 minutes
    public int CheckpointMaxDirtyPages { get; set; } = 100;
    
    // WAL Configuration
    public long WalSegmentSizeBytes { get; set; } = 16 * 1024 * 1024;  // 16MB
    public int WalBufferSizeBytes { get; set; } = 256 * 1024;  // 256KB
    public bool WalSyncAfterWrite { get; set; } = true;
    
    // Online DDL Configuration
    public bool EnableOnlineDdl { get; set; } = true;
    public int OnlineDdlMaxConcurrentOperations { get; set; } = 1;
    
    // Doublewrite Buffer Configuration
    public bool EnableDoublewriteBuffer { get; set; } = true;
    
    // Read Ahead Configuration
    public bool EnableReadAhead { get; set; } = true;
    public int ReadAheadPages { get; set; } = 32;
    
    // Query Optimization Configuration
    public bool EnableIndexScan { get; set; } = true;
    public bool EnableIndexOnlyScans { get; set; } = true;
    
    // MVCC Configuration
    public int MvccMaxVersionsPerRow { get; set; } = 100;
    
    // Performance Monitoring Configuration
    public bool EnableMetrics { get; set; } = true;
    public int MetricsUpdateIntervalMs { get; set; } = 1000;  // 1 second
    
    /// <summary>
    /// Loads configuration from a JSON file.
    /// </summary>
    public static CyscaleDbConfiguration LoadFromFile(string path)
    {
        if (!File.Exists(path))
        {
            var defaultConfig = new CyscaleDbConfiguration();
            defaultConfig.SaveToFile(path);
            return defaultConfig;
        }

        var json = File.ReadAllText(path);
        return FromJson(json);
    }

    /// <summary>
    /// Saves configuration to a JSON file.
    /// </summary>
    public void SaveToFile(string path)
    {
        var json = ToJson();
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }
        File.WriteAllText(path, json);
    }

    /// <summary>
    /// Deserializes configuration from JSON.
    /// </summary>
    public static CyscaleDbConfiguration FromJson(string json)
    {
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            WriteIndented = true,
            Converters = { new JsonStringEnumConverter() }
        };

        return JsonSerializer.Deserialize<CyscaleDbConfiguration>(json, options) 
               ?? new CyscaleDbConfiguration();
    }

    /// <summary>
    /// Serializes configuration to JSON.
    /// </summary>
    public string ToJson()
    {
        var options = new JsonSerializerOptions
        {
            WriteIndented = true,
            Converters = { new JsonStringEnumConverter() }
        };

        return JsonSerializer.Serialize(this, options);
    }

    /// <summary>
    /// Validates the configuration and returns validation errors.
    /// </summary>
    public List<string> Validate()
    {
        var errors = new List<string>();

        if (BufferPoolSizePages < 16)
            errors.Add("BufferPoolSizePages must be at least 16");

        if (BufferPoolYoungRatio is < 0.1 or > 0.9)
            errors.Add("BufferPoolYoungRatio must be between 0.1 and 0.9");

        if (RecursiveCteMaxIterations < 10)
            errors.Add("RecursiveCteMaxIterations must be at least 10");

        if (LockWaitTimeoutMs < 0)
            errors.Add("LockWaitTimeoutMs cannot be negative");

        if (SlowQueryThresholdMs < 0)
            errors.Add("SlowQueryThresholdMs cannot be negative");

        if (CheckpointIntervalSeconds < 10)
            errors.Add("CheckpointIntervalSeconds must be at least 10");

        if (WalSegmentSizeBytes < 1024 * 1024)
            errors.Add("WalSegmentSizeBytes must be at least 1MB");

        if (OnlineDdlMaxConcurrentOperations < 1)
            errors.Add("OnlineDdlMaxConcurrentOperations must be at least 1");

        return errors;
    }

    /// <summary>
    /// Creates a default configuration with recommended production settings.
    /// </summary>
    public static CyscaleDbConfiguration CreateProductionConfig()
    {
        return new CyscaleDbConfiguration
        {
            BufferPoolSizePages = 8192,  // 128MB
            EnableSlowQueryLog = true,
            SlowQueryThresholdMs = 500,
            CheckpointIntervalSeconds = 600,  // 10 minutes
            EnableMetrics = true,
            MinimumLogLevel = LogLevel.Warning
        };
    }

    /// <summary>
    /// Creates a configuration optimized for development.
    /// </summary>
    public static CyscaleDbConfiguration CreateDevelopmentConfig()
    {
        return new CyscaleDbConfiguration
        {
            BufferPoolSizePages = 512,  // 8MB
            EnableSlowQueryLog = true,
            SlowQueryThresholdMs = 100,
            MinimumLogLevel = LogLevel.Debug,
            EnableMetrics = true
        };
    }

    /// <summary>
    /// Creates a configuration optimized for testing.
    /// </summary>
    public static CyscaleDbConfiguration CreateTestConfig()
    {
        return new CyscaleDbConfiguration
        {
            BufferPoolSizePages = 128,  // 2MB
            EnableSlowQueryLog = false,
            MinimumLogLevel = LogLevel.Warning,
            CheckpointIntervalSeconds = 30,
            EnableMetrics = false
        };
    }

    public override string ToString()
    {
        return $"CyscaleDbConfiguration(BufferPool={BufferPoolSizePages}pages, " +
               $"SlowQuery={EnableSlowQueryLog}, Metrics={EnableMetrics})";
    }
}
