using CyscaleDB.Core.Common;
using CyscaleDB.Core.Transactions;
using Xunit;

namespace CyscaleDB.Tests;

public class ConfigurationTests
{
    [Fact]
    public void DefaultConfiguration_ShouldHaveExpectedValues()
    {
        // Arrange & Act
        var config = new CyscaleDbConfiguration();

        // Assert
        Assert.Equal(1024, config.BufferPoolSizePages);
        Assert.Equal(5.0 / 8.0, config.BufferPoolYoungRatio);
        Assert.Equal(1000, config.RecursiveCteMaxIterations);
        Assert.Equal(IsolationLevel.RepeatableRead, config.DefaultIsolationLevel);
        Assert.Equal(5000, config.LockWaitTimeoutMs);
        Assert.True(config.EnableOnlineDdl);
        Assert.True(config.EnableMetrics);
    }

    [Fact]
    public void Validate_WithValidConfig_ShouldReturnNoErrors()
    {
        // Arrange
        var config = new CyscaleDbConfiguration();

        // Act
        var errors = config.Validate();

        // Assert
        Assert.Empty(errors);
    }

    [Fact]
    public void Validate_WithInvalidBufferPoolSize_ShouldReturnError()
    {
        // Arrange
        var config = new CyscaleDbConfiguration
        {
            BufferPoolSizePages = 8  // Less than minimum of 16
        };

        // Act
        var errors = config.Validate();

        // Assert
        Assert.Contains(errors, e => e.Contains("BufferPoolSizePages"));
    }

    [Fact]
    public void Validate_WithInvalidYoungRatio_ShouldReturnError()
    {
        // Arrange
        var config = new CyscaleDbConfiguration
        {
            BufferPoolYoungRatio = 0.05  // Less than 0.1
        };

        // Act
        var errors = config.Validate();

        // Assert
        Assert.Contains(errors, e => e.Contains("BufferPoolYoungRatio"));
    }

    [Fact]
    public void ToJson_ShouldProduceValidJson()
    {
        // Arrange
        var config = new CyscaleDbConfiguration();

        // Act
        var json = config.ToJson();

        // Assert
        Assert.NotNull(json);
        Assert.Contains("BufferPoolSizePages", json);
        Assert.Contains("EnableOnlineDdl", json);
    }

    [Fact]
    public void FromJson_ShouldDeserializeCorrectly()
    {
        // Arrange
        var json = """
        {
          "BufferPoolSizePages": 2048,
          "RecursiveCteMaxIterations": 500,
          "DefaultIsolationLevel": "ReadCommitted",
          "EnableSlowQueryLog": false
        }
        """;

        // Act
        var config = CyscaleDbConfiguration.FromJson(json);

        // Assert
        Assert.Equal(2048, config.BufferPoolSizePages);
        Assert.Equal(500, config.RecursiveCteMaxIterations);
        Assert.Equal(IsolationLevel.ReadCommitted, config.DefaultIsolationLevel);
        Assert.False(config.EnableSlowQueryLog);
    }

    [Fact]
    public void SaveAndLoad_ShouldRoundTrip()
    {
        // Arrange
        var tempFile = Path.GetTempFileName();
        var original = new CyscaleDbConfiguration
        {
            BufferPoolSizePages = 4096,
            SlowQueryThresholdMs = 500,
            EnableMetrics = false
        };

        try
        {
            // Act
            original.SaveToFile(tempFile);
            var loaded = CyscaleDbConfiguration.LoadFromFile(tempFile);

            // Assert
            Assert.Equal(original.BufferPoolSizePages, loaded.BufferPoolSizePages);
            Assert.Equal(original.SlowQueryThresholdMs, loaded.SlowQueryThresholdMs);
            Assert.Equal(original.EnableMetrics, loaded.EnableMetrics);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public void CreateProductionConfig_ShouldHaveProductionValues()
    {
        // Act
        var config = CyscaleDbConfiguration.CreateProductionConfig();

        // Assert
        Assert.Equal(8192, config.BufferPoolSizePages);  // 128MB
        Assert.True(config.EnableSlowQueryLog);
        Assert.Equal(500, config.SlowQueryThresholdMs);
        Assert.Equal(LogLevel.Warning, config.MinimumLogLevel);
    }

    [Fact]
    public void CreateDevelopmentConfig_ShouldHaveDevelopmentValues()
    {
        // Act
        var config = CyscaleDbConfiguration.CreateDevelopmentConfig();

        // Assert
        Assert.Equal(512, config.BufferPoolSizePages);  // 8MB
        Assert.True(config.EnableSlowQueryLog);
        Assert.Equal(100, config.SlowQueryThresholdMs);
        Assert.Equal(LogLevel.Debug, config.MinimumLogLevel);
    }

    [Fact]
    public void CreateTestConfig_ShouldHaveTestValues()
    {
        // Act
        var config = CyscaleDbConfiguration.CreateTestConfig();

        // Assert
        Assert.Equal(128, config.BufferPoolSizePages);  // 2MB
        Assert.False(config.EnableSlowQueryLog);
        Assert.False(config.EnableMetrics);
        Assert.Equal(LogLevel.Warning, config.MinimumLogLevel);
    }
}
