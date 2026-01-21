using CyscaleDB.Core.Monitoring;
using Xunit;

namespace CyscaleDB.Tests;

public class MetricsTests
{
    [Fact]
    public void Counter_Increment_ShouldIncreaseValue()
    {
        // Arrange
        var counter = new Counter("test_counter");

        // Act
        counter.Increment();
        counter.Increment();
        counter.Increment(5);

        // Assert
        Assert.Equal(7, counter.Value);
    }

    [Fact]
    public void Counter_Reset_ShouldSetToZero()
    {
        // Arrange
        var counter = new Counter("test_counter");
        counter.Increment(10);

        // Act
        counter.Reset();

        // Assert
        Assert.Equal(0, counter.Value);
    }

    [Fact]
    public void Gauge_Set_ShouldUpdateValue()
    {
        // Arrange
        var gauge = new Gauge("test_gauge");

        // Act
        gauge.Set(42.5);

        // Assert
        Assert.Equal(42.5, gauge.Value);
    }

    [Fact]
    public void Gauge_Increment_ShouldIncreaseValue()
    {
        // Arrange
        var gauge = new Gauge("test_gauge");
        gauge.Set(10.0);

        // Act
        gauge.Increment(5.5);

        // Assert
        Assert.Equal(15.5, gauge.Value, precision: 2);
    }

    [Fact]
    public void Gauge_Decrement_ShouldDecreaseValue()
    {
        // Arrange
        var gauge = new Gauge("test_gauge");
        gauge.Set(10.0);

        // Act
        gauge.Decrement(3.0);

        // Assert
        Assert.Equal(7.0, gauge.Value, precision: 2);
    }

    [Fact]
    public void Histogram_Record_ShouldCalculateStatistics()
    {
        // Arrange
        var histogram = new Histogram("test_histogram");

        // Act
        histogram.Record(10.0);
        histogram.Record(20.0);
        histogram.Record(30.0);
        histogram.Record(40.0);
        histogram.Record(50.0);

        // Assert
        Assert.Equal(5, histogram.Count);
        Assert.Equal(10.0, histogram.Min);
        Assert.Equal(50.0, histogram.Max);
        Assert.Equal(30.0, histogram.Mean);
        Assert.Equal(30.0, histogram.P50);  // Median
    }

    [Fact]
    public void Histogram_P95_ShouldCalculateCorrectly()
    {
        // Arrange
        var histogram = new Histogram("test_histogram");
        
        // Add 100 values
        for (int i = 1; i <= 100; i++)
        {
            histogram.Record(i);
        }

        // Act
        var p95 = histogram.P95;

        // Assert
        Assert.True(p95 >= 94 && p95 <= 96);  // Should be around 95
    }

    [Fact]
    public void Histogram_Reset_ShouldClearValues()
    {
        // Arrange
        var histogram = new Histogram("test_histogram");
        histogram.Record(10.0);
        histogram.Record(20.0);
        histogram.Record(30.0);

        // Act
        histogram.Reset();

        // Assert
        Assert.Equal(0, histogram.Count);
        Assert.Equal(0, histogram.Min);
        Assert.Equal(0, histogram.Max);
    }

    [Fact]
    public void MetricsCollector_Instance_ShouldBeSingleton()
    {
        // Act
        var instance1 = MetricsCollector.Instance;
        var instance2 = MetricsCollector.Instance;

        // Assert
        Assert.Same(instance1, instance2);
    }

    [Fact]
    public void MetricsCollector_QueriesExecuted_ShouldIncrement()
    {
        // Arrange
        var metrics = MetricsCollector.Instance;
        var initialValue = metrics.QueriesExecuted.Value;

        // Act
        metrics.QueriesExecuted.Increment();

        // Assert
        Assert.Equal(initialValue + 1, metrics.QueriesExecuted.Value);
    }

    [Fact]
    public void MetricsCollector_RecordQuery_ShouldUpdateMetrics()
    {
        // Arrange
        var metrics = MetricsCollector.Instance;
        var initialQueries = metrics.QueriesExecuted.Value;
        var sql = "SELECT * FROM users";
        var duration = TimeSpan.FromMilliseconds(150);

        // Act
        metrics.RecordQuery(sql, duration, null);

        // Assert
        Assert.Equal(initialQueries + 1, metrics.QueriesExecuted.Value);
    }

    [Fact]
    public void MetricsCollector_GetAllCounters_ShouldReturnDictionary()
    {
        // Arrange
        var metrics = MetricsCollector.Instance;

        // Act
        var counters = metrics.GetAllCounters();

        // Assert
        Assert.NotNull(counters);
        Assert.True(counters.Count > 0);
        Assert.Contains("queries_executed", counters.Keys);
    }

    [Fact]
    public void MetricsCollector_GetAllHistograms_ShouldReturnStatistics()
    {
        // Arrange
        var metrics = MetricsCollector.Instance;
        metrics.QueryExecutionTime.Record(100);
        metrics.QueryExecutionTime.Record(200);

        // Act
        var histograms = metrics.GetAllHistograms();

        // Assert
        Assert.NotNull(histograms);
        Assert.Contains("query_execution_time_ms", histograms.Keys);
        
        var stats = histograms["query_execution_time_ms"];
        Assert.True(stats.Count >= 2);
        Assert.True(stats.Mean > 0);
    }

    [Fact]
    public void MetricsCollector_ThreadSafety_ShouldHandleConcurrentUpdates()
    {
        // Arrange
        var metrics = MetricsCollector.Instance;
        var counter = new Counter("concurrent_test");

        // Act
        var tasks = new List<Task>();
        for (int i = 0; i < 10; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 100; j++)
                {
                    counter.Increment();
                }
            }));
        }
        Task.WaitAll(tasks.ToArray());

        // Assert
        Assert.Equal(1000, counter.Value);
    }
}
