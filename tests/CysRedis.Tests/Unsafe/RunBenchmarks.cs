using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Validators;

namespace CysRedis.Tests.Unsafe;

/// <summary>
/// Entry point for running performance benchmarks.
/// Run with: dotnet run --project tests/CysRedis.Tests/CysRedis.Tests.csproj -c Release
/// </summary>
public class RunBenchmarks
{
    public static void Main(string[] args)
    {
        Console.WriteLine("===========================================");
        Console.WriteLine("  CysRedis Performance Comparison");
        Console.WriteLine("  Managed vs Unsafe Implementations");
        Console.WriteLine("===========================================\n");
        
        var config = DefaultConfig.Instance
            .AddExporter(MarkdownExporter.Default)
            .AddExporter(HtmlExporter.Default)
            .AddExporter(BenchmarkDotNet.Exporters.Csv.CsvExporter.Default)
            .AddLogger(ConsoleLogger.Default)
            .AddValidator(JitOptimizationsValidator.DontFailOnError)
            .WithSummaryStyle(BenchmarkDotNet.Reports.SummaryStyle.Default.WithRatioStyle(BenchmarkDotNet.Columns.RatioStyle.Trend));

        var summary = BenchmarkRunner.Run(typeof(PerformanceComparisonBenchmarks), config);
        
        Console.WriteLine("\n=== Benchmark Summary ===");
        Console.WriteLine($"Total benchmarks: {summary.Reports.Count()}");
        Console.WriteLine($"Reports saved to: {summary.ResultsDirectoryPath}");
        
        var resultsDir = Path.Combine(summary.ResultsDirectoryPath, "BenchmarkDotNet.Artifacts", "results");
        Console.WriteLine("\nKey files:");
        Console.WriteLine($"  - HTML Report: {Path.Combine(resultsDir, "PerformanceComparisonBenchmarks-report.html")}");
        Console.WriteLine($"  - Markdown Report: {Path.Combine(resultsDir, "PerformanceComparisonBenchmarks-report.md")}");
        Console.WriteLine($"  - CSV Report: {Path.Combine(resultsDir, "PerformanceComparisonBenchmarks-report.csv")}");
        
        // 显示性能对比摘要
        Console.WriteLine("\n=== Performance Comparison Summary ===");
        var reports = summary.Reports.Where(r => r.ResultStatistics != null).ToList();
        
        if (reports.Count > 0)
        {
            Console.WriteLine("\nTop Performance Improvements (Unsafe vs Managed):");
            var improvements = new List<(string Name, double Speedup)>();
            
            foreach (var unsafeReport in reports.Where(r => r.BenchmarkCase.DisplayInfo.Contains("Unsafe")))
            {
                var managedName = unsafeReport.BenchmarkCase.DisplayInfo.Replace("Unsafe", "Managed");
                var managedReport = reports.FirstOrDefault(r => r.BenchmarkCase.DisplayInfo == managedName);
                
                if (managedReport != null && managedReport.ResultStatistics != null && unsafeReport.ResultStatistics != null)
                {
                    var ratio = unsafeReport.ResultStatistics.Mean / managedReport.ResultStatistics.Mean;
                    var speedup = (1.0 / ratio - 1.0) * 100;
                    improvements.Add((unsafeReport.BenchmarkCase.DisplayInfo, speedup));
                }
            }
            
            foreach (var imp in improvements.OrderBy(x => x.Speedup).Take(10))
            {
                Console.WriteLine($"  {imp.Name}: {imp.Speedup:F1}% faster");
            }
        }
    }
}
