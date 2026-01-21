using CysRedis.Core.Common;
using CysRedis.Core.Monitoring;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Commands;

/// <summary>
/// LATENCY command handler.
/// Manages the latency monitoring subsystem.
/// </summary>
public class LatencyCommand : ICommandHandler
{
    public async Task ExecuteAsync(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(1);
        var subCommand = context.GetArg(0).ToUpperInvariant();

        switch (subCommand)
        {
            case "DOCTOR":
                await HandleDoctor(context, cancellationToken);
                break;
            case "HISTORY":
                await HandleHistory(context, cancellationToken);
                break;
            case "LATEST":
                await HandleLatest(context, cancellationToken);
                break;
            case "RESET":
                await HandleReset(context, cancellationToken);
                break;
            case "GRAPH":
                await HandleGraph(context, cancellationToken);
                break;
            case "HISTOGRAM":
                await HandleHistogram(context, cancellationToken);
                break;
            case "HELP":
                await HandleHelp(context, cancellationToken);
                break;
            default:
                await context.Client.WriteErrorAsync(
                    $"ERR Unknown subcommand or wrong number of arguments for '{subCommand}'",
                    cancellationToken);
                break;
        }
    }

    private static async Task HandleDoctor(CommandContext context, CancellationToken cancellationToken)
    {
        var report = context.Server.LatencyMonitor.GenerateDoctorReport();
        await context.Client.WriteBulkStringAsync(report, cancellationToken);
    }

    private static async Task HandleHistory(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var eventType = context.GetArg(1);
        var samples = context.Server.LatencyMonitor.GetHistory(eventType);

        var result = new RespValue[samples.Length];
        for (int i = 0; i < samples.Length; i++)
        {
            // Each entry: [timestamp, latency_ms]
            result[i] = RespValue.Array(
                new RespValue(samples[i].Timestamp),
                new RespValue(samples[i].LatencyMs)
            );
        }

        await context.Client.WriteResponseAsync(RespValue.Array(result), cancellationToken);
    }

    private static async Task HandleLatest(CommandContext context, CancellationToken cancellationToken)
    {
        var latest = context.Server.LatencyMonitor.GetLatest();

        var result = new List<RespValue>();
        foreach (var (eventType, sample) in latest)
        {
            if (sample != null)
            {
                // Each entry: [event_name, timestamp, latency_ms]
                result.Add(RespValue.Array(
                    RespValue.BulkString(eventType),
                    new RespValue(sample.Timestamp),
                    new RespValue(sample.LatencyMs)
                ));
            }
        }

        await context.Client.WriteResponseAsync(RespValue.Array(result.ToArray()), cancellationToken);
    }

    private static async Task HandleReset(CommandContext context, CancellationToken cancellationToken)
    {
        if (context.ArgCount > 1)
        {
            // Reset specific events
            for (int i = 1; i < context.ArgCount; i++)
            {
                var eventType = context.GetArg(i);
                context.Server.LatencyMonitor.Reset(eventType);
            }
        }
        else
        {
            // Reset all
            context.Server.LatencyMonitor.Reset();
        }

        await context.Client.WriteOkAsync(cancellationToken);
    }

    private static async Task HandleGraph(CommandContext context, CancellationToken cancellationToken)
    {
        context.EnsureMinArgs(2);
        var eventType = context.GetArg(1);
        var samples = context.Server.LatencyMonitor.GetHistory(eventType);

        if (samples.Length == 0)
        {
            await context.Client.WriteBulkStringAsync(
                $"No samples for event '{eventType}'", cancellationToken);
            return;
        }

        // Generate ASCII graph
        var graph = GenerateAsciiGraph(eventType, samples);
        await context.Client.WriteBulkStringAsync(graph, cancellationToken);
    }

    private static async Task HandleHistogram(CommandContext context, CancellationToken cancellationToken)
    {
        var percentiles = context.Server.CommandLatency.GetPercentiles();
        
        var result = new List<RespValue>
        {
            RespValue.BulkString("command"),
            RespValue.Array(
                RespValue.BulkString("calls"),
                new RespValue(percentiles.Count),
                RespValue.BulkString("avg"),
                new RespValue((long)(percentiles.Average * 1000)), // Convert to microseconds
                RespValue.BulkString("p50"),
                new RespValue((long)(percentiles.P50 * 1000)),
                RespValue.BulkString("p95"),
                new RespValue((long)(percentiles.P95 * 1000)),
                RespValue.BulkString("p99"),
                new RespValue((long)(percentiles.P99 * 1000)),
                RespValue.BulkString("min"),
                new RespValue((long)(percentiles.Min * 1000)),
                RespValue.BulkString("max"),
                new RespValue((long)(percentiles.Max * 1000))
            )
        };

        await context.Client.WriteResponseAsync(RespValue.Array(result.ToArray()), cancellationToken);
    }

    private static async Task HandleHelp(CommandContext context, CancellationToken cancellationToken)
    {
        var help = new[]
        {
            "LATENCY DOCTOR",
            "    Return a human readable latency analysis report.",
            "LATENCY HISTORY <event>",
            "    Return time series of latency samples for <event>.",
            "LATENCY LATEST",
            "    Return the latest latency samples for all events.",
            "LATENCY RESET [event ...]",
            "    Reset latency data for one or more events (default: all).",
            "LATENCY GRAPH <event>",
            "    Return an ASCII art graph of the latency samples for <event>.",
            "LATENCY HISTOGRAM",
            "    Return latency histogram statistics.",
            "LATENCY HELP",
            "    Print this help."
        };

        var result = help.Select(line => RespValue.BulkString(line)).ToArray();
        await context.Client.WriteResponseAsync(RespValue.Array(result), cancellationToken);
    }

    private static string GenerateAsciiGraph(string eventType, LatencySample[] samples)
    {
        const int graphHeight = 10;
        const int graphWidth = 60;

        if (samples.Length == 0)
            return $"No data for event: {eventType}";

        var maxLatency = samples.Max(s => s.LatencyMs);
        if (maxLatency == 0) maxLatency = 1;

        var sb = new System.Text.StringBuilder();
        sb.AppendLine($"{eventType} - max latency: {maxLatency}ms");
        sb.AppendLine();

        // Prepare buckets
        var bucketSize = Math.Max(1, samples.Length / graphWidth);
        var buckets = new List<long>();
        for (int i = 0; i < samples.Length; i += bucketSize)
        {
            var bucket = samples.Skip(i).Take(bucketSize).Max(s => s.LatencyMs);
            buckets.Add(bucket);
        }

        // Draw graph
        for (int y = graphHeight; y >= 1; y--)
        {
            var threshold = maxLatency * y / graphHeight;
            sb.Append($"{threshold,6}| ");

            foreach (var bucket in buckets)
            {
                var height = (int)(bucket * graphHeight / maxLatency);
                sb.Append(height >= y ? "#" : " ");
            }
            sb.AppendLine();
        }

        sb.Append("      +");
        sb.AppendLine(new string('-', buckets.Count));

        return sb.ToString();
    }
}
