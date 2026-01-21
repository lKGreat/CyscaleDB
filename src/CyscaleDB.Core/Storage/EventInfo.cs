using CyscaleDB.Core.Parsing.Ast;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Represents metadata about a scheduled event.
/// </summary>
public sealed class EventInfo
{
    /// <summary>
    /// The event ID.
    /// </summary>
    public int EventId { get; }

    /// <summary>
    /// The event name.
    /// </summary>
    public string EventName { get; }

    /// <summary>
    /// The schedule expression (e.g., "EVERY 1 DAY", "AT '2024-01-01 00:00:00'").
    /// </summary>
    public string Schedule { get; }

    /// <summary>
    /// The event body statements.
    /// </summary>
    public List<Statement> Body { get; }

    /// <summary>
    /// Whether ON COMPLETION PRESERVE is specified.
    /// </summary>
    public bool OnCompletionPreserve { get; }

    /// <summary>
    /// Whether the event is enabled.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Comment for the event.
    /// </summary>
    public string? Comment { get; }

    /// <summary>
    /// The definer (user@host).
    /// </summary>
    public string? Definer { get; }

    /// <summary>
    /// When this event was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// The next scheduled execution time.
    /// </summary>
    public DateTime? NextExecutionTime { get; set; }

    /// <summary>
    /// The last execution time.
    /// </summary>
    public DateTime? LastExecutionTime { get; set; }

    /// <summary>
    /// Creates a new event info.
    /// </summary>
    public EventInfo(
        int eventId,
        string eventName,
        string schedule,
        List<Statement> body,
        bool onCompletionPreserve = false,
        bool enabled = true,
        string? comment = null,
        string? definer = null,
        DateTime? createdAt = null)
    {
        EventId = eventId;
        EventName = eventName ?? throw new ArgumentNullException(nameof(eventName));
        Schedule = schedule ?? throw new ArgumentNullException(nameof(schedule));
        Body = body ?? throw new ArgumentNullException(nameof(body));
        OnCompletionPreserve = onCompletionPreserve;
        Enabled = enabled;
        Comment = comment;
        Definer = definer;
        CreatedAt = createdAt ?? DateTime.UtcNow;
        NextExecutionTime = CalculateNextExecutionTime();
    }

    /// <summary>
    /// Calculates the next execution time based on the schedule.
    /// </summary>
    private DateTime? CalculateNextExecutionTime()
    {
        var now = DateTime.UtcNow;

        if (Schedule.StartsWith("AT ", StringComparison.OrdinalIgnoreCase))
        {
            // AT 'timestamp' format
            var timestampStr = Schedule.Substring(3).Trim().Trim('\'');
            if (DateTime.TryParse(timestampStr, out var atTime))
            {
                return atTime > now ? atTime : null;
            }
        }
        else if (Schedule.StartsWith("EVERY ", StringComparison.OrdinalIgnoreCase))
        {
            // EVERY interval format
            var parts = Schedule.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length >= 3 && int.TryParse(parts[1], out var interval))
            {
                var unit = parts[2].ToUpperInvariant();
                return unit switch
                {
                    "SECOND" or "SECONDS" => now.AddSeconds(interval),
                    "MINUTE" or "MINUTES" => now.AddMinutes(interval),
                    "HOUR" or "HOURS" => now.AddHours(interval),
                    "DAY" or "DAYS" => now.AddDays(interval),
                    "WEEK" or "WEEKS" => now.AddDays(interval * 7),
                    "MONTH" or "MONTHS" => now.AddMonths(interval),
                    "YEAR" or "YEARS" => now.AddYears(interval),
                    _ => now.AddDays(1)
                };
            }
        }

        return now.AddDays(1); // Default to 1 day if parsing fails
    }

    /// <summary>
    /// Updates the next execution time after an execution.
    /// </summary>
    public void UpdateAfterExecution()
    {
        LastExecutionTime = DateTime.UtcNow;

        if (Schedule.StartsWith("EVERY ", StringComparison.OrdinalIgnoreCase))
        {
            NextExecutionTime = CalculateNextExecutionTime();
        }
        else
        {
            // One-time event (AT timestamp)
            NextExecutionTime = null;
            if (!OnCompletionPreserve)
            {
                Enabled = false;
            }
        }
    }

    /// <summary>
    /// Serializes this event info to bytes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(EventId);
        writer.Write(EventName);
        writer.Write(Schedule);
        writer.Write(OnCompletionPreserve);
        writer.Write(Enabled);
        writer.Write(Comment ?? "");
        writer.Write(Definer ?? "");
        writer.Write(CreatedAt.Ticks);
        writer.Write(NextExecutionTime?.Ticks ?? 0);
        writer.Write(LastExecutionTime?.Ticks ?? 0);
        writer.Write(Body.Count); // Body will be re-parsed from original SQL

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes an event info from bytes.
    /// </summary>
    public static EventInfo Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var eventId = reader.ReadInt32();
        var eventName = reader.ReadString();
        var schedule = reader.ReadString();
        var onCompletionPreserve = reader.ReadBoolean();
        var enabled = reader.ReadBoolean();
        var comment = reader.ReadString();
        var definer = reader.ReadString();
        var createdAt = new DateTime(reader.ReadInt64());
        var nextExecutionTicks = reader.ReadInt64();
        var lastExecutionTicks = reader.ReadInt64();
        var bodyCount = reader.ReadInt32();

        var body = new List<Statement>();

        var eventInfo = new EventInfo(
            eventId,
            eventName,
            schedule,
            body,
            onCompletionPreserve,
            enabled,
            string.IsNullOrEmpty(comment) ? null : comment,
            string.IsNullOrEmpty(definer) ? null : definer,
            createdAt);

        eventInfo.NextExecutionTime = nextExecutionTicks > 0 ? new DateTime(nextExecutionTicks) : null;
        eventInfo.LastExecutionTime = lastExecutionTicks > 0 ? new DateTime(lastExecutionTicks) : null;

        return eventInfo;
    }

    public override string ToString() => $"Event '{EventName}' ({Schedule})";
}
