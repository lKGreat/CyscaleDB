using CyscaleDB.Core.Parsing.Ast;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Represents metadata about a trigger.
/// </summary>
public sealed class TriggerInfo
{
    /// <summary>
    /// The trigger ID.
    /// </summary>
    public int TriggerId { get; }

    /// <summary>
    /// The trigger name.
    /// </summary>
    public string TriggerName { get; }

    /// <summary>
    /// The table this trigger is on.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// The trigger timing (BEFORE or AFTER).
    /// </summary>
    public TriggerTiming Timing { get; }

    /// <summary>
    /// The trigger event (INSERT, UPDATE, DELETE).
    /// </summary>
    public TriggerEvent Event { get; }

    /// <summary>
    /// The trigger body statements.
    /// </summary>
    public List<Statement> Body { get; }

    /// <summary>
    /// The definer (user@host).
    /// </summary>
    public string? Definer { get; }

    /// <summary>
    /// When this trigger was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// Creates a new trigger info.
    /// </summary>
    public TriggerInfo(
        int triggerId,
        string triggerName,
        string tableName,
        TriggerTiming timing,
        TriggerEvent @event,
        List<Statement> body,
        string? definer = null,
        DateTime? createdAt = null)
    {
        TriggerId = triggerId;
        TriggerName = triggerName ?? throw new ArgumentNullException(nameof(triggerName));
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        Timing = timing;
        Event = @event;
        Body = body ?? throw new ArgumentNullException(nameof(body));
        Definer = definer;
        CreatedAt = createdAt ?? DateTime.UtcNow;
    }

    /// <summary>
    /// Serializes this trigger info to bytes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(TriggerId);
        writer.Write(TriggerName);
        writer.Write(TableName);
        writer.Write((int)Timing);
        writer.Write((int)Event);
        writer.Write(Definer ?? "");
        writer.Write(CreatedAt.Ticks);
        writer.Write(Body.Count); // Body will be re-parsed from original SQL

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes a trigger info from bytes.
    /// </summary>
    public static TriggerInfo Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var triggerId = reader.ReadInt32();
        var triggerName = reader.ReadString();
        var tableName = reader.ReadString();
        var timing = (TriggerTiming)reader.ReadInt32();
        var @event = (TriggerEvent)reader.ReadInt32();
        var definer = reader.ReadString();
        var createdAt = new DateTime(reader.ReadInt64());
        var bodyCount = reader.ReadInt32();

        // Create empty body - will be populated when needed
        var body = new List<Statement>();

        return new TriggerInfo(
            triggerId,
            triggerName,
            tableName,
            timing,
            @event,
            body,
            string.IsNullOrEmpty(definer) ? null : definer,
            createdAt);
    }

    public override string ToString() => $"Trigger '{TriggerName}' {Timing} {Event} ON {TableName}";
}
