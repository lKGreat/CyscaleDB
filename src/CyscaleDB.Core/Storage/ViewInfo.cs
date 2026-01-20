using CyscaleDB.Core.Common;
using CyscaleDB.Core.Parsing.Ast;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Represents metadata about a database view.
/// </summary>
public sealed class ViewInfo
{
    /// <summary>
    /// Unique identifier for this view.
    /// </summary>
    public int ViewId { get; }

    /// <summary>
    /// The name of this view.
    /// </summary>
    public string ViewName { get; }

    /// <summary>
    /// The database containing this view.
    /// </summary>
    public string DatabaseName { get; }

    /// <summary>
    /// The original SQL definition of this view.
    /// </summary>
    public string Definition { get; }

    /// <summary>
    /// The parsed SELECT statement representing this view.
    /// </summary>
    public SelectStatement? ParsedQuery { get; private set; }

    /// <summary>
    /// The column names of this view (can be explicitly specified or derived from query).
    /// </summary>
    public IReadOnlyList<string> ColumnNames { get; }

    /// <summary>
    /// When this view was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// Whether this view was created with OR REPLACE option.
    /// </summary>
    public bool OrReplace { get; }

    /// <summary>
    /// Creates a new ViewInfo instance.
    /// </summary>
    public ViewInfo(
        int viewId,
        string viewName,
        string databaseName,
        string definition,
        IEnumerable<string>? columnNames = null,
        bool orReplace = false,
        DateTime? createdAt = null)
    {
        if (string.IsNullOrWhiteSpace(viewName))
            throw new ArgumentException("View name cannot be empty", nameof(viewName));
        if (viewName.Length > Constants.MaxViewNameLength)
            throw new ArgumentException($"View name exceeds maximum length of {Constants.MaxViewNameLength}", nameof(viewName));
        if (string.IsNullOrWhiteSpace(definition))
            throw new ArgumentException("View definition cannot be empty", nameof(definition));
        if (definition.Length > Constants.MaxViewDefinitionLength)
            throw new ArgumentException($"View definition exceeds maximum length of {Constants.MaxViewDefinitionLength}", nameof(definition));

        ViewId = viewId;
        ViewName = viewName;
        DatabaseName = databaseName;
        Definition = definition;
        ColumnNames = columnNames?.ToList().AsReadOnly() ?? (IReadOnlyList<string>)[];
        OrReplace = orReplace;
        CreatedAt = createdAt ?? DateTime.UtcNow;
    }

    /// <summary>
    /// Sets the parsed query for this view.
    /// </summary>
    public void SetParsedQuery(SelectStatement query)
    {
        ParsedQuery = query;
    }

    /// <summary>
    /// Creates a derived schema for this view based on the underlying query.
    /// </summary>
    public TableSchema CreateDerivedSchema(List<ColumnDefinition> columns)
    {
        // If explicit column names were provided, rename the columns
        if (ColumnNames.Count > 0)
        {
            if (ColumnNames.Count != columns.Count)
                throw new CyscaleException($"View column count ({ColumnNames.Count}) does not match query column count ({columns.Count})");

            var renamedColumns = new List<ColumnDefinition>();
            for (int i = 0; i < columns.Count; i++)
            {
                var col = columns[i];
                renamedColumns.Add(new ColumnDefinition(
                    ColumnNames[i],
                    col.DataType,
                    col.MaxLength,
                    col.Precision,
                    col.Scale,
                    col.IsNullable)
                {
                    OrdinalPosition = i
                });
            }
            columns = renamedColumns;
        }

        return new TableSchema(ViewId, DatabaseName, ViewName, columns);
    }

    /// <summary>
    /// Serializes this view info to bytes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(ViewId);
        writer.Write(ViewName);
        writer.Write(DatabaseName);
        writer.Write(Definition);
        writer.Write(OrReplace);
        writer.Write(CreatedAt.Ticks);

        // Column names
        writer.Write(ColumnNames.Count);
        foreach (var colName in ColumnNames)
        {
            writer.Write(colName);
        }

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes a ViewInfo from bytes.
    /// </summary>
    public static ViewInfo Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var viewId = reader.ReadInt32();
        var viewName = reader.ReadString();
        var databaseName = reader.ReadString();
        var definition = reader.ReadString();
        var orReplace = reader.ReadBoolean();
        var createdAt = new DateTime(reader.ReadInt64());

        var columnCount = reader.ReadInt32();
        var columnNames = new List<string>(columnCount);
        for (int i = 0; i < columnCount; i++)
        {
            columnNames.Add(reader.ReadString());
        }

        return new ViewInfo(viewId, viewName, databaseName, definition, columnNames, orReplace, createdAt);
    }

    public override string ToString() => $"View '{ViewName}' AS {Definition}";
}
