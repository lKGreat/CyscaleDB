using CyscaleDB.Core.Parsing.Ast;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Represents metadata about a stored procedure or function.
/// </summary>
public sealed class ProcedureInfo
{
    /// <summary>
    /// The procedure or function ID.
    /// </summary>
    public int ProcedureId { get; }

    /// <summary>
    /// The procedure or function name.
    /// </summary>
    public string ProcedureName { get; }

    /// <summary>
    /// Whether this is a function (true) or procedure (false).
    /// </summary>
    public bool IsFunction { get; }

    /// <summary>
    /// The parameters for the procedure/function.
    /// </summary>
    public List<ProcedureParameter> Parameters { get; }

    /// <summary>
    /// The return type for functions (null for procedures).
    /// </summary>
    public Common.DataType? ReturnType { get; }

    /// <summary>
    /// Return type size (for VARCHAR, DECIMAL, etc.).
    /// </summary>
    public int? ReturnSize { get; }

    /// <summary>
    /// Return type scale (for DECIMAL).
    /// </summary>
    public int? ReturnScale { get; }

    /// <summary>
    /// The procedure/function body statements.
    /// </summary>
    public List<Statement> Body { get; }

    /// <summary>
    /// Whether the function is deterministic.
    /// </summary>
    public bool IsDeterministic { get; }

    /// <summary>
    /// The definer (user@host).
    /// </summary>
    public string? Definer { get; }

    /// <summary>
    /// SQL SECURITY (DEFINER or INVOKER).
    /// </summary>
    public string? SqlSecurity { get; }

    /// <summary>
    /// Comment for the procedure/function.
    /// </summary>
    public string? Comment { get; }

    /// <summary>
    /// When this procedure/function was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// Creates a new procedure info.
    /// </summary>
    public ProcedureInfo(
        int procedureId,
        string procedureName,
        bool isFunction,
        List<ProcedureParameter> parameters,
        List<Statement> body,
        Common.DataType? returnType = null,
        int? returnSize = null,
        int? returnScale = null,
        bool isDeterministic = false,
        string? definer = null,
        string? sqlSecurity = null,
        string? comment = null,
        DateTime? createdAt = null)
    {
        ProcedureId = procedureId;
        ProcedureName = procedureName ?? throw new ArgumentNullException(nameof(procedureName));
        IsFunction = isFunction;
        Parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
        Body = body ?? throw new ArgumentNullException(nameof(body));
        ReturnType = returnType;
        ReturnSize = returnSize;
        ReturnScale = returnScale;
        IsDeterministic = isDeterministic;
        Definer = definer;
        SqlSecurity = sqlSecurity;
        Comment = comment;
        CreatedAt = createdAt ?? DateTime.UtcNow;
    }

    /// <summary>
    /// Serializes this procedure info to bytes.
    /// For now, we'll use a simple format that stores the statement text.
    /// In a full implementation, this would serialize the AST.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(ProcedureId);
        writer.Write(ProcedureName);
        writer.Write(IsFunction);
        writer.Write(CreatedAt.Ticks);
        
        // Parameters
        writer.Write(Parameters.Count);
        foreach (var param in Parameters)
        {
            writer.Write(param.Name);
            writer.Write((int)param.Mode);
            writer.Write((int)param.DataType);
            writer.Write(param.Size ?? -1);
            writer.Write(param.Scale ?? -1);
        }

        // Return type info (for functions)
        writer.Write(ReturnType.HasValue);
        if (ReturnType.HasValue)
        {
            writer.Write((int)ReturnType.Value);
            writer.Write(ReturnSize ?? -1);
            writer.Write(ReturnScale ?? -1);
        }

        writer.Write(IsDeterministic);
        writer.Write(Definer ?? "");
        writer.Write(SqlSecurity ?? "");
        writer.Write(Comment ?? "");

        // Body - for now, just store the count (body will be re-parsed from original SQL)
        writer.Write(Body.Count);

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes a procedure info from bytes.
    /// </summary>
    public static ProcedureInfo Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var procedureId = reader.ReadInt32();
        var procedureName = reader.ReadString();
        var isFunction = reader.ReadBoolean();
        var createdAt = new DateTime(reader.ReadInt64());

        // Parameters
        var paramCount = reader.ReadInt32();
        var parameters = new List<ProcedureParameter>();
        for (int i = 0; i < paramCount; i++)
        {
            var name = reader.ReadString();
            var mode = (ParameterMode)reader.ReadInt32();
            var dataType = (Common.DataType)reader.ReadInt32();
            var size = reader.ReadInt32();
            var scale = reader.ReadInt32();

            parameters.Add(new ProcedureParameter
            {
                Name = name,
                Mode = mode,
                DataType = dataType,
                Size = size >= 0 ? size : null,
                Scale = scale >= 0 ? scale : null
            });
        }

        // Return type
        Common.DataType? returnType = null;
        int? returnSize = null;
        int? returnScale = null;
        if (reader.ReadBoolean())
        {
            returnType = (Common.DataType)reader.ReadInt32();
            var rs = reader.ReadInt32();
            var rsc = reader.ReadInt32();
            returnSize = rs >= 0 ? rs : null;
            returnScale = rsc >= 0 ? rsc : null;
        }

        var isDeterministic = reader.ReadBoolean();
        var definer = reader.ReadString();
        var sqlSecurity = reader.ReadString();
        var comment = reader.ReadString();

        // Body count (we don't restore the body - it needs to be re-parsed)
        var bodyCount = reader.ReadInt32();

        // Create empty body - will be populated when needed
        var body = new List<Statement>();

        return new ProcedureInfo(
            procedureId,
            procedureName,
            isFunction,
            parameters,
            body,
            returnType,
            returnSize,
            returnScale,
            isDeterministic,
            string.IsNullOrEmpty(definer) ? null : definer,
            string.IsNullOrEmpty(sqlSecurity) ? null : sqlSecurity,
            string.IsNullOrEmpty(comment) ? null : comment,
            createdAt);
    }

    public override string ToString() => IsFunction 
        ? $"Function '{ProcedureName}'" 
        : $"Procedure '{ProcedureName}'";
}
