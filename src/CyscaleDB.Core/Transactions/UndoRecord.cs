using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Transactions;

/// <summary>
/// Type of undo operation.
/// </summary>
public enum UndoRecordType : byte
{
    /// <summary>
    /// Undo for INSERT - stores the primary key of inserted row.
    /// Rollback: delete the row by primary key.
    /// </summary>
    Insert = 1,

    /// <summary>
    /// Undo for UPDATE - stores the old values of modified columns.
    /// Rollback: restore the old values.
    /// </summary>
    Update = 2,

    /// <summary>
    /// Undo for DELETE - stores the complete deleted row.
    /// Rollback: re-insert the row.
    /// </summary>
    Delete = 3
}

/// <summary>
/// Represents an undo log record for MVCC and rollback.
/// 
/// Record Format:
/// +----------+----------+----------+----------+----------+----------+----------+
/// | Type(1B) | TrxId(8B)| TableId(4B)| RowId(6B)| PrevPtr(8B)| DataLen(4B)| Data    |
/// +----------+----------+----------+----------+----------+----------+----------+
/// 
/// For INSERT: Data = primary key values
/// For UPDATE: Data = old column values (before update)
/// For DELETE: Data = complete old row data
/// </summary>
public sealed class UndoRecord
{
    /// <summary>
    /// The type of undo operation.
    /// </summary>
    public UndoRecordType Type { get; }

    /// <summary>
    /// The transaction ID that created this undo record.
    /// </summary>
    public long TransactionId { get; }

    /// <summary>
    /// The table ID this undo belongs to.
    /// </summary>
    public int TableId { get; }

    /// <summary>
    /// The database name.
    /// </summary>
    public string DatabaseName { get; }

    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// The row ID of the affected row.
    /// </summary>
    public RowId RowId { get; }

    /// <summary>
    /// Pointer to the previous undo record in the version chain.
    /// </summary>
    public long PreviousUndoPointer { get; }

    /// <summary>
    /// The undo data (format depends on Type).
    /// </summary>
    public byte[] Data { get; }

    /// <summary>
    /// Timestamp when this record was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// The LSN of the corresponding redo log entry.
    /// </summary>
    public long RedoLsn { get; set; }

    /// <summary>
    /// Creates a new undo record.
    /// </summary>
    public UndoRecord(
        UndoRecordType type,
        long transactionId,
        int tableId,
        string databaseName,
        string tableName,
        RowId rowId,
        long previousUndoPointer,
        byte[] data)
    {
        Type = type;
        TransactionId = transactionId;
        TableId = tableId;
        DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        RowId = rowId;
        PreviousUndoPointer = previousUndoPointer;
        Data = data ?? throw new ArgumentNullException(nameof(data));
        CreatedAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Creates an INSERT undo record.
    /// For INSERT, we only need to store the primary key to delete on rollback.
    /// </summary>
    public static UndoRecord CreateInsertUndo(
        long transactionId,
        int tableId,
        string databaseName,
        string tableName,
        RowId rowId,
        DataValue[] primaryKeyValues,
        long previousUndoPointer = 0)
    {
        var data = SerializePrimaryKey(primaryKeyValues);
        return new UndoRecord(
            UndoRecordType.Insert,
            transactionId,
            tableId,
            databaseName,
            tableName,
            rowId,
            previousUndoPointer,
            data);
    }

    /// <summary>
    /// Creates an UPDATE undo record.
    /// For UPDATE, we store the old row values to restore on rollback.
    /// </summary>
    public static UndoRecord CreateUpdateUndo(
        long transactionId,
        int tableId,
        string databaseName,
        string tableName,
        RowId rowId,
        Row oldRow,
        long previousUndoPointer = 0)
    {
        var data = oldRow.Serialize();
        return new UndoRecord(
            UndoRecordType.Update,
            transactionId,
            tableId,
            databaseName,
            tableName,
            rowId,
            previousUndoPointer,
            data);
    }

    /// <summary>
    /// Creates a DELETE undo record.
    /// For DELETE, we store the complete old row to re-insert on rollback.
    /// </summary>
    public static UndoRecord CreateDeleteUndo(
        long transactionId,
        int tableId,
        string databaseName,
        string tableName,
        RowId rowId,
        Row deletedRow,
        long previousUndoPointer = 0)
    {
        var data = deletedRow.Serialize();
        return new UndoRecord(
            UndoRecordType.Delete,
            transactionId,
            tableId,
            databaseName,
            tableName,
            rowId,
            previousUndoPointer,
            data);
    }

    /// <summary>
    /// Deserializes the primary key from an INSERT undo record.
    /// </summary>
    public DataValue[] GetPrimaryKeyValues(DataType[] keyTypes)
    {
        if (Type != UndoRecordType.Insert)
            throw new InvalidOperationException("GetPrimaryKeyValues is only valid for INSERT undo records");

        return DeserializePrimaryKey(Data, keyTypes);
    }

    /// <summary>
    /// Deserializes the old row from an UPDATE/DELETE undo record.
    /// </summary>
    public Row GetOldRow(TableSchema schema)
    {
        if (Type != UndoRecordType.Update && Type != UndoRecordType.Delete)
            throw new InvalidOperationException("GetOldRow is only valid for UPDATE/DELETE undo records");

        return Row.Deserialize(Data, schema);
    }

    /// <summary>
    /// Serializes this undo record to bytes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write((byte)Type);
        writer.Write(TransactionId);
        writer.Write(TableId);
        writer.Write(DatabaseName);
        writer.Write(TableName);
        writer.Write(RowId.PageId);
        writer.Write(RowId.SlotNumber);
        writer.Write(PreviousUndoPointer);
        writer.Write(CreatedAt.Ticks);
        writer.Write(RedoLsn);
        writer.Write(Data.Length);
        writer.Write(Data);

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes an undo record from bytes.
    /// </summary>
    public static UndoRecord Deserialize(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream);

        var type = (UndoRecordType)reader.ReadByte();
        var transactionId = reader.ReadInt64();
        var tableId = reader.ReadInt32();
        var databaseName = reader.ReadString();
        var tableName = reader.ReadString();
        var pageId = reader.ReadInt32();
        var slotNumber = reader.ReadInt16();
        var rowId = new RowId(pageId, slotNumber);
        var previousUndoPointer = reader.ReadInt64();
        var createdAtTicks = reader.ReadInt64();
        var redoLsn = reader.ReadInt64();
        var dataLength = reader.ReadInt32();
        var data = reader.ReadBytes(dataLength);

        return new UndoRecord(type, transactionId, tableId, databaseName, tableName, rowId, previousUndoPointer, data)
        {
            RedoLsn = redoLsn
        };
    }

    /// <summary>
    /// Gets the size of this undo record in bytes.
    /// </summary>
    public int GetSize()
    {
        // Type(1) + TrxId(8) + TableId(4) + DbName(variable) + TableName(variable) +
        // RowId(6) + PrevPtr(8) + CreatedAt(8) + RedoLsn(8) + DataLen(4) + Data
        var baseSize = 1 + 8 + 4 + 6 + 8 + 8 + 8 + 4 + Data.Length;
        // Add string length prefixes (BinaryWriter uses 7-bit encoded length)
        baseSize += GetStringByteCount(DatabaseName);
        baseSize += GetStringByteCount(TableName);
        return baseSize;
    }

    private static int GetStringByteCount(string s)
    {
        var byteCount = System.Text.Encoding.UTF8.GetByteCount(s);
        // BinaryWriter uses 7-bit encoded length prefix
        return byteCount + (byteCount < 128 ? 1 : byteCount < 16384 ? 2 : 3);
    }

    private static byte[] SerializePrimaryKey(DataValue[] values)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(values.Length);
        foreach (var value in values)
        {
            var serialized = value.Serialize();
            writer.Write(serialized.Length);
            writer.Write(serialized);
        }

        return stream.ToArray();
    }

    private static DataValue[] DeserializePrimaryKey(byte[] data, DataType[] keyTypes)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var count = reader.ReadInt32();
        var values = new DataValue[count];

        for (int i = 0; i < count; i++)
        {
            var length = reader.ReadInt32();
            var valueData = reader.ReadBytes(length);
            values[i] = DataValue.Deserialize(valueData);
        }

        return values;
    }

    public override string ToString()
    {
        return $"UndoRecord({Type}, TrxId={TransactionId}, Table={DatabaseName}.{TableName}, Row={RowId}, DataLen={Data.Length})";
    }
}

/// <summary>
/// Builder for creating undo records with fluent API.
/// </summary>
public sealed class UndoRecordBuilder
{
    private UndoRecordType _type;
    private long _transactionId;
    private int _tableId;
    private string _databaseName = "";
    private string _tableName = "";
    private RowId _rowId;
    private long _previousUndoPointer;
    private byte[] _data = [];

    public UndoRecordBuilder WithType(UndoRecordType type)
    {
        _type = type;
        return this;
    }

    public UndoRecordBuilder WithTransactionId(long transactionId)
    {
        _transactionId = transactionId;
        return this;
    }

    public UndoRecordBuilder WithTableId(int tableId)
    {
        _tableId = tableId;
        return this;
    }

    public UndoRecordBuilder WithDatabaseName(string databaseName)
    {
        _databaseName = databaseName;
        return this;
    }

    public UndoRecordBuilder WithTableName(string tableName)
    {
        _tableName = tableName;
        return this;
    }

    public UndoRecordBuilder WithRowId(RowId rowId)
    {
        _rowId = rowId;
        return this;
    }

    public UndoRecordBuilder WithPreviousUndoPointer(long previousUndoPointer)
    {
        _previousUndoPointer = previousUndoPointer;
        return this;
    }

    public UndoRecordBuilder WithData(byte[] data)
    {
        _data = data;
        return this;
    }

    public UndoRecord Build()
    {
        return new UndoRecord(
            _type,
            _transactionId,
            _tableId,
            _databaseName,
            _tableName,
            _rowId,
            _previousUndoPointer,
            _data);
    }
}
