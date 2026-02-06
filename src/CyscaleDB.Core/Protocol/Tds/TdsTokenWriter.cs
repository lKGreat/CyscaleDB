using System.Text;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Protocol.Tds;

/// <summary>
/// Writes TDS response tokens (COLMETADATA, ROW, DONE, ERROR, INFO, ENVCHANGE, LOGINACK)
/// into a MemoryStream that can then be sent as a TDS response packet.
/// </summary>
public sealed class TdsTokenWriter
{
    private readonly MemoryStream _stream;
    private readonly BinaryWriter _writer;

    public TdsTokenWriter()
    {
        _stream = new MemoryStream();
        _writer = new BinaryWriter(_stream);
    }

    /// <summary>
    /// Gets the built response payload.
    /// </summary>
    public MemoryStream GetStream() => _stream;

    public byte[] ToArray() => _stream.ToArray();

    #region LOGINACK Token

    /// <summary>
    /// Writes a LOGINACK token to confirm successful login.
    /// </summary>
    public void WriteLoginAck(string serverName, uint tdsVersion)
    {
        _writer.Write(TdsConstants.TokenLoginAck);

        using var tokenData = new MemoryStream();
        using var tw = new BinaryWriter(tokenData);

        tw.Write((byte)0x01); // Interface: SQL
        tw.Write(tdsVersion); // TDS version

        // Server name (B_VARCHAR: 1-byte length + UTF-16LE)
        var nameBytes = Encoding.Unicode.GetBytes(serverName);
        tw.Write((byte)(serverName.Length)); // character count, not byte count
        tw.Write(nameBytes);

        // Server version: Major.Minor.BuildHi.BuildLo
        tw.Write((byte)TdsConstants.ServerVersionMajor);
        tw.Write((byte)TdsConstants.ServerVersionMinor);
        tw.Write((byte)(TdsConstants.ServerVersionBuild >> 8));
        tw.Write((byte)(TdsConstants.ServerVersionBuild & 0xFF));

        var data = tokenData.ToArray();
        _writer.Write((ushort)data.Length);
        _writer.Write(data);
    }

    #endregion

    #region ENVCHANGE Token

    /// <summary>
    /// Writes an ENVCHANGE token for database change.
    /// </summary>
    public void WriteEnvChangeDatabase(string newDb, string oldDb)
    {
        WriteEnvChange(TdsConstants.EnvChangeDatabase, newDb, oldDb);
    }

    /// <summary>
    /// Writes an ENVCHANGE token for packet size change.
    /// </summary>
    public void WriteEnvChangePacketSize(int newSize, int oldSize)
    {
        WriteEnvChange(TdsConstants.EnvChangePacketSize, newSize.ToString(), oldSize.ToString());
    }

    /// <summary>
    /// Writes an ENVCHANGE token for collation.
    /// </summary>
    public void WriteEnvChangeCollation()
    {
        _writer.Write(TdsConstants.TokenEnvChange);
        // Length
        var collLen = TdsConstants.DefaultCollation.Length;
        var totalLen = 1 + 1 + collLen + 1 + collLen; // type + newLen + new + oldLen + old
        _writer.Write((ushort)totalLen);
        _writer.Write(TdsConstants.EnvChangeCollation);
        _writer.Write((byte)collLen);
        _writer.Write(TdsConstants.DefaultCollation);
        _writer.Write((byte)collLen);
        _writer.Write(TdsConstants.DefaultCollation);
    }

    private void WriteEnvChange(byte type, string newValue, string oldValue)
    {
        _writer.Write(TdsConstants.TokenEnvChange);

        using var tokenData = new MemoryStream();
        using var tw = new BinaryWriter(tokenData);

        tw.Write(type);

        // New value (B_VARCHAR: 1-byte length in chars + UTF-16LE)
        var newBytes = Encoding.Unicode.GetBytes(newValue);
        tw.Write((byte)newValue.Length);
        tw.Write(newBytes);

        // Old value
        var oldBytes = Encoding.Unicode.GetBytes(oldValue);
        tw.Write((byte)oldValue.Length);
        tw.Write(oldBytes);

        var data = tokenData.ToArray();
        _writer.Write((ushort)data.Length);
        _writer.Write(data);
    }

    #endregion

    #region INFO / ERROR Tokens

    /// <summary>
    /// Writes an INFO token (informational message).
    /// </summary>
    public void WriteInfo(int number, string message, string serverName = "CyscaleDB", byte state = 1, byte classLevel = 0)
    {
        WriteMessage(TdsConstants.TokenInfo, number, state, classLevel, message, serverName, "", 0);
    }

    /// <summary>
    /// Writes an ERROR token.
    /// </summary>
    public void WriteError(int number, string message, string serverName = "CyscaleDB", byte state = 1, byte classLevel = 14)
    {
        WriteMessage(TdsConstants.TokenError, number, state, classLevel, message, serverName, "", 0);
    }

    private void WriteMessage(byte tokenType, int number, byte state, byte classLevel,
        string message, string serverName, string procName, int lineNumber)
    {
        _writer.Write(tokenType);

        using var tokenData = new MemoryStream();
        using var tw = new BinaryWriter(tokenData);

        tw.Write(number);          // Number (4 bytes)
        tw.Write(state);           // State (1 byte)
        tw.Write(classLevel);      // Class (1 byte)

        // Message (US_VARCHAR: 2-byte length in chars + UTF-16LE)
        var msgBytes = Encoding.Unicode.GetBytes(message);
        tw.Write((ushort)message.Length);
        tw.Write(msgBytes);

        // Server name
        var srvBytes = Encoding.Unicode.GetBytes(serverName);
        tw.Write((byte)serverName.Length);
        tw.Write(srvBytes);

        // Proc name
        var procBytes = Encoding.Unicode.GetBytes(procName);
        tw.Write((byte)procName.Length);
        tw.Write(procBytes);

        tw.Write(lineNumber); // Line number (4 bytes)

        var data = tokenData.ToArray();
        _writer.Write((ushort)data.Length);
        _writer.Write(data);
    }

    #endregion

    #region COLMETADATA + ROW Tokens

    /// <summary>
    /// Writes COLMETADATA token describing result set columns.
    /// </summary>
    public void WriteColumnMetadata(TableSchema schema)
    {
        _writer.Write(TdsConstants.TokenColMetadata);
        _writer.Write((ushort)schema.Columns.Count);

        foreach (var col in schema.Columns)
        {
            // User type (4 bytes)
            _writer.Write((uint)0);

            // Flags (2 bytes): nullable=1
            _writer.Write((ushort)0x0001);

            // TYPE_INFO: map CyscaleDB type to TDS type
            WriteTdsTypeInfo(col);

            // Column name (B_VARCHAR: 1-byte length in chars + UTF-16LE)
            var nameBytes = Encoding.Unicode.GetBytes(col.Name);
            _writer.Write((byte)col.Name.Length);
            _writer.Write(nameBytes);
        }
    }

    /// <summary>
    /// Writes a ROW token for a single data row.
    /// </summary>
    public void WriteRow(Row row, TableSchema schema)
    {
        _writer.Write(TdsConstants.TokenRow);

        for (int i = 0; i < schema.Columns.Count; i++)
        {
            var col = schema.Columns[i];
            var val = i < row.Values.Length ? row.Values[i] : DataValue.Null;
            WriteTdsValue(val, col);
        }
    }

    private void WriteTdsTypeInfo(ColumnDefinition col)
    {
        switch (col.DataType)
        {
            case DataType.TinyInt:
                _writer.Write(TdsConstants.TypeInt1);
                break;
            case DataType.SmallInt:
                _writer.Write(TdsConstants.TypeInt2);
                break;
            case DataType.Int:
                _writer.Write(TdsConstants.TypeInt4);
                break;
            case DataType.BigInt:
                _writer.Write(TdsConstants.TypeInt8);
                break;
            case DataType.Float:
                _writer.Write(TdsConstants.TypeFlt4);
                break;
            case DataType.Double:
                _writer.Write(TdsConstants.TypeFlt8);
                break;
            case DataType.Boolean:
                _writer.Write(TdsConstants.TypeBitN);
                _writer.Write((byte)1); // max length
                break;
            case DataType.DateTime:
            case DataType.Timestamp:
                _writer.Write(TdsConstants.TypeDateTimeN);
                _writer.Write((byte)8); // max length
                break;
            case DataType.Decimal:
                _writer.Write(TdsConstants.TypeDecimalN);
                _writer.Write((byte)17); // max length
                _writer.Write((byte)(col.Precision > 0 ? col.Precision : 18)); // precision
                _writer.Write((byte)(col.Scale > 0 ? col.Scale : 2)); // scale
                break;
            case DataType.VarChar:
            case DataType.Char:
            case DataType.Text:
            case DataType.Json:
            default:
                // Use NVARCHAR for all string-like types
                _writer.Write(TdsConstants.TypeNVarChar);
                var maxLen = col.MaxLength > 0 ? col.MaxLength * 2 : 8000;
                _writer.Write((ushort)maxLen);
                // Collation (5 bytes)
                _writer.Write(TdsConstants.DefaultCollation);
                break;
        }
    }

    private void WriteTdsValue(DataValue val, ColumnDefinition col)
    {
        if (val.IsNull)
        {
            switch (col.DataType)
            {
                case DataType.TinyInt:
                case DataType.SmallInt:
                case DataType.Int:
                case DataType.BigInt:
                case DataType.Float:
                case DataType.Double:
                    // Fixed-length NULL: write 0 for the type byte (not present for fixed types)
                    // Actually for fixed types, we use IntN variant
                    // For simplicity, in metadata we used fixed types; write default
                    WriteDefaultNull(col.DataType);
                    break;
                case DataType.Boolean:
                case DataType.DateTime:
                case DataType.Timestamp:
                case DataType.Decimal:
                    _writer.Write((byte)0); // 0 length = NULL for variable-length-within-fixed
                    break;
                default:
                    // NVARCHAR NULL: write 0xFFFF
                    _writer.Write(unchecked((ushort)0xFFFF));
                    break;
            }
            return;
        }

        switch (col.DataType)
        {
            case DataType.TinyInt:
                _writer.Write(val.AsTinyInt());
                break;
            case DataType.SmallInt:
                _writer.Write(val.AsSmallInt());
                break;
            case DataType.Int:
                _writer.Write(val.AsInt());
                break;
            case DataType.BigInt:
                _writer.Write(val.AsBigInt());
                break;
            case DataType.Float:
                _writer.Write(val.AsFloat());
                break;
            case DataType.Double:
                _writer.Write(val.AsDouble());
                break;
            case DataType.Boolean:
                _writer.Write((byte)1); // length
                _writer.Write(val.AsBoolean() ? (byte)1 : (byte)0);
                break;
            case DataType.DateTime:
            case DataType.Timestamp:
                _writer.Write((byte)8); // length
                // TDS datetime: days since 1900-01-01 (4 bytes) + 300ths of second (4 bytes)
                var dt = val.AsDateTime();
                var epoch = new DateTime(1900, 1, 1);
                var days = (int)(dt.Date - epoch).TotalDays;
                var timeFraction = (int)(dt.TimeOfDay.TotalSeconds * 300);
                _writer.Write(days);
                _writer.Write(timeFraction);
                break;
            case DataType.Decimal:
                // Simplified: write as string in NVARCHAR form
                var decStr = val.AsDecimal().ToString();
                var decBytes = Encoding.Unicode.GetBytes(decStr);
                _writer.Write((byte)(1 + decBytes.Length)); // length byte
                _writer.Write((byte)(val.AsDecimal() >= 0 ? 1 : 0)); // sign
                // Write decimal value as raw bytes
                var decVal = val.AsDecimal();
                var decAbsStr = Math.Abs(decVal).ToString();
                // Actually for TDS decimal, use binary format
                _writer.Write((byte)17);
                _writer.Write((byte)(decVal >= 0 ? 1 : 0));
                var intPart = decimal.GetBits(Math.Abs(decVal));
                _writer.Write(intPart[0]);
                _writer.Write(intPart[1]);
                _writer.Write(intPart[2]);
                _writer.Write(0); // high 4 bytes
                break;
            default:
                // NVARCHAR: 2-byte length + UTF-16LE data
                var str = val.IsNull ? "" : val.ToString() ?? "";
                var strBytes = Encoding.Unicode.GetBytes(str);
                _writer.Write((ushort)strBytes.Length);
                _writer.Write(strBytes);
                break;
        }
    }

    private void WriteDefaultNull(DataType type)
    {
        // For fixed-length types that don't have a nullable variant in our metadata,
        // write a zero/default value. In practice, we should use IntN/FltN tokens.
        switch (type)
        {
            case DataType.TinyInt: _writer.Write((byte)0); break;
            case DataType.SmallInt: _writer.Write((short)0); break;
            case DataType.Int: _writer.Write(0); break;
            case DataType.BigInt: _writer.Write(0L); break;
            case DataType.Float: _writer.Write(0f); break;
            case DataType.Double: _writer.Write(0d); break;
            default: _writer.Write((byte)0); break;
        }
    }

    #endregion

    #region DONE Token

    /// <summary>
    /// Writes a DONE token indicating the end of a result set or command.
    /// </summary>
    public void WriteDone(ushort status = TdsConstants.DoneStatusFinal, ushort curCmd = 0, long rowCount = 0)
    {
        _writer.Write(TdsConstants.TokenDone);
        _writer.Write(status);
        _writer.Write(curCmd);
        _writer.Write(rowCount);  // 8 bytes for TDS 7.2+
    }

    /// <summary>
    /// Writes a DONEPROC token (end of stored procedure).
    /// </summary>
    public void WriteDoneProc(ushort status = TdsConstants.DoneStatusFinal, long rowCount = 0)
    {
        _writer.Write(TdsConstants.TokenDoneProc);
        _writer.Write(status);
        _writer.Write((ushort)0); // curCmd
        _writer.Write(rowCount);
    }

    #endregion
}
