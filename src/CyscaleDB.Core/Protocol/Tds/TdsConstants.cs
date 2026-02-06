namespace CyscaleDB.Core.Protocol.Tds;

/// <summary>
/// TDS protocol constants based on [MS-TDS] specification v39.0.
/// </summary>
public static class TdsConstants
{
    /// <summary>
    /// Default TDS port (SQL Server default).
    /// </summary>
    public const int DefaultPort = 1433;

    /// <summary>
    /// TDS packet header size (8 bytes).
    /// </summary>
    public const int PacketHeaderSize = 8;

    /// <summary>
    /// Default TDS packet size for TDS 7.0+.
    /// </summary>
    public const int DefaultPacketSize = 4096;

    /// <summary>
    /// Maximum TDS packet size.
    /// </summary>
    public const int MaxPacketSize = 32768;

    /// <summary>
    /// TDS version 7.4 (SQL Server 2012+).
    /// </summary>
    public const uint TdsVersion74 = 0x74000004;

    /// <summary>
    /// TDS version 7.3 (SQL Server 2008).
    /// </summary>
    public const uint TdsVersion73 = 0x730B0003;

    /// <summary>
    /// Server version reported to clients (simulates SQL Server 2022).
    /// Major=16, Minor=0, Build=1000.
    /// </summary>
    public const uint ServerVersionMajor = 16;
    public const uint ServerVersionMinor = 0;
    public const ushort ServerVersionBuild = 1000;

    #region Packet Types

    /// <summary>SQL batch (COM_QUERY equivalent).</summary>
    public const byte PacketTypeSqlBatch = 0x01;

    /// <summary>Legacy login (TDS 4.2/5.0).</summary>
    public const byte PacketTypeLegacyLogin = 0x02;

    /// <summary>RPC request.</summary>
    public const byte PacketTypeRpc = 0x03;

    /// <summary>Tabular response.</summary>
    public const byte PacketTypeResponse = 0x04;

    /// <summary>Attention signal (cancel).</summary>
    public const byte PacketTypeAttention = 0x06;

    /// <summary>Bulk load data.</summary>
    public const byte PacketTypeBulkLoad = 0x07;

    /// <summary>Transaction manager request.</summary>
    public const byte PacketTypeTransactionManager = 0x0E;

    /// <summary>TDS 7.0+ login (LOGIN7).</summary>
    public const byte PacketTypeLogin7 = 0x10;

    /// <summary>SSPI authentication.</summary>
    public const byte PacketTypeSspi = 0x11;

    /// <summary>Pre-login handshake.</summary>
    public const byte PacketTypePreLogin = 0x12;

    #endregion

    #region Packet Status

    /// <summary>Normal packet (more to follow).</summary>
    public const byte StatusNormal = 0x00;

    /// <summary>End of message (last packet in message).</summary>
    public const byte StatusEndOfMessage = 0x01;

    /// <summary>Reset connection.</summary>
    public const byte StatusResetConnection = 0x08;

    /// <summary>Reset connection, keep transaction state.</summary>
    public const byte StatusResetConnectionSkipTran = 0x10;

    #endregion

    #region Token Types (Response Tokens)

    public const byte TokenColMetadata = 0x81;
    public const byte TokenRow = 0xD1;
    public const byte TokenNbcRow = 0xD2;
    public const byte TokenError = 0xAA;
    public const byte TokenInfo = 0xAB;
    public const byte TokenLoginAck = 0xAD;
    public const byte TokenDone = 0xFD;
    public const byte TokenDoneProc = 0xFE;
    public const byte TokenDoneInProc = 0xFF;
    public const byte TokenEnvChange = 0xE3;
    public const byte TokenOrder = 0xA9;
    public const byte TokenReturnStatus = 0x79;
    public const byte TokenReturnValue = 0xAC;
    public const byte TokenFeatureExtAck = 0xAE;

    #endregion

    #region DONE Status Flags

    public const ushort DoneStatusFinal = 0x0000;
    public const ushort DoneStatusMore = 0x0001;
    public const ushort DoneStatusError = 0x0002;
    public const ushort DoneStatusCount = 0x0010;

    #endregion

    #region EnvChange Types

    public const byte EnvChangeDatabase = 1;
    public const byte EnvChangeLanguage = 2;
    public const byte EnvChangeCharset = 3;
    public const byte EnvChangePacketSize = 4;
    public const byte EnvChangeCollation = 7;

    #endregion

    #region TDS Data Types

    public const byte TypeNull = 0x1F;
    public const byte TypeInt1 = 0x30;    // tinyint
    public const byte TypeBit = 0x32;     // bit
    public const byte TypeInt2 = 0x34;    // smallint
    public const byte TypeInt4 = 0x38;    // int
    public const byte TypeDateTim4 = 0x3A;// smalldatetime
    public const byte TypeFlt4 = 0x3B;    // real (float4)
    public const byte TypeMoney = 0x3C;   // money
    public const byte TypeDateTime = 0x3D;// datetime
    public const byte TypeFlt8 = 0x3E;    // float (float8)
    public const byte TypeInt8 = 0x7F;    // bigint

    // Variable-length types
    public const byte TypeBigVarChar = 0xA7;  // varchar
    public const byte TypeBigChar = 0xAF;     // char
    public const byte TypeNVarChar = 0xE7;    // nvarchar
    public const byte TypeNChar = 0xEF;       // nchar
    public const byte TypeBigVarBin = 0xA5;   // varbinary
    public const byte TypeBigBinary = 0xAD;   // binary
    public const byte TypeText = 0x23;        // text
    public const byte TypeNText = 0x63;       // ntext
    public const byte TypeImage = 0x22;       // image

    // Precision types
    public const byte TypeDecimalN = 0x6A;  // decimal
    public const byte TypeNumericN = 0x6C;  // numeric
    public const byte TypeBitN = 0x68;      // bit (nullable)
    public const byte TypeIntN = 0x26;      // intn (nullable int variant)
    public const byte TypeFltN = 0x6D;      // floatn
    public const byte TypeMoneyN = 0x6E;    // moneyn
    public const byte TypeDateTimeN = 0x6F; // datetimen

    #endregion

    #region PreLogin Option Tokens

    public const byte PreLoginVersion = 0x00;
    public const byte PreLoginEncryption = 0x01;
    public const byte PreLoginInstOpt = 0x02;
    public const byte PreLoginThreadId = 0x03;
    public const byte PreLoginMars = 0x04;
    public const byte PreLoginTraceId = 0x05;
    public const byte PreLoginTerminator = 0xFF;

    // Encryption options
    public const byte EncryptOff = 0x00;
    public const byte EncryptOn = 0x01;
    public const byte EncryptNotSup = 0x02;
    public const byte EncryptReq = 0x03;

    #endregion

    #region Collation

    /// <summary>
    /// Default collation: SQL_Latin1_General_CP1_CI_AS (codepage 1252, CI_AS).
    /// </summary>
    public static readonly byte[] DefaultCollation = { 0x09, 0x04, 0xD0, 0x00, 0x34 };

    #endregion
}
