using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using CysRedis.Core.Protocol;

namespace CysRedis.Tests;

/// <summary>
/// Tests for RESP protocol parsing (RespPipeReader) and writing (RespPipeWriter).
/// Verifies round-trip encoding/decoding compatibility for RESP2 and RESP3 types.
/// </summary>
public class RespProtocolTests
{
    #region RESP2 Types

    [Fact]
    public async Task ReadSimpleString()
    {
        var reader = CreateReader("+OK\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(RespType.SimpleString, value!.Value.Type);
        Assert.Equal("OK", value.Value.GetString());
    }

    [Fact]
    public async Task ReadError()
    {
        var reader = CreateReader("-ERR unknown command\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(RespType.Error, value!.Value.Type);
        Assert.Equal("ERR unknown command", value.Value.GetString());
    }

    [Fact]
    public async Task ReadInteger()
    {
        var reader = CreateReader(":1000\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(RespType.Integer, value!.Value.Type);
        Assert.Equal(1000, value.Value.Integer);
    }

    [Fact]
    public async Task ReadNegativeInteger()
    {
        var reader = CreateReader(":-42\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(-42, value!.Value.Integer);
    }

    [Fact]
    public async Task ReadBulkString()
    {
        var reader = CreateReader("$6\r\nfoobar\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(RespType.BulkString, value!.Value.Type);
        Assert.Equal("foobar", value.Value.GetString());
    }

    [Fact]
    public async Task ReadEmptyBulkString()
    {
        var reader = CreateReader("$0\r\n\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(RespType.BulkString, value!.Value.Type);
        Assert.Equal("", value.Value.GetString());
    }

    [Fact]
    public async Task ReadNullBulkString()
    {
        var reader = CreateReader("$-1\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.True(value!.Value.IsNull);
    }

    [Fact]
    public async Task ReadArray()
    {
        var data = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"u8;
        var reader = CreateReader(data);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(RespType.Array, value!.Value.Type);
        Assert.Equal(2, value.Value.Count);
        Assert.Equal("foo", value.Value[0].GetString());
        Assert.Equal("bar", value.Value[1].GetString());
    }

    [Fact]
    public async Task ReadEmptyArray()
    {
        var reader = CreateReader("*0\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(RespType.Array, value!.Value.Type);
        Assert.Equal(0, value.Value.Count);
    }

    [Fact]
    public async Task ReadNullArray()
    {
        var reader = CreateReader("*-1\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.True(value!.Value.IsNull);
    }

    [Fact]
    public async Task ReadNestedArray()
    {
        // *2\r\n *2\r\n :1\r\n :2\r\n *2\r\n :3\r\n :4\r\n
        var data = "*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n"u8;
        var reader = CreateReader(data);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(2, value!.Value.Count);
        Assert.Equal(2, value.Value[0].Count);
        Assert.Equal(1, value.Value[0][0].Integer);
        Assert.Equal(4, value.Value[1][1].Integer);
    }

    [Fact]
    public async Task ReadCommand()
    {
        var data = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"u8;
        var reader = CreateReader(data);
        var command = await reader.ReadCommandAsync();
        Assert.NotNull(command);
        Assert.Equal(3, command!.Length);
        Assert.Equal("SET", command[0]);
        Assert.Equal("key", command[1]);
        Assert.Equal("value", command[2]);
    }

    [Fact]
    public async Task ReadInlineCommand()
    {
        var data = "+PING\r\n"u8;
        var reader = CreateReader(data);
        var command = await reader.ReadCommandAsync();
        Assert.NotNull(command);
        Assert.Single(command!);
        Assert.Equal("PING", command[0]);
    }

    [Fact]
    public async Task ReadMultipleCommands()
    {
        var data = "*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n"u8;
        var reader = CreateReader(data);

        var cmd1 = await reader.ReadCommandAsync();
        Assert.Equal("PING", cmd1![0]);

        var cmd2 = await reader.ReadCommandAsync();
        Assert.Equal("SET", cmd2![0]);
        Assert.Equal("a", cmd2[1]);
        Assert.Equal("b", cmd2[2]);
    }

    #endregion

    #region RESP3 Types

    [Fact]
    public async Task ReadBoolean_True()
    {
        var reader = CreateReader("#t\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(RespType.Boolean, value!.Value.Type);
        Assert.True(value.Value.Boolean);
    }

    [Fact]
    public async Task ReadBoolean_False()
    {
        var reader = CreateReader("#f\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.False(value!.Value.Boolean);
    }

    [Fact]
    public async Task ReadDouble()
    {
        var reader = CreateReader(",3.14\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(RespType.Double, value!.Value.Type);
        Assert.Equal(3.14, value.Value.Double!.Value, 2);
    }

    [Fact]
    public async Task ReadDouble_Inf()
    {
        var reader = CreateReader(",inf\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.True(double.IsPositiveInfinity(value!.Value.Double!.Value));
    }

    [Fact]
    public async Task ReadDouble_NegInf()
    {
        var reader = CreateReader(",-inf\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.True(double.IsNegativeInfinity(value!.Value.Double!.Value));
    }

    [Fact]
    public async Task ReadNull_Resp3()
    {
        var reader = CreateReader("_\r\n"u8);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(RespType.Null, value!.Value.Type);
        Assert.True(value.Value.IsNull);
    }

    [Fact]
    public async Task ReadMap()
    {
        // %2\r\n +key1\r\n :1\r\n +key2\r\n :2\r\n
        var data = "%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n"u8;
        var reader = CreateReader(data);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        // Map stored as flat array: [key1, val1, key2, val2]
        Assert.Equal(4, value!.Value.Elements!.Length);
        Assert.Equal("key1", value.Value.Elements[0].GetString());
        Assert.Equal(1, value.Value.Elements[1].Integer);
        Assert.Equal("key2", value.Value.Elements[2].GetString());
        Assert.Equal(2, value.Value.Elements[3].Integer);
    }

    [Fact]
    public async Task ReadSet()
    {
        // ~3\r\n :1\r\n :2\r\n :3\r\n
        var data = "~3\r\n:1\r\n:2\r\n:3\r\n"u8;
        var reader = CreateReader(data);
        var value = await reader.ReadValueAsync();
        Assert.NotNull(value);
        Assert.Equal(3, value!.Value.Elements!.Length);
        Assert.Equal(1, value.Value.Elements[0].Integer);
        Assert.Equal(2, value.Value.Elements[1].Integer);
        Assert.Equal(3, value.Value.Elements[2].Integer);
    }

    #endregion

    #region Writer Tests

    [Fact]
    public async Task WriteSimpleString_OK()
    {
        var result = await WriteAndRead(writer => writer.WriteSimpleString("OK"));
        Assert.Equal("+OK\r\n", result);
    }

    [Fact]
    public async Task WriteError()
    {
        var result = await WriteAndRead(writer => writer.WriteError("ERR test"));
        Assert.Equal("-ERR test\r\n", result);
    }

    [Fact]
    public async Task WriteInteger_Zero()
    {
        var result = await WriteAndRead(writer => writer.WriteInteger(0));
        Assert.Equal(":0\r\n", result);
    }

    [Fact]
    public async Task WriteInteger_Positive()
    {
        var result = await WriteAndRead(writer => writer.WriteInteger(42));
        Assert.Equal(":42\r\n", result);
    }

    [Fact]
    public async Task WriteInteger_Negative()
    {
        var result = await WriteAndRead(writer => writer.WriteInteger(-1));
        Assert.Equal(":-1\r\n", result);
    }

    [Fact]
    public async Task WriteBulkString()
    {
        var result = await WriteAndRead(writer => writer.WriteBulkString("hello"));
        Assert.Equal("$5\r\nhello\r\n", result);
    }

    [Fact]
    public async Task WriteNullBulkString()
    {
        var result = await WriteAndRead(writer => writer.WriteNullBulkString());
        Assert.Equal("$-1\r\n", result);
    }

    [Fact]
    public async Task WriteNull_Resp3()
    {
        var result = await WriteAndRead(writer => writer.WriteNull());
        Assert.Equal("_\r\n", result);
    }

    [Fact]
    public async Task WriteBoolean_True()
    {
        var result = await WriteAndRead(writer => writer.WriteBoolean(true));
        Assert.Equal("#t\r\n", result);
    }

    [Fact]
    public async Task WriteBoolean_False()
    {
        var result = await WriteAndRead(writer => writer.WriteBoolean(false));
        Assert.Equal("#f\r\n", result);
    }

    [Fact]
    public async Task WriteEmptyArray()
    {
        var result = await WriteAndRead(writer => writer.WriteArray(Array.Empty<RespValue>()));
        Assert.Equal("*0\r\n", result);
    }

    [Fact]
    public async Task WriteNullArray()
    {
        var result = await WriteAndRead(writer => writer.WriteNullArray());
        Assert.Equal("*-1\r\n", result);
    }

    [Fact]
    public async Task WriteArray_WithElements()
    {
        var result = await WriteAndRead(writer => writer.WriteArray(new[]
        {
            RespValue.BulkString("foo"),
            new RespValue(42)
        }));
        Assert.Equal("*2\r\n$3\r\nfoo\r\n:42\r\n", result);
    }

    [Fact]
    public async Task WriteBulkString_Binary()
    {
        var data = new byte[] { 0x00, 0x01, 0xFF, 0xFE };
        var result = await WriteAndReadBytes(writer => writer.WriteBulkString(new ReadOnlyMemory<byte>(data)));
        // $4\r\n<4 binary bytes>\r\n
        Assert.Equal(4 + 2 + 4 + 2, result.Length); // "$4\r\n" + 4 bytes + "\r\n"
        Assert.Equal((byte)'$', result[0]);
        Assert.Equal((byte)'4', result[1]);
        Assert.Equal(0x00, result[4]);
        Assert.Equal(0x01, result[5]);
        Assert.Equal(0xFF, result[6]);
        Assert.Equal(0xFE, result[7]);
    }

    #endregion

    #region Round-trip Tests

    [Fact]
    public async Task RoundTrip_SimpleString()
    {
        var original = RespValue.SimpleString("hello world");
        var result = await WriteAndReadBack(original);
        Assert.Equal(RespType.SimpleString, result.Type);
        Assert.Equal("hello world", result.GetString());
    }

    [Fact]
    public async Task RoundTrip_Error()
    {
        var original = RespValue.Error("ERR something went wrong");
        var result = await WriteAndReadBack(original);
        Assert.Equal(RespType.Error, result.Type);
        Assert.Equal("ERR something went wrong", result.GetString());
    }

    [Fact]
    public async Task RoundTrip_Integer()
    {
        var original = new RespValue(long.MaxValue);
        var result = await WriteAndReadBack(original);
        Assert.Equal(long.MaxValue, result.Integer);
    }

    [Fact]
    public async Task RoundTrip_BulkString()
    {
        var original = RespValue.BulkString("The quick brown fox jumps over the lazy dog");
        var result = await WriteAndReadBack(original);
        Assert.Equal("The quick brown fox jumps over the lazy dog", result.GetString());
    }

    [Fact]
    public async Task RoundTrip_NullBulkString()
    {
        var original = RespValue.Null;
        var result = await WriteAndReadBack(original);
        Assert.True(result.IsNull);
    }

    [Fact]
    public async Task RoundTrip_Array()
    {
        var original = RespValue.Array(
            RespValue.BulkString("SET"),
            RespValue.BulkString("mykey"),
            RespValue.BulkString("myvalue")
        );
        var result = await WriteAndReadBack(original);
        Assert.Equal(3, result.Count);
        Assert.Equal("SET", result[0].GetString());
        Assert.Equal("mykey", result[1].GetString());
        Assert.Equal("myvalue", result[2].GetString());
    }

    #endregion

    #region Helpers

    private static RespPipeReader CreateReader(ReadOnlySpan<byte> data)
    {
        var stream = new MemoryStream(data.ToArray());
        return RespPipeReader.Create(stream);
    }

    private static async Task<string> WriteAndRead(Action<RespPipeWriter> writeAction)
    {
        var bytes = await WriteAndReadBytes(writeAction);
        return Encoding.UTF8.GetString(bytes);
    }

    private static async Task<byte[]> WriteAndReadBytes(Action<RespPipeWriter> writeAction)
    {
        var stream = new MemoryStream();
        var writer = RespPipeWriter.Create(stream);
        writer.WriteTimeout = TimeSpan.Zero; // Disable for tests
        writeAction(writer);
        await writer.FlushAsync();
        writer.Dispose();
        return stream.ToArray();
    }

    private static async Task<RespValue> WriteAndReadBack(RespValue value)
    {
        var stream = new MemoryStream();
        var writer = RespPipeWriter.Create(stream);
        writer.WriteTimeout = TimeSpan.Zero;
        writer.WriteValue(value);
        await writer.FlushAsync();
        writer.Dispose();

        stream.Position = 0;
        var reader = RespPipeReader.Create(stream);
        var result = await reader.ReadValueAsync();
        Assert.NotNull(result);
        reader.Dispose();
        return result!.Value;
    }

    #endregion
}
