using System.Buffers;
using System.Globalization;
using System.IO.Pipelines;
using System.Text;
using CysRedis.Core.Common;

namespace CysRedis.Core.Protocol;

/// <summary>
/// High-performance RESP protocol writer using System.IO.Pipelines.
/// Provides efficient buffering and batch flushing.
/// </summary>
public sealed class RespPipeWriter : IDisposable
{
    private readonly PipeWriter _pipeWriter;
    private bool _disposed;

    // Pre-computed common responses
    private static readonly byte[] CrLf = "\r\n"u8.ToArray();
    private static readonly byte[] NullBulkString = "$-1\r\n"u8.ToArray();
    private static readonly byte[] NullArray = "*-1\r\n"u8.ToArray();
    private static readonly byte[] EmptyBulkString = "$0\r\n\r\n"u8.ToArray();
    private static readonly byte[] EmptyArray = "*0\r\n"u8.ToArray();
    private static readonly byte[] OkResponse = "+OK\r\n"u8.ToArray();
    private static readonly byte[] PongResponse = "+PONG\r\n"u8.ToArray();
    private static readonly byte[] QueuedResponse = "+QUEUED\r\n"u8.ToArray();
    private static readonly byte[] ZeroResponse = ":0\r\n"u8.ToArray();
    private static readonly byte[] OneResponse = ":1\r\n"u8.ToArray();
    private static readonly byte[] RespNullResponse = "_\r\n"u8.ToArray();
    private static readonly byte[] TrueResponse = "#t\r\n"u8.ToArray();
    private static readonly byte[] FalseResponse = "#f\r\n"u8.ToArray();

    /// <summary>
    /// Creates a new RESP pipe writer.
    /// </summary>
    public RespPipeWriter(PipeWriter pipeWriter)
    {
        _pipeWriter = pipeWriter ?? throw new ArgumentNullException(nameof(pipeWriter));
    }

    /// <summary>
    /// Creates a new RESP pipe writer from a stream.
    /// </summary>
    public static RespPipeWriter Create(Stream stream, RedisServerOptions? options = null)
    {
        options ??= RedisServerOptions.Default;
        
        var pipeOptions = new StreamPipeWriterOptions(
            minimumBufferSize: options.MinimumSegmentSize,
            pool: BufferPool.CreateMemoryPool(),
            leaveOpen: true
        );
        
        var pipeWriter = PipeWriter.Create(stream, pipeOptions);
        return new RespPipeWriter(pipeWriter);
    }

    /// <summary>
    /// Writes a RESP value.
    /// </summary>
    public void WriteValue(in RespValue value)
    {
        switch (value.Type)
        {
            case RespType.SimpleString:
                WriteSimpleString(value.GetString() ?? string.Empty);
                break;
            case RespType.Error:
                WriteError(value.GetString() ?? "ERR unknown error");
                break;
            case RespType.Integer:
                WriteInteger(value.Integer ?? 0);
                break;
            case RespType.BulkString:
                if (value.IsNull)
                    WriteNullBulkString();
                else
                    WriteBulkString(value.Bytes ?? ReadOnlyMemory<byte>.Empty);
                break;
            case RespType.Array:
                if (value.IsNull)
                    WriteNullArray();
                else
                    WriteArray(value.Elements ?? Array.Empty<RespValue>());
                break;
            case RespType.Null:
                WriteNull();
                break;
            case RespType.Boolean:
                WriteBoolean(value.Boolean ?? false);
                break;
            case RespType.Double:
                WriteDouble(value.Double ?? 0.0);
                break;
            default:
                throw new InvalidOperationException($"Unknown RESP type: {value.Type}");
        }
    }

    /// <summary>
    /// Writes a RESP value asynchronously (includes flush).
    /// </summary>
    public async ValueTask WriteValueAsync(RespValue value, CancellationToken cancellationToken = default)
    {
        WriteValue(value);
        await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a simple string (+OK\r\n).
    /// </summary>
    public void WriteSimpleString(string value)
    {
        // Optimized common cases
        if (value == "OK")
        {
            WriteRaw(OkResponse);
            return;
        }
        if (value == "PONG")
        {
            WriteRaw(PongResponse);
            return;
        }
        if (value == "QUEUED")
        {
            WriteRaw(QueuedResponse);
            return;
        }

        var byteCount = Encoding.UTF8.GetByteCount(value);
        var span = _pipeWriter.GetSpan(1 + byteCount + 2);
        
        span[0] = (byte)'+';
        var written = 1 + Encoding.UTF8.GetBytes(value, span[1..]);
        span[written++] = (byte)'\r';
        span[written++] = (byte)'\n';
        
        _pipeWriter.Advance(written);
    }

    /// <summary>
    /// Writes an error (-ERR message\r\n).
    /// </summary>
    public void WriteError(string message)
    {
        var byteCount = Encoding.UTF8.GetByteCount(message);
        var span = _pipeWriter.GetSpan(1 + byteCount + 2);
        
        span[0] = (byte)'-';
        var written = 1 + Encoding.UTF8.GetBytes(message, span[1..]);
        span[written++] = (byte)'\r';
        span[written++] = (byte)'\n';
        
        _pipeWriter.Advance(written);
    }

    /// <summary>
    /// Writes an integer (:1000\r\n).
    /// </summary>
    public void WriteInteger(long value)
    {
        // Optimized common cases
        if (value == 0)
        {
            WriteRaw(ZeroResponse);
            return;
        }
        if (value == 1)
        {
            WriteRaw(OneResponse);
            return;
        }

        // Max long is 19 digits + sign + : + \r\n = 24 bytes
        var span = _pipeWriter.GetSpan(24);
        var written = FormatInteger(span, value);
        _pipeWriter.Advance(written);
    }

    /// <summary>
    /// Writes a bulk string ($6\r\nfoobar\r\n).
    /// </summary>
    public void WriteBulkString(ReadOnlyMemory<byte> data)
    {
        if (data.Length == 0)
        {
            WriteRaw(EmptyBulkString);
            return;
        }

        // $length\r\n + data + \r\n
        var lengthStr = data.Length.ToString();
        var headerSize = 1 + lengthStr.Length + 2;
        var totalSize = headerSize + data.Length + 2;
        
        var span = _pipeWriter.GetSpan(totalSize);
        
        span[0] = (byte)'$';
        var pos = 1;
        pos += Encoding.UTF8.GetBytes(lengthStr, span[pos..]);
        span[pos++] = (byte)'\r';
        span[pos++] = (byte)'\n';
        data.Span.CopyTo(span[pos..]);
        pos += data.Length;
        span[pos++] = (byte)'\r';
        span[pos++] = (byte)'\n';
        
        _pipeWriter.Advance(pos);
    }

    /// <summary>
    /// Writes a bulk string from a string value.
    /// </summary>
    public void WriteBulkString(string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        
        if (byteCount == 0)
        {
            WriteRaw(EmptyBulkString);
            return;
        }

        var lengthStr = byteCount.ToString();
        var headerSize = 1 + lengthStr.Length + 2;
        var totalSize = headerSize + byteCount + 2;
        
        var span = _pipeWriter.GetSpan(totalSize);
        
        span[0] = (byte)'$';
        var pos = 1;
        pos += Encoding.UTF8.GetBytes(lengthStr, span[pos..]);
        span[pos++] = (byte)'\r';
        span[pos++] = (byte)'\n';
        pos += Encoding.UTF8.GetBytes(value, span[pos..]);
        span[pos++] = (byte)'\r';
        span[pos++] = (byte)'\n';
        
        _pipeWriter.Advance(pos);
    }

    /// <summary>
    /// Writes a null bulk string ($-1\r\n).
    /// </summary>
    public void WriteNullBulkString()
    {
        WriteRaw(NullBulkString);
    }

    /// <summary>
    /// Writes an array (*2\r\n...).
    /// </summary>
    public void WriteArray(RespValue[] elements)
    {
        if (elements.Length == 0)
        {
            WriteRaw(EmptyArray);
            return;
        }

        var lengthStr = elements.Length.ToString();
        var span = _pipeWriter.GetSpan(1 + lengthStr.Length + 2);
        
        span[0] = (byte)'*';
        var pos = 1;
        pos += Encoding.UTF8.GetBytes(lengthStr, span[pos..]);
        span[pos++] = (byte)'\r';
        span[pos++] = (byte)'\n';
        
        _pipeWriter.Advance(pos);

        foreach (var element in elements)
        {
            WriteValue(element);
        }
    }

    /// <summary>
    /// Writes an array of strings as bulk strings.
    /// </summary>
    public void WriteStringArray(string[] values)
    {
        var lengthStr = values.Length.ToString();
        var span = _pipeWriter.GetSpan(1 + lengthStr.Length + 2);
        
        span[0] = (byte)'*';
        var pos = 1;
        pos += Encoding.UTF8.GetBytes(lengthStr, span[pos..]);
        span[pos++] = (byte)'\r';
        span[pos++] = (byte)'\n';
        
        _pipeWriter.Advance(pos);

        foreach (var value in values)
        {
            WriteBulkString(value);
        }
    }

    /// <summary>
    /// Writes a null array (*-1\r\n).
    /// </summary>
    public void WriteNullArray()
    {
        WriteRaw(NullArray);
    }

    /// <summary>
    /// Writes a RESP3 null value (_\r\n).
    /// </summary>
    public void WriteNull()
    {
        WriteRaw(RespNullResponse);
    }

    /// <summary>
    /// Writes a boolean (#t\r\n or #f\r\n).
    /// </summary>
    public void WriteBoolean(bool value)
    {
        WriteRaw(value ? TrueResponse : FalseResponse);
    }

    /// <summary>
    /// Writes a double (,1.23\r\n).
    /// </summary>
    public void WriteDouble(double value)
    {
        string str;
        if (double.IsPositiveInfinity(value))
            str = "inf";
        else if (double.IsNegativeInfinity(value))
            str = "-inf";
        else if (double.IsNaN(value))
            str = "nan";
        else
            str = value.ToString("G17", CultureInfo.InvariantCulture);

        var span = _pipeWriter.GetSpan(1 + str.Length + 2);
        
        span[0] = (byte)',';
        var pos = 1 + Encoding.UTF8.GetBytes(str, span[1..]);
        span[pos++] = (byte)'\r';
        span[pos++] = (byte)'\n';
        
        _pipeWriter.Advance(pos);
    }

    /// <summary>
    /// Flushes buffered data to the underlying stream.
    /// </summary>
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        await _pipeWriter.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes raw bytes to the pipe.
    /// </summary>
    private void WriteRaw(ReadOnlySpan<byte> data)
    {
        var span = _pipeWriter.GetSpan(data.Length);
        data.CopyTo(span);
        _pipeWriter.Advance(data.Length);
    }

    /// <summary>
    /// Formats an integer into the span as :value\r\n.
    /// </summary>
    private static int FormatInteger(Span<byte> span, long value)
    {
        span[0] = (byte)':';
        var pos = 1;
        
        if (value < 0)
        {
            span[pos++] = (byte)'-';
            value = -value;
        }

        // Write digits in reverse, then reverse them
        var digitStart = pos;
        do
        {
            span[pos++] = (byte)('0' + value % 10);
            value /= 10;
        } while (value > 0);

        // Reverse the digits
        var digitEnd = pos - 1;
        while (digitStart < digitEnd)
        {
            (span[digitStart], span[digitEnd]) = (span[digitEnd], span[digitStart]);
            digitStart++;
            digitEnd--;
        }

        span[pos++] = (byte)'\r';
        span[pos++] = (byte)'\n';
        
        return pos;
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        
        _pipeWriter.Complete();
    }
}
