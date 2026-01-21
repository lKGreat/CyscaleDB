using System.Buffers;
using System.Globalization;
using System.Text;

namespace CysRedis.Core.Protocol;

/// <summary>
/// RESP protocol writer for generating Redis responses.
/// </summary>
public class RespWriter
{
    private readonly Stream _stream;
    private readonly byte[] _buffer;
    private int _position;

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
    /// Creates a new RESP writer.
    /// </summary>
    public RespWriter(Stream stream, int bufferSize = 16 * 1024)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _buffer = new byte[bufferSize];
        _position = 0;
    }

    /// <summary>
    /// Writes a RESP value to the stream.
    /// </summary>
    public async Task WriteValueAsync(RespValue value, CancellationToken cancellationToken = default)
    {
        switch (value.Type)
        {
            case RespType.SimpleString:
                await WriteSimpleStringAsync(value.GetString() ?? string.Empty, cancellationToken);
                break;
            case RespType.Error:
                await WriteErrorAsync(value.GetString() ?? "ERR unknown error", cancellationToken);
                break;
            case RespType.Integer:
                await WriteIntegerAsync(value.Integer ?? 0, cancellationToken);
                break;
            case RespType.BulkString:
                if (value.IsNull)
                    await WriteNullBulkStringAsync(cancellationToken);
                else
                    await WriteBulkStringAsync(value.Bytes ?? ReadOnlyMemory<byte>.Empty, cancellationToken);
                break;
            case RespType.Array:
                if (value.IsNull)
                    await WriteNullArrayAsync(cancellationToken);
                else
                    await WriteArrayAsync(value.Elements ?? System.Array.Empty<RespValue>(), cancellationToken);
                break;
            case RespType.Null:
                await WriteNullAsync(cancellationToken);
                break;
            case RespType.Boolean:
                await WriteBooleanAsync(value.Boolean ?? false, cancellationToken);
                break;
            case RespType.Double:
                await WriteDoubleAsync(value.Double ?? 0.0, cancellationToken);
                break;
            default:
                throw new InvalidOperationException($"Unknown RESP type: {value.Type}");
        }
    }

    /// <summary>
    /// Writes a simple string (+OK\r\n).
    /// </summary>
    public async Task WriteSimpleStringAsync(string value, CancellationToken cancellationToken = default)
    {
        // Optimized common cases
        if (value == "OK")
        {
            await WriteRawAsync(OkResponse, cancellationToken);
            return;
        }
        if (value == "PONG")
        {
            await WriteRawAsync(PongResponse, cancellationToken);
            return;
        }
        if (value == "QUEUED")
        {
            await WriteRawAsync(QueuedResponse, cancellationToken);
            return;
        }

        await WriteByteAsync((byte)'+', cancellationToken);
        await WriteStringAsync(value, cancellationToken);
        await WriteCrLfAsync(cancellationToken);
    }

    /// <summary>
    /// Writes an error (-ERR message\r\n).
    /// </summary>
    public async Task WriteErrorAsync(string message, CancellationToken cancellationToken = default)
    {
        await WriteByteAsync((byte)'-', cancellationToken);
        await WriteStringAsync(message, cancellationToken);
        await WriteCrLfAsync(cancellationToken);
    }

    /// <summary>
    /// Writes an integer (:1000\r\n).
    /// </summary>
    public async Task WriteIntegerAsync(long value, CancellationToken cancellationToken = default)
    {
        // Optimized common cases
        if (value == 0)
        {
            await WriteRawAsync(ZeroResponse, cancellationToken);
            return;
        }
        if (value == 1)
        {
            await WriteRawAsync(OneResponse, cancellationToken);
            return;
        }

        await WriteByteAsync((byte)':', cancellationToken);
        await WriteStringAsync(value.ToString(), cancellationToken);
        await WriteCrLfAsync(cancellationToken);
    }

    /// <summary>
    /// Writes a bulk string ($6\r\nfoobar\r\n).
    /// </summary>
    public async Task WriteBulkStringAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        if (data.Length == 0)
        {
            await WriteRawAsync(EmptyBulkString, cancellationToken);
            return;
        }

        await WriteByteAsync((byte)'$', cancellationToken);
        await WriteStringAsync(data.Length.ToString(), cancellationToken);
        await WriteCrLfAsync(cancellationToken);
        await WriteBytesAsync(data, cancellationToken);
        await WriteCrLfAsync(cancellationToken);
    }

    /// <summary>
    /// Writes a bulk string from a string value.
    /// </summary>
    public async Task WriteBulkStringAsync(string value, CancellationToken cancellationToken = default)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        await WriteBulkStringAsync(bytes, cancellationToken);
    }

    /// <summary>
    /// Writes a null bulk string ($-1\r\n).
    /// </summary>
    public async Task WriteNullBulkStringAsync(CancellationToken cancellationToken = default)
    {
        await WriteRawAsync(NullBulkString, cancellationToken);
    }

    /// <summary>
    /// Writes an array (*2\r\n...).
    /// </summary>
    public async Task WriteArrayAsync(RespValue[] elements, CancellationToken cancellationToken = default)
    {
        if (elements.Length == 0)
        {
            await WriteRawAsync(EmptyArray, cancellationToken);
            return;
        }

        await WriteByteAsync((byte)'*', cancellationToken);
        await WriteStringAsync(elements.Length.ToString(), cancellationToken);
        await WriteCrLfAsync(cancellationToken);

        foreach (var element in elements)
        {
            await WriteValueAsync(element, cancellationToken);
        }
    }

    /// <summary>
    /// Writes an array of strings as bulk strings.
    /// </summary>
    public async Task WriteStringArrayAsync(string[] values, CancellationToken cancellationToken = default)
    {
        await WriteByteAsync((byte)'*', cancellationToken);
        await WriteStringAsync(values.Length.ToString(), cancellationToken);
        await WriteCrLfAsync(cancellationToken);

        foreach (var value in values)
        {
            await WriteBulkStringAsync(value, cancellationToken);
        }
    }

    /// <summary>
    /// Writes a null array (*-1\r\n).
    /// </summary>
    public async Task WriteNullArrayAsync(CancellationToken cancellationToken = default)
    {
        await WriteRawAsync(NullArray, cancellationToken);
    }

    /// <summary>
    /// Writes a RESP3 null value (_\r\n).
    /// </summary>
    public async Task WriteNullAsync(CancellationToken cancellationToken = default)
    {
        await WriteRawAsync(RespNullResponse, cancellationToken);
    }

    /// <summary>
    /// Writes a boolean (#t\r\n or #f\r\n).
    /// </summary>
    public async Task WriteBooleanAsync(bool value, CancellationToken cancellationToken = default)
    {
        await WriteRawAsync(value ? TrueResponse : FalseResponse, cancellationToken);
    }

    /// <summary>
    /// Writes a double (,1.23\r\n).
    /// </summary>
    public async Task WriteDoubleAsync(double value, CancellationToken cancellationToken = default)
    {
        await WriteByteAsync((byte)',', cancellationToken);

        string str;
        if (double.IsPositiveInfinity(value))
            str = "inf";
        else if (double.IsNegativeInfinity(value))
            str = "-inf";
        else if (double.IsNaN(value))
            str = "nan";
        else
            str = value.ToString("G17", CultureInfo.InvariantCulture);

        await WriteStringAsync(str, cancellationToken);
        await WriteCrLfAsync(cancellationToken);
    }

    /// <summary>
    /// Flushes any buffered data to the stream.
    /// </summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_position > 0)
        {
            await _stream.WriteAsync(_buffer, 0, _position, cancellationToken);
            _position = 0;
        }
        await _stream.FlushAsync(cancellationToken);
    }

    private async Task WriteByteAsync(byte b, CancellationToken cancellationToken)
    {
        if (_position >= _buffer.Length)
            await FlushAsync(cancellationToken);
        
        _buffer[_position++] = b;
    }

    private async Task WriteBytesAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        var span = data.Span;
        
        if (span.Length <= _buffer.Length - _position)
        {
            // Fits in current buffer
            span.CopyTo(_buffer.AsSpan(_position));
            _position += span.Length;
        }
        else
        {
            // Flush and write directly
            await FlushAsync(cancellationToken);
            await _stream.WriteAsync(data, cancellationToken);
        }
    }

    private async Task WriteStringAsync(string value, CancellationToken cancellationToken)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        await WriteBytesAsync(bytes, cancellationToken);
    }

    private async Task WriteCrLfAsync(CancellationToken cancellationToken)
    {
        await WriteBytesAsync(CrLf, cancellationToken);
    }

    private async Task WriteRawAsync(byte[] data, CancellationToken cancellationToken)
    {
        await WriteBytesAsync(data, cancellationToken);
    }
}
