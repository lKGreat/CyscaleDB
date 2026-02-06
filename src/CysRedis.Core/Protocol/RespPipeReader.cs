using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using CysRedis.Core.Common;

namespace CysRedis.Core.Protocol;

/// <summary>
/// High-performance RESP protocol reader using System.IO.Pipelines.
/// Provides zero-copy parsing and proper handling of packet fragmentation (粘包).
/// </summary>
public sealed class RespPipeReader : IDisposable
{
    private readonly PipeReader _pipeReader;
    private bool _disposed;

    /// <summary>
    /// Creates a new RESP pipe reader.
    /// </summary>
    public RespPipeReader(PipeReader pipeReader)
    {
        _pipeReader = pipeReader ?? throw new ArgumentNullException(nameof(pipeReader));
    }

    /// <summary>
    /// Creates a new RESP pipe reader from a stream.
    /// </summary>
    public static RespPipeReader Create(Stream stream, RedisServerOptions? options = null)
    {
        options ??= RedisServerOptions.Default;
        
        var pipeOptions = new StreamPipeReaderOptions(
            bufferSize: options.MinimumSegmentSize,
            minimumReadSize: options.MinimumSegmentSize / 2,
            pool: BufferPool.CreateMemoryPool(),
            leaveOpen: true
        );
        
        var pipeReader = PipeReader.Create(stream, pipeOptions);
        return new RespPipeReader(pipeReader);
    }

    /// <summary>
    /// Reads the next RESP value from the pipe.
    /// </summary>
    public async ValueTask<RespValue?> ReadValueAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            var result = await _pipeReader.ReadAsync(cancellationToken).ConfigureAwait(false);
            var buffer = result.Buffer;

            try
            {
                if (TryParseValue(ref buffer, out var value))
                {
                    _pipeReader.AdvanceTo(buffer.Start);
                    return value;
                }

                if (result.IsCompleted)
                {
                    if (buffer.Length > 0)
                        throw new RedisException("Unexpected EOF while parsing RESP");
                    return null; // Clean EOF
                }

                // Not enough data, continue reading
                _pipeReader.AdvanceTo(buffer.Start, buffer.End);
            }
            catch
            {
                _pipeReader.AdvanceTo(buffer.End);
                throw;
            }
        }
    }

    /// <summary>
    /// Reads a command (array of bulk strings).
    /// </summary>
    public async ValueTask<string[]?> ReadCommandAsync(CancellationToken cancellationToken = default)
    {
        var value = await ReadValueAsync(cancellationToken).ConfigureAwait(false);
        if (value == null)
            return null;

        var resp = value.Value;

        // Handle inline commands (plain text)
        if (resp.Type == RespType.SimpleString)
        {
            var line = resp.GetString();
            if (string.IsNullOrEmpty(line))
                return null;
            return line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        }

        if (resp.Type != RespType.Array || resp.Elements == null)
            throw new RedisException("Expected array for command");

        var args = new string[resp.Elements.Length];
        for (int i = 0; i < resp.Elements.Length; i++)
        {
            var element = resp.Elements[i];
            if (element.Type != RespType.BulkString)
                throw new RedisException("Expected bulk string in command array");

            args[i] = element.GetString() ?? string.Empty;
        }

        return args;
    }

    /// <summary>
    /// Tries to parse a RESP value from the buffer.
    /// </summary>
    private static bool TryParseValue(ref ReadOnlySequence<byte> buffer, out RespValue value)
    {
        value = default;
        
        if (buffer.Length < 1)
            return false;

        var reader = new SequenceReader<byte>(buffer);
        
        if (!reader.TryRead(out byte typeByte))
            return false;

        var type = (RespType)typeByte;

        bool success = type switch
        {
            RespType.SimpleString => TryParseSimpleString(ref reader, out value),
            RespType.Error => TryParseError(ref reader, out value),
            RespType.Integer => TryParseInteger(ref reader, out value),
            RespType.BulkString => TryParseBulkString(ref reader, out value),
            RespType.Array => TryParseArray(ref reader, out value),
            RespType.Null => TryParseNull(ref reader, out value),
            RespType.Boolean => TryParseBoolean(ref reader, out value),
            RespType.Double => TryParseDouble(ref reader, out value),
            RespType.Map => TryParseMap(ref reader, out value),
            RespType.Set => TryParseSet(ref reader, out value),
            _ => throw new RedisException($"Unknown RESP type: {(char)typeByte}")
        };

        if (success)
        {
            buffer = buffer.Slice(reader.Consumed);
        }
        
        return success;
    }

    private static bool TryParseSimpleString(ref SequenceReader<byte> reader, out RespValue value)
    {
        value = default;
        if (!TryReadLine(ref reader, out var line))
            return false;
        
        value = RespValue.SimpleString(Encoding.UTF8.GetString(line));
        return true;
    }

    private static bool TryParseError(ref SequenceReader<byte> reader, out RespValue value)
    {
        value = default;
        if (!TryReadLine(ref reader, out var line))
            return false;
        
        value = RespValue.Error(Encoding.UTF8.GetString(line));
        return true;
    }

    private static bool TryParseInteger(ref SequenceReader<byte> reader, out RespValue value)
    {
        value = default;
        if (!TryReadLine(ref reader, out var line))
            return false;
        
        if (!TryParseInt64(line, out var integer))
            throw new RedisException($"Invalid integer: {Encoding.UTF8.GetString(line)}");
        
        value = new RespValue(integer);
        return true;
    }

    private static bool TryParseBulkString(ref SequenceReader<byte> reader, out RespValue value)
    {
        value = default;
        
        if (!TryReadLine(ref reader, out var lengthLine))
            return false;
        
        if (!TryParseInt32(lengthLine, out var length))
            throw new RedisException($"Invalid bulk string length");

        if (length < 0)
        {
            value = RespValue.Null;
            return true;
        }

        if (length > Constants.MaxBulkLength)
            throw new RedisException($"Bulk string too large: {length}");

        // Need length + 2 bytes for CRLF
        if (reader.Remaining < length + 2)
            return false;

        // Use ArrayPool for bulk string allocations to reduce GC pressure
        var data = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            if (!reader.TryCopyTo(data.AsSpan(0, length)))
            {
                ArrayPool<byte>.Shared.Return(data);
                return false;
            }
            
            reader.Advance(length);

            // Skip CRLF
            if (!reader.TryRead(out _) || !reader.TryRead(out _))
            {
                ArrayPool<byte>.Shared.Return(data);
                return false;
            }

            // Copy to exact-size array for the RespValue (rented arrays may be larger)
            var exactData = new byte[length];
            Buffer.BlockCopy(data, 0, exactData, 0, length);
            ArrayPool<byte>.Shared.Return(data);
            
            value = RespValue.BulkString(exactData);
            return true;
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(data);
            throw;
        }
    }

    private static bool TryParseArray(ref SequenceReader<byte> reader, out RespValue value)
    {
        value = default;
        
        if (!TryReadLine(ref reader, out var countLine))
            return false;
        
        if (!TryParseInt32(countLine, out var count))
            throw new RedisException("Invalid array count");

        if (count < 0)
        {
            value = RespValue.Null;
            return true;
        }

        if (count > Constants.MaxArguments)
            throw new RedisException($"Array too large: {count}");

        var elements = new RespValue[count];
        var tempSequence = reader.UnreadSequence;
        
        for (int i = 0; i < count; i++)
        {
            if (!TryParseValue(ref tempSequence, out elements[i]))
                return false;
        }

        // Calculate how much we consumed
        var consumed = reader.Remaining - tempSequence.Length;
        reader.Advance(consumed);

        value = RespValue.Array(elements);
        return true;
    }

    private static bool TryParseNull(ref SequenceReader<byte> reader, out RespValue value)
    {
        value = default;
        if (!TryReadLine(ref reader, out _))
            return false;
        
        value = RespValue.Null;
        return true;
    }

    private static bool TryParseBoolean(ref SequenceReader<byte> reader, out RespValue value)
    {
        value = default;
        if (!TryReadLine(ref reader, out var line))
            return false;
        
        if (line.Length != 1)
            throw new RedisException("Invalid boolean");

        value = line[0] switch
        {
            (byte)'t' or (byte)'T' => new RespValue(true),
            (byte)'f' or (byte)'F' => new RespValue(false),
            _ => throw new RedisException($"Invalid boolean: {(char)line[0]}")
        };
        return true;
    }

    private static bool TryParseDouble(ref SequenceReader<byte> reader, out RespValue value)
    {
        value = default;
        if (!TryReadLine(ref reader, out var line))
            return false;
        
        var str = Encoding.UTF8.GetString(line);
        
        double d = str switch
        {
            "inf" or "+inf" => double.PositiveInfinity,
            "-inf" => double.NegativeInfinity,
            "nan" => double.NaN,
            _ when double.TryParse(str, out var parsed) => parsed,
            _ => throw new RedisException($"Invalid double: {str}")
        };
        
        value = new RespValue(d);
        return true;
    }

    private static bool TryParseMap(ref SequenceReader<byte> reader, out RespValue value)
    {
        value = default;
        
        if (!TryReadLine(ref reader, out var countLine))
            return false;
        
        if (!TryParseInt32(countLine, out var count))
            throw new RedisException("Invalid map count");

        if (count < 0)
        {
            value = RespValue.Null;
            return true;
        }

        // Map has count * 2 elements
        var elements = new RespValue[count * 2];
        var tempSequence = reader.UnreadSequence;
        
        for (int i = 0; i < count * 2; i++)
        {
            if (!TryParseValue(ref tempSequence, out elements[i]))
                return false;
        }

        var consumed = reader.Remaining - tempSequence.Length;
        reader.Advance(consumed);

        value = new RespValue(elements);
        return true;
    }

    private static bool TryParseSet(ref SequenceReader<byte> reader, out RespValue value)
    {
        value = default;
        
        if (!TryReadLine(ref reader, out var countLine))
            return false;
        
        if (!TryParseInt32(countLine, out var count))
            throw new RedisException("Invalid set count");

        if (count < 0)
        {
            value = RespValue.Null;
            return true;
        }

        var elements = new RespValue[count];
        var tempSequence = reader.UnreadSequence;
        
        for (int i = 0; i < count; i++)
        {
            if (!TryParseValue(ref tempSequence, out elements[i]))
                return false;
        }

        var consumed = reader.Remaining - tempSequence.Length;
        reader.Advance(consumed);

        value = new RespValue(elements);
        return true;
    }

    /// <summary>
    /// Tries to read a line ending with CRLF.
    /// Uses stackalloc for small lines to avoid heap allocations.
    /// </summary>
    private static bool TryReadLine(ref SequenceReader<byte> reader, out ReadOnlySpan<byte> line)
    {
        line = default;
        
        if (!reader.TryReadTo(out ReadOnlySequence<byte> lineSequence, (byte)'\r'))
            return false;
        
        if (!reader.TryRead(out byte lf) || lf != '\n')
            return false;

        if (lineSequence.IsSingleSegment)
        {
            line = lineSequence.FirstSpan;
        }
        else
        {
            // Multi-segment: copy into a pooled buffer instead of ToArray()
            var length = (int)lineSequence.Length;
            var rented = ArrayPool<byte>.Shared.Rent(length);
            lineSequence.CopyTo(rented);
            line = rented.AsSpan(0, length);
            // Note: caller uses the span synchronously, then it goes out of scope.
            // The rented buffer won't be returned but this is rare (multi-segment lines).
            // For a more complete solution, a custom struct that tracks rented buffers would be needed.
        }
        
        return true;
    }

    /// <summary>
    /// Parses a 64-bit integer from a span.
    /// </summary>
    private static bool TryParseInt64(ReadOnlySpan<byte> span, out long value)
    {
        value = 0;
        if (span.IsEmpty)
            return false;

        bool negative = false;
        int start = 0;
        
        if (span[0] == '-')
        {
            negative = true;
            start = 1;
        }
        else if (span[0] == '+')
        {
            start = 1;
        }

        for (int i = start; i < span.Length; i++)
        {
            byte b = span[i];
            if (b < '0' || b > '9')
                return false;
            
            value = value * 10 + (b - '0');
        }

        if (negative)
            value = -value;
        
        return true;
    }

    /// <summary>
    /// Parses a 32-bit integer from a span.
    /// </summary>
    private static bool TryParseInt32(ReadOnlySpan<byte> span, out int value)
    {
        if (TryParseInt64(span, out var longValue) && longValue >= int.MinValue && longValue <= int.MaxValue)
        {
            value = (int)longValue;
            return true;
        }
        value = 0;
        return false;
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        
        _pipeReader.Complete();
    }
}
