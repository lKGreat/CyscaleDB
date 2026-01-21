using System.Buffers;
using System.Text;
using CysRedis.Core.Common;

namespace CysRedis.Core.Protocol;

/// <summary>
/// RESP protocol reader for parsing Redis commands.
/// </summary>
public class RespReader
{
    private readonly Stream _stream;
    private readonly byte[] _buffer;
    private int _position;
    private int _length;

    /// <summary>
    /// Creates a new RESP reader.
    /// </summary>
    public RespReader(Stream stream, int bufferSize = Constants.DefaultBufferSize)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _buffer = new byte[bufferSize];
        _position = 0;
        _length = 0;
    }

    /// <summary>
    /// Reads the next RESP value from the stream.
    /// </summary>
    public async Task<RespValue?> ReadValueAsync(CancellationToken cancellationToken = default)
    {
        // Read type byte
        var typeByte = await ReadByteAsync(cancellationToken);
        if (typeByte < 0)
            return null; // EOF

        var type = (RespType)typeByte;

        return type switch
        {
            RespType.SimpleString => await ReadSimpleStringAsync(cancellationToken),
            RespType.Error => await ReadErrorAsync(cancellationToken),
            RespType.Integer => await ReadIntegerAsync(cancellationToken),
            RespType.BulkString => await ReadBulkStringAsync(cancellationToken),
            RespType.Array => await ReadArrayAsync(cancellationToken),
            RespType.Null => RespValue.Null,
            RespType.Boolean => await ReadBooleanAsync(cancellationToken),
            RespType.Double => await ReadDoubleAsync(cancellationToken),
            RespType.Map => await ReadMapAsync(cancellationToken),
            RespType.Set => await ReadSetAsync(cancellationToken),
            _ => throw new RedisException($"Unknown RESP type: {(char)typeByte}")
        };
    }

    /// <summary>
    /// Reads a command (array of bulk strings).
    /// </summary>
    public async Task<string[]?> ReadCommandAsync(CancellationToken cancellationToken = default)
    {
        var value = await ReadValueAsync(cancellationToken);
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

    private async Task<RespValue> ReadSimpleStringAsync(CancellationToken cancellationToken)
    {
        var line = await ReadLineAsync(cancellationToken);
        return RespValue.SimpleString(line);
    }

    private async Task<RespValue> ReadErrorAsync(CancellationToken cancellationToken)
    {
        var line = await ReadLineAsync(cancellationToken);
        return RespValue.Error(line);
    }

    private async Task<RespValue> ReadIntegerAsync(CancellationToken cancellationToken)
    {
        var line = await ReadLineAsync(cancellationToken);
        if (!long.TryParse(line, out var value))
            throw new RedisException($"Invalid integer: {line}");
        return new RespValue(value);
    }

    private async Task<RespValue> ReadBulkStringAsync(CancellationToken cancellationToken)
    {
        var lengthLine = await ReadLineAsync(cancellationToken);
        if (!int.TryParse(lengthLine, out var length))
            throw new RedisException($"Invalid bulk string length: {lengthLine}");

        if (length < 0)
            return RespValue.Null;

        if (length > Constants.MaxBulkLength)
            throw new RedisException($"Bulk string too large: {length}");

        var data = new byte[length];
        await ReadExactAsync(data, 0, length, cancellationToken);

        // Read trailing CRLF
        await ReadByteAsync(cancellationToken); // CR
        await ReadByteAsync(cancellationToken); // LF

        return RespValue.BulkString(data);
    }

    private async Task<RespValue> ReadArrayAsync(CancellationToken cancellationToken)
    {
        var countLine = await ReadLineAsync(cancellationToken);
        if (!int.TryParse(countLine, out var count))
            throw new RedisException($"Invalid array count: {countLine}");

        if (count < 0)
            return RespValue.Null;

        if (count > Constants.MaxArguments)
            throw new RedisException($"Array too large: {count}");

        var elements = new RespValue[count];
        for (int i = 0; i < count; i++)
        {
            var element = await ReadValueAsync(cancellationToken);
            if (element == null)
                throw new RedisException("Unexpected EOF in array");
            elements[i] = element.Value;
        }

        return RespValue.Array(elements);
    }

    private async Task<RespValue> ReadBooleanAsync(CancellationToken cancellationToken)
    {
        var line = await ReadLineAsync(cancellationToken);
        return line.ToLowerInvariant() switch
        {
            "t" => new RespValue(true),
            "f" => new RespValue(false),
            _ => throw new RedisException($"Invalid boolean: {line}")
        };
    }

    private async Task<RespValue> ReadDoubleAsync(CancellationToken cancellationToken)
    {
        var line = await ReadLineAsync(cancellationToken);
        
        if (line == "inf" || line == "+inf")
            return new RespValue(double.PositiveInfinity);
        if (line == "-inf")
            return new RespValue(double.NegativeInfinity);
        if (line == "nan")
            return new RespValue(double.NaN);
            
        if (!double.TryParse(line, out var value))
            throw new RedisException($"Invalid double: {line}");
        return new RespValue(value);
    }

    private async Task<RespValue> ReadMapAsync(CancellationToken cancellationToken)
    {
        var countLine = await ReadLineAsync(cancellationToken);
        if (!int.TryParse(countLine, out var count))
            throw new RedisException($"Invalid map count: {countLine}");

        if (count < 0)
            return RespValue.Null;

        // Map has count * 2 elements (key-value pairs)
        var elements = new RespValue[count * 2];
        for (int i = 0; i < count * 2; i++)
        {
            var element = await ReadValueAsync(cancellationToken);
            if (element == null)
                throw new RedisException("Unexpected EOF in map");
            elements[i] = element.Value;
        }

        return new RespValue(elements);
    }

    private async Task<RespValue> ReadSetAsync(CancellationToken cancellationToken)
    {
        var countLine = await ReadLineAsync(cancellationToken);
        if (!int.TryParse(countLine, out var count))
            throw new RedisException($"Invalid set count: {countLine}");

        if (count < 0)
            return RespValue.Null;

        var elements = new RespValue[count];
        for (int i = 0; i < count; i++)
        {
            var element = await ReadValueAsync(cancellationToken);
            if (element == null)
                throw new RedisException("Unexpected EOF in set");
            elements[i] = element.Value;
        }

        return new RespValue(elements);
    }

    private async Task<string> ReadLineAsync(CancellationToken cancellationToken)
    {
        var sb = new StringBuilder();
        int b;
        
        while ((b = await ReadByteAsync(cancellationToken)) >= 0)
        {
            if (b == '\r')
            {
                // Expect LF
                await ReadByteAsync(cancellationToken);
                break;
            }
            sb.Append((char)b);
        }

        return sb.ToString();
    }

    private async Task<int> ReadByteAsync(CancellationToken cancellationToken)
    {
        if (_position >= _length)
        {
            _length = await _stream.ReadAsync(_buffer, 0, _buffer.Length, cancellationToken);
            _position = 0;
            
            if (_length == 0)
                return -1; // EOF
        }

        return _buffer[_position++];
    }

    private async Task ReadExactAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        int totalRead = 0;
        
        while (totalRead < count)
        {
            // First, use any buffered data
            int bufferedAvailable = _length - _position;
            if (bufferedAvailable > 0)
            {
                int toCopy = Math.Min(bufferedAvailable, count - totalRead);
                Buffer.BlockCopy(_buffer, _position, buffer, offset + totalRead, toCopy);
                _position += toCopy;
                totalRead += toCopy;
            }
            else
            {
                // Need to read more from stream
                int remaining = count - totalRead;
                if (remaining >= _buffer.Length)
                {
                    // Read directly into target buffer
                    int read = await _stream.ReadAsync(buffer, offset + totalRead, remaining, cancellationToken);
                    if (read == 0)
                        throw new RedisException("Unexpected EOF");
                    totalRead += read;
                }
                else
                {
                    // Refill our buffer
                    _length = await _stream.ReadAsync(_buffer, 0, _buffer.Length, cancellationToken);
                    _position = 0;
                    if (_length == 0)
                        throw new RedisException("Unexpected EOF");
                }
            }
        }
    }

    /// <summary>
    /// Tries to read an inline command (plain text without RESP framing).
    /// </summary>
    public async Task<string[]?> TryReadInlineCommandAsync(CancellationToken cancellationToken = default)
    {
        // Peek at the first byte to determine if this is an inline command
        var firstByte = await ReadByteAsync(cancellationToken);
        if (firstByte < 0)
            return null;

        // If it's a RESP type indicator, we need to handle it as RESP
        if (firstByte == '+' || firstByte == '-' || firstByte == ':' || 
            firstByte == '$' || firstByte == '*')
        {
            // Put back conceptually - we'll handle this in the main read path
            // For now, just read the line and parse as RESP
            throw new InvalidOperationException("Use ReadValueAsync for RESP data");
        }

        // Otherwise, read as inline command
        var sb = new StringBuilder();
        sb.Append((char)firstByte);

        int b;
        while ((b = await ReadByteAsync(cancellationToken)) >= 0)
        {
            if (b == '\r')
            {
                await ReadByteAsync(cancellationToken); // LF
                break;
            }
            if (b == '\n')
                break;
                
            sb.Append((char)b);
        }

        var line = sb.ToString().Trim();
        if (string.IsNullOrEmpty(line))
            return null;

        return line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
    }
}
