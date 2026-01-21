using System.Buffers;
using System.IO.Compression;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Protocol.Transport;

/// <summary>
/// Compression algorithm types.
/// </summary>
public enum CompressionAlgorithm
{
    /// <summary>
    /// No compression.
    /// </summary>
    None,

    /// <summary>
    /// zlib compression (MySQL standard).
    /// </summary>
    Zlib,

    /// <summary>
    /// Zstandard compression (MySQL 8.0+).
    /// </summary>
    Zstd
}

/// <summary>
/// Compression configuration options.
/// </summary>
public class CompressionOptions
{
    /// <summary>
    /// Whether compression is enabled.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Preferred compression algorithm.
    /// </summary>
    public CompressionAlgorithm PreferredAlgorithm { get; set; } = CompressionAlgorithm.Zlib;

    /// <summary>
    /// Minimum payload size to trigger compression (in bytes).
    /// </summary>
    public int CompressionThreshold { get; set; } = 50;

    /// <summary>
    /// Compression level (1-9 for zlib, 1-22 for zstd).
    /// </summary>
    public int CompressionLevel { get; set; } = 6;
}

/// <summary>
/// Handles protocol compression for MySQL connections.
/// Supports zlib and zstd compression algorithms.
/// </summary>
public sealed class CompressionHandler
{
    private readonly CompressionOptions _options;
    private readonly Logger _logger;

    // Statistics
    private long _compressedBytes;
    private long _uncompressedBytes;
    private long _compressionCount;
    private long _decompressionCount;

    /// <summary>
    /// Gets whether compression is enabled.
    /// </summary>
    public bool IsEnabled => _options.Enabled;

    /// <summary>
    /// Gets the negotiated compression algorithm.
    /// </summary>
    public CompressionAlgorithm Algorithm { get; private set; }

    /// <summary>
    /// Creates a new compression handler.
    /// </summary>
    public CompressionHandler(CompressionOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = LogManager.Default.GetLogger<CompressionHandler>();
        Algorithm = options.Enabled ? options.PreferredAlgorithm : CompressionAlgorithm.None;
    }

    /// <summary>
    /// Sets the negotiated compression algorithm based on client capabilities.
    /// </summary>
    public void SetAlgorithm(CompressionAlgorithm algorithm)
    {
        if (_options.Enabled)
        {
            Algorithm = algorithm;
            _logger.Debug("Compression algorithm set to: {0}", algorithm);
        }
    }

    /// <summary>
    /// Compresses data if compression is enabled and payload exceeds threshold.
    /// </summary>
    /// <param name="data">Data to compress.</param>
    /// <returns>Compressed data or original data if compression not beneficial.</returns>
    public byte[] Compress(ReadOnlySpan<byte> data)
    {
        if (!_options.Enabled || Algorithm == CompressionAlgorithm.None)
        {
            return data.ToArray();
        }

        // Don't compress small payloads
        if (data.Length < _options.CompressionThreshold)
        {
            return data.ToArray();
        }

        try
        {
            byte[] compressed;

            switch (Algorithm)
            {
                case CompressionAlgorithm.Zlib:
                    compressed = CompressZlib(data);
                    break;

                case CompressionAlgorithm.Zstd:
                    compressed = CompressZstd(data);
                    break;

                default:
                    return data.ToArray();
            }

            // Only use compressed data if it's actually smaller
            if (compressed.Length < data.Length)
            {
                Interlocked.Add(ref _uncompressedBytes, data.Length);
                Interlocked.Add(ref _compressedBytes, compressed.Length);
                Interlocked.Increment(ref _compressionCount);
                return compressed;
            }

            return data.ToArray();
        }
        catch (Exception ex)
        {
            _logger.Error("Compression failed", ex);
            return data.ToArray();
        }
    }

    /// <summary>
    /// Decompresses data.
    /// </summary>
    /// <param name="compressedData">Compressed data.</param>
    /// <param name="uncompressedLength">Expected uncompressed length.</param>
    /// <returns>Decompressed data.</returns>
    public byte[] Decompress(ReadOnlySpan<byte> compressedData, int uncompressedLength)
    {
        if (!_options.Enabled || Algorithm == CompressionAlgorithm.None)
        {
            return compressedData.ToArray();
        }

        try
        {
            byte[] decompressed;

            switch (Algorithm)
            {
                case CompressionAlgorithm.Zlib:
                    decompressed = DecompressZlib(compressedData, uncompressedLength);
                    break;

                case CompressionAlgorithm.Zstd:
                    decompressed = DecompressZstd(compressedData, uncompressedLength);
                    break;

                default:
                    return compressedData.ToArray();
            }

            Interlocked.Increment(ref _decompressionCount);
            return decompressed;
        }
        catch (Exception ex)
        {
            _logger.Error("Decompression failed", ex);
            throw;
        }
    }

    /// <summary>
    /// Compresses data using zlib.
    /// </summary>
    private byte[] CompressZlib(ReadOnlySpan<byte> data)
    {
        using var output = new MemoryStream();
        using (var zlib = new ZLibStream(output, GetZlibCompressionLevel(), leaveOpen: true))
        {
            zlib.Write(data);
        }
        return output.ToArray();
    }

    /// <summary>
    /// Decompresses data using zlib.
    /// </summary>
    private static byte[] DecompressZlib(ReadOnlySpan<byte> compressedData, int uncompressedLength)
    {
        var result = new byte[uncompressedLength];
        using var input = new MemoryStream(compressedData.ToArray());
        using var zlib = new ZLibStream(input, CompressionMode.Decompress);
        
        int totalRead = 0;
        while (totalRead < uncompressedLength)
        {
            int read = zlib.Read(result, totalRead, uncompressedLength - totalRead);
            if (read == 0) break;
            totalRead += read;
        }
        
        return result;
    }

    /// <summary>
    /// Compresses data using zstd.
    /// Note: Requires ZstdSharp.Port NuGet package for full implementation.
    /// This is a placeholder that uses zlib as fallback.
    /// </summary>
    private byte[] CompressZstd(ReadOnlySpan<byte> data)
    {
        // Note: For full zstd support, install ZstdSharp.Port package
        // and use: using var compressor = new ZstdSharp.Compressor(_options.CompressionLevel);
        //          return compressor.Wrap(data).ToArray();
        
        // Fallback to zlib for now
        _logger.Trace("Zstd not available, falling back to zlib");
        return CompressZlib(data);
    }

    /// <summary>
    /// Decompresses data using zstd.
    /// Note: Requires ZstdSharp.Port NuGet package for full implementation.
    /// This is a placeholder that uses zlib as fallback.
    /// </summary>
    private byte[] DecompressZstd(ReadOnlySpan<byte> compressedData, int uncompressedLength)
    {
        // Note: For full zstd support, install ZstdSharp.Port package
        // and use: using var decompressor = new ZstdSharp.Decompressor();
        //          return decompressor.Unwrap(compressedData).ToArray();
        
        // Fallback to zlib for now
        return DecompressZlib(compressedData, uncompressedLength);
    }

    /// <summary>
    /// Gets the zlib compression level.
    /// </summary>
    private CompressionLevel GetZlibCompressionLevel()
    {
        return _options.CompressionLevel switch
        {
            <= 3 => CompressionLevel.Fastest,
            >= 7 => CompressionLevel.SmallestSize,
            _ => CompressionLevel.Optimal
        };
    }

    /// <summary>
    /// Gets compression statistics.
    /// </summary>
    public CompressionStats GetStats()
    {
        var ratio = _uncompressedBytes > 0
            ? 1.0 - ((double)_compressedBytes / _uncompressedBytes)
            : 0;

        return new CompressionStats
        {
            Enabled = _options.Enabled,
            Algorithm = Algorithm,
            UncompressedBytes = _uncompressedBytes,
            CompressedBytes = _compressedBytes,
            CompressionRatio = ratio,
            CompressionCount = _compressionCount,
            DecompressionCount = _decompressionCount
        };
    }
}

/// <summary>
/// Compression statistics.
/// </summary>
public sealed class CompressionStats
{
    public bool Enabled { get; init; }
    public CompressionAlgorithm Algorithm { get; init; }
    public long UncompressedBytes { get; init; }
    public long CompressedBytes { get; init; }
    public double CompressionRatio { get; init; }
    public long CompressionCount { get; init; }
    public long DecompressionCount { get; init; }

    public override string ToString()
    {
        return $"Compression: {Algorithm}, ratio={CompressionRatio:P2}, compressed={CompressionCount}";
    }
}
