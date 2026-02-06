using System.Collections;

namespace CyscaleDB.Core.Execution.Optimizer;

/// <summary>
/// Bloom Filter implementation for runtime join optimization.
/// 
/// During a Hash Join, after the build phase completes, a Bloom Filter is constructed
/// from the build-side join keys. This filter is then pushed down to the probe-side
/// TableScan operator to pre-filter rows before they reach the join.
///
/// For a probe table with billions of rows where only a small fraction matches,
/// the Bloom Filter can eliminate 90%+ of rows at the scan level, dramatically
/// reducing I/O and CPU costs.
///
/// Properties:
///   - No false negatives: if a key is in the set, the filter always returns true
///   - Possible false positives: controlled by the number of bits and hash functions
///   - Space efficient: typically 10 bits per element for ~1% false positive rate
/// </summary>
public sealed class BloomFilter
{
    private readonly BitArray _bits;
    private readonly int _numHashFunctions;
    private readonly int _numBits;

    /// <summary>
    /// Gets the number of bits in the filter.
    /// </summary>
    public int NumBits => _numBits;

    /// <summary>
    /// Gets the number of hash functions.
    /// </summary>
    public int NumHashFunctions => _numHashFunctions;

    /// <summary>
    /// Creates a new Bloom Filter.
    /// </summary>
    /// <param name="expectedElements">Expected number of elements to add.</param>
    /// <param name="falsePositiveRate">Desired false positive rate (default 1%).</param>
    public BloomFilter(long expectedElements, double falsePositiveRate = 0.01)
    {
        if (expectedElements <= 0) expectedElements = 1;
        if (falsePositiveRate <= 0 || falsePositiveRate >= 1) falsePositiveRate = 0.01;

        // Optimal number of bits: -n * ln(p) / (ln(2))^2
        _numBits = (int)Math.Min(
            int.MaxValue - 1,
            Math.Ceiling(-expectedElements * Math.Log(falsePositiveRate) / (Math.Log(2) * Math.Log(2))));
        _numBits = Math.Max(_numBits, 64);

        // Optimal number of hash functions: (m/n) * ln(2)
        _numHashFunctions = Math.Max(1,
            (int)Math.Round((double)_numBits / expectedElements * Math.Log(2)));
        _numHashFunctions = Math.Min(_numHashFunctions, 20);

        _bits = new BitArray(_numBits);
    }

    /// <summary>
    /// Creates a Bloom Filter with explicit bit count and hash count.
    /// </summary>
    public BloomFilter(int numBits, int numHashFunctions)
    {
        _numBits = Math.Max(64, numBits);
        _numHashFunctions = Math.Max(1, numHashFunctions);
        _bits = new BitArray(_numBits);
    }

    /// <summary>
    /// Adds an element to the Bloom Filter.
    /// </summary>
    public void Add(int hashCode)
    {
        for (int i = 0; i < _numHashFunctions; i++)
        {
            var idx = GetBitIndex(hashCode, i);
            _bits[idx] = true;
        }
    }

    /// <summary>
    /// Adds an element using its hash code from a DataValue.
    /// </summary>
    public void Add(object? value)
    {
        var hash = value?.GetHashCode() ?? 0;
        Add(hash);
    }

    /// <summary>
    /// Tests if an element MIGHT be in the set.
    /// Returns true if the element is probably in the set (with possible false positives).
    /// Returns false if the element is DEFINITELY NOT in the set.
    /// </summary>
    public bool MightContain(int hashCode)
    {
        for (int i = 0; i < _numHashFunctions; i++)
        {
            var idx = GetBitIndex(hashCode, i);
            if (!_bits[idx]) return false;
        }
        return true;
    }

    /// <summary>
    /// Tests using a DataValue.
    /// </summary>
    public bool MightContain(object? value)
    {
        var hash = value?.GetHashCode() ?? 0;
        return MightContain(hash);
    }

    /// <summary>
    /// Estimates the current false positive rate based on the number of set bits.
    /// </summary>
    public double EstimateFalsePositiveRate()
    {
        int setBits = 0;
        for (int i = 0; i < _numBits; i++)
        {
            if (_bits[i]) setBits++;
        }

        var fillRatio = (double)setBits / _numBits;
        return Math.Pow(fillRatio, _numHashFunctions);
    }

    /// <summary>
    /// Gets the memory usage in bytes.
    /// </summary>
    public long MemoryUsageBytes => (_numBits + 7) / 8 + 32; // BitArray + overhead

    private int GetBitIndex(int hashCode, int hashFunctionIndex)
    {
        // Double hashing: h(i) = h1 + i * h2
        // Use two independent hash functions derived from the input
        var h1 = hashCode;
        var h2 = hashCode * 0x5BD1E995 + hashFunctionIndex;

        var combined = h1 + hashFunctionIndex * h2;
        return Math.Abs(combined) % _numBits;
    }
}

/// <summary>
/// Manages Bloom Filter creation and pushdown during query execution.
/// </summary>
public static class BloomFilterPushdown
{
    /// <summary>
    /// Creates a Bloom Filter from a set of hash codes (e.g., from Hash Join build side).
    /// </summary>
    public static BloomFilter CreateFromHashCodes(IEnumerable<int> hashCodes, long estimatedCount)
    {
        var filter = new BloomFilter(estimatedCount, 0.01);
        foreach (var hash in hashCodes)
        {
            filter.Add(hash);
        }
        return filter;
    }

    /// <summary>
    /// Estimates the selectivity benefit of applying a Bloom Filter.
    /// Returns the estimated fraction of rows that will pass the filter.
    /// </summary>
    public static double EstimateSelectivity(long probeRows, long buildRows, double falsePositiveRate = 0.01)
    {
        if (probeRows == 0 || buildRows == 0) return 0;

        // Best case: buildRows / probeRows + falsePositiveRate * (1 - buildRows / probeRows)
        var trueMatchRate = Math.Min(1.0, (double)buildRows / probeRows);
        return trueMatchRate + falsePositiveRate * (1 - trueMatchRate);
    }
}
