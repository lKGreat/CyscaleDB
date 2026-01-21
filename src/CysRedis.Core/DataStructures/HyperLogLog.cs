namespace CysRedis.Core.DataStructures;

/// <summary>
/// HyperLogLog probabilistic data structure for cardinality estimation.
/// Based on the HyperLogLog algorithm by Flajolet et al.
/// </summary>
public class RedisHyperLogLog : RedisObject
{
    public override string TypeName => "string"; // HLL is stored as string type in Redis

    private const int NumRegisters = 16384; // 2^14
    private const int RegisterBits = 6;
    private readonly byte[] _registers;

    public RedisHyperLogLog()
    {
        _registers = new byte[NumRegisters];
    }

    /// <summary>
    /// Adds elements to the HyperLogLog.
    /// </summary>
    public bool Add(params string[] elements)
    {
        bool modified = false;
        foreach (var element in elements)
        {
            if (AddElement(element))
                modified = true;
        }
        return modified;
    }

    /// <summary>
    /// Estimates the cardinality (number of unique elements).
    /// </summary>
    public long Count()
    {
        double sum = 0;
        int zeros = 0;

        for (int i = 0; i < NumRegisters; i++)
        {
            sum += Math.Pow(2, -_registers[i]);
            if (_registers[i] == 0)
                zeros++;
        }

        double estimate = AlphaM() * NumRegisters * NumRegisters / sum;

        // Small range correction
        if (estimate <= 2.5 * NumRegisters && zeros > 0)
        {
            estimate = NumRegisters * Math.Log((double)NumRegisters / zeros);
        }

        return (long)Math.Round(estimate);
    }

    /// <summary>
    /// Merges other HyperLogLogs into this one.
    /// </summary>
    public void Merge(params RedisHyperLogLog[] others)
    {
        foreach (var other in others)
        {
            for (int i = 0; i < NumRegisters; i++)
            {
                if (other._registers[i] > _registers[i])
                    _registers[i] = other._registers[i];
            }
        }
    }

    /// <summary>
    /// Gets the raw registers (for serialization).
    /// </summary>
    public byte[] GetRegisters() => (byte[])_registers.Clone();

    /// <summary>
    /// Sets registers from data.
    /// </summary>
    public void SetRegisters(byte[] data)
    {
        Buffer.BlockCopy(data, 0, _registers, 0, Math.Min(data.Length, _registers.Length));
    }

    private bool AddElement(string element)
    {
        ulong hash = Hash(element);
        int index = (int)(hash & (NumRegisters - 1));
        int rank = CountLeadingZeros(hash >> 14) + 1;

        if (rank > _registers[index])
        {
            _registers[index] = (byte)rank;
            return true;
        }
        return false;
    }

    private static ulong Hash(string value)
    {
        // MurmurHash3 64-bit
        ulong h = 0;
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        
        for (int i = 0; i < bytes.Length; i++)
        {
            h ^= bytes[i];
            h *= 0x5bd1e9955bd1e995;
            h ^= h >> 47;
        }
        
        return h;
    }

    private static int CountLeadingZeros(ulong value)
    {
        if (value == 0) return 64;
        int n = 0;
        if ((value & 0xFFFFFFFF00000000) == 0) { n += 32; value <<= 32; }
        if ((value & 0xFFFF000000000000) == 0) { n += 16; value <<= 16; }
        if ((value & 0xFF00000000000000) == 0) { n += 8; value <<= 8; }
        if ((value & 0xF000000000000000) == 0) { n += 4; value <<= 4; }
        if ((value & 0xC000000000000000) == 0) { n += 2; value <<= 2; }
        if ((value & 0x8000000000000000) == 0) { n += 1; }
        return n;
    }

    private static double AlphaM()
    {
        // Alpha constant for 2^14 registers
        return 0.7213 / (1 + 1.079 / NumRegisters);
    }
}
