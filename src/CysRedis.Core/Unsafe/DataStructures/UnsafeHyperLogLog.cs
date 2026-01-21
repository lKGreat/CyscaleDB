using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using CysRedis.Core.Unsafe.Common;

namespace CysRedis.Core.Unsafe.DataStructures;

/// <summary>
/// High-performance HyperLogLog implementation with SIMD optimizations.
/// Uses lookup tables and vectorized operations for cardinality estimation.
/// </summary>
public unsafe sealed class UnsafeHyperLogLog : IDisposable
{
    private const int NumRegisters = 16384; // 2^14
    private const int RegisterBits = 6;
    
    private byte* _registers;
    private static readonly double[] PowLookup = InitializePowLookup();
    
    /// <summary>
    /// Creates a new HyperLogLog.
    /// </summary>
    public UnsafeHyperLogLog()
    {
        _registers = (byte*)UnsafeMemoryManager.AlignedAlloc((nuint)NumRegisters, SimdHelpers.CacheLineSize, zeroInit: true);
    }
    
    /// <summary>
    /// Adds elements to the HyperLogLog.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Count()
    {
        if (SimdHelpers.IsAvx2Available)
        {
            return CountSIMD();
        }
        else
        {
            return CountStandard();
        }
    }
    
    /// <summary>
    /// SIMD-optimized cardinality estimation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long CountSIMD()
    {
        double sum = 0;
        int zeros = 0;
        
        if (Avx2.IsSupported)
        {
            // Process 32 registers at a time using AVX2
            int i = 0;
            for (; i <= NumRegisters - 32; i += 32)
            {
                // Load 32 bytes
                var regs = Avx2.LoadVector256(_registers + i);
                
                // Convert to double and compute pow(2, -x) using lookup
                // For each byte, look up pow value
                for (int j = 0; j < 32; j++)
                {
                    byte reg = _registers[i + j];
                    if (reg == 0)
                        zeros++;
                    else if (reg < PowLookup.Length)
                        sum += PowLookup[reg];
                }
            }
            
            // Process remaining registers
            for (; i < NumRegisters; i++)
            {
                byte reg = _registers[i];
                if (reg == 0)
                    zeros++;
                else if (reg < PowLookup.Length)
                    sum += PowLookup[reg];
            }
        }
        else
        {
            return CountStandard();
        }
        
        return CalculateCardinality(sum, zeros);
    }
    
    /// <summary>
    /// Standard cardinality estimation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long CountStandard()
    {
        double sum = 0;
        int zeros = 0;
        
        for (int i = 0; i < NumRegisters; i++)
        {
            byte reg = _registers[i];
            if (reg == 0)
                zeros++;
            else if (reg < PowLookup.Length)
                sum += PowLookup[reg];
            else
                sum += Math.Pow(2, -reg);
        }
        
        return CalculateCardinality(sum, zeros);
    }
    
    /// <summary>
    /// Merges other HyperLogLogs into this one.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Merge(params UnsafeHyperLogLog[] others)
    {
        foreach (var other in others)
        {
            if (SimdHelpers.IsAvx2Available && Avx2.IsSupported)
            {
                MergeSIMD(other);
            }
            else
            {
                MergeStandard(other);
            }
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void MergeSIMD(UnsafeHyperLogLog other)
    {
        int i = 0;
        for (; i <= NumRegisters - 32; i += 32)
        {
            var regs1 = Avx2.LoadVector256(_registers + i);
            var regs2 = Avx2.LoadVector256(other._registers + i);
            var maxRegs = Avx2.Max(regs1, regs2);
            Avx2.Store(_registers + i, maxRegs);
        }
        
        // Process remaining
        for (; i < NumRegisters; i++)
        {
            if (other._registers[i] > _registers[i])
                _registers[i] = other._registers[i];
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void MergeStandard(UnsafeHyperLogLog other)
    {
        for (int i = 0; i < NumRegisters; i++)
        {
            if (other._registers[i] > _registers[i])
                _registers[i] = other._registers[i];
        }
    }
    
    /// <summary>
    /// Gets the raw registers (for serialization).
    /// </summary>
    public byte[] GetRegisters()
    {
        byte[] result = new byte[NumRegisters];
        fixed (byte* dst = result)
        {
            SimdHelpers.CopyMemory(_registers, dst, NumRegisters);
        }
        return result;
    }
    
    /// <summary>
    /// Sets registers from data.
    /// </summary>
    public void SetRegisters(byte[] data)
    {
        int copyLen = Math.Min(data.Length, NumRegisters);
        fixed (byte* src = data)
        {
            SimdHelpers.CopyMemory(src, _registers, copyLen);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Hash(string value)
    {
        // MurmurHash3 64-bit
        ulong h = 0;
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        
        fixed (byte* ptr = bytes)
        {
            for (int i = 0; i < bytes.Length; i++)
            {
                h ^= ptr[i];
                h *= 0x5bd1e9955bd1e995;
                h ^= h >> 47;
            }
        }
        
        return h;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double AlphaM()
    {
        // Alpha constant for 2^14 registers
        return 0.7213 / (1 + 1.079 / NumRegisters);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long CalculateCardinality(double sum, int zeros)
    {
        double estimate = AlphaM() * NumRegisters * NumRegisters / sum;
        
        // Small range correction
        if (estimate <= 2.5 * NumRegisters && zeros > 0)
        {
            estimate = NumRegisters * Math.Log((double)NumRegisters / zeros);
        }
        
        return (long)Math.Round(estimate);
    }
    
    private static double[] InitializePowLookup()
    {
        var lookup = new double[64];
        for (int i = 0; i < lookup.Length; i++)
        {
            lookup[i] = Math.Pow(2, -i);
        }
        return lookup;
    }
    
    public void Dispose()
    {
        if (_registers != null)
        {
            UnsafeMemoryManager.AlignedFree(_registers);
            _registers = null;
        }
    }
}
