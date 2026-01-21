using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace CysRedis.Core.Unsafe.Common;

/// <summary>
/// SIMD acceleration helpers for high-performance operations.
/// Provides vectorized implementations for common operations.
/// </summary>
public static unsafe class SimdHelpers
{
    /// <summary>
    /// Cache line size for memory alignment.
    /// </summary>
    public const int CacheLineSize = 64;

    /// <summary>
    /// Checks if SIMD hardware acceleration is available.
    /// </summary>
    public static bool IsHardwareAccelerated => Vector.IsHardwareAccelerated;

    /// <summary>
    /// Checks if AVX2 is available.
    /// </summary>
    public static bool IsAvx2Available => Avx2.IsSupported;

    /// <summary>
    /// Checks if SSE2 is available.
    /// </summary>
    public static bool IsSse2Available => Sse2.IsSupported;

    /// <summary>
    /// Fast memory copy using SIMD when available.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CopyMemory(byte* src, byte* dst, int length)
    {
        if (length == 0) return;

        if (IsAvx2Available && length >= 256)
        {
            CopyMemoryAvx2(src, dst, length);
        }
        else if (IsSse2Available && length >= 128)
        {
            CopyMemorySse2(src, dst, length);
        }
        else if (Vector.IsHardwareAccelerated && length >= 32)
        {
            CopyMemoryVector(src, dst, length);
        }
        else
        {
            CopyMemoryStandard(src, dst, length);
        }
    }

    /// <summary>
    /// AVX2 optimized memory copy.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CopyMemoryAvx2(byte* src, byte* dst, int length)
    {
        int i = 0;
        int end = length - 255;

        // Copy 256 bytes at a time
        for (; i < end; i += 256)
        {
            var v1 = Avx2.LoadVector256(src + i);
            var v2 = Avx2.LoadVector256(src + i + 32);
            var v3 = Avx2.LoadVector256(src + i + 64);
            var v4 = Avx2.LoadVector256(src + i + 96);
            var v5 = Avx2.LoadVector256(src + i + 128);
            var v6 = Avx2.LoadVector256(src + i + 160);
            var v7 = Avx2.LoadVector256(src + i + 192);
            var v8 = Avx2.LoadVector256(src + i + 224);

            Avx2.Store(dst + i, v1);
            Avx2.Store(dst + i + 32, v2);
            Avx2.Store(dst + i + 64, v3);
            Avx2.Store(dst + i + 96, v4);
            Avx2.Store(dst + i + 128, v5);
            Avx2.Store(dst + i + 160, v6);
            Avx2.Store(dst + i + 192, v7);
            Avx2.Store(dst + i + 224, v8);
        }

        // Copy remaining bytes
        if (i < length)
        {
            CopyMemoryStandard(src + i, dst + i, length - i);
        }
    }

    /// <summary>
    /// SSE2 optimized memory copy.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CopyMemorySse2(byte* src, byte* dst, int length)
    {
        int i = 0;
        int end = length - 127;

        // Copy 128 bytes at a time
        for (; i < end; i += 128)
        {
            var v1 = Sse2.LoadVector128(src + i);
            var v2 = Sse2.LoadVector128(src + i + 16);
            var v3 = Sse2.LoadVector128(src + i + 32);
            var v4 = Sse2.LoadVector128(src + i + 48);
            var v5 = Sse2.LoadVector128(src + i + 64);
            var v6 = Sse2.LoadVector128(src + i + 80);
            var v7 = Sse2.LoadVector128(src + i + 96);
            var v8 = Sse2.LoadVector128(src + i + 112);

            Sse2.Store(dst + i, v1);
            Sse2.Store(dst + i + 16, v2);
            Sse2.Store(dst + i + 32, v3);
            Sse2.Store(dst + i + 48, v4);
            Sse2.Store(dst + i + 64, v5);
            Sse2.Store(dst + i + 80, v6);
            Sse2.Store(dst + i + 96, v7);
            Sse2.Store(dst + i + 112, v8);
        }

        // Copy remaining bytes
        if (i < length)
        {
            CopyMemoryStandard(src + i, dst + i, length - i);
        }
    }

    /// <summary>
    /// Vector<T> optimized memory copy.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CopyMemoryVector(byte* src, byte* dst, int length)
    {
        int vectorSize = Vector<byte>.Count;
        int i = 0;
        int end = length - vectorSize + 1;

        for (; i < end; i += vectorSize)
        {
            var v = new Vector<byte>(new ReadOnlySpan<byte>(src + i, vectorSize));
            v.CopyTo(new Span<byte>(dst + i, vectorSize));
        }

        // Copy remaining bytes
        if (i < length)
        {
            CopyMemoryStandard(src + i, dst + i, length - i);
        }
    }

    /// <summary>
    /// Standard memory copy fallback.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CopyMemoryStandard(byte* src, byte* dst, int length)
    {
        Buffer.MemoryCopy(src, dst, length, length);
    }

    /// <summary>
    /// Fast memory comparison using SIMD.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CompareMemory(byte* ptr1, byte* ptr2, int length)
    {
        if (length == 0) return true;

        if (IsAvx2Available && length >= 256)
        {
            return CompareMemoryAvx2(ptr1, ptr2, length);
        }
        else if (IsSse2Available && length >= 128)
        {
            return CompareMemorySse2(ptr1, ptr2, length);
        }
        else if (Vector.IsHardwareAccelerated && length >= 32)
        {
            return CompareMemoryVector(ptr1, ptr2, length);
        }
        else
        {
            return CompareMemoryStandard(ptr1, ptr2, length);
        }
    }

    /// <summary>
    /// AVX2 optimized memory comparison.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool CompareMemoryAvx2(byte* ptr1, byte* ptr2, int length)
    {
        int i = 0;
        int end = length - 255;

        for (; i < end; i += 256)
        {
            var v1 = Avx2.LoadVector256(ptr1 + i);
            var v2 = Avx2.LoadVector256(ptr2 + i);
            var cmp1 = Avx2.CompareEqual(v1, v2);

            var v3 = Avx2.LoadVector256(ptr1 + i + 32);
            var v4 = Avx2.LoadVector256(ptr2 + i + 32);
            var cmp2 = Avx2.CompareEqual(v3, v4);

            var v5 = Avx2.LoadVector256(ptr1 + i + 64);
            var v6 = Avx2.LoadVector256(ptr2 + i + 64);
            var cmp3 = Avx2.CompareEqual(v5, v6);

            var v7 = Avx2.LoadVector256(ptr1 + i + 96);
            var v8 = Avx2.LoadVector256(ptr2 + i + 96);
            var cmp4 = Avx2.CompareEqual(v7, v8);

            var v9 = Avx2.LoadVector256(ptr1 + i + 128);
            var v10 = Avx2.LoadVector256(ptr2 + i + 128);
            var cmp5 = Avx2.CompareEqual(v9, v10);

            var v11 = Avx2.LoadVector256(ptr1 + i + 160);
            var v12 = Avx2.LoadVector256(ptr2 + i + 160);
            var cmp6 = Avx2.CompareEqual(v11, v12);

            var v13 = Avx2.LoadVector256(ptr1 + i + 192);
            var v14 = Avx2.LoadVector256(ptr2 + i + 192);
            var cmp7 = Avx2.CompareEqual(v13, v14);

            var v15 = Avx2.LoadVector256(ptr1 + i + 224);
            var v16 = Avx2.LoadVector256(ptr2 + i + 224);
            var cmp8 = Avx2.CompareEqual(v15, v16);

            var combined = Avx2.And(Avx2.And(Avx2.And(cmp1, cmp2), Avx2.And(cmp3, cmp4)),
                                    Avx2.And(Avx2.And(cmp5, cmp6), Avx2.And(cmp7, cmp8)));

            if (Avx2.MoveMask(combined) != unchecked((int)0xFFFFFFFF))
            {
                return false;
            }
        }

        // Compare remaining bytes
        if (i < length)
        {
            return CompareMemoryStandard(ptr1 + i, ptr2 + i, length - i);
        }

        return true;
    }

    /// <summary>
    /// SSE2 optimized memory comparison.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool CompareMemorySse2(byte* ptr1, byte* ptr2, int length)
    {
        int i = 0;
        int end = length - 127;

        for (; i < end; i += 128)
        {
            var v1 = Sse2.LoadVector128(ptr1 + i);
            var v2 = Sse2.LoadVector128(ptr2 + i);
            var cmp1 = Sse2.CompareEqual(v1, v2);

            var v3 = Sse2.LoadVector128(ptr1 + i + 16);
            var v4 = Sse2.LoadVector128(ptr2 + i + 16);
            var cmp2 = Sse2.CompareEqual(v3, v4);

            var v5 = Sse2.LoadVector128(ptr1 + i + 32);
            var v6 = Sse2.LoadVector128(ptr2 + i + 32);
            var cmp3 = Sse2.CompareEqual(v5, v6);

            var v7 = Sse2.LoadVector128(ptr1 + i + 48);
            var v8 = Sse2.LoadVector128(ptr2 + i + 48);
            var cmp4 = Sse2.CompareEqual(v7, v8);

            var v9 = Sse2.LoadVector128(ptr1 + i + 64);
            var v10 = Sse2.LoadVector128(ptr2 + i + 64);
            var cmp5 = Sse2.CompareEqual(v9, v10);

            var v11 = Sse2.LoadVector128(ptr1 + i + 80);
            var v12 = Sse2.LoadVector128(ptr2 + i + 80);
            var cmp6 = Sse2.CompareEqual(v11, v12);

            var v13 = Sse2.LoadVector128(ptr1 + i + 96);
            var v14 = Sse2.LoadVector128(ptr2 + i + 96);
            var cmp7 = Sse2.CompareEqual(v13, v14);

            var v15 = Sse2.LoadVector128(ptr1 + i + 112);
            var v16 = Sse2.LoadVector128(ptr2 + i + 112);
            var cmp8 = Sse2.CompareEqual(v15, v16);

            var combined = Sse2.And(Sse2.And(Sse2.And(cmp1, cmp2), Sse2.And(cmp3, cmp4)),
                                   Sse2.And(Sse2.And(cmp5, cmp6), Sse2.And(cmp7, cmp8)));

            if (Sse2.MoveMask(combined) != 0xFFFF)
            {
                return false;
            }
        }

        // Compare remaining bytes
        if (i < length)
        {
            return CompareMemoryStandard(ptr1 + i, ptr2 + i, length - i);
        }

        return true;
    }

    /// <summary>
    /// Vector<T> optimized memory comparison.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool CompareMemoryVector(byte* ptr1, byte* ptr2, int length)
    {
        int vectorSize = Vector<byte>.Count;
        int i = 0;
        int end = length - vectorSize + 1;

        for (; i < end; i += vectorSize)
        {
            var v1 = new Vector<byte>(new ReadOnlySpan<byte>(ptr1 + i, vectorSize));
            var v2 = new Vector<byte>(new ReadOnlySpan<byte>(ptr2 + i, vectorSize));
            if (v1 != v2)
            {
                return false;
            }
        }

        // Compare remaining bytes
        if (i < length)
        {
            return CompareMemoryStandard(ptr1 + i, ptr2 + i, length - i);
        }

        return true;
    }

    /// <summary>
    /// Standard memory comparison fallback.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool CompareMemoryStandard(byte* ptr1, byte* ptr2, int length)
    {
        for (int i = 0; i < length; i++)
        {
            if (ptr1[i] != ptr2[i])
                return false;
        }
        return true;
    }

    /// <summary>
    /// Fast hash computation using SIMD for large buffers.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint ComputeHashFast(byte* data, int length, uint seed = 0)
    {
        // Simple hash for now, can be enhanced with SIMD
        uint hash = seed;
        int i = 0;

        // Process 4 bytes at a time
        int end = length - 3;
        for (; i < end; i += 4)
        {
            hash ^= *(uint*)(data + i);
            hash = (hash << 13) | (hash >> 19);
            hash = hash * 0x5bd1e995 + 0x5bd1e995;
        }

        // Process remaining bytes
        for (; i < length; i++)
        {
            hash ^= data[i];
            hash = (hash << 13) | (hash >> 19);
            hash = hash * 0x5bd1e995 + 0x5bd1e995;
        }

        return hash;
    }

    /// <summary>
    /// Zero memory using SIMD.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ZeroMemory(byte* ptr, int length)
    {
        if (length == 0) return;

        if (IsAvx2Available && length >= 256)
        {
            ZeroMemoryAvx2(ptr, length);
        }
        else if (IsSse2Available && length >= 128)
        {
            ZeroMemorySse2(ptr, length);
        }
        else if (Vector.IsHardwareAccelerated && length >= 32)
        {
            ZeroMemoryVector(ptr, length);
        }
        else
        {
            ZeroMemoryStandard(ptr, length);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ZeroMemoryAvx2(byte* ptr, int length)
    {
        var zero = Vector256<byte>.Zero;
        int i = 0;
        int end = length - 255;

        for (; i < end; i += 256)
        {
            Avx2.Store(ptr + i, zero);
            Avx2.Store(ptr + i + 32, zero);
            Avx2.Store(ptr + i + 64, zero);
            Avx2.Store(ptr + i + 96, zero);
            Avx2.Store(ptr + i + 128, zero);
            Avx2.Store(ptr + i + 160, zero);
            Avx2.Store(ptr + i + 192, zero);
            Avx2.Store(ptr + i + 224, zero);
        }

        if (i < length)
        {
            ZeroMemoryStandard(ptr + i, length - i);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ZeroMemorySse2(byte* ptr, int length)
    {
        var zero = Vector128<byte>.Zero;
        int i = 0;
        int end = length - 127;

        for (; i < end; i += 128)
        {
            Sse2.Store(ptr + i, zero);
            Sse2.Store(ptr + i + 16, zero);
            Sse2.Store(ptr + i + 32, zero);
            Sse2.Store(ptr + i + 48, zero);
            Sse2.Store(ptr + i + 64, zero);
            Sse2.Store(ptr + i + 80, zero);
            Sse2.Store(ptr + i + 96, zero);
            Sse2.Store(ptr + i + 112, zero);
        }

        if (i < length)
        {
            ZeroMemoryStandard(ptr + i, length - i);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ZeroMemoryVector(byte* ptr, int length)
    {
        var zero = Vector<byte>.Zero;
        int vectorSize = Vector<byte>.Count;
        int i = 0;
        int end = length - vectorSize + 1;

        for (; i < end; i += vectorSize)
        {
            zero.CopyTo(new Span<byte>(ptr + i, vectorSize));
        }

        if (i < length)
        {
            ZeroMemoryStandard(ptr + i, length - i);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ZeroMemoryStandard(byte* ptr, int length)
    {
        for (int i = 0; i < length; i++)
        {
            ptr[i] = 0;
        }
    }
}
