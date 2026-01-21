namespace CysRedis.Core.Storage;

/// <summary>
/// LZF压缩算法实现 (用于RDB)
/// 基于LibLZF的简化版本
/// </summary>
public static class LzfCompression
{
    private const int HLOG = 14;
    private const int HSIZE = 1 << HLOG;
    private const int MAX_LIT = 1 << 5;
    private const int MAX_OFF = 1 << 13;
    private const int MAX_REF = (1 << 8) + (1 << 3);

    /// <summary>
    /// 压缩数据
    /// </summary>
    public static byte[]? Compress(byte[] input)
    {
        if (input.Length == 0)
            return Array.Empty<byte>();

        var output = new byte[input.Length]; // 最坏情况
        var htab = new long[HSIZE];
        
        int iidx = 0, oidx = 0;
        var hval = (uint)((input[iidx] << 8) | input[iidx + 1]);
        
        int lit = 0;

        while (iidx < input.Length - 2)
        {
            hval = (hval << 8) | input[iidx + 2];
            var hslot = ((hval ^ (hval << 5)) >> (int)(24 - HLOG - hval * 5) & (HSIZE - 1));
            var reference = htab[hslot];
            htab[hslot] = iidx;

            var off = iidx - (int)reference - 1;

            if (off < MAX_OFF && reference > 0 && input[reference] == input[iidx]
                && input[reference + 1] == input[iidx + 1]
                && input[reference + 2] == input[iidx + 2])
            {
                // 找到匹配
                var maxlen = input.Length - iidx - 2;
                maxlen = maxlen > MAX_REF ? MAX_REF : maxlen;

                if (oidx + lit + 1 + 3 >= output.Length)
                    return null;

                int len = 2;
                while (len < maxlen && input[reference + len] == input[iidx + len])
                    len++;

                if (lit != 0)
                {
                    output[oidx++] = (byte)(lit - 1);
                    lit = -lit;
                    do
                        output[oidx++] = input[iidx + lit++];
                    while (lit != 0);
                }

                len -= 2;
                iidx++;

                if (len < 7)
                {
                    output[oidx++] = (byte)((off >> 8) + (len << 5));
                }
                else
                {
                    output[oidx++] = (byte)((off >> 8) + (7 << 5));
                    output[oidx++] = (byte)(len - 7);
                }

                output[oidx++] = (byte)off;
                iidx += len;
                hval = (uint)((input[iidx] << 8) | input[iidx + 1]);
                continue;
            }

            // 字面量
            lit++;
            iidx++;

            if (lit == MAX_LIT)
            {
                if (oidx + 1 + MAX_LIT >= output.Length)
                    return null;

                output[oidx++] = (byte)(MAX_LIT - 1);
                lit = -lit;
                do
                    output[oidx++] = input[iidx + lit++];
                while (lit != 0);
            }
        }

        // 剩余的字面量
        if (lit != 0)
        {
            if (oidx + lit + 1 >= output.Length)
                return null;

            output[oidx++] = (byte)(lit - 1);
            lit = -lit;
            do
                output[oidx++] = input[iidx + lit++];
            while (lit != 0);
        }

        // 复制剩余字节
        while (iidx < input.Length)
        {
            if (oidx >= output.Length)
                return null;
            
            lit++;
            if (lit == MAX_LIT || iidx == input.Length - 1)
            {
                output[oidx++] = (byte)(lit - 1);
                for (int i = 0; i < lit; i++)
                    output[oidx++] = input[iidx - lit + 1 + i];
                lit = 0;
            }
            iidx++;
        }

        // 返回压缩后的数据
        var result = new byte[oidx];
        Array.Copy(output, result, oidx);
        return result;
    }

    /// <summary>
    /// 解压数据
    /// </summary>
    public static byte[] Decompress(byte[] input, int outputLength)
    {
        var output = new byte[outputLength];
        int iidx = 0, oidx = 0;

        while (iidx < input.Length)
        {
            var ctrl = input[iidx++];

            if (ctrl < 32)
            {
                // 字面量
                for (int i = 0; i <= ctrl; i++)
                    output[oidx++] = input[iidx++];
            }
            else
            {
                // 引用
                var len = ctrl >> 5;
                var reference = oidx - ((ctrl & 0x1f) << 8) - 1;

                if (len == 7)
                    len += input[iidx++];

                reference -= input[iidx++];
                len += 2;

                for (int i = 0; i < len; i++)
                    output[oidx++] = output[reference++];
            }
        }

        return output;
    }
}
