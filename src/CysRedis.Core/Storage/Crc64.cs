namespace CysRedis.Core.Storage;

/// <summary>
/// CRC64 校验和计算 (用于RDB文件完整性检查)
/// </summary>
public class Crc64
{
    private static readonly ulong[] Table;
    private const ulong Poly = 0xC96C5795D7870F42UL; // Redis使用的多项式

    static Crc64()
    {
        Table = new ulong[256];
        for (uint i = 0; i < 256; i++)
        {
            ulong crc = i;
            for (int j = 0; j < 8; j++)
            {
                if ((crc & 1) != 0)
                    crc = (crc >> 1) ^ Poly;
                else
                    crc >>= 1;
            }
            Table[i] = crc;
        }
    }

    /// <summary>
    /// 计算CRC64校验和
    /// </summary>
    public static ulong Compute(byte[] data)
    {
        return Compute(data, 0, data.Length);
    }

    /// <summary>
    /// 计算CRC64校验和（指定范围）
    /// </summary>
    public static ulong Compute(byte[] data, int offset, int length)
    {
        ulong crc = 0;
        for (int i = offset; i < offset + length; i++)
        {
            crc = Table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
        }
        return crc;
    }

    /// <summary>
    /// 更新CRC64校验和
    /// </summary>
    public static ulong Update(ulong crc, byte[] data, int offset, int length)
    {
        for (int i = offset; i < offset + length; i++)
        {
            crc = Table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
        }
        return crc;
    }
}
