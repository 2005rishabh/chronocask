package io.chronocask.index;

/**
 * 64-bit hash utilities for key fingerprinting and hash table indexing.
 * Implements 64-bit MurmurHash3 / SplitMix-inspired mixing.
 */
public final class HashUtil {

    private HashUtil() {
    }

    /**
     * Computes a 64-bit hash for a byte array.
     */
    public static long hash64(byte[] data) {
        if (data == null || data.length == 0) {
            return 0L;
        }
        return hash64(data, 0, data.length, 0x9747b28cL);
    }

    /**
     * MurmurHash3 64-bit variant for byte slices.
     */
    public static long hash64(byte[] data, int offset, int length, long seed) {
        long h = seed ^ (length * 0x517cc1b727220a95L);

        int i = offset;
        int end = offset + (length & ~7);

        // Process 8-byte blocks
        while (i < end) {
            long k = getLongLE(data, i);
            k *= 0xc6a4a7935bd1e995L;
            k ^= k >>> 47;
            k *= 0xc6a4a7935bd1e995L;

            h ^= k;
            h *= 0x517cc1b727220a95L;
            i += 8;
        }

        // Process remaining 1..7 bytes
        int remaining = (offset + length) - i;
        if (remaining > 0) {
            long k = 0;
            for (int b = 0; b < remaining; b++) {
                k |= ((long) (data[i + b] & 0xFF)) << (b * 8);
            }
            k *= 0xc6a4a7935bd1e995L;
            k ^= k >>> 47;
            k *= 0xc6a4a7935bd1e995L;
            h ^= k;
            h *= 0x517cc1b727220a95L;
        }

        // Final avalanche mix
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;

        return h == 0 ? 1 : h; // Reserve 0 if needed
    }

    private static long getLongLE(byte[] data, int index) {
        return ((long) (data[index] & 0xFF)) |
                (((long) (data[index + 1] & 0xFF)) << 8) |
                (((long) (data[index + 2] & 0xFF)) << 16) |
                (((long) (data[index + 3] & 0xFF)) << 24) |
                (((long) (data[index + 4] & 0xFF)) << 32) |
                (((long) (data[index + 5] & 0xFF)) << 40) |
                (((long) (data[index + 6] & 0xFF)) << 48) |
                (((long) (data[index + 7] & 0xFF)) << 56);
    }
}
