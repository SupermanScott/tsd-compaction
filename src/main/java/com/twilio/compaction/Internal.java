package com.twilio.compaction;
import java.util.ArrayList;
import java.util.Arrays;

class Internal {
    private Internal() {
    }

    /**
     * When this bit is set, the value is a floating point value.
     * Otherwise it's an integer value.
     */
    public static final short FLAG_FLOAT = 0x8;

    /** Mask to select the size of a value from the qualifier.  */
    public static final short LENGTH_MASK = 0x7;

    /** Mask for the millisecond qualifier flag */
    private static final byte MS_BYTE_FLAG = (byte)0xF0;

    /** Number of LSBs in time_deltas reserved for flags.  */
    private static final short MS_FLAG_BITS = 6;

    /** Number of LSBs in time_deltas reserved for flags.  */
    private static final short FLAG_BITS = 4;

    /** Mask to select all the FLAG_BITS.  */
    private static final short FLAGS_MASK = FLAG_FLOAT | LENGTH_MASK;

    /**
     * Returns whether or not this is a floating value that needs to be fixed.
     * <p>
     * OpenTSDB used to encode all floating point values as `float' (4 bytes)
     * but actually store them on 8 bytes, with 4 leading 0 bytes, and flags
     * correctly stating the value was on 4 bytes.
     * (from CompactionQueue)
     * @param flags The least significant byte of a qualifier.
     * @param value The value that may need to be corrected.
     */
    public static boolean floatingPointValueToFix(final byte flags,
                                                  final byte[] value) {
        return (flags & FLAG_FLOAT) != 0   // We need a floating point value.
            && (flags & LENGTH_MASK) == 0x3  // That pretends to be on 4 bytes.
            && value.length == 8;                  // But is actually using 8 bytes.
    }

    /**
     * Fix the flags inside the last byte of a qualifier.
     * <p>
     * OpenTSDB used to not rely on the size recorded in the flags being
     * correct, and so for a long time it was setting the wrong size for
     * floating point values (pretending they were encoded on 8 bytes when
     * in fact they were on 4).  So overwrite these bits here to make sure
     * they're correct now, because once they're compacted it's going to
     * be quite hard to tell if the flags are right or wrong, and we need
     * them to be correct to easily decode the values.
     * @param flags The least significant byte of a qualifier.
     * @param val_len The number of bytes in the value of this qualifier.
     * @return The least significant byte of the qualifier with correct flags.
     */
    public static byte fixQualifierFlags(byte flags, final int val_len) {
        // Explanation:
        //   (1) Take the last byte of the qualifier.
        //   (2) Zero out all the flag bits but one.
        //       The one we keep is the type (floating point vs integer value).
        //   (3) Set the length properly based on the value we have.
        return (byte) ((flags & ~(FLAGS_MASK >>> 1)) | (val_len - 1));
        //              ^^^^^   ^^^^^^^^^^^^^^^^^^^^^^^^^    ^^^^^^^^^^^^^
        //               (1)               (2)                    (3)
    }

    /**
     * Returns a corrected value if this is a floating point value to fix.
     * <p>
     * OpenTSDB used to encode all floating point values as `float' (4 bytes)
     * but actually store them on 8 bytes, with 4 leading 0 bytes, and flags
     * correctly stating the value was on 4 bytes.
     * <p>
     * This function detects such values and returns a corrected value, without
     * the 4 leading zeros.  Otherwise it returns the value unchanged.
     * (from CompactionQueue)
     * @param flags The least significant byte of a qualifier.
     * @param value The value that may need to be corrected.
     * @throws IllegalStateException if the value is malformed.
     */
    public static byte[] fixFloatingPointValue(final byte flags,
                                               final byte[] value) {
        if (floatingPointValueToFix(flags, value)) {
            // The first 4 bytes should really be zeros.
            if (value[0] == 0 && value[1] == 0 && value[2] == 0 && value[3] == 0) {
                // Just keep the last 4 bytes.
                return new byte[] { value[4], value[5], value[6], value[7] };
            } else {  // Very unlikely.
                throw new IllegalStateException("Corrupted floating point value: "
                                               + Arrays.toString(value) + " flags=0x" + Integer.toHexString(flags)
                                               + " -- first 4 bytes are expected to be zeros.");
            }
        }
        return value;
    }

    /**
     * Reads a big-endian 2-byte short from an offset in the given array.
     * @param b The array to read from.
     * @param offset The offset in the array to start reading from.
     * @return A short integer.
     * @throws IndexOutOfBoundsException if the byte array is too small.
     */
    public static short getShort(final byte[] b, final int offset) {
        return (short) (b[offset] << 8 | b[offset + 1] & 0xFF);
    }


    /**
     * Reads a big-endian 2-byte unsigned short from an offset in the
     * given array.
     * @param b The array to read from.
     * @param offset The offset in the array to start reading from.
     * @return A positive short integer.
     * @throws IndexOutOfBoundsException if the byte array is too small.
     */
    public static int getUnsignedShort(final byte[] b, final int offset) {
        return getShort(b, offset) & 0x0000FFFF;
    }

    /**
     * Reads a big-endian 4-byte integer from an offset in the given array.
     * @param b The array to read from.
     * @param offset The offset in the array to start reading from.
     * @return An integer.
     * @throws IndexOutOfBoundsException if the byte array is too small.
     */
    public static int getInt(final byte[] b, final int offset) {
        return (b[offset + 0] & 0xFF) << 24
            | (b[offset + 1] & 0xFF) << 16
            | (b[offset + 2] & 0xFF) << 8
            | (b[offset + 3] & 0xFF) << 0;
    }

    /**
     * Reads a big-endian 4-byte unsigned integer from an offset in the
     * given array.
     * @param b The array to read from.
     * @param offset The offset in the array to start reading from.
     * @return A positive integer.
     * @throws IndexOutOfBoundsException if the byte array is too small.
     */
    public static long getUnsignedInt(final byte[] b, final int offset) {
        return getInt(b, offset) & 0x00000000FFFFFFFFL;
    }

    public static int getOffsetFromQualifier(final byte[] qualifier, final int offset) {
        if ((qualifier[offset] & MS_BYTE_FLAG) == MS_BYTE_FLAG) {
            return (int)(getUnsignedInt(qualifier, offset) & 0x0FFFFC0) >>> MS_FLAG_BITS;
        }
        else {
            final int seconds = (getUnsignedShort(qualifier, offset) & 0xFFFF) >>> FLAG_BITS;
            return seconds * 1000;
        }
    }

    /**
     * Returns the length of the value, in bytes, parsed from the qualifier
     * @param qualifier The qualifier to parse
     * @param offset An offset within the byte array
     * @return The length of the value in bytes, from 1 to 8.
     * @throws IllegalArgumentException if the qualifier is null or the offset falls
     * outside of the qualifier array
     * @since 2.0
     */
    public static byte getValueLengthFromQualifier(final byte[] qualifier,
                                                   final int offset) {
        //validateQualifier(qualifier, offset);
        short length;
        if ((qualifier[offset] & MS_BYTE_FLAG) == MS_BYTE_FLAG) {
            length = (short) (qualifier[offset + 3] & LENGTH_MASK);
        } else {
            length = (short) (qualifier[offset + 1] & LENGTH_MASK);
        }
        return (byte) (length + 1);
    }

    /**
     * Determines if the qualifier is in milliseconds or not
     * @param qualifier The first byte of a qualifier
     * @return True if the qualifier is in milliseconds, false if not
     * @since 2.0
     */
    public static boolean inMilliseconds(final byte qualifier) {
        return (qualifier & MS_BYTE_FLAG) == MS_BYTE_FLAG;
    }

}
