package number;

public class Unsigned {
    public static int toUnsignedByte(byte data) {
        return data & 0xFF;
    }

    public static int toUnsignedShort(short data) {
        return data & 0xFFFF;
    }

    public static long toUnsignedLong(int data) {
        return data & 0xFFFFFFFF;
    }
}
