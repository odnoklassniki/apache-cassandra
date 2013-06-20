package org.apache.cassandra.cache;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author roman.antipin
 *         Date: 14.06.13
 */
public class ByteArrayStreamOutput implements DataOutput {

    private static final int INIT_SIZE = 4096; // 4kb

    private byte[] buffer;
    private int count;

    public ByteArrayStreamOutput() {
        this(INIT_SIZE);
    }

    public ByteArrayStreamOutput(int size) {
        buffer = new byte[size];
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public int getCount() {
        return count;
    }

    @Override
    public void write(int b) throws IOException {
        checkAndExpand(1);
        buffer[count++] = (byte) b;
    }

    @Override
    public void write(byte[] b) throws IOException {
        checkAndExpand(b.length);
        System.arraycopy(b, 0, buffer, count, b.length);
        count += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkAndExpand(len);
        System.arraycopy(b, off, buffer, count, len);
        count += len;
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        checkAndExpand(1);
        buffer[count++] = (byte)(v ? 1 : 0);
    }

    @Override
    public void writeByte(int v) throws IOException {
        checkAndExpand(1);
        buffer[count++] = (byte)v;
    }

    @Override
    public void writeShort(int v) throws IOException {
        checkAndExpand(2);
        buffer[count++] = (byte) v;
        buffer[count++] = (byte) (v >>> 8);
    }

    @Override
    public void writeChar(int v) throws IOException {
        checkAndExpand(2);
        buffer[count++] = (byte) v;
        buffer[count++] = (byte) (v >>> 8);
    }

    @Override
    public void writeInt(int v) throws IOException {
        checkAndExpand(4);
        buffer[count++] = (byte) v;
        buffer[count++] = (byte) (v >>> 8);
        buffer[count++] = (byte) (v >>> 16);
        buffer[count++] = (byte) (v >>> 24);
    }

    @Override
    public void writeLong(long v) throws IOException {
        checkAndExpand(8);
        buffer[count++] = (byte) v;
        buffer[count++] = (byte) (v >>> 8);
        buffer[count++] = (byte) (v >>> 16);
        buffer[count++] = (byte) (v >>> 24);
        buffer[count++] = (byte) (v >>> 32);
        buffer[count++] = (byte) (v >>> 40);
        buffer[count++] = (byte) (v >>> 48);
        buffer[count++] = (byte) (v >>> 56);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToRawIntBits(v));
    }

    @Override
    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToRawLongBits(v));
    }

    @Override
    /**
     *  Implementation similar as in java.io.DataOutputStream
     */
    public void writeBytes(String s) throws IOException {
        int length = s.length();
        checkAndExpand(length);
        for (int i = 0; i < length; i++) {
            buffer[count++] = (byte) s.charAt(i);
        }
        count += length;
    }

    @Override
    public void writeChars(String s) throws IOException {
        int length = s.length();
        checkAndExpand(length * 2);
        for (int i = 0; i < length; i++) {
            int v = s.charAt(i);
            buffer[count++] = (byte) v;
            buffer[count++] = (byte) (v >>> 8);
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        int length = s.length();
        int utflen = 0;
        int c;

        for (int i = 0; i < length; i++) {
            c = s.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        writeShort(utflen);

        checkAndExpand(utflen);
        for (int i = 0; i < length; i++){
            c = s.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                buffer[count++] = (byte) c;
            } else if (c > 0x07FF) {
                buffer[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                buffer[count++] = (byte) (0x80 | ((c >>  6) & 0x3F));
                buffer[count++] = (byte) (0x80 | ( c        & 0x3F));
            } else {
                buffer[count++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
                buffer[count++] = (byte) (0x80 | ( c        & 0x3F));
            }
        }
    }

    private void checkAndExpand(int dataLength) {
        if (buffer.length - count < dataLength) {
            buffer = Arrays.copyOf(buffer, Math.max(buffer.length << 1, buffer.length + dataLength));
        }
    }
}
