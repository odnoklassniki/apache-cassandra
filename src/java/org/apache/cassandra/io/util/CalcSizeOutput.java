package org.apache.cassandra.io.util;

import java.io.DataOutput;

/**
 * This output not writes anywhere - it just counts number of bytes written
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 */
public class CalcSizeOutput implements DataOutput {
    protected int count;

    public CalcSizeOutput() {
        this.count = 0;
    }

    public int byteCount() {
        return count;
    }

    public void write(int b) {
        count++;
    }

    public void write(byte[] b) {
        count += b.length;
    }

    public void write(byte[] b, int off, int len) {
        count += len;
    }

    public void writeBoolean(boolean v) {
        count++;
    }

    public void writeByte(int v) {
        count++;
    }

    public void writeShort(int v) {
        count += 2;
    }

    public void writeChar(int v) {
        count += 2;
    }

    public void writeInt(int v) {
        count += 4;
    }

    public void writeLong(long v) {
        count += 8;
    }

    public void writeFloat(float v) {
        count += 4;
    }

    public void writeDouble(double v) {
        count += 8;
    }

    public void writeBytes(String s) {
        count += s.length();
    }

    public void writeChars(String s) {
        count += s.length() << 1;
    }

    public void writeUTF(String s) {
        int length = utfLength(s);
        count += length + (length <= 0x7fff ? 2 : 4);
    }

    private int utfLength(String s) {
        int result = 0;
        int length = s.length();
        for (int i = 0; i < length; i++) {
            int v = s.charAt(i);
            if (v <= 0x7f && v != 0) {
                result++;
            } else if (v > 0x7ff) {
                result += 3;
            } else {
                result += 2;
            }
        }
        return result;
    }

}
