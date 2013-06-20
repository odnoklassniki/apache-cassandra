package org.apache.cassandra.cache;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;

/**
 * @author roman.antipin
 *         Date: 17.06.13
 */
public class ByteArrayStreamInput implements DataInput {

    private final MemoryWrapper memory;
    private final long size;
    private long position;

    public ByteArrayStreamInput(MemoryWrapper memory) {
        this.memory = memory;
        this.size = memory.getSize();
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        checkBounds(len);
        memory.getBytes(position, b, off, len);
        position += len;
    }

    @Override
    public int skipBytes(int n) throws IOException {
        checkBounds(n);
        position += n;
        return n;
    }

    @Override
    public boolean readBoolean() throws IOException {
        checkBounds(1);
        return memory.getByte(position++) != 0;
    }

    @Override
    public byte readByte() throws IOException {
        checkBounds(1);
        return memory.getByte(position++);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xFF;
    }

    @Override
    public short readShort() throws IOException {
        checkBounds(2);
        short value = memory.getShort(position);
        position += 2;
        return value;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & 0xFFFF;
    }

    @Override
    public char readChar() throws IOException {
        checkBounds(2);
        char character = memory.getChar(position);
        position += 2;
        return character;
    }

    @Override
    public int readInt() throws IOException {
        checkBounds(4);
        int value = memory.getInt(position);
        position += 4;
        return value;
    }

    @Override
    public long readLong() throws IOException {
        checkBounds(8);
        long value = memory.getLong(position);
        position += 8;
        return value;
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public String readLine() throws IOException {
        if (position >= size) {
            return null;
        }

        StringBuilder builder = new StringBuilder();
        do {
            char ch = (char)readByte();
            if (ch == '\n' || ch == '\r') {
                break;
            }
            builder.append(ch);
        } while (position < size);

        return builder.toString();
    }

    @Override
    public String readUTF() throws IOException {
        int utflen = readUnsignedShort();
        byte[] bytearr = new byte[utflen];
        char[] chararr = new char[utflen];

        readFully(bytearr, 0, utflen);

        byte b;
        int size=0;
        for (int count = 0; count < utflen;) {
            b = bytearr[count++];
            if (b >= 0x00) {
                /* 0xxxxxxx*/
                chararr[size++] = (char)b;
            } else if ((b & 0xE0) == 0xC0) {
                /* 110x xxxx   10xx xxxx*/
                chararr[size++] = (char)(((b & 0x1F) << 6) | (bytearr[count++] & 0x3F));
            } else if ((b & 0xF0) == 0xE0) {
                /* 1110 xxxx  10xx xxxx  10xx xxxx */
                chararr[size++] = (char)(((b & 0x0F) << 12) | ((bytearr[count++] & 0x3F) << 6) | (bytearr[count++] & 0x3F));
            } else {
                /* 10xx xxxx,  1111 xxxx */
                throw new UTFDataFormatException("malformed input around byte " + count);
            }
        }

        return new String(chararr, 0, size);
    }

    private void checkBounds(long offset) throws EOFException {
        if (position + offset > size) {
            throw new EOFException();
        }
    }
}
