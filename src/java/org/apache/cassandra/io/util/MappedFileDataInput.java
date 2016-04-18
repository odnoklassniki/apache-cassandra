package org.apache.cassandra.io.util;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.*;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;

public class MappedFileDataInput extends InputStream implements FileDataInput
{
    private final MappedByteBuffer buffer;
    private final String filename;
    private int position;
    private int markedPosition;
    private final long absoluteStartPosition;

    public MappedFileDataInput(MappedByteBuffer buffer, String filename, long absoluteStartPosition)
    {
        this(buffer, filename, absoluteStartPosition, 0);
    }

    public MappedFileDataInput(MappedByteBuffer buffer, String filename, long absoluteStartPosition, int position)
    {
        assert buffer != null;
        assert buffer.order()==ByteOrder.BIG_ENDIAN; 
        
        this.absoluteStartPosition = absoluteStartPosition;
        this.buffer = buffer;
        this.filename = filename;
        this.position = position;
    }

    public long getAbsolutePosition()
    {
        return absoluteStartPosition + position;
    }

// don't make this public, this is only for seeking WITHIN the current mapped segment
    private void seekInternal(int pos) throws IOException
    {
        position = pos;
    }

    @Override
    public boolean markSupported()
    {
        return true;
    }

    @Override
    public void mark(int ignored)
    {
        markedPosition = position;
    }

    @Override
    public void reset() throws IOException
    {
        seekInternal(markedPosition);
    }

    public void mark()
    {
        mark(-1);
    }

    public int bytesPastMark()
    {
        assert position >= markedPosition;
        return position - markedPosition;
    }

    public boolean isEOF() throws IOException
    {
        return position == buffer.capacity();
    }

    public boolean isEOF(int len) throws IOException
    {
        return position + len > buffer.capacity();
    }

    public String getPath()
    {
        return filename;
    }

    public int read() throws IOException
    {
        if (isEOF())
            return -1;
        return buffer.get(position++) & 0xFF;
    }

    public int skipBytes(int n) throws IOException
    {
        assert n >= 0 : "skipping negative bytes is illegal: " + n;
        if (n == 0)
            return 0;
        int oldPosition = position;
        assert ((long)oldPosition) + n <= Integer.MAX_VALUE;
        position = Math.min(buffer.capacity(), position + n);
        return position - oldPosition;
    }

    public  void skipLong() throws IOException {
        if ( (position+=8) > buffer.capacity()) {
            position = buffer.capacity();
            throw new EOFException();
        };
    }

    /*
     !! DataInput methods below are copied from the implementation in Apache Harmony RandomAccessFile.
     */

    /**
     * Reads a boolean from the current position in this file. Blocks until one
     * byte has been read, the end of the file is reached or an exception is
     * thrown.
     *
     * @return the next boolean value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final boolean readBoolean() throws IOException {
        int temp = this.read();
        if (temp < 0) {
            throw new EOFException();
        }
        return temp != 0;
    }

    /**
     * Reads an 8-bit byte from the current position in this file. Blocks until
     * one byte has been read, the end of the file is reached or an exception is
     * thrown.
     *
     * @return the next signed 8-bit byte value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final byte readByte() throws IOException {
        int temp = this.read();
        if (temp < 0) {
            throw new EOFException();
        }
        return (byte) temp;
    }

    /**
     * Reads a 16-bit character from the current position in this file. Blocks until
     * two bytes have been read, the end of the file is reached or an exception is
     * thrown.
     *
     * @return the next char value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final char readChar() throws IOException {
        if ( isEOF(2) ) {
            skipBytes(2);
            throw new EOFException();
        }
        char c = buffer.getChar(position); 
        position+=2;
        return c;
    }

    /**
     * Reads a 64-bit double from the current position in this file. Blocks
     * until eight bytes have been read, the end of the file is reached or an
     * exception is thrown.
     *
     * @return the next double value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    /**
     * Reads a 32-bit float from the current position in this file. Blocks
     * until four bytes have been read, the end of the file is reached or an
     * exception is thrown.
     *
     * @return the next float value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    /**
     * Reads bytes from this file into {@code buffer}. Blocks until {@code
     * buffer.length} number of bytes have been read, the end of the file is
     * reached or an exception is thrown.
     *
     * @param buffer
     *            the buffer to read bytes into.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     * @throws NullPointerException
     *             if {@code buffer} is {@code null}.
     */
    public final void readFully(byte[] buffer) throws IOException {
        readFully(buffer, 0, buffer.length);
    }

    /**
     * Read bytes from this file into {@code buffer} starting at offset {@code
     * offset}. This method blocks until {@code count} number of bytes have been
     * read.
     *
     * @param buffer
     *            the buffer to read bytes into.
     * @param offset
     *            the initial position in {@code buffer} to store the bytes read
     *            from this file.
     * @param count
     *            the maximum number of bytes to store in {@code buffer}.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IndexOutOfBoundsException
     *             if {@code offset < 0} or {@code count < 0}, or if {@code
     *             offset + count} is greater than the length of {@code buffer}.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     * @throws NullPointerException
     *             if {@code buffer} is {@code null}.
     */
    public final void readFully(byte[] buffer, int offset, int count)
            throws IOException {
        // avoid int overflow
        if (offset < 0 || offset > buffer.length || count < 0
                || count > buffer.length - offset) {
            throw new IndexOutOfBoundsException();
        }
        if (isEOF(count)) {
            // rare path
            read(buffer, offset, count);
            throw new EOFException();
        }
        // fast common path
        while (count-- > 0) {
            buffer[ offset ++ ] = this.buffer.get( position++ );
        }
    }

    /**
     * Reads a 32-bit integer from the current position in this file. Blocks
     * until four bytes have been read, the end of the file is reached or an
     * exception is thrown.
     *
     * @return the next int value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final int readInt() throws IOException {
        if ( isEOF(4) ) {
            skipBytes(4);
            throw new EOFException();
        }
        int i = buffer.getInt(position); 
        position+=4;
        return i;
    }

    /**
     * Reads a line of text form the current position in this file. A line is
     * represented by zero or more characters followed by {@code '\n'}, {@code
     * '\r'}, {@code "\r\n"} or the end of file marker. The string does not
     * include the line terminating sequence.
     * <p>
     * Blocks until a line terminating sequence has been read, the end of the
     * file is reached or an exception is thrown.
     *
     * @return the contents of the line or {@code null} if no characters have
     *         been read before the end of the file has been reached.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final String readLine() throws IOException {
        StringBuilder line = new StringBuilder(80); // Typical line length
        boolean foundTerminator = false;
        int unreadPosition = 0;
        while (true) {
            int nextByte = read();
            switch (nextByte) {
                case -1:
                    return line.length() != 0 ? line.toString() : null;
                case (byte) '\r':
                    if (foundTerminator) {
                        seekInternal(unreadPosition);
                        return line.toString();
                    }
                    foundTerminator = true;
                    /* Have to be able to peek ahead one byte */
                    unreadPosition = position;
                    break;
                case (byte) '\n':
                    return line.toString();
                default:
                    if (foundTerminator) {
                        seekInternal(unreadPosition);
                        return line.toString();
                    }
                    line.append((char) nextByte);
            }
        }
    }

    /**
     * Reads a 64-bit long from the current position in this file. Blocks until
     * eight bytes have been read, the end of the file is reached or an
     * exception is thrown.
     *
     * @return the next long value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final long readLong() throws IOException {
        if ( isEOF(8) ) {
            skipBytes(8);
            throw new EOFException();
        }
        long l = buffer.getLong(position); 
        position+=8;
        return l;
    }

    /**
     * Reads a 16-bit short from the current position in this file. Blocks until
     * two bytes have been read, the end of the file is reached or an exception
     * is thrown.
     *
     * @return the next short value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final short readShort() throws IOException {
        if ( isEOF(2) ) {
            skipBytes(2);
            throw new EOFException();
        }
        short s = buffer.getShort(position); 
        position +=2;
        return s;
    }

    /**
     * Reads an unsigned 8-bit byte from the current position in this file and
     * returns it as an integer. Blocks until one byte has been read, the end of
     * the file is reached or an exception is thrown.
     *
     * @return the next unsigned byte value from this file as an int.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final int readUnsignedByte() throws IOException {
        int temp = this.read();
        if (temp < 0) {
            throw new EOFException();
        }
        return temp;
    }

    /**
     * Reads an unsigned 16-bit short from the current position in this file and
     * returns it as an integer. Blocks until two bytes have been read, the end of
     * the file is reached or an exception is thrown.
     *
     * @return the next unsigned short value from this file as an int.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final int readUnsignedShort() throws IOException {
        if ( isEOF(2) ) {
            skipBytes(2);
            throw new EOFException();
        }
        int i = buffer.getShort(position) & 0xFFFF; 
        position +=2;
        return i;
    }

    /**
     * Reads a string that is encoded in {@link DataInput modified UTF-8} from
     * this file. The number of bytes that must be read for the complete string
     * is determined by the first two bytes read from the file. Blocks until all
     * required bytes have been read, the end of the file is reached or an
     * exception is thrown.
     *
     * @return the next string encoded in {@link DataInput modified UTF-8} from
     *         this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     * @throws UTFDataFormatException
     *             if the bytes read cannot be decoded into a character string.
     */
    public final String readUTF() throws IOException {
        int utflen = readUnsignedShort();
        
        if (isEOF(utflen)) {
            skipBytes(utflen);
            throw new EOFException();
        }
        
        char[] chars = null;
        chars = new char[utflen];

        int c, c2, c3;
        int count = 0;
        int chararr_count=0;

        while (count < utflen) {
            c = this.buffer.get( position ) & 0xff;
            if (c > 127) break;
            chars[count++]=(char)c;
            position++;
        }
        
        if (count==utflen) 
            return new String(chars);
        
        chararr_count = count;

        while (count < utflen) {
            c = this.buffer.get( position++ ) & 0xff;
            switch (c >> 4) {
                case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                    /* 0xxxxxxx*/
                    count++;
                    chars[chararr_count++]=(char)c;
                    break;
                case 12: case 13:
                    /* 110x xxxx   10xx xxxx*/
                    count += 2;
                    if (count > utflen) {
                        skipBytes(1);
                        throw new UTFDataFormatException(
                            "malformed input: partial character at end");
                    }
                    c2 = this.buffer.get( position++ );
                    if ((c2 & 0xC0) != 0x80) {
                        skipBytes(utflen - count);
                        throw new UTFDataFormatException(
                            "malformed input around byte " + count);
                    }
                    chars[chararr_count++]=(char)(((c & 0x1F) << 6) |
                                                    (c2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    count += 3;
                    if (count > utflen) {
                        skipBytes(2);
                        throw new UTFDataFormatException(
                            "malformed input: partial character at end");
                    }
                    c2 = this.buffer.get( position++ );
                    c3 = this.buffer.get( position++ );
                    if (((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80)) {
                        skipBytes( utflen - count );
                        throw new UTFDataFormatException(
                            "malformed input around byte " + (count-1));
                    }
                    chars[chararr_count++]=(char)(((c     & 0x0F) << 12) |
                                                    ((c2 & 0x3F) << 6)  |
                                                    ((c3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx,  1111 xxxx */
                    skipBytes( utflen - count );
                    throw new UTFDataFormatException(
                        "malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chars, 0, chararr_count);
    }
}
