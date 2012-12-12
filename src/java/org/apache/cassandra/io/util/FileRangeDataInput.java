/*
 * @(#) FileRangeDataInput.java
 * Created May 17, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.io.util;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

/**
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class FileRangeDataInput implements DataInput
{
    
    private final BufferedRandomAccessFile in;
    private final long finishAt;
    
    

    /**
     * @param in
     * @param dataStart
     * @param finishAt throws EOF when file pointer hits this position
     * @throws IOException 
     */
    public FileRangeDataInput(BufferedRandomAccessFile in, long dataStart,
            long finishAt) throws IOException
    {
        this.in = in;
        this.finishAt = finishAt;
        
        this.in.seek(dataStart);
    }
    
    private void checkPosition(int expectedRead) throws EOFException
    {
        if (available(expectedRead)<expectedRead)
            throw new EOFException("pointer:"+in.getFilePointer()+"+exp:"+expectedRead+">="+finishAt+" of "+in.getPath());
    }
    
    private int available(int expectedRead)
    {
        return (int) Math.min( finishAt-in.getFilePointer(),expectedRead );
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readFully(byte[])
     */
    @Override
    public void readFully(byte[] buf) throws IOException
    {
        checkPosition(buf.length);
        
        in.readFully(buf);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readFully(byte[], int, int)
     */
    @Override
    public void readFully(byte[] buf, int ofs, int len) throws IOException
    {
        checkPosition(len);

        in.readFully(buf, ofs, len);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#skipBytes(int)
     */
    @Override
    public int skipBytes(int i) throws IOException
    {
        i = available(i);
        
        in.skipBytes(i);
        
        return i;
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readBoolean()
     */
    @Override
    public boolean readBoolean() throws IOException
    {
        checkPosition(1);
        return in.readBoolean();
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readByte()
     */
    @Override
    public byte readByte() throws IOException
    {
        checkPosition(1);
        return in.readByte();
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readUnsignedByte()
     */
    @Override
    public int readUnsignedByte() throws IOException
    {
        checkPosition(1);
        
        return in.readUnsignedByte();
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readShort()
     */
    @Override
    public short readShort() throws IOException
    {
        checkPosition(2);
        return in.readShort();
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readUnsignedShort()
     */
    @Override
    public int readUnsignedShort() throws IOException
    {
        checkPosition(2);
        return in.readUnsignedShort();
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readChar()
     */
    @Override
    public char readChar() throws IOException
    {
        checkPosition(2);
        return in.readChar();
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readInt()
     */
    @Override
    public int readInt() throws IOException
    {
        checkPosition(4);
        return in.readInt();
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readLong()
     */
    @Override
    public long readLong() throws IOException
    {
        checkPosition(8);
        return in.readLong();
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readFloat()
     */
    @Override
    public float readFloat() throws IOException
    {
        checkPosition(4);
        return in.readFloat();
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readDouble()
     */
    @Override
    public double readDouble() throws IOException
    {        
        checkPosition(8);
        return in.readDouble();

    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readLine()
     */
    @Override
    public String readLine() throws IOException
    {
        throw new UnsupportedOperationException(
                "Method FileRangeDataInput.readLine() is not supported");
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readUTF()
     */
    @Override
    public String readUTF() throws IOException
    {
        throw new UnsupportedOperationException(
                "Method FileRangeDataInput.readUTF() is not supported");
    }

}
