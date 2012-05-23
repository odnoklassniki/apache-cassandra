/*
 * @(#) WritingDataInput.java
 * Created May 17, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.io.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

/**
 * Writes all data being read from this data input to supplied {@link DataOutput} object.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class DataInputSink implements DataInput
{
    /**
     * all data read from here...
     */
    private final DataInput in;

    /**
     * ...will be written here
     */
    private final DataOutput out;

    public DataInputSink(DataInput in, DataOutput out)
    {
        this.in = in;
        this.out = out;
    }
    
    public void readFully(byte[] buf) throws IOException
    {
        readFully(buf,0,buf.length);
    }
    
    public void readFully(byte[] buf, int offs, int len) throws IOException
    {
        in.readFully(buf, offs, len);
        out.write(buf, offs, len);
    }
    
    public int skipBytes(int bytecount) throws IOException
    {
        for (int i=0;i<bytecount;i++)
        {
            try {
                int b=in.readUnsignedByte();
                out.writeByte(i);
            } catch (EOFException e) {
                return i;
            }
        }

        return bytecount;
    }
    
    public boolean readBoolean() throws IOException
    {
        boolean b = in.readBoolean();
        out.writeBoolean(b);
        return b;
    }
    
    public byte readByte() throws IOException
    {
        byte b = in.readByte();
        out.write(b);
        return b;
    }
    
    public int readUnsignedByte() throws IOException
    {
        int b = in.readUnsignedByte();
        out.writeByte(b);
        return b;
    }
    
    public short readShort() throws IOException
    {
        short s = in.readShort();
        out.writeShort(s);
        return s;
    }
    
    public int readUnsignedShort() throws IOException
    {
        int s = in.readUnsignedShort();
        out.writeShort(s);
        return s;
    }
    
    public char readChar() throws IOException
    {
        char c = in.readChar();
        out.writeChar(c);
        return c;
    }
    
    public int readInt() throws IOException
    {
        int i = in.readInt();
        out.writeInt(i);
        return i;
    }

    public long readLong() throws IOException
    {
        long l = in.readLong();
        out.writeLong(l);
        return l;
    }

    public float readFloat() throws IOException
    {
        float f = in.readFloat();
        out.writeFloat(f);
        return f;
    }
    
    public double readDouble() throws IOException
    {
        double d = in.readDouble();
        out.writeDouble(d);
        return d;
    }
    
    public String readLine() throws IOException
    {
        String string = in.readLine();
        if (string!=null) {
            out.writeBytes(string); out.writeChar('\n');
        }
        return string;
    }
    
    public String readUTF() throws IOException
    {
        String string = in.readUTF();
        if (string!=null) {
            out.writeUTF(string);
            
        }
        return string;
    }

}
