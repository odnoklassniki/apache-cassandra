/*
 * @(#) DataInputBuffer.java
 * Created Jun 6, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.io.util;

import java.io.DataInputStream;

import org.apache.cassandra.utils.ReentrantByteArrayInputStream;

/**
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class DataInputBuffer extends DataInputStream
{

    /**
     * @param in
     */
    public DataInputBuffer(DataOutputBuffer buffer)
    {
        super(new ReentrantByteArrayInputStream(buffer.getData(), 0, buffer.getLength()));
    }
    
    public void setBuffer(DataOutputBuffer buffer)
    {
        ((ReentrantByteArrayInputStream)in).reset(buffer.getData(), 0, buffer.getLength());
    }

}
