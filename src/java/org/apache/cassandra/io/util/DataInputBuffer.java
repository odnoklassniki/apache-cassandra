/*
 * @(#) DataInputBuffer.java
 * Created Jun 6, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.io.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

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
        super(new ByteArrayInputStream(buffer.getData(), 0, buffer.getLength()));
    }
    

}
