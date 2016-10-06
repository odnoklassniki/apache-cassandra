/*
 * @(#) BloomFilterWithElementCountSerializer.java
 * Created 01.07.2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class BloomFilterWithElementCountSerializer extends
        BloomFilterSerializer
{
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.utils.BloomFilterSerializer#deserialize(java.io.DataInput)
     */
    @Override
    public BloomFilter deserialize(DataInput dis) throws IOException
    {
        long elementCount = dis.readLong();
        BloomFilter bf = super.deserialize(dis);
        bf.setElementCount(elementCount);
        
        return bf;
    }
    
    @Override
    public long headerSize( long words )
    {
        return super.headerSize( words ) + 8;
    }

    protected void serializeHeader( BloomFilter bf, DataOutput dos ) throws IOException
    {
        dos.writeLong(bf.getElementCount());
        
        super.serializeHeader( bf, dos );
    }
}
