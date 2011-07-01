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
     * @see org.apache.cassandra.utils.BloomFilterSerializer#serialize(org.apache.cassandra.utils.BloomFilter, java.io.DataOutput)
     */
    @Override
    public void serialize(BloomFilter bf, DataOutput dos) throws IOException
    {
        dos.writeLong(bf.getElementCount());

        super.serialize(bf, dos);
    }
    
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
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.utils.BloomFilterSerializer#serializeSize(org.apache.cassandra.utils.BloomFilter)
     */
    @Override
    public long serializeSize(BloomFilter bf)
    {
        return super.serializeSize(bf)+8;
    }
}
