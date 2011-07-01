/*
 * @(#) FreshTimestampType.java
 * Created 29.06.2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.db.marshal;

import java.sql.Timestamp;


/**
 * This expects to find timestamp micros in high 8 bytes and sequence and uniq sequence in lower 8 bytes
 * of column name.
 * 
 * Order is fresh first
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class FreshTimestampType extends BytesType
{
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.marshal.BytesType#compare(byte[], byte[])
     */
    @Override
    public int compare(byte[] o1, byte[] o2)
    {
        return -super.compare(o1, o2);
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.marshal.BytesType#getString(byte[])
     */
    @Override
    public String getString(byte[] bytes)
    {
        if (bytes.length==0)
            return "-any-";
        
        long time = toLong(bytes, 0, 8);
        
        return new Timestamp(time/1000).toString()+time%1000+'-'+toLong(bytes, 8, 8);
    }

    public static final long toLong(byte[] b,int offset,int size) {
        long l = 0;
        for (int i=0; i<size; ++i)
            l |= ((long)b[offset+i]&0xff)<<((size-i-1)<<3);
        return l;
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.marshal.AbstractType#validate(byte[])
     */
    @Override
    public void validate(byte[] bytes)
    {
        if (bytes.length==0)
            return; // 0 length array is special kind of 'any'
        
        if (bytes.length<16)
            throw new MarshalException("FreshTimestamp column name must be min 16 bytes length");
    }
    
    
}
