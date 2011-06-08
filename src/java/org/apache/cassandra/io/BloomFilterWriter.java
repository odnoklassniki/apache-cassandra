/*
 * @(#) BloomFilterBuilder.java
 * Created Jun 2, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.io;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.SortedSet;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.utils.BloomFilter;

/**
 * Unifies way of builder key-only and key+column bloom filters.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class BloomFilterWriter implements IColumnNameObserver
{
    public final static byte[] MARKEDFORDELETE = {'-','d','e','l'};
    
    private final String filterFilename;
    private BloomFilter bf;
    private ByteBuffer bb;
    private final boolean bloomColumns;
    private int keyCount;
    
    /**
     * @throws IOException 
     * 
     */
    public BloomFilterWriter(String filterFilename,long keyCount, boolean bloomColumns) throws IOException
    {
        this.filterFilename = filterFilename;
        this.bloomColumns = bloomColumns;
        
        this.bf = BloomFilter.getFilter(keyCount, bloomColumns ? 20 : 15);
        this.bb = ByteBuffer.allocate(512);
        
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.io.IColumnNameObserver#add(org.apache.cassandra.db.DecoratedKey, byte[])
     */
    @Override
    public void add(DecoratedKey<?> key, byte[] name)
    {
        assert bloomColumns : "This method is unneeded extra load for keys only index. Plz dont callit";
    
        bb.clear();
        
        bb = BloomFilter.toByteBuffer(key.key, bb);
        
        bb.position(bb.limit()).limit(bb.capacity());
        
        ensureRemaining(name.length);
        
        bb.put(name).flip();
        
        bf.add(bb);
        
    }
    
    @Override
    public void add(DecoratedKey<?> key, ColumnFamily cf)
    {
        bb = BloomFilter.toByteBuffer(key.key, bb);
        
        int keyPosition = bb.limit();
        
        SortedSet<byte[]> columns = cf.getColumnNames();
        for (byte[] bs : columns) {
            bb.limit(bb.capacity()).position(keyPosition);
            
            ensureRemaining(bs.length);
            
            bb.put(bs).flip();
            
            bf.add(bb);
            
        }
        
        if (cf.isMarkedForDelete())
            add(key,MARKEDFORDELETE);
    }
    
    private void ensureRemaining(int length)
    {
        if (bb.remaining()<length)
        {
            bb = ByteBuffer.allocate(bb.capacity()+length*2).put((ByteBuffer) bb.flip());
        }
    }

    public void add(DecoratedKey<?> key)
    {
        bf.add(key.key);
        
        keyCount++;
    }
    
    /**
     * @return the bloomColumns
     */
    public boolean isBloomColumns()
    {
        return bloomColumns;
    }
    
    /**
     * @return the bf
     */
    public BloomFilter getFilter()
    {
        return bf;
    }

    /**
     * Finishes bloom filter and flushes its data to disk. 
     * 
     * @return
     * @throws IOException
     */
    public BloomFilter build() throws IOException
    {
        assert !bloomColumns || keyCount<=1 || keyCount<bf.getElementCount() : "FilterWriter is in bloomColumns mode, but no columns were actually placed to it"; 
        
        // bloom filter
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(filterFilename, "rw", 128*1024);
        file.setSkipCache(true);
        BloomFilter.serializer().serialize(bf, file);
        file.close();
        
        return bf;
    }
}
