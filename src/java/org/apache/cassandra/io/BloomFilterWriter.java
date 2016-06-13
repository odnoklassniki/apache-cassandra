/*
 * @(#) BloomFilterBuilder.java
 * Created Jun 2, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.io;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.SortedSet;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;

/**
 * Unifies way of builder key-only and key+column bloom filters.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class BloomFilterWriter implements IColumnNameObserver
{
    private static final Logger logger = Logger.getLogger(BloomFilterWriter.class);

    public final static byte[] MARKEDFORDELETE = {'-','d','e','l'};
    
    private final String filterFilename;
    private BloomFilter bf;
    private final boolean bloomColumns;
    private long keyCount;
    private long estimatedKeyCount;
    
    /**
     * @throws IOException 
     * 
     */
    public BloomFilterWriter(String filterFilename,long keyCount, boolean bloomColumns) throws IOException
    {
        this.filterFilename = filterFilename;
        this.bloomColumns = bloomColumns;
        
        this.estimatedKeyCount = keyCount;
        
    }
    
    public void setEstimatedColumnCount(long columnCount)
    {
        estimatedKeyCount+=columnCount;
    }

    /**
     * @return the bf
     */
    private BloomFilter bf()
    {
        if (this.bf==null)
            this.bf = BloomFilter.getFilter(estimatedKeyCount, 15);
        
        return bf;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.io.IColumnNameObserver#add(org.apache.cassandra.db.DecoratedKey, byte[])
     */
    @Override
    public void add(DecoratedKey<?> key, byte[] name)
    {
        bf().add(key.key,name);
    }
    
    @Override
    public void add(DecoratedKey<?> key, ColumnFamily cf)
    {
        
        SortedSet<byte[]> columns = cf.getColumnNames();
        for (byte[] bs : columns) {
            bf().add(key.key,bs);
        }
        
        if (cf.isMarkedForDelete())
            add(key,MARKEDFORDELETE);
    }
    
    public void add(DecoratedKey<?> key)
    {
        bf().add(key.key);
        
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
        return bf();
    }

    /**
     * Finishes bloom filter and flushes its data to disk. 
     * 
     * @return
     * @throws IOException
     */
    public BloomFilter build() throws IOException
    {
        assert !bloomColumns || keyCount<=1 || keyCount<bf().getElementCount() : "FilterWriter is in bloomColumns mode, but no columns were actually placed to it"; 
        
        // bloom filter
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(filterFilename, "rw", 128*1024);
        file.setSkipCache(true);
        BloomFilter.serializerForSSTable().serialize(bf(), file);
        file.close();
        
        if (logger.isInfoEnabled())
            logger.info("Written filter "+filterFilename+", with actual elements (estimated elements) counts: "+this.bf().getElementCount()+'('+estimatedKeyCount+')'+" with page size="+bf().bitset.getPageSize()+", count="+bf().bitset.getPageCount());
        
        return bf();
    }
}
