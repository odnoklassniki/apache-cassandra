/*
 * @(#) CompactionWriterIterator.java
 * Created May 18, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.io;

import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.FSWriteError;
import org.apache.cassandra.db.proc.IRowProcessor;
import org.apache.cassandra.service.StorageService;

/**
 * 
 * Compacts row to sstablewriter
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class CompactionWriterIterator extends CompactionIterator
{

    private final String newFilename;
    private final long expectedBloomFilterSize;

    protected SSTableWriter writer;

    /**
     * @param cfs
     * @param sstables
     * @param gcBefore
     * @param major
     * @param columnNameObserver
     * @throws IOException
     */
    public CompactionWriterIterator(ColumnFamilyStore cfs,
            Iterable<SSTableReader> sstables, IRowProcessor rowp, boolean major,                                         
            String newFilename,
            long expectedBloomFilterSize
            ) throws IOException
    {
        super(cfs, sstables, rowp, major);
        
        this.newFilename = newFilename;
        this.expectedBloomFilterSize = expectedBloomFilterSize;
    }
    
    protected CompactionWriterIterator(ColumnFamilyStore cfs, Iterator iter,IRowProcessor rowp, boolean major, 
            String newFilename,
            long expectedBloomFilterSize)
    {
        super(cfs,iter,rowp,major);
        
        this.newFilename = newFilename;
        this.expectedBloomFilterSize = expectedBloomFilterSize;
    }

    
    private SSTableWriter getWriter() 
    {
        if (writer!=null)
            return writer;
        
        try {
            writer = new SSTableWriter(newFilename, expectedBloomFilterSize, 0, StorageService.getPartitioner(),cfs.metadata.bloomColumns);
            if (cfs.metadata.bloomColumns)
                setColumnNameObserver(writer.getBloomFilterWriter());
            
            return writer;
        } catch (IOException e) {
            throw new FSWriteError(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.io.CompactionIterator#startRowWrite(org.apache.cassandra.db.DecoratedKey, int)
     */
    @Override
    protected CompactedRow startRowWrite(DecoratedKey key, int cfSize) 
    {
        try {
            SSTableWriter tableWriter = getWriter();
            
            long startRowPosition = tableWriter.startAppend(key, cfSize);
            
            return new CompactedRow(key, tableWriter.getRowOutput(), startRowPosition );
        } catch (IOException e) {
            throw new FSWriteError(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.io.CompactionIterator#finishRowWrite(org.apache.cassandra.db.DecoratedKey)
     */
    @Override
    protected void finishRowWrite(CompactedRow compactedRow)
    {
        try {
            long rowSize = getWriter().afterAppend(compactedRow.key, compactedRow.rowPosition);
            compactedRow.setRowSize(rowSize);
        } catch (IOException e) {
            throw new FSWriteError(e);
        }
        
    }

    public SSTableReader closeAndOpenReader() throws IOException
    {
        assert writer != null;
        
        return writer.closeAndOpenReader();
    }
}
