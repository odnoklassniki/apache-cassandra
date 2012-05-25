/*
 * @(#) RemoveTombstonesRowProcessor.java
 * Created May 25, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.db.proc;

import java.util.Properties;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;

/**
 * This processor removes tombstones after GCGracePeriod expiration
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class RemoveDeletedRowProcessor implements IRowProcessor
{
    /**
     * removes tombstones created earlier than this millis
     */
    private int gcBefore;

    public RemoveDeletedRowProcessor(int gcBefore)
    {
        this.gcBefore = gcBefore;
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#setConfiguration(org.w3c.dom.Node)
     */
    @Override
    public void setConfiguration(Properties config)
    {
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#setColumnFamilyStore(org.apache.cassandra.db.ColumnFamilyStore)
     */
    @Override
    public void setColumnFamilyStore(ColumnFamilyStore cfs)
    {
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#shouldProcessIncomplete()
     */
    @Override
    public boolean shouldProcessIncomplete()
    {
        return false;
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#shouldProcessUnchanged()
     */
    @Override
    public boolean shouldProcessUnchanged()
    {
        return false;
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#process(org.apache.cassandra.db.DecoratedKey, org.apache.cassandra.db.ColumnFamily, boolean)
     */
    @Override
    public ColumnFamily process(DecoratedKey key, ColumnFamily columns,
            boolean incomplete)
    {
        assert !incomplete;
        
        return ColumnFamilyStore.removeDeleted(columns, gcBefore);
    }
}
