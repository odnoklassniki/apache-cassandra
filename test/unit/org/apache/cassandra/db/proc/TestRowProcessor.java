/*
 * @(#) TestRowProcessor.java
 * Created May 25, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.db.proc;

import java.util.Properties;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;

/**
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class TestRowProcessor implements IRowProcessor
{
    public static boolean shouldProcessIncomplete = false, shouldProcessUnchanged = false, active = false;
    public static int count = 0;
    
    public int mult=0;

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#setConfiguration(java.util.Properties)
     */
    @Override
    public void setConfiguration(Properties config)
    {
        assert config.getProperty("class").equals("Test") : config.getProperty("class") +" != "+"Test";
        
        mult = Integer.parseInt( config.getProperty("multiplier") );
        
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#setColumnFamilyStore(org.apache.cassandra.db.ColumnFamilyStore)
     */
    @Override
    public void setColumnFamilyStore(ColumnFamilyStore cfs)
    {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#process(org.apache.cassandra.db.DecoratedKey, org.apache.cassandra.db.ColumnFamily, boolean)
     */
    @Override
    public ColumnFamily process(DecoratedKey key, ColumnFamily columns,
            boolean incomplete)
    {
        if (!active)
            return columns;
        
        count +=mult;
        
        return columns;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#shouldProcessUnchanged()
     */
    @Override
    public boolean shouldProcessUnchanged()
    {
        return shouldProcessUnchanged;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#shouldProcessIncomplete()
     */
    @Override
    public boolean shouldProcessIncomplete()
    {
        return shouldProcessIncomplete;
    }

}
