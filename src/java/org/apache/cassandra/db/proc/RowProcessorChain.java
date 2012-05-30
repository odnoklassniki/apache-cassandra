/*
 * @(#) CompositeRowProcessor.java
 * Created May 25, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.db.proc;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.Pair;

/**
 * If several row processors are configured for a column family store, this class ensures all processors will be called
 * one by one.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class RowProcessorChain implements IRowProcessor
{
    private final ArrayList<IRowProcessor> processors = new ArrayList<IRowProcessor>();
    
    boolean procUnchanged, procIncomplete, procEmpty;

    public RowProcessorChain add(IRowProcessor p)
    {
        processors.add(p);
        
        procUnchanged|=p.shouldProcessUnchanged();
        procIncomplete|=p.shouldProcessIncomplete();
        procEmpty|=p.shouldProcessEmpty();

        return this;
    }
    
    public RowProcessorChain add(Pair<Class<? extends IRowProcessor>, Properties> processorDef)
    {
        try {
            IRowProcessor rp = processorDef.left.newInstance();
            
            rp.setConfiguration(processorDef.right);
            
            add(rp);
        } catch (Exception e) {
            throw new RuntimeException("Cannot init row processor "+processorDef.left.getName(),e);
        }
        
        return this;
    }
    
    public RowProcessorChain addAll(List<Pair<Class<? extends IRowProcessor>,Properties>> defs)
    {
        if (defs==null)
            return this;
        
        for (Pair<Class<? extends IRowProcessor>, Properties> pair : defs) {
            add(pair);
        }
        
        return this;
    }

    public IRowProcessor build()
    {
        if (processors.size()==1)
            return processors.get(0);

        return this;
    }
    

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#shouldProcessIncomplete()
     */
    @Override
    public boolean shouldProcessIncomplete()
    {
        return procIncomplete;
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#shouldProcessUnchanged()
     */
    @Override
    public boolean shouldProcessUnchanged()
    {
        return procUnchanged;
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor.shouldProcessEmpty()
     */
    @Override
    public boolean shouldProcessEmpty()
    {
        return procEmpty;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#setColumnFamilyStore(org.apache.cassandra.db.ColumnFamilyStore)
     */
    @Override
    public void setColumnFamilyStore(ColumnFamilyStore cfs)
    {
        for (int i=0;i<processors.size();i++) {
            processors.get(i).setColumnFamilyStore(cfs);
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#process(org.apache.cassandra.db.DecoratedKey, org.apache.cassandra.db.ColumnFamily, boolean)
     */
    @Override
    public ColumnFamily process(DecoratedKey key, ColumnFamily columns,
            boolean incomplete)
    {
        for (int i=0;i<processors.size();i++) 
        {
            
            IRowProcessor p = processors.get(i);
            
            boolean empty = (columns == null || columns.getColumnsMap().isEmpty());
            boolean procIncomplete = p.shouldProcessIncomplete();
            boolean procEmpty = p.shouldProcessEmpty();
            
            if ((!incomplete || procIncomplete) && (!empty || procEmpty))
                columns=p.process(key,columns,incomplete);
        }
        
        return columns;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.proc.IRowProcessor#setConfiguration(java.util.Properties)
     */
    @Override
    public void setConfiguration(Properties config)
    {
        
    }
}
