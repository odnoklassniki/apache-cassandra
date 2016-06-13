/*
 * @(#) RowMayExistQueryFilter.java
 * Created Dec 8, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.IColumnContainer;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.SSTableReader;

/**
 * This query checks for possible existance of several columns in a row.
 * 
 * Only memtable and bloom filters are consulted.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class ColumnsMayExistQueryFilter extends QueryFilter
{
    private EmptyColumnIterator emptyColumnIterator ;
    private final Iterable<byte[]> names;
    private final ColumnCollector collector;
    private final int limit;
    
    private int collectedCount = 0;
    
    private DecoratedKey decoratedKey;
    
    /**
     * 
     * @param key
     * @param path
     * @param names source of column names. Not all names could be fetched.
     * @param collector destination for may be found column names.
     * @param limit max number of columns to return
     */
    public ColumnsMayExistQueryFilter(String key, QueryPath path, Iterable<byte[]> names, ColumnCollector collector, int limit)
    {
        super(key, path);
        this.names = names;
        this.collector = collector;
        this.limit = limit;
    }
    

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.filter.QueryFilter#getMemColumnIterator(org.apache.cassandra.db.Memtable, org.apache.cassandra.db.ColumnFamily, org.apache.cassandra.db.marshal.AbstractType)
     */
    @Override
    public ColumnIterator getMemColumnIterator(Memtable memtable,
            ColumnFamily cf, AbstractType comparator)
    {
        this.emptyColumnIterator = new EmptyColumnIterator(memtable.getTableName(), path.columnFamilyName);

        if (cf==null)
            return emptyColumnIterator; // row not found
        
        Iterator<byte[]> it = names.iterator();
        while (collectedCount<limit && it.hasNext())
        {
            byte[] name = it.next();
            
            if (!collector.isCollected(name) && cf.getColumn(name)!=null)
            {
                collector.collect(name);
                collectedCount++;
            }
        }
        
        return emptyColumnIterator;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.filter.QueryFilter#getSSTableColumnIterator(org.apache.cassandra.io.SSTableReader)
     */
    @Override
    public ColumnIterator getSSTableColumnIterator(SSTableReader sstable)
            throws IOException
    {
        assert sstable.isColumnBloom() : "Only column families with column level bloom filters can use this query";
        
        if (collectedCount>=limit)
            return emptyColumnIterator; // we already found all columns in memtable
         
        Iterator<byte[]> it = names.iterator();
        while (collectedCount<limit && it.hasNext())
        {
            byte[] name = it.next();
            
            if (collector.isCollected(name))
                continue;
            
            // did not found it previously. inspecting sstable bloom filter
            if (sstable.mayPresent(key, name))
            {
                collector.collect(name);
                collectedCount++;
            }
        }
        
        return emptyColumnIterator;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.filter.QueryFilter#collectReducedColumns(org.apache.cassandra.db.IColumnContainer, java.util.Iterator, int)
     */
    @Override
    public void collectReducedColumns(IColumnContainer container,
            Iterator<IColumn> reducedColumns, int gcBefore)
    {

    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.filter.QueryFilter#collectCollatedColumns(org.apache.cassandra.db.ColumnFamily, java.util.Iterator, int)
     */
    @Override
    public void collectCollatedColumns(ColumnFamily returnCF,
            Iterator<IColumn> collatedColumns, int gcBefore)
    {
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.filter.QueryFilter#filterSuperColumn(org.apache.cassandra.db.SuperColumn, int)
     */
    @Override
    public SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore)
    {
        throw new UnsupportedOperationException(
                "Method FastRowMayExistQueryFilter.filterSuperColumn(superColumn, gcBefore) is not supported");
    }

    public interface ColumnCollector
    {
        /**
         * Called by filter when column may exist in store, according to bloom filter
         * @param name
         */
        void collect(byte[] name);

        /**
         * @return true, if column with name 'name' was previously collected by this filter 
         *         (this is neccessary for performance optimization)
         */
        boolean isCollected(byte[] name);
    }
}
