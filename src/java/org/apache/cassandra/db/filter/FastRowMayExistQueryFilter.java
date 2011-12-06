/*
 * @(#) RowMayExistQueryFilter.java
 * Created Dec 8, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.IColumnContainer;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.SSTableReader;

/**
 * This query checks for possible existance of a row. 
 * 
 * Only memtable and bloom filters are consulted.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class FastRowMayExistQueryFilter extends QueryFilter
{
    private final EmptyColumnIterator EMPTY_COLUMN_ITERATOR = new EmptyColumnIterator();
    /**
     * result is placed here
     */
    private boolean mayExist = false;
    
    public FastRowMayExistQueryFilter(String key, QueryPath path)
    {
        super(key, path);
    }

    /**
     * @return true, if row may exist in local store
     */
    public boolean mayExist()
    {
        return mayExist;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.filter.QueryFilter#getMemColumnIterator(org.apache.cassandra.db.Memtable, org.apache.cassandra.db.ColumnFamily, org.apache.cassandra.db.marshal.AbstractType)
     */
    @Override
    public ColumnIterator getMemColumnIterator(Memtable memtable,
            ColumnFamily cf, AbstractType comparator)
    {
        mayExist = cf !=null ;
        
        return EMPTY_COLUMN_ITERATOR;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.filter.QueryFilter#getSSTableColumnIterator(org.apache.cassandra.io.SSTableReader)
     */
    @Override
    public ColumnIterator getSSTableColumnIterator(SSTableReader sstable)
            throws IOException
    {
        if (!mayExist)
        {
            // did not found it in memtable. inspecting sstable bloom filters
            mayExist=sstable.getBloomFilter().isPresent(key);
        }
        
        return EMPTY_COLUMN_ITERATOR;
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

    private class EmptyColumnIterator extends AbstractColumnIterator
    {
        /* (non-Javadoc)
         * @see java.util.Iterator#hasNext()
         */
        @Override
        public boolean hasNext()
        {
            return false;
        }
        
        /* (non-Javadoc)
         * @see java.util.Iterator#next()
         */
        @Override
        public IColumn next()
        {
            return null;
        }
        
        /* (non-Javadoc)
         * @see org.apache.cassandra.db.filter.ColumnIterator#getColumnFamily()
         */
        @Override
        public ColumnFamily getColumnFamily()
        {
            return null;
        }
    }
}
