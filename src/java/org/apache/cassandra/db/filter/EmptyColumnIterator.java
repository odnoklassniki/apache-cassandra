package org.apache.cassandra.db.filter;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;

class EmptyColumnIterator extends AbstractColumnIterator
{
    private final ColumnFamily cf;

    /**
     * @param tableName
     * @param columnFamilyName
     */
    public EmptyColumnIterator(String tableName, String columnFamilyName)
    {
        cf =ColumnFamily.create(tableName, columnFamilyName);
    }

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
        return cf;
    }
}
