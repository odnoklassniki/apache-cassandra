/*
 * @(#) ColumnFamilyObserver.java
 * Created Jun 6, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.io.IColumnNameObserver;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.io.IndexHelper;

/**
 * This deserializes binary stream and feeds columnnames directly to {@link IColumnNameObserver}
 * without creating a lot of temporary objects, which deserializing to ColumnFamily will result in. 
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 * @see ColumnFamilySerializer
 *
 */
public class ObservingColumnFamilyDeserializer
{
    private final IColumnNameObserver columnNameObserver;

    /**
     * @param columnNameObserver
     */
    public ObservingColumnFamilyDeserializer(IColumnNameObserver columnNameObserver)
    {
        this.columnNameObserver = columnNameObserver;
    }

    public void deserialize(DecoratedKey<?> key, DataInput dis) throws IOException
    {
        IndexHelper.skipBloomFilter(dis);
        IndexHelper.skipIndex(dis);
        
        dis.readInt(); dis.readLong(); // skip over CF delete timestamps
        
        int size = dis.readInt();
        for (int i = 0; i < size; ++i)
        {
            deserializeColumn(key,dis);
        }
    }

    /**
     * @param dis
     * @throws IOException 
     * @see ColumnSerializer#deserialize(DataInput)
     */
    private void deserializeColumn(DecoratedKey<?> key, DataInput dis) throws IOException
    {
        byte[] name = ColumnSerializer.readName(dis);
        boolean delete = dis.readBoolean();
        long ts = dis.readLong();
        int length = dis.readInt();
        if (length < 0)
        {
            throw new IOException("Corrupt (negative) value length encountered");
        }
        if (length > 0)
        {
            dis.skipBytes(length); // we are not interested in value
        }
        
        columnNameObserver.add(key, name);
        
    }
    

}
