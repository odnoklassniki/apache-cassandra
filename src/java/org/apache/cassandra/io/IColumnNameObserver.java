/*
 * @(#) IColumnNameObserver.java
 * Created Jun 3, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.io;

import java.io.IOException;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;

/**
 * Represents some party, interested in inspecting all column names.
 *  
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public interface IColumnNameObserver
{
    /**
     * Notifies observer about seen column name, which will be a part of sstable.
     * 
     * @param key
     * @param name
     * @throws IOException
     */
    public void add(DecoratedKey<?> key, byte[] name) throws IOException;

    /**
     * Same, but notifies about column family at once. 
     * 
     * @param key
     * @param cf
     */
    void add(DecoratedKey<?> key, ColumnFamily cf);
}