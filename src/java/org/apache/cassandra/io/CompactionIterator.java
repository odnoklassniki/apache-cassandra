package org.apache.cassandra.io;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.FSWriteError;
import org.apache.cassandra.db.ObservingColumnFamilyDeserializer;
import org.apache.cassandra.io.util.DataInputSink;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ReducingIterator;
import org.apache.commons.collections.iterators.CollatingIterator;
import org.apache.log4j.Logger;

public abstract class CompactionIterator extends ReducingIterator<IteratingRow, CompactionIterator.CompactedRow> implements Closeable
{
    private static Logger logger = Logger.getLogger(CompactionIterator.class);

    protected static final int FILE_BUFFER_SIZE = 1024 * 1024;

    private final List<IteratingRow> rows = new ArrayList<IteratingRow>();
    protected final ColumnFamilyStore cfs;
    private final int gcBefore;
    private final boolean major;
    
    // MM:listens for all column names seen by this iterator
    private ObservingColumnFamilyDeserializer observingDeserializer;
    private final boolean skipBloom;
    
    private IColumnNameObserver columnNameObserver;

    private long totalBytes;
    private long bytesRead;
    private long row;

    public CompactionIterator(ColumnFamilyStore cfs, Iterable<SSTableReader> sstables, int gcBefore, boolean major) throws IOException
    {
        this(cfs, getCollatingIterator(sstables), gcBefore, major);
    }

    @SuppressWarnings("unchecked")
    protected CompactionIterator(ColumnFamilyStore cfs, Iterator iter, int gcBefore, boolean major)
    {
        super(iter);
        row = 0;
        totalBytes = bytesRead = 0;
        for (SSTableScanner scanner : getScanners())
        {
            totalBytes += scanner.getFileLength();
        }
        this.cfs = cfs;
        this.gcBefore = gcBefore;
        this.major = major;
        this.skipBloom = cfs.metadata.bloomColumns;
    }

    protected static CollatingIterator getCollatingIterator(Iterable<SSTableReader> sstables) throws IOException
    {
        CollatingIterator iter = FBUtilities.<IteratingRow>getCollatingIterator();
        for (SSTableReader sstable : sstables)
        {
            iter.addIterator(sstable.getDirectScanner(FILE_BUFFER_SIZE));
        }
        return iter;
    }

    @Override
    protected boolean isEqual(IteratingRow o1, IteratingRow o2)
    {
        return o1.getKey().equals(o2.getKey());
    }

    public void reduce(IteratingRow current)
    {
        rows.add(current);
    }
    
    protected abstract CompactedRow startRowWrite(DecoratedKey key, int cfSize);
    
    protected abstract void finishRowWrite(CompactedRow compactedRow);

    protected CompactedRow getReduced()
    {
        assert rows.size() > 0;
        
        IteratingRow row0 = rows.get(0);
        DecoratedKey key = row0.getKey();
        CompactedRow compactedRow = null;

        Set<SSTable> sstables = new HashSet<SSTable>();
        for (IteratingRow row : rows)
            sstables.add(row.sstable);
        boolean shouldPurge = major || !cfs.isKeyInRemainingSSTables(key, sstables);

        try
        {
            if (rows.size() > 1 || shouldPurge)
            {
                ColumnFamily cf = null;
                for (IteratingRow row : rows)
                {
                    ColumnFamily thisCF;
                    try
                    {
                        thisCF = row.getColumnFamily();
                    }
                    catch (IOException e)
                    {
                        logger.error("Skipping row " + key + " in " + row.getPath(), e);
                        continue;
                    }
                    if (cf == null)
                    {
                        cf = thisCF;
                    }
                    else
                    {
                        cf.addAll(thisCF);
                    }
                }
                
                ColumnFamily cfPurged = shouldPurge ? ColumnFamilyStore.removeDeleted(cf, gcBefore) : cf;
                if (cfPurged == null)
                    return null;

                int cfSize = ColumnFamily.serializer().serializeWithIndexesSize(cfPurged, skipBloom);
                compactedRow = startRowWrite(key,cfSize);
                ColumnFamily.serializer().serializeWithIndexes(cfPurged, compactedRow.buffer, skipBloom);
                
                finishRowWrite(compactedRow);

                if (columnNameObserver!=null)
                    columnNameObserver.add(key,cfPurged);
            }
            else
            {
                assert rows.size() == 1;
                try
                {
                    compactedRow = startRowWrite( key, row0.getDataSize() );
                    
                    assert columnNameObserver == null || !this.skipBloom;
                    
                    if (columnNameObserver==null)
                    {
                        row0.echoData(compactedRow.buffer); // no processing neccessary. just dump it
                    } else
                    {
                        observingDeserializer.deserialize(key, new DataInputSink(row0.getDataInput(), compactedRow.buffer));
                    }

                    finishRowWrite(compactedRow);
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e);
                }
            }
        }
        finally
        {
            rows.clear();
            if ((row++ % 1000) == 0)
            {
                bytesRead = 0;
                for (SSTableScanner scanner : getScanners())
                {
                    bytesRead += scanner.getFilePointer();
                }
            }
        }
        
        assert compactedRow != null;
        
        return compactedRow;
    }

    public void close() throws IOException
    {
        for (SSTableScanner scanner : getScanners())
        {
            scanner.close();
        }
    }

    protected Iterable<SSTableScanner> getScanners()
    {
        return ((CollatingIterator)source).getIterators();
    }
    
    /**
     * @param columnNameObserver the columnNameObserver to set
     */
    protected void setColumnNameObserver(IColumnNameObserver columnNameObserver)
    {
        assert row == 0;
        assert this.skipBloom;

        this.columnNameObserver = columnNameObserver;
        this.observingDeserializer = new ObservingColumnFamilyDeserializer(columnNameObserver);
    }
    
    public long getTotalBytes()
    {
        return totalBytes;
    }

    public long getBytesRead()
    {
        return bytesRead;
    }
    
    public boolean isMajor()
    {
        return major;
    }
    
    /**
     * @return currently read rows count
     */
    public long getRow()
    {
        return row;
    }

    public static class CompactedRow
    {
        public final DecoratedKey key;
        public final DataOutput buffer;
        public final long rowPosition;
        public long  rowSize;

        public CompactedRow(DecoratedKey key, DataOutput buffer, long position)
        {
            this.key = key;
            this.buffer = buffer;
            
            this.rowPosition = position;
        }
        
        /**
         * @param rowSize the rowSize to set
         */
        public void setRowSize(long rowSize)
        {
            this.rowSize = rowSize;
        }
    }
}
