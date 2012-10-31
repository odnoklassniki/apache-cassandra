/*
 * @(#) IRowProcessor.java
 * Created May 25, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.db.proc;

import java.util.Properties;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;

/**
 * Generic interface for data processors, which manipulate data by entire row.
 * 
 * One instance of row processor is called from single thread and is used to process rows of single column family store.
 * 
 * This processing could happen during:
 * 1. (Major) Compaction. If shouldProcessIncomplete and shouldProcessUnchanged are both false processor is called only when major compacting,
 *    so your processor sees rows with full column set (data currently in memtable is not normally available. You can still get it by yourself 
 *    if you need it)
 * 2. Minor compaction. If shouldProcessIncomplete = false and shouldProcessUnchanged=true processor is also called for all rows, which cassandra can
 *    guarantee it has complete set of columns. Note, that no guarantee is set you will get ALL of complete rows - only majority of them.
 *    This processing implies additional costs, because normally cassandra do not process unchanged rows - it just copies them byte by byte to target sstable.
 * 3. Minor compaction and memtable flush, if shouldProcessIncomplete is set to true. This is the only way to get control when memtable is flushing. Such a processor
 *    will be called even if no warranty can be set on completeness of row's columns passed to {@link IRowProcessor#process(DecoratedKey, ColumnFamily, boolean)} method.
 *    incomplete will be passed true.
 *    
 * During processing you can do whatever you want with ColumnFamily - all your changes will be placed to target sstable after you made them. You can signal
 * skipping the whole row over by returning null. Row will not hit target sstable and disappear forever.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public interface IRowProcessor
{
    /**
     * Called when configuring instance according to config in storage-conf.xml.
     * 
     * @param config parsed from attributes of RowProcessor xml node as specified in file
     */
    void setConfiguration(Properties config);
    
    /**
     * @param cfs sets column family store this row processor instance will be used to process rows of. 
     */
    void setColumnFamilyStore(ColumnFamilyStore cfs);
    
    /**
     * Processes a row. 
     * 
     * @param key row key being processed
     * @param columns data to process. read and write it in place. == null if data was just discarded by one of processors in chain
     * @param incomplete false - columns contain all columns of the row - guaranteed. true - incomplete set of columns (during minor compation for example) or no warranty can be set
     * 
     * @return null - whole row data should be discarded or data to be written to sstable
     */
    ColumnFamily process(DecoratedKey key,ColumnFamily columns, boolean incomplete);

    /**
     * @return true if even unchanged rows should be deserialized and processed by this processor. Unneeded deserialization will happen,
     *         because normally these rows are just written to target file like byte stream.
     */
    boolean shouldProcessUnchanged();

    /**
     * @return true, if even incomplete sets of row columns should be processed. This implies processing on memtable flush as well.
     */
    boolean shouldProcessIncomplete();

    /**
     * @return true, if even empty sets of row columns should be processed.
     */
    boolean shouldProcessEmpty();
}
