/*
 * @(#) CompositeColumnFamilyStore.java
 * Created Aug 18, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import com.sun.jmx.snmp.tasks.Task;

/**
 * This is just mbean proxy to column family mbeans, split by domain, to make their management simpler.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 */
public class CompositeColumnFamilyStore implements ColumnFamilyStoreMBean
{
    private String table;
    private String compositeName;

    private ArrayList<String> childStores=new ArrayList<String>();
    
    interface Task<V>
    {
        boolean process(ColumnFamilyStore cfs) throws IOException;
        V result();
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getColumnFamilyName()
     */
    @Override
    public String getColumnFamilyName()
    {
        return compositeName;
    }
    
    private <V> V traverse(Task<V> task) throws IOException
    {
        Table t = Table.open(table);
        for (String cfName : childStores) {
            if ( !task.process(t.getColumnFamilyStore(cfName)) )
                break;
        }
        
        return task.result();
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getMemtableDataSize()
     */
    @Override
    public int getMemtableDataSize()
    {
        try {
            return traverse(new Task<Integer>()
            {
                int r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getMemtableDataSize();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Integer result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getMemtableColumnsCount()
     */
    @Override
    public int getMemtableColumnsCount()
    {
        try {
            return traverse(new Task<Integer>()
            {
                int r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getMemtableColumnsCount();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Integer result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getMemtableSwitchCount()
     */
    @Override
    public int getMemtableSwitchCount()
    {
        try {
            return traverse(new Task<Integer>()
            {
                int r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getMemtableSwitchCount();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Integer result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#forceFlush()
     */
    @Override
    public Object forceFlush() throws IOException
    {
        traverse(new Task<Void>()
                {
            /* (non-Javadoc)
             * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
             */
            @Override
            public boolean process(ColumnFamilyStore cfs) throws IOException
            {
                cfs.forceFlush();
                return true;
            }

            /* (non-Javadoc)
             * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
             */
            @Override
            public Void result()
            {
                return null;
            }
                });

        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getReadCount()
     */
    @Override
    public long getReadCount()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getReadCount();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getTotalReadLatencyMicros()
     */
    @Override
    public long getTotalReadLatencyMicros()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                int c=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getTotalReadLatencyMicros();
                    c++;
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    if (c==0) return 0l;
                    
                    return r/c;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getLifetimeReadLatencyHistogramMicros()
     */
    @Override
    public long[] getLifetimeReadLatencyHistogramMicros()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getRecentReadLatencyHistogramMicros()
     */
    @Override
    public long[] getRecentReadLatencyHistogramMicros()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getRecentReadLatencyMicros()
     */
    @Override
    public double getRecentReadLatencyMicros()
    {
        try {
            return traverse(new Task<Double>()
            {
                double r=0;
                int c=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getTotalReadLatencyMicros();
                    c++;
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Double result()
                {
                    if (c==0) return 0.0;
                    
                    return r/c;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getWriteCount()
     */
    @Override
    public long getWriteCount()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getWriteCount();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getTotalWriteLatencyMicros()
     */
    @Override
    public long getTotalWriteLatencyMicros()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getLifetimeWriteLatencyHistogramMicros()
     */
    @Override
    public long[] getLifetimeWriteLatencyHistogramMicros()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getRecentWriteLatencyHistogramMicros()
     */
    @Override
    public long[] getRecentWriteLatencyHistogramMicros()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getRecentWriteLatencyMicros()
     */
    @Override
    public double getRecentWriteLatencyMicros()
    {
        try {
            return traverse(new Task<Double>()
            {
                double r=0;
                int c=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    double size = cfs.getRecentWriteLatencyMicros();
                    if (size>0)
                    {
                        r+=size;
                        c++;
                    }
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Double result()
                {
                    if (c==0) return 0.0;
                    
                    return r/c;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getPendingTasks()
     */
    @Override
    public int getPendingTasks()
    {
        try {
            return traverse(new Task<Integer>()
            {
                int r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getPendingTasks();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Integer result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getLiveSSTableCount()
     */
    @Override
    public int getLiveSSTableCount()
    {
        try {
            return traverse(new Task<Integer>()
            {
                int r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getLiveSSTableCount();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Integer result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getLiveDiskSpaceUsed()
     */
    @Override
    public long getLiveDiskSpaceUsed()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getLiveDiskSpaceUsed();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getTotalDiskSpaceUsed()
     */
    @Override
    public long getTotalDiskSpaceUsed()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getTotalDiskSpaceUsed();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#forceMajorCompaction()
     */
    @Override
    public void forceMajorCompaction()
    {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#invalidateRowCache()
     */
    @Override
    public void invalidateRowCache()
    {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getMinRowCompactedSize()
     */
    @Override
    public long getMinRowCompactedSize()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r= Math.min(r, cfs.getMinRowCompactedSize() );
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getMaxRowCompactedSize()
     */
    @Override
    public long getMaxRowCompactedSize()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r= Math.max(r, cfs.getMinRowCompactedSize() );
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getMeanRowCompactedSize()
     */
    @Override
    public long getMeanRowCompactedSize()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                int c=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    long size = cfs.getMeanRowCompactedSize();
                    if (size>0)
                    {
                        r+=size;
                        c++;
                    }
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    if (c==0) return 0l;
                    
                    return r/c;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getBloomFilterFalsePositives()
     */
    @Override
    public long getBloomFilterFalsePositives()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getBloomFilterFalsePositives();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getRecentBloomFilterFalsePositives()
     */
    @Override
    public long getRecentBloomFilterFalsePositives()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getRecentBloomFilterFalsePositives();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getBloomFilterFalseRatio()
     */
    @Override
    public double getBloomFilterFalseRatio()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getRecentBloomFilterFalseRatio()
     */
    @Override
    public double getRecentBloomFilterFalseRatio()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#estimateKeys()
     */
    @Override
    public long estimateKeys()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.estimateKeys();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getRecentBloomFilterNegatives()
     */
    @Override
    public long getRecentBloomFilterNegatives()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getRecentBloomFilterNegatives();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getBloomFilterNegatives()
     */
    @Override
    public long getBloomFilterNegatives()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getBloomFilterNegatives();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getRecentBloomFilterColumnNegativeRatio()
     */
    @Override
    public double getRecentBloomFilterColumnNegativeRatio()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getRecentBloomFilterColumnReads()
     */
    @Override
    public long getRecentBloomFilterColumnReads()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getRecentBloomFilterColumnReads();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getBloomFilterColumnReads()
     */
    @Override
    public long getBloomFilterColumnReads()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getBloomFilterColumnReads();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getRecentBloomFilterColumnNegatives()
     */
    @Override
    public long getRecentBloomFilterColumnNegatives()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getRecentBloomFilterColumnNegatives();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getBloomFilterColumnNegatives()
     */
    @Override
    public long getBloomFilterColumnNegatives()
    {
        try {
            return traverse(new Task<Long>()
            {
                long r=0;
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#process(org.apache.cassandra.db.ColumnFamilyStore)
                 */
                @Override
                public boolean process(ColumnFamilyStore cfs)
                {
                    r+=cfs.getBloomFilterColumnNegatives();
                    return true;
                }
                
                /* (non-Javadoc)
                 * @see org.apache.cassandra.db.CompositeColumnFamilyStore.Task#result()
                 */
                @Override
                public Long result()
                {
                    return r;
                }
            });
        } catch (IOException e) {
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.ColumnFamilyStoreMBean#getRecentBloomFilterNegativeRatio()
     */
    @Override
    public double getRecentBloomFilterNegativeRatio()
    {
        // TODO Auto-generated method stub
        return 0;
    }

}
