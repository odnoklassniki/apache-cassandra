/*
 * @(#) ClusterSnapshotTask.java
 * Created Sep 23, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.maint;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;

/**
 * Runs major compactions during maint window, trying to estimate time of compaction not ging beyond it.
 * Ensures that X nodes from all serving every single range is not compacting at all.
 * 
 * This is rack unaware version.
 * 
 * Compactions are performed on tables with max number of sstables first.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class MajorCompactionTask implements MaintenanceTask
{
    protected final Logger logger = Logger.getLogger(MaintenanceTaskManager.class);

    private long lastSuccessfulWindowMillis = 0l;
    
    /**
     * CFs major compacted during current window to avoid 
     * compacting same CF twice
     */
    private HashSet<String> recentlyCompacted = new HashSet<String>();
    
    /**
     * How many nodes to leave not compacting in range
     */
    protected final int leaveSpareInRange;
    
    public MajorCompactionTask(int leaveSpare)
    {
        this.leaveSpareInRange = leaveSpare;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.maint.MaintenanceTask#maybeRun(org.apache.cassandra.maint.MaintenanceContext)
     */
    @Override
    public Runnable maybeRun(MaintenanceContext ctx)
    {
        if (lastSuccessfulWindowMillis!=ctx.startedMillis())
            recentlyCompacted.clear();

        if (mayCompactToday(ctx))
        {
            
            // selecting which CF we compact this time by looking at cumulative time of read requests
            // Reads * Read Latency
            ColumnFamilyStore compactionCandidate = null;
            
            for (String table : DatabaseDescriptor.getTables())
            {
                try {
                    for (ColumnFamilyStore cfs : Table.open(table).getColumnFamilyStores()) 
                    {
                        if (cfs.getLiveSSTableCount()<2 || recentlyCompacted.contains(table+'.'+cfs.getColumnFamilyName()))
                            continue;
                        
                        double cumulativeRead = ((double) cfs.getReadCount()) * ((double) cfs.getTotalReadLatencyMicros());
                        
                        if (compactionCandidate==null || cumulativeRead> ( (double) compactionCandidate.getReadCount() * (double) compactionCandidate.getTotalReadLatencyMicros() ) )
                        {
                            // estimating time to compact it to do not get out of maint window too much
                            // speed of compaction is about 1G per minute
                            if ( cfs.getLiveDiskSpaceUsed()/1000000 < ctx.millisLeft()/60 )
                                compactionCandidate = cfs;
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            
            lastSuccessfulWindowMillis = ctx.startedMillis();

            if (compactionCandidate!=null)
            {
                
                recentlyCompacted.add(compactionCandidate.getTable().name+'.'+compactionCandidate.getColumnFamilyName());
                
                return new CompactionTask(compactionCandidate);
            }
            
        }
        
        lastSuccessfulWindowMillis = ctx.startedMillis();
        
        return null;
    }
    
    /**
     * Will not allow to compact, if:
     * 
     * 1. There are dead endpoints range of any of this endpoint's range
     * 2. There are less than #leaveSpareInRange endpoints
     * 3. This node is bootstrapping
     * 
     * @param ctx
     * @return
     */
    protected boolean mayCompactToday(MaintenanceContext ctx)
    {
        if (StorageService.instance.isBootstrapMode())
            return false;
        
        int dayOfEpoch = (int) (ctx.startedMillis() /1000 / 3600 / 24);
        
        int rf = DatabaseDescriptor.getReplicationFactor(DatabaseDescriptor.getNonSystemTables().get(0));

        List<Range> ringRanges = StorageService.instance.getAllRanges();
        
        int n = dayOfEpoch % rf; // so every Nth range in ring must compact this day
        
        Map<Range, Collection<InetAddress>> rangeToAddressMap = StorageService.instance.getRangeToAddressMap(DatabaseDescriptor.getNonSystemTables().get(0));
        
        for ( int i = n; i<ringRanges.size(); i+=rf)
        {
            Range compactingRange = ringRanges.get(i);
            
            if (StorageService.instance.getLocalPrimaryRange().equals(compactingRange))
                return true; // may primary range is eligible for compaction

            List<InetAddress> endpoints = new ArrayList<InetAddress>( rangeToAddressMap.get(compactingRange) );
            int replicaPosition=endpoints.indexOf(FBUtilities.getLocalAddress());
            if (replicaPosition>0)
            {
                if ( rf - leaveSpareInRange - replicaPosition > 0 )
                {
                    // checking all nodes are alive
                    for (InetAddress inetAddress : endpoints) {
                        if (!FailureDetector.instance.isAlive(inetAddress))
                        {
                            logger.info("Major compaction maitenance task is cancelled, because "+inetAddress+" is dead");
                            return false;
                        }
                    }
                    
                    return true;
                } else
                {
                    return false;
                }
            }
        }
        
        logger.info("Cannot determine compacting range for this node. Is it bootstrapping ?");
        
        return false;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return String.format(" Major compaction with %d endpoint(s) spare in range ",this.leaveSpareInRange);
    }

    /**
     * @author Oleg Anastasyev<oa@hq.one.lv>
     *
     */
    private class CompactionTask implements Runnable
    {

        private final ColumnFamilyStore store;

        /**
         * @param compactionCandidate
         */
        public CompactionTask(ColumnFamilyStore compactionCandidate)
        {
            this.store = compactionCandidate;
        }

        /* (non-Javadoc)
         * @see java.lang.Runnable#run()
         */
        @Override
        public void run()
        {
            Future<?> future = CompactionManager.instance.submitMajor(store);
            
            if (future!=null)
                try {
                    future.get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
        }

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString()
        {
            return String.format("Major compaction maintenance of %s with read count = %d, read latency=%01.3f (%01.3f recent) ms, sstable count=%d",
                    store.getColumnFamilyName(),
                    store.getReadCount(),
                    store.getTotalReadLatencyMicros() / 1000f,
                    store.getRecentReadLatencyMicros() / 1000,
                    store.getLiveSSTableCount()
                    );
        }
    }

    

}
