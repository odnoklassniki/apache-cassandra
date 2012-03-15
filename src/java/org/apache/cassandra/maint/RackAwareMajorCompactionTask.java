/*
 * @(#) RackAwareMajorCompactionTask.java
 * Created Mar 15, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.maint;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.RackAwareOdklEvenStrategy;
import org.apache.cassandra.service.StorageService;

/**
 * This is rack aware verision of {@link MajorCompactionTask}. It will compact nodes from single racks, leaving
 * leaving spare configured number of racks.
 * 
 * This works only when {@link RackAwareOdklEvenStrategy} is configured in cassandra
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class RackAwareMajorCompactionTask extends MajorCompactionTask
{
    
    private final RackAwareOdklEvenStrategy strategy;

    /**
     * @param leaveSpare
     */
    public RackAwareMajorCompactionTask(int leaveSpare)
    {
        super(leaveSpare);
        
        this.strategy = (RackAwareOdklEvenStrategy) StorageService.instance.getReplicationStrategy(DatabaseDescriptor.getNonSystemTables().get(0));
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.maint.MajorCompactionTask#mayCompactToday(org.apache.cassandra.maint.MaintenanceContext)
     */
    @Override
    protected boolean mayCompactToday(MaintenanceContext ctx)
    {
        if (StorageService.instance.isBootstrapMode())
        {
            logger.warn("This node is currently in bootstrap mode. Will not compact");

            return false;
        }
        
        int dayOfEpoch = (int) (ctx.startedMillis() /1000 / 3600 / 24);
        
        ArrayList<String> racks = new ArrayList<String>(this.strategy.ringRacks(StorageService.instance.getTokenMetadata(), StorageService.instance.getTokenMetadata().sortedTokens()));
        Collections.sort(racks);
        
        int rackCount = racks.size();
        
        String myRack = strategy.myRack();
        
        if (myRack == null)
        {
            logger.error("Cannot determine my rack. Will not compact");
            return false;
        }
        
        int rackPosition = racks.indexOf(myRack);
        int num = rackCount - this.leaveSpareInRange;
        
        for (int n = dayOfEpoch % rackCount; num-->0; n = (n+1) % rackCount )
        {
            if ( n == rackPosition )
            {
                // checking all endpoints from other racks are live before going to compaction
                for (InetAddress endp : Gossiper.instance.getUnreachableMembers() )
                {
                    if ( myRack.equals( strategy.getRack(endp) ) )
                        continue;
                    
                    logger.warn("Endpoint "+endp+" from other rack is dead. Will not compact");
                    return false;
                }
                
                return true;
            }
        }
        
        return false;
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.maint.MajorCompactionTask#toString()
     */
    @Override
    public String toString()
    {
        return String.format("Rack aware major compaction with %d racks spare",this.leaveSpareInRange);
    }
}
