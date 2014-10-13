/*
 * @(#) ClusterSnapshotTask.java
 * Created Sep 23, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.maint;

import java.io.IOException;
import java.util.Set;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;

/**
 * Run only by 0th node in ring - launches global cluster wide snapshot on a cluster, like clustertool global_snapshot do
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class ClusterSnapshotTask implements MaintenanceTask, Runnable
{
    private final Logger logger = Logger.getLogger(MaintenanceTaskManager.class);

    private long lastSuccessfulWindowMillis = 0l;
    
    private MaintenanceContext currentCtx ;
    
    protected final String tagname;
    protected final String cfRegExp;

    /*example of config: night(tablename1,tablename2)*/
    public ClusterSnapshotTask(String config)
    {
        int openBraceIndex = config.indexOf('(');
        if (openBraceIndex != -1)
        {
            tagname = config.substring(0, openBraceIndex);
            int closeBraceIndex = config.lastIndexOf(')');
            if (closeBraceIndex != -1 && closeBraceIndex > openBraceIndex)
            {
                cfRegExp = config.substring(openBraceIndex + 1, closeBraceIndex);
            } else {
                logger.error("Can't parse table names from snapshot config string");
                cfRegExp = null;
            }
        } else {
            tagname = config;
            cfRegExp = null;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.maint.MaintenanceTask#maybeRun(org.apache.cassandra.maint.MaintenanceContext)
     */
    @Override
    public Runnable maybeRun(MaintenanceContext ctx)
    {
        this.currentCtx = ctx;
        return ctx.ringPosition() == 0 && ctx.startedMillis()>lastSuccessfulWindowMillis ? this : null;
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run()
    {
        Set<String> liveNodes = StorageService.instance.getLiveNodes();
        liveNodes.remove(FBUtilities.getLocalAddress().getHostAddress());

        try
        {
            if (cfRegExp == null)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Take snapshot for all tables");
                }
                StorageService.instance.takeAllSnapshot(tagname);
            } else {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Take snapshot for all tables for column families according regexp: " + cfRegExp);
                }

                StorageService.instance.takeAllSnapshot(cfRegExp, tagname);
            }

            if (logger.isDebugEnabled())
            {
                logger.debug("local snapshot taken");
            }
        }
        catch (IOException e)
        {
            logger.error("local snapshot FAILED: ", e);
        }

        for (String liveNode : liveNodes)
        {
            try
            {
                NodeProbe probe = new NodeProbe( liveNode );

                if (cfRegExp == null)
                {
                    probe.takeSnapshot(tagname);
                } else {

                    probe.takeSnapshot(cfRegExp, tagname);
                }

                logger.debug(liveNode + " snapshot taken");
            }
            catch (Exception e)
            {
                logger.error(liveNode + " snapshot FAILED: ", e);
            }
        }

        lastSuccessfulWindowMillis = currentCtx.startedMillis();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return "Cluster snapshot with tag "+tagname;
    }
}
