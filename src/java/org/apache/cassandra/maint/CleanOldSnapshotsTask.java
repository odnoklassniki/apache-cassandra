/*
 * @(#) ClusterSnapshotTask.java
 * Created Sep 23, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.maint;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Calendar;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

/**
 * Runs on every node in cluster. Removes snapshots older than X days.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class CleanOldSnapshotsTask implements MaintenanceTask, Runnable
{
    private final Logger logger = Logger.getLogger(MaintenanceTaskManager.class);

    private long lastSuccessfulWindowMillis = 0l;
    
    private MaintenanceContext currentCtx ;
    
    private int daysOld = 3;
    
    public CleanOldSnapshotsTask(int daysold)
    {
        this.daysOld = daysold;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.maint.MaintenanceTask#maybeRun(org.apache.cassandra.maint.MaintenanceContext)
     */
    @Override
    public Runnable maybeRun(MaintenanceContext ctx)
    {
        this.currentCtx = ctx;
        return ctx.startedMillis()>lastSuccessfulWindowMillis ? this : null;
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run()
    {
        String[] dataFileLocations = DatabaseDescriptor.getAllDataFileLocations();
        
        final long earliestSnapshot = earliestSnapshotMillis();
         
        for (String dataDir : dataFileLocations) 
        {
            for (Table table : Table.all()) 
            {
                String snapshotDir = Table.getSnapshotPath(dataDir, table.name, "");
                
                File[] obsoleteSnapshotDirs = new File(snapshotDir).listFiles(new FileFilter()
                {
                    
                    @Override
                    public boolean accept(File dir)
                    {
                        try {
                            return dir.isDirectory() && Long.parseLong(dir.getName().split("-")[0])<=earliestSnapshot;
                        } catch (Exception e)
                        {
                            logger.warn("Snapshot directory is not understood and skipped: "+dir);
                            return false;
                        }
                    }

                });
                
                if (obsoleteSnapshotDirs!=null && obsoleteSnapshotDirs.length>0)
                {
                    logger.info("Deleting obsolete snapshot directories (older than "+daysOld+" days): "+ArrayUtils.toString(obsoleteSnapshotDirs));

                    for (File file : obsoleteSnapshotDirs) {
                        try {
                            FileUtils.deleteDir(file);
                        } catch (IOException e) {
                            logger.error("Cannot remove "+file,e);
                        }
                    }
                }
            }
        }
        

        lastSuccessfulWindowMillis = currentCtx.startedMillis();
    }
    
    /**
     * @return
     */
    private long earliestSnapshotMillis()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(currentCtx.startedMillis());
        calendar.add(Calendar.DAY_OF_YEAR, -daysOld);
        
        return calendar.getTimeInMillis();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return "Cleaning snapshots older than "+daysOld+" days";
    }
}
