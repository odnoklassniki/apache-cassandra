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
import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

/**
 * Runs on every node in cluster. Removes archived commit logs older than X days.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class CleanArchivedLogsTask implements MaintenanceTask, Runnable
{
    private final Logger logger = Logger.getLogger(MaintenanceTaskManager.class);

    private long lastSuccessfulWindowMillis = 0l;
    
    private MaintenanceContext currentCtx ;
    
    private int daysOld = 3;
    
    public CleanArchivedLogsTask(int daysold)
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
        return DatabaseDescriptor.isLogArchiveActive() && ctx.startedMillis()>lastSuccessfulWindowMillis ? this : null;
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run()
    {
        String logArchDir = DatabaseDescriptor.getLogArchiveDestination();
        
        final long earliestLogfile = earliestLogFile();
         
        File[] obsoleteLogs = new File(logArchDir).listFiles(new FileFilter()
        {

            @Override
            public boolean accept(File file)
            {
                try {
                    String name = file.getName();
                    return name.matches("CommitLog-\\d+.log") && Long.parseLong(name.split("-|[.]")[1])<=earliestLogfile;
                } catch (Exception e)
                {
                    logger.warn("Commit log name is not understood and skipped: "+file);
                    return false;
                }
            }

        });

        if (obsoleteLogs!=null && obsoleteLogs.length>0)
        {
            logger.info("Deleting obsolete archived logs (older than "+daysOld+" days): "+ArrayUtils.toString(obsoleteLogs));

            try {
                FileUtils.delete(obsoleteLogs);
            } catch (IOException e) 
            {
                logger.error("Cannot remove archived logs",e);
            }    
        }        

        lastSuccessfulWindowMillis = currentCtx.startedMillis();
    }
    
    /**
     * @return
     */
    private long earliestLogFile()
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
        return "Cleaning archived log files older than "+daysOld+" days old";
    }
}
