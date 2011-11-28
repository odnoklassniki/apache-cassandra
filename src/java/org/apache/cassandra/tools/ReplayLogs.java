/*
 * @(#) ReplayLogs.java
 * Created Apr 19, 2010 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Just replays all logs it founds in data directories and exits.
 * 
 * Arguments:
 *  forced - will ignore dirty information from commit log file headers. This is useful when restoring from backup.
 *  normal - will do not ignore dirty information from commit log file headers. 
 *  This is useful when you want to replay logs normally in a healthy node. 
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class ReplayLogs
{
    private static final Logger logger = Logger.getLogger(ReplayLogs.class);

    public static void main(String[] args)
    {
        // log4j
        String file = System.getProperty("storage-config") + File.separator + "log4j.properties";
        PropertyConfigurator.configure(file);
        
        if (args.length<1)
        {
            printUsage();
            System.exit(1);
        }
        
        boolean forcedMode=false;
        boolean compact = false;
        
        if ( "-forced".equals(args[0]) )
            forcedMode=true;
        else
            if ( "-forcedcompact".equals(args[0]) )
            {
                forcedMode=true;
                compact = true;
            }
            else
                if (!"-normal".equalsIgnoreCase(args[0]))
                    throw new IllegalArgumentException("You must specify either -forced or -normal as 1st argument");
        
        long timestamp = Long.MAX_VALUE;
        if (args.length>1)
        {
            timestamp = Long.parseLong(args[1]);
            logger.info("Will stop replay as soon as any column is seen with timestamp > "+timestamp);
        }
        
        // initialize keyspaces
        try {
            for (String table : DatabaseDescriptor.getTables())
            {
                if (logger.isDebugEnabled())
                    logger.debug("opening keyspace " + table);
                Table.open(table);
            }
            
            Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
            {
                
                @Override
                public void uncaughtException(Thread t, Throwable e)
                {
                    logger.error("Unexpected error",e);
                }
            });
            
            // anyway will check for compaction after logs replay applied. So this is to speedup log application.
            int maximumCompactionThreshold = CompactionManager.instance.getMaximumCompactionThreshold();
            int minimumCompactionThreshold = CompactionManager.instance.getMinimumCompactionThreshold();
            CompactionManager.instance.disableAutoCompaction();

            // replay the log if necessary and check for compaction candidates
            if (forcedMode)
                CommitLog.forcedRecover(timestamp);
            else
                CommitLog.recover(timestamp);

            if (compact)
            {
                for (ColumnFamilyStore cf : ColumnFamilyStore.all()) 
                {
                    CompactionManager.instance.submitMajor(cf);
                    logger.info("Started major compaction of "+cf.getColumnFamilyName());
                }
            } else 
            {
                // restoring autocompact values
                CompactionManager.instance.setMaximumCompactionThreshold(maximumCompactionThreshold);
                CompactionManager.instance.setMinimumCompactionThreshold(minimumCompactionThreshold);
                CompactionManager.instance.checkAllColumnFamilies();
                logger.info("Invoking minor compactions if needed...");
            }

            while (!CompactionManager.instance.waitForCompletion(60))
            {
                logger.info("... waiting for compaction manager completion. Compacted so far/total: "+CompactionManager.instance.getBytesCompacted()+"/"+CompactionManager.instance.getBytesTotalInProgress());
            }
            
            System.gc();
            System.gc();

            logger.info("Log replay completed. All replayed log files were removed from "+DatabaseDescriptor.getLogFileLocation()+".");
            System.exit(0);
        } 
        catch (IOException e) {
            logger.error("",e);
        }
    }

    /**
     * 
     */
    private static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        
        hf.printHelp("logreplay <option> maxtimestamp",new Options()
                .addOption("forced", false, "Replay all logs. Used to restore by rolling all logs to previously snap shotted data files")
                .addOption("forcedcompact", false, "Replay all logs, like -forced do and do a major compaction. ")
                .addOption("normal", false, "Apply commit logs to datafiles like cassandra does normally on startup and exit.")
                .addOption("maxtimestamp", false, "Max column timestamp value to replay commit log until (as soon as any mutation seen with column fresher than this one - replay stops).\n"+
                                                  "Beware: This is client supplied value. It can differ for different clusters. You should better ask programmer about this value.")
                )
                ;
    }
}
