/*
 * @(#) ReplayLogs.java
 * Created Apr 19, 2010 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.thrift.CassandraDaemon;
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
 * @author Oleg Anastasyev<oa@one.lv>
 *
 */
public class ReplayLogs
{
    private static Logger logger = Logger.getLogger(ReplayLogs.class);

    public static void main(String[] args)
    {
        // log4j
        String file = System.getProperty("storage-config") + File.separator + "log4j.properties";
        PropertyConfigurator.configure(file);
        
        boolean forcedMode=false;
        if ( "forced".equals(args[0]) )
            forcedMode=true;
        else
            if (!"normal".equalsIgnoreCase(args[0]))
                throw new IllegalArgumentException("You must specify either forced or normal as 1st argument");
        
        // initialize keyspaces
        try {
            for (String table : DatabaseDescriptor.getTables())
            {
                if (logger.isDebugEnabled())
                    logger.debug("opening keyspace " + table);
                Table.open(table);
            }

            // replay the log if necessary and check for compaction candidates
            if (forcedMode)
                CommitLog.forcedRecover();
            else
                CommitLog.recover();

            CompactionManager.instance.checkAllColumnFamilies();
            
            while (!CompactionManager.instance.waitForCompletion(60))
            {
                logger.info("... waiting for compaction manager completion");
            }
            
            System.exit(0);
        } 
        catch (IOException e) {
            logger.error("",e);
        }
    }
}
