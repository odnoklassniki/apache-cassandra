/*
 * @(#) DAOStatsAspect.java
 * Created Dec 23, 2010 by oleg
 * (C) ONE, SIA
 */
package odkl.cassandra.stat;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LatencyTracker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import one.log.util.LoggerUtil;

/**
 * This collects latencies of internal column family store operations
 * 
 * It uses {@link LoggerUtil#operationSuccess(Log, long, String, Object...)}
 * and {@link LoggerUtil#operationFailure(Log, long, long, String, Object...)} to
 * log method execution facts and their outcomes. Futher aggregation is performed
 * by one-log code in usual way.
 * 
 * @author Oleg Anastasyev<oa@one.lv>
 *
 */
@Aspect("perthis(cfsPointcut())")
public class StoreLatencyTrackerAspect extends SystemArchitectureAspect
{
    private final Log log= LogFactory.getLog(getClass());
    
    private final Object[] parameters = new Object[] { DatabaseDescriptor.getClusterName(), FBUtilities.getLocalAddress().getHostAddress(), null /* cf name */ };
    
    @Around("readLatencyTrackerPointcut()")
    public void readTracker(ProceedingJoinPoint join) throws Throwable
    {
        // this is inside column family store
        join.proceed(new Object[] { buildTracker("READ.STORE") });
    }
    
    @Around("writeLatencyTrackerPointcut()")
    public void writeTracker(ProceedingJoinPoint join) throws Throwable
    {
        // this is inside column family store
        join.proceed(new Object[] { buildTracker("WRITE.STORE") });
    }
    
    @After("cfsPointcut()")
    public void afterCFsInit(JoinPoint join)
    {
        ColumnFamilyStore cfs = (ColumnFamilyStore) join.getThis();
        
        String cfname = cfs.getColumnFamilyName();
        
        String table = (String) join.getArgs()[0];

        CFMetaData cfm = DatabaseDescriptor.getCFMetaData(table, cfname);
        if (cfm.domainSplit)
        {
            cfname = cfm.domainCFName;
        }

        // now we know column family name for statistics
        parameters[2] = cfname;
    }

    protected LatencyTracker buildTracker(String op)
    {
        log.debug("Initialized logging tracker for "+op+" by "+this);
        return new LoggerLatencyTracker(OP_LOGGER_NAME, op, parameters);
    }
}
