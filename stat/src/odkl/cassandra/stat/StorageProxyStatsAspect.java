/*
 * @(#) StorageProxyStatsAspect.java
 * Created Nov 4, 2011 by oleg
 * (C) ONE, SIA
 */
package odkl.cassandra.stat;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageService.Verb;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LatencyTracker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

import one.log.util.LoggerUtil;

/**
 * This collects one-log statistics from StorageProxy - i.e. coordinator node perofrmance
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
@Aspect
public class StorageProxyStatsAspect extends SystemArchitectureAspect
{
    private final Log log= LogFactory.getLog(getClass());

    private final Object[] parameters = new Object[] { DatabaseDescriptor.getClusterName(), FBUtilities.getLocalAddress().getHostAddress() };
    
    /**
     * Logger of one-log to file stats to.
     * for current DAO instance.
     */
    private Log opLogger = LogFactory.getLog(OP_LOGGER_NAME);
    
    /**
     * timeout to feed into stats
     */
    private long timeout = DatabaseDescriptor.getRpcTimeout() ;
    


    @Around("proxyHintLatencyTrackerPointcut()")
    public void hintTracker(ProceedingJoinPoint join) throws Throwable
    {
        // this is inside column family store
        join.proceed(new Object[] { buildTracker("WRITE.HINTING") });
    }
    
    @Around("proxyRangeLatencyTrackerPointcut()")
    public void rangeTracker(ProceedingJoinPoint join) throws Throwable
    {
        // this is inside column family store
        join.proceed(new Object[] { buildTracker("RANGE") });
    }
    
    @Around("proxyWeakReadPointcut()")
    public Object weakRead(ProceedingJoinPoint join) throws Throwable
    {
        return collectStats(join, "READ.ONE");
    }
    
    @Around("proxyStrongReadPointcut(cl)")
    public Object strongRead(ProceedingJoinPoint join, ConsistencyLevel cl) throws Throwable
    {
        return collectStats(join, "READ."+cl);
    }
    
    @Around("proxyWriteOnePointcut()")
    public Object writeOne(ProceedingJoinPoint join) throws Throwable
    {
        return collectStats(join, "WRITE.ONE");
    }

    @Around("proxyWriteQuorumPointcut(cl)")
    public Object writeQuorum(ProceedingJoinPoint join, ConsistencyLevel cl) throws Throwable
    {
        return collectStats(join, "WRITE."+cl);
    }

    @Around("proxyRangePointcut(cl)")
    public Object range(ProceedingJoinPoint join, ConsistencyLevel cl) throws Throwable
    {
        return collectStats(join, "RANGE."+cl);
    }

    @After("proxyRRPointcut()")
    public void readRepair() throws Throwable
    {
        LoggerUtil.operationsSuccess(OP_LOGGER_NAME, 0, 1, "READ.REPAIR", parameters);
    }
    
    @After("droppedMessage(verb)")
    public void dropped(StorageService.Verb verb)
    {
        long rpcTimeout = DatabaseDescriptor.getRpcTimeout();
        
        String opName = (verb == Verb.MUTATION ? "WRITE" : verb.toString())+".STORE";
        
        LoggerUtil.operationFailure(OP_LOGGER_NAME, LoggerUtil.getMeasureStartTime() - rpcTimeout - 1, rpcTimeout, opName, parameters);
    }

    private Object collectStats(ProceedingJoinPoint join, String opName) throws Throwable
    {
        
        final long time = LoggerUtil.getMeasureStartTime();
        boolean ok=false;
        try {
            final Object result=join.proceed();
            ok=true;
            
            // method call finished successfully
            LoggerUtil.operationSuccess(OP_LOGGER_NAME, time, timeout, opName, parameters);
            return result;
        }
        finally
        {
            if (!ok)
                LoggerUtil.operationFailure(OP_LOGGER_NAME, time, timeout, opName, parameters);
        }
    }

    protected LatencyTracker buildTracker(String op)
    {
        log.debug("Initialized logging tracker for "+op+" by "+this);
        return new LoggerLatencyTracker(OP_LOGGER_NAME, op, parameters);
    }
}
