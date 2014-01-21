/*
 * @(#) StorageProxyStatsAspect.java
 * Created Nov 4, 2011 by oleg
 * (C) ONE, SIA
 */
package odkl.cassandra.stat;

import java.net.InetAddress;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import one.log.util.LoggerUtil;

/**
 * This collects one-log statistics from HintedHandoff manager - i.e. 
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
@Aspect
public class HintedHandoffStatsAspect extends SystemArchitectureAspect
{

    /**
     * timeout to feed into stats
     */
    private long timeout = DatabaseDescriptor.getRpcTimeout();
    
    
    @Around("hintStorePointcut(endpoint)")
    public Object hintStore(ProceedingJoinPoint join, InetAddress endpoint) throws Throwable
    {
        return collectStats(join, "HINT.STORE", endpoint);
    }
    
    @Around("hintDeliveryPointcut(endpoint)")
    public Object hintDelivery(ProceedingJoinPoint join, InetAddress endpoint) throws Throwable
    {
        return collectStats(join, "HINT.DELIVERY", endpoint);
    }
    
    private Object collectStats(ProceedingJoinPoint join, String opName, InetAddress endpoint) throws Throwable
    {
        
        final long time = LoggerUtil.getMeasureStartTime();
        boolean ok = false;
        try {
            final Object result = join.proceed();

            // if return is boolean and it is false - this is unsuccesful opration. see deliverHint & sendMessage
            ok = result != Boolean.FALSE;
            
            // method call finished successfully
            return result;
        }
        finally
        {
            if (ok)
                LoggerUtil.operationSuccess(OP_LOGGER_NAME, time, timeout, opName, DatabaseDescriptor.getClusterName(), endpoint.getHostAddress());
            else
                LoggerUtil.operationFailure(OP_LOGGER_NAME, time, timeout, opName, DatabaseDescriptor.getClusterName(), endpoint.getHostAddress());
        }
    }

    
}
