/*
 * @(#) LoggerLatencyTracker.java
 * Created Nov 4, 2011 by oleg
 * (C) ONE, SIA
 */
package odkl.cassandra.stat;

import org.apache.cassandra.utils.LatencyTracker;

import one.log.util.LoggerUtil;

/**
 * Substitutes stock latency tracker to file one-log statistics.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class LoggerLatencyTracker extends LatencyTracker
{

    private String statId;
    private String opName;
    private Object[] parameters;
    
    public LoggerLatencyTracker(String statId, String opName, Object[] parameters)
    {
        this.statId = statId;
        this.opName = opName;
        this.parameters = parameters;
    }

    /**
     * @param parameters the parameters to set
     */
    public void setParameters(Object[] parameters)
    {
        this.parameters = parameters;
    }

    public void addNano(long nanos) 
    {
        super.addNano(nanos);
        
        LoggerUtil.operationsSuccess(statId, nanos, 1, opName, parameters);
    };

}
