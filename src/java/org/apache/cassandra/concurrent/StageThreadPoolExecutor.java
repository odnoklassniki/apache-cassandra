/*
 * @(#) StageThreadPoolExecutor.java
 * Created Dec 27, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This pool executor is tied to its stage and launches tasks in current thread, if it belogs to
 * the neccessary stage.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class StageThreadPoolExecutor extends JMXConfigurableThreadPoolExecutor
{

    /**
     * @param corePoolSize
     * @param maximumPoolSize
     * @param keepAliveTime
     * @param unit
     * @param workQueue
     * @param name          stage name
     */
    StageThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
            long keepAliveTime, TimeUnit unit,
            BlockingQueue<Runnable> workQueue, String stageName)
    {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
                new StageThreadFactory(stageName));
        
    }

    /* (non-Javadoc)
     * @see java.util.concurrent.ThreadPoolExecutor#execute(java.lang.Runnable)
     */
    @Override
    public void execute(Runnable runnable)
    {
        // running in the same thread, if current thread is of required stage
        if (Thread.currentThread() instanceof StageThread )
        {
            if ( ((StageThread)Thread.currentThread()).wasCreatedUsing(getThreadFactory()) )
            {
                runnable.run();
                return;
            }
        }
        
        super.execute(runnable);
    }
}
