/*
 * @(#) StageThreadFactory.java
 * Created Dec 27, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.concurrent;

/**
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class StageThreadFactory extends NamedThreadFactory
{

    /**
     * @param id
     * @param priority
     */
    public StageThreadFactory(String id, int priority)
    {
        super(id, priority);
    }

    /**
     * @param id
     */
    public StageThreadFactory(String id)
    {
        super(id);
    }

    public Thread newThread(Runnable runnable)
    {        
        String name = id + ":" + n.getAndIncrement();
        Thread thread = new StageThread(this,runnable, name);
        thread.setPriority(priority);
        return thread;
    }
}
