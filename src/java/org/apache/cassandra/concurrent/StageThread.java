/*
 * @(#) StageThread.java
 * Created Dec 27, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public final class StageThread extends Thread
{
    private final StageThreadFactory creatorThreadFactory;

    /**
     * @param runnable
     * @param s
     */
    StageThread(StageThreadFactory creator, Runnable runnable, String s)
    {
        super(runnable, s);
        
        this.creatorThreadFactory = creator;
    }


    /**
     * @return true, if this thread was created using specified thread factory
     */
    public boolean wasCreatedUsing(ThreadFactory factory)
    {
        return creatorThreadFactory == factory;
    }
}
