/*
 * @(#) RateControl.java
 * Created Jul 1, 2010 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.streaming;

/**
 * Limits rate of operations to be no more than opsSec by pausing current thread.
 * 
 * It also tries to AVG ops during 10 seconds.
 * 
 * Using it to limit stream In rate (otherwise it overloades node on decomission)
 * 
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public class RateControl
{
    private final int AVG_SECS=10;
    
    private long lastSecond;
    
    private int ops;
    private long nanosPerOp;
    
    /**
     * @param opsSec ops Sec limit
     */
    public RateControl(int opsSec)
    {
        this.nanosPerOp = 1000000000/opsSec;
    }
    
    /**
     * Counts 1 operation, pausing thread if neccessary to meet rate limit
     */
    public synchronized void control()
    {
        if (lastSecond==0L)
        {
            long last=System.nanoTime();
            lastSecond=last;
            ops=1;
            return;
        }
        
        long now=System.nanoTime();
        ops++;
        
        long nowEstimated= lastSecond + nanosPerOp*ops;
        
        if (nowEstimated>now+nanosPerOp/10) // dont pause, if delta is not so much <10% of total operation
        {
            // sleeping whole millis with sleep to conserve cpu
            long millis=(nowEstimated-now) / 1000000;
            if (millis>0)
                millis=(nowEstimated-System.nanoTime()) / 1000000;
            if (millis>0)
                try {
                    Thread.sleep(millis);
                } catch (InterruptedException e) {
                }

            // sleeping rest with nanosecond precision (sleep cannot sleep less than 1ms)
            while (nowEstimated>System.nanoTime())
                Thread.yield(); // gives pause from 40 till 100 microseconds
            
        }
        
        if (now>lastSecond + 1000000000*AVG_SECS)
        {
            ops=1;
            lastSecond=now;
        }
        
    }

}
