/*
 * @(#) MaintenanceContext.java
 * Created Sep 23, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.maint;

/**
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public interface MaintenanceContext
{
    /**
     * @return current endpoint's position in ring;
     */
    int ringPosition();
    /**
     * @return number of millis left till end of maint window
     */
    long millisLeft();
    
    /**
     * @return current maintenance window started millis
     */
    long startedMillis();
}
