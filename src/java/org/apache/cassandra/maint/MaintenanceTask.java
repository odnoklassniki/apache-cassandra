/*
 * @(#) MaintenanceTask.java
 * Created Sep 23, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.maint;

/**
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public interface MaintenanceTask 
{
    /**
     * Should decide, must this task run and will it fit into maintenance window.
     * 
     * @return !=null - a task to run, null - cannot find a task
     */
    Runnable maybeRun( MaintenanceContext ctx );
    
}
