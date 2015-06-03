/*
 * @(#) MaintenanceTask.java
 * Created Sep 23, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.maint;

/**
 * Maintenance task run right after maintenance window has been closed.
 * 
 * @author Yuriy Krutko <yuri.krutko@odnoklassniki.ru>
 *
 */
public interface FinalMaintenanceTask {
    void runFinally();
}
