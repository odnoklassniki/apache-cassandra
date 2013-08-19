/*
 * @(#) MaintenanceTaskManager.java
 * Created Sep 23, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.maint;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.service.AntiEntropyService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;

/**
 * Controls execution of maintenance tasks inside maintenance window.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class MaintenanceTaskManager implements Runnable {
    private static final Logger logger = Logger.getLogger(MaintenanceTaskManager.class);

    public static MaintenanceTaskManager instance;

    /**
     * start and end of maintenance window in millis since day start
     */
    private long windowStartMillis, windowsEndMillis;

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(
            "MAINTENANCE", Thread.MIN_PRIORITY));

    /**
     * Configured tasks list
     */
    private List<MaintenanceTask> tasks;

    boolean stopped = false;

    public static void init(List<MaintenanceTask> tasks, String windowStart, String windowEnd) {
        if (instance != null)
            instance.stop();

        instance = new MaintenanceTaskManager(tasks);

        // parse window times
        instance.setWindow(parseTime(windowStart), parseTime(windowEnd));

    }

    public static boolean isConfigured() {
        return instance != null;
    }

    /**
     * @param windowStart time spec HH:MM
     * @return millis since day start
     */
    public static long parseTime(String timeString) {
        String[] times = timeString.split(":");

        int hour = Integer.parseInt(times[0]), minute = 0;

        assert hour >= 0 && hour < 24 : "Invalid hour in " + timeString;

        if (times.length > 1)
            minute = Integer.parseInt(times[1]);

        assert minute >= 0 && minute < 60 : "Invalid minute in " + timeString;

        return (hour * 60 + minute) * 60000;
    }

    /**
     * @param windowEnd 
     * @param windowStart 
     * 
     */
    private MaintenanceTaskManager(List<MaintenanceTask> tasks) {
        this.tasks = tasks;
    }

    private boolean inWindow() {
        return windowMillisLeft() > 0l;
    }

    /**
     * @return millis left to maint window end
     */
    private long windowMillisLeft() {
        Calendar c = Calendar.getInstance();

        long now = c.getTimeInMillis();

        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        long startOfDay = c.getTimeInMillis();

        if (now < startOfDay + windowStartMillis || now > startOfDay + windowsEndMillis)
            return 0;

        return startOfDay + windowsEndMillis - now;
    }

    private long windowStartedMillis() {
        Calendar c = Calendar.getInstance();

        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        return c.getTimeInMillis();

    }

    public void setWindow(long startMillis, long endMillis) {
        assert startMillis < endMillis;

        this.windowStartMillis = startMillis;
        this.windowsEndMillis = endMillis;
    }

    /**
     * 
     */
    public int positionInRing() {
        List<Range> ranges = StorageService.instance.getAllRanges();

        for (int i = 0; i < ranges.size(); i++) {
            if (StorageService.instance.getLocalPrimaryRange().equals(ranges.get(i)))
                return i;
        }

        return -1;
    }

    public void stop() {
        stopped = true;

        executor.shutdownNow();
    }

    /**
     * 
     */
    public void start() {
        stopped = false;

        if (tasks.size() > 0)
            executor.scheduleWithFixedDelay(this, 1, 1, TimeUnit.MINUTES);

        logger.info(String.format("Maitenance started for time window %s - %s with tasks: %s",
                stringifyTimeOffset(windowStartMillis), stringifyTimeOffset(windowsEndMillis), tasks));
    }

    private String stringifyTimeOffset(long timeOffset) {
        return String.format("%02d:%02d", timeOffset / 3600000, (timeOffset % 3600000) / 60000);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        if (stopped)
            return;

        final long millisLeft = windowMillisLeft();

        if (millisLeft == 0) {
            return;
        }
        try {

            final int positionInRing = positionInRing();

            if (positionInRing < 0) {
                logger.error("Cannot determine this endpoint's position in ring. Cannot run maintenance");
                return;
            }

            final long windowStartedMillis = windowStartedMillis();

            MaintenanceContext ctx = new MaintenanceContext() {

                @Override
                public long startedMillis() {
                    return windowStartedMillis;
                }

                @Override
                public long millisLeft() {
                    return millisLeft;
                }

                @Override
                public int ringPosition() {
                    return positionInRing;
                }
            };

            for (MaintenanceTask task : tasks) {
                Runnable r = task.maybeRun(ctx);

                if (r != null) {
                    try {

                        logger.info("Starting " + r);

                        r.run();

                    } catch (Throwable e) {
                        logger.error("Maintenance task " + r + "failed.", e);
                    }
                    return;
                }
            }

        } catch (Throwable e) {
            logger.error("Maintenance manager error .", e);
        }
    }
}
