package org.apache.cassandra.io;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DatabaseDescriptor.DiskAccessMode;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.WrappedRunnable;

public class SSTableDeletingReference extends PhantomReference<SSTableReader>
{
    private static final Logger logger = Logger.getLogger(SSTableDeletingReference.class);

    private static final ScheduledExecutorService timer = Executors.newScheduledThreadPool(2,new NamedThreadFactory( "SSTABLE-CLEANUP-TIMER",Thread.MIN_PRIORITY ) );
    public static final int RETRY_DELAY = 10000;

    private final SSTableTracker tracker;
    public final String path;
    private final long size;
    private volatile BloomFilter bf;
    private boolean deleteOnCleanup;

    SSTableDeletingReference(SSTableTracker tracker, SSTableReader referent, ReferenceQueue<? super SSTableReader> q)
    {
        super(referent, q);
        this.tracker = tracker;
        this.path = referent.path;
        this.size = referent.bytesOnDisk();
        
        this.bf = DatabaseDescriptor.getFilterAccessMode() == DiskAccessMode.standard ? null : referent.bf;
    }

    public void deleteOnCleanup()
    {
        deleteOnCleanup = true;
    }
    
    public void scheduleBloomFilterClose(long delayMillis) {
        if ( bf != null ) {
            timer.schedule(new CloseBFTask(), delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    public void cleanup() throws IOException
    {
        new CloseBFTask().run();

        if (deleteOnCleanup)
        {
            // this is tricky because the mmapping might not have been finalized yet,
            // and delete will fail until it is.  additionally, we need to make sure to
            // delete the data file first, so on restart the others will be recognized as GCable
            // even if the compaction marker gets deleted next.
            timer.schedule(new CleanupTask(), RETRY_DELAY, TimeUnit.MILLISECONDS);
        }
    }

    private class CleanupTask extends WrappedRunnable
    {
        int attempts = 0;

        @Override
        public void runMayThrow()
        {
            File datafile = new File(path);
            if (!datafile.delete())
            {
                if (attempts++ < DeletionService.MAX_RETRIES)
                {
                    timer.schedule(this, RETRY_DELAY, TimeUnit.MILLISECONDS); 
                    return;
                }
                else
                {
                    // don't throw an exception; it will prevent any future tasks from running in this Timer
                    logger.error("Unable to delete " + datafile + " (it will be removed on server restart)");
                    return;
                }
            }

            try
            {
                FileUtils.deleteWithConfirm(new File(SSTable.indexFilename(path)));
                FileUtils.deleteWithConfirm(new File(SSTable.filterFilename(path)));
                FileUtils.deleteWithConfirm(new File(SSTable.compactedFilename(path)));
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            tracker.spaceReclaimed(size);
            logger.info("Deleted " + path);
        }
    }
    
    private class CloseBFTask extends WrappedRunnable {
        @Override
        protected void runMayThrow() throws Exception
        {
            
            BloomFilter bloomFilter = bf;
            if (bloomFilter != null) {
                bloomFilter.close();
                bf = null;
            }
        }
    }
}
