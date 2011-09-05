package org.apache.cassandra.db.hints;
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


import java.lang.management.ManagementFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.db.commitlog.PeriodicCommitLogExecutorServiceMBean;
import org.apache.cassandra.utils.WrappedRunnable;

public class PeriodicHintLogExecutorService implements PeriodicHintLogExecutorServiceMBean
{
    private final BlockingQueue<Runnable> queue;
    protected volatile long completedTaskCount = 0;

    public PeriodicHintLogExecutorService()
    {
        this(1024 * Runtime.getRuntime().availableProcessors());
    }

    public PeriodicHintLogExecutorService(int queueSize)
    {
        queue = new LinkedBlockingQueue<Runnable>(queueSize);
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                while (true)
                {
                    queue.take().run();
                    completedTaskCount++;
                }
            }
        };
        new Thread(runnable, "HINT-LOG-WRITER").start();

        registerMBean(this);
    }
    
    protected static void registerMBean(Object o)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(o, new ObjectName("org.apache.cassandra.db:type=Hintlog"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void add(HintLog.LogRecordAdder adder)
    {
        try
        {
            queue.put(adder);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public <T> Future<T> submit(Callable<T> task)
    {
        FutureTask<T> ft = new FutureTask<T>(task);
        try
        {
            queue.put(ft);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        return ft;
    }

    public long getPendingTasks()
    {
        return queue.size();
    }

    public int getActiveCount()
    {
        return 1;
    }

    public long getCompletedTasks()
    {
        return completedTaskCount;
    }
}