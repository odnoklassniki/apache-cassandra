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


import java.util.concurrent.atomic.AtomicLong;

// TODO stats about column bloom
// TODO add bloomed out/passed ratio (see SSTable#getPosition)
public class BloomFilterTracker
{
    private AtomicLong columnReadsCount = new AtomicLong(0);
    private AtomicLong columnNegativeCount = new AtomicLong(0);
    private AtomicLong negativeCount = new AtomicLong(0);
    private AtomicLong falsePositiveCount = new AtomicLong(0);
    private AtomicLong truePositiveCount = new AtomicLong(0);
    private long lastFalsePositiveCount = 0L;
    private long lastTruePositiveCount = 0L;
    private long lastNegativeCount = 0L;
    private long lastColumnNegativeCount = 0L;
    private long lastColumnReadsCount = 0L;

    public void addNegativeCount()
    {
        negativeCount.incrementAndGet();
    }
    
    public void addColumnNegativeCount()
    {
        columnNegativeCount.incrementAndGet();
    }
    
    public void addColumnReadsCount()
    {
        columnReadsCount.incrementAndGet();
    }

    public void addFalsePositive()
    {
        falsePositiveCount.incrementAndGet();
    }

    public void addTruePositive()
    {
        truePositiveCount.incrementAndGet();
    }
    
    public long getNegativeCount()
    {
        return negativeCount.get();
    }

    public long getRecentNegativeCount()
    {
        long fpc = getNegativeCount();
        try
        {
            return (fpc - lastNegativeCount);
        }
        finally
        {
            lastNegativeCount = fpc;
        }
    }

    public long getColumnNegativeCount()
    {
        return columnNegativeCount.get();
    }

    public long getRecentColumnNegativeCount()
    {
        long fpc = getColumnNegativeCount();
        try
        {
            return (fpc - lastColumnNegativeCount);
        }
        finally
        {
            lastColumnNegativeCount = fpc;
        }
    }

    public long getColumnReadsCount()
    {
        return columnReadsCount.get();
    }

    public long getRecentColumnReadsCount()
    {
        long fpc = getColumnReadsCount();
        try
        {
            return (fpc - lastColumnReadsCount);
        }
        finally
        {
            lastColumnReadsCount = fpc;
        }
    }

    public long getFalsePositiveCount()
    {
        return falsePositiveCount.get();
    }

    public long getRecentFalsePositiveCount()
    {
        long fpc = getFalsePositiveCount();
        try
        {
            return (fpc - lastFalsePositiveCount);
        }
        finally
        {
            lastFalsePositiveCount = fpc;
        }
    }

    public long getTruePositiveCount()
    {
        return truePositiveCount.get();
    }

    public long getRecentTruePositiveCount()
    {
        long tpc = getTruePositiveCount();
        try
        {
            return (tpc - lastTruePositiveCount);
        }
        finally
        {
            lastTruePositiveCount = tpc;
        }
    }
    
    public String toString()
    {
        return "CR/N "+getColumnReadsCount()+'/'+getColumnNegativeCount()+"; FP/TP/TN "+getFalsePositiveCount()+'/'+getTruePositiveCount()+'/'+getNegativeCount(); 
    }
}
