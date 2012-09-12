/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SimpleCondition;

public class WriteResponseHandler implements IAsyncCallback
{
    protected static final Logger logger = Logger.getLogger( WriteResponseHandler.class );
    protected final SimpleCondition condition = new SimpleCondition();
    protected final SimpleCondition conditionAll = new SimpleCondition();
    // start time in NANOSECONDS (because need to evaluate 10ths of millis in #getAllResponses
    private final long startNanos;
    private final int allEndpointCount;
    private int remoteEndpointCount = 0;
    private final int responseCount;
    // keeping all stuff in single atomicintegerobject is faster 1.5 - 2x for RF=3 than LBQ
    // 0tn element has count of remaining responses
    // endpoint addresses are stored starting from 1st and till the end
    private AtomicIntegerArray responses;
    

    public WriteResponseHandler(int responseCount, int endpoints, String table)
    {
        this.allEndpointCount = endpoints;
        // at most one node per range can bootstrap at a time, and these will be added to the write until
        // bootstrap finishes (at which point we no longer need to write to the old ones).
        assert 1 <= responseCount && responseCount <= 2 * DatabaseDescriptor.getReplicationFactor(table)
            : "invalid response count " + responseCount;

        responses = new AtomicIntegerArray(endpoints+1);
        this.responseCount = responseCount;
        startNanos = System.nanoTime();
    }
    
    public static final int toInt(byte[] b) {
        return b[0]<<24 | (b[1]&0xff)<<16 | (b[2]&0xff)<<8 | (b[3]&0xff);
    }

    public void addEndpoint(InetAddress endpoint)
    {
        responses.set(++remoteEndpointCount, toInt(endpoint.getAddress()) );
    }

    public void get() throws TimeoutException{
        long timeout = DatabaseDescriptor.getRpcTimeout() - (System.nanoTime() - startNanos)/1000000;
        get(timeout);
    }
    
    public void get(long timeout) throws TimeoutException
    {
        boolean success;
        try
        {
            success = condition.await(timeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }

        if (!success)
        {
            throw new TimeoutException("Operation timed out - not received " + responses.get(0) + " responses");
        }
    }
    
    public void response(Message message)
    {
        for (int i = 1;i<=remoteEndpointCount && !responses.compareAndSet(i, toInt(message.getFrom().getAddress()), 0);i++);
        
        maybeSignal();
    }

    public void localResponse()
    {
        maybeSignal();
    }

    /**
     * Wait at most for a specified millis for all endpoints' responses
     * 
     * @param timeout additional time to wait (tenths of millis)
     * @return true - all endpoints responded, false otherwise
     */
    public boolean getAllResponses(long timeout)
    {
        if ( conditionAll.isSignaled() )
            return true;
        
        timeout -= (System.nanoTime() - startNanos)/1000000;
        
        if (timeout<=0l)
            return conditionAll.isSignaled();

        // waiting
        try {
            return conditionAll.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * @return endpoints form which we did not received responses
     */
    public List<InetAddress> getLaggingEndpoints()
    {
        int laggedEndpointCount = allEndpointCount - responses.get(0);
        ArrayList<InetAddress> rc = new ArrayList<InetAddress>(laggedEndpointCount);
        
        try {
            for (int i=remoteEndpointCount+1;i-->1;)
            {
                int endpoint = responses.get(i);
                if (endpoint!=0)
                    rc.add(InetAddress.getByAddress(FBUtilities.toByteArray(endpoint)));
            }
            
        } catch (UnknownHostException e) {
            throw new AssertionError(responses);
        }
        
        // there is a small chance that while we did checking remote endpoint did registered
        // its response. So rechecking response count and returning null,
        // if it is registered itself
        if (rc.size()>0)
        {
            return allEndpointCount - responses.get(0) > 0 ? rc : null;
        }
        else
        {
            // not found failed remote endpoint for this missed mutation. So this is local one!
            if (allEndpointCount - responses.get(0) > 0)
            {
                rc.add(FBUtilities.getLocalAddress());
                return rc;
            }
                
        }
        
        return null;
                
    }

    private void maybeSignal()
    {
        int responsesReceived = responses.incrementAndGet(0);
        if (responsesReceived == responseCount )
        {
            condition.signal();
        }
        if (responsesReceived == allEndpointCount)
        {
            conditionAll.signal();
        }
    }
}