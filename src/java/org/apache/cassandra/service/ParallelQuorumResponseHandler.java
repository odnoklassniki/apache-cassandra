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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.log4j.Logger;

public class ParallelQuorumResponseHandler implements IAsyncCallback, Runnable, ILocalCallback
{
    protected static final Logger logger = Logger.getLogger( ParallelQuorumResponseHandler.class );
    protected final Semaphore condition;
    protected final AtomicReferenceArray<Pair<InetAddress, ReadResponse>> responses;
    private SimpleReadResponseResolver responseResolver;
    private final long startTime;
    private final int responseCount;
    
    private ColumnFamily resolvedSuperset;

    public ParallelQuorumResponseHandler(int endpointCount, int responseCount, SimpleReadResponseResolver responseResolver)
    {
        this.responseCount = responseCount;
        this.condition= new Semaphore(endpointCount);
        int permits = this.condition.drainPermits();
        
        assert permits == endpointCount;
                
        responses = new AtomicReferenceArray<Pair<InetAddress, ReadResponse>>(endpointCount);
        this.responseResolver = responseResolver;
        startTime = System.currentTimeMillis();
    }
    
    /**
     * Waits for the very first response (from local or remote) and returns result as soon as it is received.
     * 
     * @return
     * @throws TimeoutException
     * @throws IOException
     */
    public Row get() throws TimeoutException, IOException
    {
        long timeout = DatabaseDescriptor.getRpcTimeout() - (System.currentTimeMillis() - startTime);
        boolean success;
        try
        {
            success = condition.tryAcquire(responseCount, timeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }

        if (!success)
        {
            throw new TimeoutException("Parallel read operation timed out .");
        }
        
        return resolve();
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run()
    {
        consistencyCheck();
    }

    // only resolving superset WITHOUT sending out repairs for all responses we got so far.
    private Row resolve()
    {
        
        ArrayList<ColumnFamily> versions = new ArrayList<ColumnFamily>(responses.length());
        
        for (int i=0;i<responses.length();i++)
        {
            Pair<InetAddress, ReadResponse> pair = responses.get(i);
            if (pair==null)
                break;
            
            versions.add(pair.right.row().cf);
        }

        if (versions.size()==responses.length())
        {
            // if we've got ALL responses - caching the result of superset resolution,
            // so consistency stage dont have to do superset resolution again
            Row row = responseResolver.resolve(versions);
            this.resolvedSuperset = row.cf;
            return row;
        }
        
        return responseResolver.resolve(versions);
    }
    
    /**
     * Waits when all requested endpoints respond and does read repair, if neccessary
     */
    public void consistencyCheck()
    {
        if (this.resolvedSuperset == null)
        {
            long timeout = DatabaseDescriptor.getRpcTimeout() - (System.currentTimeMillis() - startTime);
            try
            {
                // responseCount permits are already acquired by get(). We hit here only after successful get()
                boolean success = condition.tryAcquire( responses.length() - responseCount,timeout, TimeUnit.MILLISECONDS);
                
                if (success)
                    StorageProxy.countStrongConsistencyAll();
                else
                    StorageProxy.countStrongConsistencyUnder();
            }
            catch (InterruptedException ex)
            {
                throw new AssertionError(ex);
            }
        }

        ArrayList<InetAddress> endpoints = new ArrayList<InetAddress>(responses.length());
        ArrayList<ColumnFamily> versions = new ArrayList<ColumnFamily>(responses.length());
        for (int i=0;i<responses.length();i++)
        {
            Pair<InetAddress, ReadResponse> pair = responses.get(i);
            if (pair==null)
                break;

            endpoints.add(pair.left);
            versions.add(pair.right.row().cf);
        }

        if (this.resolvedSuperset == null)
        {
            // resolving and submitting repair for all responses we got so far
            responseResolver.resolve(versions, endpoints);
        } else
        {
            // only submitting repairs reusing precomputed superset
            responseResolver.maybeScheduleRepairs(this.resolvedSuperset, versions, endpoints);

            StorageProxy.countStrongConsistencyReuseSuperset();
        }
    }
    
    /**
     * Adds response to collection
     * @param response
     * @return number of this response. 0 is the very 1st
     */
    private int addResponse(Pair<InetAddress,ReadResponse> response)
    {
        for (int i=0; i<responses.length() ;i++)
        {
            if (responses.compareAndSet(i, null, response))
                return i;
        }
        
        assert false : "All messages already arrived: "+responses+", message: "+response;
        return -1;
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.service.ILocalCallback#localResponse(org.apache.cassandra.db.Row)
     */
    @Override
    public void localResponse(Row data)
    {
        ReadResponse readResponse = new ReadResponse(data);
        addResponse(new Pair<InetAddress, ReadResponse>(FBUtilities.getLocalAddress(), readResponse ));

        condition.release();
    }
    
    public void response(Message message)
    {
        try {
            
            ReadResponse data = responseResolver.parseResponse(message);

            addResponse(new Pair<InetAddress, ReadResponse>(message.getFrom(), data));

            condition.release();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
