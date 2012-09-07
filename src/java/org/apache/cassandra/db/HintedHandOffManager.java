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

package org.apache.cassandra.db;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.hints.HintLogHandoffManager;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.DigestMismatchException;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.cliffc.high_scale_lib.NonBlockingHashSet;


/**
 * For each table (keyspace), there is a row in the system hints CF.
 * SuperColumns in that row are keys for which we have hinted data.
 * Subcolumns names within that supercolumn are host IPs. Subcolumn values are always empty.
 * Instead, we store the row data "normally" in the application table it belongs in.
 *
 * So when we deliver hints we look up endpoints that need data delivered
 * on a per-key basis, then read that entire row out and send it over.
 * (TODO handle rows that have incrementally grown too large for a single message.)
 *
 * HHM never deletes the row from Application tables; there is no way to distinguish that
 * from hinted tombstones!  instead, rely on cleanup compactions to remove data
 * that doesn't belong on this node.  (Cleanup compactions may be started manually
 * -- on a per node basis -- with "nodeprobe cleanup.")
 *
 * TODO this avoids our hint rows from growing excessively large by offloading the
 * message data into application tables.  But, this means that cleanup compactions
 * will nuke HH data.  Probably better would be to store the RowMutation messages
 * in a HHData (non-super) CF, modifying the above to store a UUID value in the
 * HH subcolumn value, which we use as a key to a [standard] HHData system CF
 * that would contain the message bytes.
 *
 * There are two ways hinted data gets delivered to the intended nodes.
 *
 * runHints() runs periodically and pushes the hinted data on this node to
 * every intended node.
 *
 * runDelieverHints() is called when some other node starts up (potentially
 * from a failure) and delivers the hinted data just to that node.
 */

public class HintedHandOffManager
{
    private static HintedHandOffManager instance ;
    
    public static HintedHandOffManager instance()
    {
        return instance;
    }
    
    public static void setInstance(Class<? extends HintedHandOffManager> clazz) throws ConfigurationException
    {
        // TODO previous HHM deactivation
        try {
            if (instance==null || instance.getClass()!=clazz)
                instance = clazz.newInstance();
        } catch (Exception e) {
            throw new ConfigurationException("Cannot create instance of "+clazz+" due to "+e);
        }
    }

    private static final Logger logger_ = Logger.getLogger(HintedHandOffManager.class);
    public static final String HINTS_CF = "HintsColumnFamily";
    private static final int PAGE_SIZE = 10000;
    private static final String APPSTATE_PAYING_HINTS = "PLAYING_HINTS";

    protected final NonBlockingHashSet<InetAddress> queuedDeliveries = new NonBlockingHashSet<InetAddress>();

    private final ExecutorService executor_;

    public HintedHandOffManager()
    {
        int hhPriority = System.getProperty("cassandra.compaction.priority") == null
                         ? Thread.NORM_PRIORITY
                         : Integer.parseInt(System.getProperty("cassandra.compaction.priority"));
        executor_ = new JMXEnabledThreadPoolExecutor("HINTED-HANDOFF-POOL", hhPriority);
    }

    private static boolean sendMessage(InetAddress endPoint, String tableName, String key) throws IOException
    {
        if (!Gossiper.instance.isKnownEndpoint(endPoint))
        {
            logger_.warn("Hints found for endpoint " + endPoint + " which is not part of the gossip network.  discarding.");
            return true;
        }
        if (!FailureDetector.instance.isAlive(endPoint))
        {
            return false;
        }

        Table table = Table.open(tableName);
        for (ColumnFamilyStore cfs : table.getColumnFamilyStores())
        {
            byte[] startColumn = ArrayUtils.EMPTY_BYTE_ARRAY;
            while (true)
            {
                QueryFilter filter = new SliceQueryFilter(key, new QueryPath(cfs.getColumnFamilyName()), startColumn, ArrayUtils.EMPTY_BYTE_ARRAY, false, PAGE_SIZE);
                ColumnFamily cf = cfs.getColumnFamily(filter);
                if (pagingFinished(cf, startColumn))
                    break;
                if (cf.getColumnNames().isEmpty())
                {
                    if (logger_.isDebugEnabled())
                        logger_.debug("Nothing to hand off for " + key);
                    break;
                }

                startColumn = cf.getColumnNames().last();
                RowMutation rm = new RowMutation(tableName, key);
                rm.add(cf);
                Message message = rm.makeRowMutationMessage();
                WriteResponseHandler responseHandler = new WriteResponseHandler(1, 1, tableName);
                MessagingService.instance.sendRR(message, endPoint, responseHandler);
                try
                {
                    responseHandler.get();
                }
                catch (TimeoutException e)
                {
                    return false;
                }
            }

            String throttleRaw = System.getProperty("hinted_handoff_throttle");
            Integer throttle = throttleRaw == null ? null : Integer.valueOf(throttleRaw);
            try
            {
                Thread.sleep(throttle == null ? 0 : throttle);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
        }
        return true;
    }

    private static void deleteEndPoint(byte[] endpointAddress, String tableName, byte[] key, long timestamp) throws IOException
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, tableName);
        rm.delete(new QueryPath(HINTS_CF, key, endpointAddress), timestamp);
        rm.apply();
    }

    private static void deleteHintKey(String tableName, byte[] key) throws IOException
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, tableName);
        rm.delete(new QueryPath(HINTS_CF, key, null), System.currentTimeMillis());
        rm.apply();
    }

    private static boolean pagingFinished(ColumnFamily hintColumnFamily, byte[] startColumn)
    {
        // done if no hints found or the start column (same as last column processed in previous iteration) is the only one
        return hintColumnFamily == null
               || (hintColumnFamily.getSortedColumns().size() == 1 && hintColumnFamily.getColumn(startColumn) != null);
    }

    protected void deliverHintsToEndpoint(InetAddress endPoint) throws IOException, DigestMismatchException, InvalidRequestException, TimeoutException
    {
        logger_.info("Started hinted handoff for endPoint " + endPoint);
        queuedDeliveries.remove(endPoint);

        byte[] targetEPBytes = endPoint.getAddress();
        // 1. Scan through all the keys that we need to handoff
        // 2. For each key read the list of recipients if the endpoint matches send
        // 3. Delete that recipient from the key if write was successful
        // 4. Now force a flush
        // 5. Do major compaction to clean up all deletes etc.
        int rowsReplayed = 0;
        ColumnFamilyStore hintStore = Table.open(Table.SYSTEM_TABLE).getColumnFamilyStore(HINTS_CF);
        for (String tableName : DatabaseDescriptor.getTables())
        {
            byte[] startColumn = ArrayUtils.EMPTY_BYTE_ARRAY;
            while (true)
            {
                QueryFilter filter = new SliceQueryFilter(tableName, new QueryPath(HINTS_CF), startColumn, ArrayUtils.EMPTY_BYTE_ARRAY, false, PAGE_SIZE);
                ColumnFamily hintColumnFamily = ColumnFamilyStore.removeDeleted(hintStore.getColumnFamily(filter), Integer.MAX_VALUE);
                if (pagingFinished(hintColumnFamily, startColumn))
                    break;
                Collection<IColumn> keys = hintColumnFamily.getSortedColumns();

                for (IColumn keyColumn : keys)
                {
                    String keyStr = new String(keyColumn.name(), "UTF-8");
                    Collection<IColumn> endpoints = keyColumn.getSubColumns();
                    for (IColumn hintEndPoint : endpoints)
                    {
                        if (Arrays.equals(hintEndPoint.name(), targetEPBytes) && sendMessage(endPoint, tableName, keyStr))
                        {
                            rowsReplayed++;
                            if (endpoints.size() == 1)
                                deleteHintKey(tableName, keyColumn.name());
                            else
                                deleteEndPoint(hintEndPoint.name(), tableName, keyColumn.name(), System.currentTimeMillis());
                            break;
                        }
                    }

                    startColumn = keyColumn.name();
                }
            }
        }

        if (rowsReplayed > 0)
        {
            hintStore.forceFlush();
            try
            {
                CompactionManager.instance.submitMajor(hintStore, 0, Integer.MAX_VALUE).get();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        logger_.info(String.format("Finished hinted handoff of %s rows to endpoint %s",
                                   rowsReplayed, endPoint));
    }

    /*
     * This method is used to deliver hints to a particular endpoint.
     * When we learn that some endpoint is back up we deliver the data
     * to him via an event driven mechanism.
    */
    public void deliverHints(final InetAddress to)
    {
        if (!queuedDeliveries.add(to))
            return;
        
        notifyStartPlayingHints(to);
        
        Runnable r = new WrappedRunnable()
        {   
            public void runMayThrow() throws Exception
            {
                try{
                    deliverHintsToEndpoint(to);
                }finally{
                    //если не пришло новое задание, то нотифицируем об окончании
                    if (!queuedDeliveries.contains(to)){
                        notifyFinishedPlayingHints(to);
                    }
                }
            }
        };
    	executor_.submit(r);
    }

    public void deliverHints(String to) throws UnknownHostException
    {
        deliverHints(InetAddress.getByName(to));
    }

    /**
     * Stores new hint for later delivery
     * 
     * @param hint
     * @param rm
     * @param serializedMutation 
     * @throws IOException 
     */
    public void storeHint(InetAddress hint, RowMutation rm, byte[] serializedMutation) throws IOException
    {
        if (logger_.isDebugEnabled())
            logger_.debug("Adding hint for " + hint.getHostAddress());
        
        RowMutation hintedMutation = new RowMutation(Table.SYSTEM_TABLE, rm.getTable());
        hintedMutation.addHints(rm.key(), hint.getAddress());
        hintedMutation.apply();
    }
    
    public boolean isSomebodyPlayingHits(){
        InetAddress localAddress = FBUtilities.getLocalAddress();
        Set<InetAddress> endpoints = Gossiper.instance.getLiveMembers();
        for (InetAddress endpoint : endpoints) {
            List<InetAddress> playingHints = getPlayingHints(endpoint);
            if (playingHints != null  && playingHints.contains(localAddress)){
                return true;
            }
        }
        return false;
    }
    
    public List<InetAddress> getNodesPlayingHits(){
        List<InetAddress>  res = new ArrayList<InetAddress>();
        InetAddress localAddress = FBUtilities.getLocalAddress();
        Set<InetAddress> endpoints = Gossiper.instance.getLiveMembers();
        for (InetAddress endpoint : endpoints) {
            List<InetAddress> playingHints = getPlayingHints(endpoint);
            if (playingHints != null  && playingHints.contains(localAddress)){
                res.add(endpoint);  
            }
        }
        return res;
    }
    
    public  List<InetAddress> getPlayingHints(InetAddress endpoint) {
        ApplicationState applicationState = Gossiper.instance.getEndPointStateForEndPoint(endpoint).getApplicationState(APPSTATE_PAYING_HINTS);
        if (applicationState==null)
            return Collections.<InetAddress>emptyList();
        
        String value = applicationState.getValue();
        if (value == null){
            return Collections.<InetAddress>emptyList();
        }
        
        String[] tokens = StringUtils.split(value, ",");
        List<InetAddress> res = new ArrayList<InetAddress>(tokens.length);
        for (String token : tokens) {
            try {
                byte[] addr = FBUtilities.hexToBytes(token);
                res.add(Inet4Address.getByAddress(addr));
            } catch (Exception e) {
                logger_.error("Invalid end point addreess", e);
            }
        }
        return res;
    }
    
    private List<InetAddress> currentylPlayingHints = new ArrayList<InetAddress>();
    
    private void notifyStartPlayingHints(InetAddress endPoint){
        synchronized(currentylPlayingHints){
            currentylPlayingHints.add(endPoint);
        }
        setPlayingHints(currentylPlayingHints);
    }
    
    private void notifyFinishedPlayingHints(InetAddress endPoint){
        synchronized(currentylPlayingHints){
            currentylPlayingHints.remove(endPoint);
        }
        setPlayingHints(currentylPlayingHints);
    }
    
    private  void setPlayingHints(List<InetAddress> endpoints) {
        StringBuilder sb = new StringBuilder();
        if (endpoints != null){
            for (InetAddress endpoint : endpoints) {
                String token = FBUtilities.bytesToHex(endpoint.getAddress());
                if (sb.length() > 0){
                    sb.append(",");
                }
                sb.append(token);
            }
        }
        Gossiper.instance.addLocalApplicationState(APPSTATE_PAYING_HINTS, new ApplicationState( sb.toString()));
    } 
}
