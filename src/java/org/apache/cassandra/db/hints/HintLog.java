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

package org.apache.cassandra.db.hints;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.log4j.Logger;

/**
 * This is alternative hints storage implementation, modeled after commit log. 
 * 
 * I believe it is better than default. because:
 * 1. It has smaller memory requirements to operate than Hints CF, especially on long runs. 
 * 2. It tracks mutations, not rows to hint, which takes less space and memory, if you have large rows.
 * 3. It does not require compaction. If all mutations from hint log are delivered - whole hint log file is just removed.
 * 4. It saves data to commit log device, not to data devices. This is good, if commit log is underloaded, 
 * 5. It does not require random reads on delivery - mutations are read sequentially from disk. (Hints CF up to 1.0 require it)
 * 6. It has all io operations performed by single hint log writer thread, so writer threads are not affected much by disk io delays.
 * 
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 */
public class HintLog
{
    private static volatile int SEGMENT_SIZE = 128*1024*1024; // roll after log gets this big
    private static volatile int SYNC_PERIOD = DatabaseDescriptor.getCommitLogSyncPeriod();
    private static boolean HINT_DELIVERY_ON_SYNC = true; 
    

    private static final Logger logger = Logger.getLogger(HintLog.class);

    public static HintLog instance()
    {
        return CLHandle.instance;
    }

    private static class CLHandle
    {
        public static HintLog instance = new HintLog();
    }

    private final HashMap<String,Deque<HintLogSegment>> segments = new HashMap<String, Deque<HintLogSegment>>();
    
    private final PeriodicHintLogExecutorService executor;
    private Thread syncerThread;

    public static void setSegmentSize(int size)
    {
        SEGMENT_SIZE = size;
    }
    
    public static void setSyncPeiod(int period)
    {
        SYNC_PERIOD = period;
    }

    /**
     * param @ table - name of table for which we are maintaining
     *                 this commit log.
     * param @ recoverymode - is commit log being instantiated in
     *                        in recovery mode.
    */
    HintLog()
    {
        // loading hint logs from disk
        loadSegments();
        
        executor = new PeriodicHintLogExecutorService();
        final Callable<Void> syncer = new Callable<Void>()
        {
            public Void call() throws Exception
            {
                sync();
                return null;
            }
        };

        (
                syncerThread=new Thread(new Runnable()
                {

                    public void run()
                    {

                        // wait for ring stabilization first
                        try {
                            Thread.sleep(StorageService.RING_DELAY);
                        } catch (InterruptedException e1) {
                            logger.error("err",e1);
                        }

                        while (true)
                        {
                            try
                            {
                                executor.submit(syncer).get();
                                Thread.sleep(SYNC_PERIOD);
                            }
                            catch (InterruptedException e)
                            {
                                throw new AssertionError(e);
                            }
                            catch (ExecutionException e)
                            {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }, "PERIODIC-HINT-LOG-SYNCER") 
                ).start();
    }
    
    private void loadSegments()
    {
        String directory = DatabaseDescriptor.getHintLogDirectory();
        File file = new File(directory);
        File[] files = file.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.matches("Hints-[^-]+-\\d+.log");
            }
        });
        if (files.length == 0)
            return;
        
        Arrays.sort(files, new FileUtils.FileComparator());
       
        for (int i=files.length;i-->0; ) 
        {
            File f=files[i];
            
            String[] nameElements=f.getName().split("-");
            String token = nameElements[1];
            
            Deque<HintLogSegment> endpSegments = getEndpointSegments(token);

            HintLogSegment segment = new HintLogSegment(token, f.getAbsolutePath());
            if (!segment.isEmpty() && segment.isFullyReplayed())
            {
                segment.close();
                segment.delete();
            } else
            {
                endpSegments.addFirst(segment);
            }
        }
    }
    

    private Deque<HintLogSegment> getEndpointSegments(String token)
    {
        Deque<HintLogSegment> endpSegments = segments.get(token);
        if (endpSegments==null)
        {
            endpSegments = new ArrayDeque<HintLogSegment>();
            segments.put(token, endpSegments);
            endpSegments.add( new HintLogSegment(token) );
        }
        
        return endpSegments;
    }


    /**
     * Obtains iterator of hint row mutations to send to specified endpoints. This also tries to send
     * hints made old way (to endpoint address)
     * 
     * You must call {@link Iterator#remove()} for all successfully delivered hints to avoid their resending in future.
     * 
     * @param destination endpoint to send.
     * @return iterator (or empty iterator if no hints to deliver)
     */
    public Iterator<List<byte[]>> getHintsToDeliver(InetAddress endpoint)
    {
        String address = endpoint.getHostAddress();
        if  (segments.get(address)!=null)
        {
            List<HintLogSegment> segments = forceNewSegment(address);
            if (!segments.isEmpty())
                return new HintLogReader(address, segments);
        }
        
        return getHintsToDeliver(endpointToToken(endpoint));
    }
    
    /**
     * Obtains iterator of hint row mutations to send to specified endpoints. 
     * You must call {@link Iterator#remove()} for all successfully delivered hints to avoid their resending in future.
     * 
     * @param destination endpoint to send.
     * @return iterator (or empty iterator if no hints to deliver)
     */
    public Iterator<List<byte[]>> getHintsToDeliver(String destination)
    {
        Deque<HintLogSegment> endpSegments = segments.get(destination);
        
        if (endpSegments==null)
            return Collections.<List<byte[]>>emptyList().iterator();
        
        return new HintLogReader(destination, forceNewSegment(destination));
    }
    
    private HintLogSegment currentSegment(String token)
    {
        return getEndpointSegments(token).getLast();
    }
    
    /**
     * 
     * @param token either 111.222.333.444 or token string value
     * @return
     */
    private InetAddress tokenToEndpoint(String token)
    {
        if (token.matches("\\d+[.]\\d+[.]\\d+[.]\\d"))
        {
            // this is ip address
            try {
                InetAddress ip = InetAddress.getByName(token);
                
                return ip;
            } catch (UnknownHostException e) {
            }
        }
        
        Token<?> hintToken = StorageService.getPartitioner().getTokenFactory().fromString(token);
        
        InetAddress endPoint = StorageService.instance.getTokenMetadata().getEndPointHint(hintToken);
        
        return endPoint;
    }
    
    private String endpointToToken(InetAddress endpoint)
    {
        Token<?> token = StorageService.instance.getTokenMetadata().getTokenHint(endpoint);
        
        return StorageService.instance.getPartitioner().getTokenFactory().toString(token);
    }
    
    private void sync()
    {
        Collection<Deque<HintLogSegment>> values = segments.values();
        
        for (Deque<HintLogSegment> deque : values) {
            HintLogSegment last = deque.peekLast();
            
            if (last!=null)
                try {
                    last.sync();
                    
                    // roll log if necessary
                    if (last.length() >= SEGMENT_SIZE)
                    {
                        last.close();
                        getEndpointSegments(last.getToken()).add(new HintLogSegment(last.getToken()));
                    }

                    if ( (deque.size()>1 || !last.isEmpty()) && HINT_DELIVERY_ON_SYNC) {

                        InetAddress endp = tokenToEndpoint(last.getToken());
                        
                        if (endp !=null && FailureDetector.instance.isAlive(endp))
                        {
                            HintedHandOffManager.instance().deliverHints(endp);
                        }
                        else
                            if (endp==null || !Gossiper.instance.isKnownEndpoint(endp))
                            {
                                logger.info("Endpoint is not not known for token "+last.getToken()+" removing hint logs");
                                
                                for (HintLogSegment hintLogSegment : deque) 
                                {
                                    hintLogSegment.close();
                                    hintLogSegment.delete();
                                }

                                deque.clear();
                                deque.add( new HintLogSegment( last.getToken() ) );
                            }
                    }
                        
                    
                } catch (IOException e) {
                    logger.error("Cannot sync "+last,e);
                }
        }
    }
    
    public void close()
    {
        Collection<Deque<HintLogSegment>> values = segments.values();
        
        for (Deque<HintLogSegment> deque : values) {
            HintLogSegment last = deque.peekLast();
            
            if (last!=null)
                try {
                    last.sync();
                    
                    if (!last.isEmpty()) {
                        last.close();
                    }
                        
                    
                } catch (IOException e) {
                    logger.error("Cannot close "+last,e);
                }
        }
    }
    
    /*
     * Adds the specified row to the commit log. This method will reset the
     * file offset to what it is before the start of the operation in case
     * of any problems. This way we can assume that the subsequent commit log
     * entry will override the garbage left over by the previous write.
    */
    public void add(InetAddress endpoint, byte[] serializedRow) throws IOException
    {
        executor.add(new LogRecordAdder(endpointToToken(endpoint), serializedRow));
    }


    /*
     * this is called in HintLogReader to remove hint log segment with all its mutations successfully played back.
    */
    public void discardPlayedbackSegment(final String token, final HintLogSegment segment)
    {
        Callable task = new Callable()
        {
            public Object call() throws IOException
            {
                assert segment.isFullyReplayed();
                
                Deque<HintLogSegment> endpointSegments = getEndpointSegments(token);
                
                if (endpointSegments.remove(segment))
                {
                    segment.delete();
                } else
                {
                    logger.warn("Hm. "+segment+" not found in list of hintlog segments for "+token);
                }
                
                return null;
            }
        };
        try
        {
            executor.submit(task).get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public List<HintLogSegment> forceNewSegment(final InetAddress endpoint)
    {
        return forceNewSegment(endpointToToken(endpoint));
    }
    
    public List<HintLogSegment> forceNewSegment(final String token)
    {
        Callable<List<HintLogSegment>> task = new Callable<List<HintLogSegment>>()
        {
            public List<HintLogSegment> call() throws Exception
            {
                Deque<HintLogSegment> endpointSegments = getEndpointSegments(token);
                
                HintLogSegment last = endpointSegments.getLast();
                if (last==null)
                    return Collections.emptyList();
                
                if (!last.isEmpty())
                {
                    last.close();

                    ArrayList<HintLogSegment> readable = new ArrayList<HintLogSegment>(endpointSegments);
                    
                    endpointSegments.add(new HintLogSegment(token));
                    
                    return readable;
                } else
                {
                    // last one is empty - returning all except last
                    if (endpointSegments.size()<=1)
                        return Collections.emptyList();
                    
                    endpointSegments.pollLast();
                    ArrayList<HintLogSegment> readable = new ArrayList<HintLogSegment>(endpointSegments);
                    
                    endpointSegments.add(last);
                    
                    return readable;
                }
                
            }
        };
        try
        {
            return executor.submit(task).get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    // TODO this should be a Runnable since it doesn't actually return anything, but it's difficult to do that
    // without breaking the fragile CheaterFutureTask in BatchCLES.
    class LogRecordAdder implements Callable, Runnable
    {
        final String token;
        final byte[] serializedRow;

        LogRecordAdder(String token, byte[] serializedRow)
        {
            this.token = token;
            this.serializedRow = serializedRow;
        }


        public void run()
        {
            try
            {
                HintLogSegment currentSegment = currentSegment(token);
                currentSegment.write(serializedRow);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        public Object call() throws Exception
        {
            run();
            return null;
        }
    }
    
    public class HintLogReader implements Iterator<List<byte[]>>, Closeable
    {
        private final String token;
        
        private Iterator<HintLogSegment> segments;
        
        private HintLogSegment current = null;
        
        private BufferedRandomAccessFile reader = null;
        
        private ArrayList<byte[]> currentMutation = null, nextMutation = null;

        private long lastHeaderWrite = 0l;
        private int  unwrittenConfirmations = 0;

        /**
         * @param
         * @param segments shared queue
         * 
         */
        public HintLogReader(String destination, List<HintLogSegment> segments )
        {
            this.token = destination;
            this.segments = segments.iterator();
            
            nextSegment();
        }
        
        private boolean nextSegment()
        {
            if (current!=null)
            {
                try {
                    current.writeHeader();
                    reader.close();
                } catch (IOException e) {
                    logger.warn("Cannot write header of segment "+current);
                }
                
                assert current.isFullyReplayed();
                
                discardPlayedbackSegment(token, current);
                
                current = null;
                reader = null;
            }
            
            while (segments.hasNext())
            {
                current = segments.next();
                
                if (!current.isEmpty() && !current.isFullyReplayed() )
                {
                    try 
                    {
                        reader = current.createReader();

                        /* seek to the lowest position where last unconfirmed HH mutation is written 
                         * if replay was forced - reading all records, regardless of commit log header 
                         */
                        reader.seek(current.getHeader().getPosition());
                        
                        logger.info("Delivering " + current + " starting at " + reader.getFilePointer());
                        
                        lastHeaderWrite = System.currentTimeMillis();
                        unwrittenConfirmations = 0;

                    } catch (IOException e) {
                        logger.error("Cannot open "+current+". Skipping its replay. Consider starting repair on this node or "+tokenToEndpoint( token ),e);
                        current = null;
                        continue;
                    }
                    
                    return true;
                }

                if (!current.isEmpty())
                    discardPlayedbackSegment(token, current);
                
                current = null;
            }
            
            return false;
        }
        
        private boolean nextMutation()
        {
            if (nextMutation!=null && !nextMutation.isEmpty())
                return true;
            
            assert currentMutation == null : "Previous Mutation must be confirmed before getting next one";

            if (current == null && ! nextSegment() )
                return false;
            
            int batchSize = DatabaseDescriptor.getHintLogPlayBatchSize();
            long batchBytesLimit = DatabaseDescriptor.getHintLogPlayBatchBytes();
            
            nextMutation = new ArrayList<byte[]>(batchSize);

            while (nextMutation.size()<batchSize && batchBytesLimit>=0)
            {
                /* read the next valid RowMutation  */

                long claimedCRC32;
                byte[] bytes;
                try
                {
                    if ( reader.isEOF() ) {
                        if ( !nextMutation.isEmpty() ) // last batch of the segment can be smaller, but we cannot discard segment until all of its hints are confirmed
                            return true;
                        if ( !nextSegment() ) // batch is empty here, so we continue filling it, if there are more segments
                            return false;
                    }

                    if (logger.isDebugEnabled())
                        logger.debug("Reading mutation at " + reader.getFilePointer());

                    long length = reader.readLong();
                    // RowMutation must be at LEAST 10 bytes:
                    // 3 each for a non-empty Table and Key (including the 2-byte length from writeUTF), 4 bytes for column count.
                    // This prevents CRC by being fooled by special-case garbage in the file; see CASSANDRA-2128
                    if (length < 10 || length > Integer.MAX_VALUE)
                    {
                        // garbage at the EOF. Skipping.
                        logger.warn("Garbage detected in "+current+" at position "+reader.getFilePointer()+". Skipping rest of file");

                        current.getHeader().setReplayedPosition(reader.length());

                        if (nextSegment())
                            continue;
                        else
                            break;
                    }
                    bytes = new byte[(int) length]; // readlong can throw EOFException too
                    reader.readFully(bytes);
                    claimedCRC32 = reader.readLong();
                }
                catch (IOException e)
                {
                    if ( ! ( e instanceof EOFException ) )
                        logger.error("Cannot read "+current+". Skipping ",e);

                    // OK. this is EOF last CL entry didn't get completely written.  that's ok.
                    current.getHeader().setReplayedPosition(reader.getFilePointer());
                    if (nextSegment())
                        continue;
                    else
                        break;
                }

                Checksum checksum = new CRC32();
                checksum.update(bytes, 0, bytes.length);
                if (claimedCRC32 != checksum.getValue())
                {
                    // this part of the log must not have been fsynced.  probably the rest is bad too,
                    // but just in case there is no harm in trying them.
                    continue;
                }

                /* deserialize the commit log entry */
                nextMutation.add(bytes);
                batchBytesLimit-=bytes.length;
                
            }
            
            return !nextMutation.isEmpty();
        }
        
        /**
         * @return next mutation to deliver
         */
        public List<byte[]> next()
        {
            if (nextMutation())
            {
                currentMutation = nextMutation;
                nextMutation = null;
            }
            
            return currentMutation;
        }
        
        /* (non-Javadoc)
         * @see java.io.Closeable#close()
         */
        @Override
        public void close() throws IOException
        {
            if (current!=null)
            {
                current.writeHeader();
                reader.close();
                
                current = null;
                reader = null;
            }
        }
        

        @Override
        public boolean hasNext()
        {
            return nextMutation();
        }
        
        

        /**
         * Confirms successful delivery of current row mutation to destination. 
         * So this mutation could be omitted from future hinted handoffs.
         * (note, that a couple of confirmed mutations still could be sent twice and more)
         */
        @Override
        public void remove()
        {
            assert currentMutation !=null;

            current.getHeader().setReplayedPosition(reader.getFilePointer());
            
            unwrittenConfirmations+=currentMutation.size();

            if ( unwrittenConfirmations > 1000 && System.currentTimeMillis()-lastHeaderWrite > DatabaseDescriptor.getCommitLogSyncPeriod() )
            {
                try {
                    current.writeHeader();
                } catch (IOException e) {
                }
                
                unwrittenConfirmations = 0;
                lastHeaderWrite = System.currentTimeMillis();
            }
            
            currentMutation = null;
        }
    }

    /**
     * @return test only
     */
    public int getSegmentCount(InetAddress endpoint)
    {
        return getEndpointSegments( endpointToToken(endpoint) ).size();
    }

    /**
     * @param b
     */
    public static void setHintDelivery(boolean b)
    {
        HINT_DELIVERY_ON_SYNC  = b;
    }
    
}
