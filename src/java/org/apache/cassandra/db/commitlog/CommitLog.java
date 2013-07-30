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

package org.apache.cassandra.db.commitlog;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.FSWriteError;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLogSegment.CommitLogContext;
import org.apache.cassandra.io.DeletionService;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.CLibrary;
import org.xerial.snappy.Snappy;

/*
 * Commit Log tracks every write operation into the system. The aim
 * of the commit log is to be able to successfully recover data that was
 * not stored to disk via the Memtable. Every Commit Log maintains a
 * header represented by the abstraction CommitLogHeader. The header
 * contains a bit array and an array of longs and both the arrays are
 * of size, #column families for the Table, the Commit Log represents.
 *
 * Whenever a ColumnFamily is written to, for the first time its bit flag
 * is set to one in the CommitLogHeader. When it is flushed to disk by the
 * Memtable its corresponding bit in the header is set to zero. This helps
 * track which CommitLogs can be thrown away as a result of Memtable flushes.
 * Additionally, when a ColumnFamily is flushed and written to disk, its
 * entry in the array of longs is updated with the offset in the Commit Log
 * file where it was written. This helps speed up recovery since we can seek
 * to these offsets and start processing the commit log.
 *
 * Every Commit Log is rolled over everytime it reaches its threshold in size;
 * the new log inherits the "dirty" bits from the old.
 *
 * Over time there could be a number of commit logs that would be generated.
 * To allow cleaning up non-active commit logs, whenever we flush a column family and update its bit flag in
 * the active CL, we take the dirty bit array and bitwise & it with the headers of the older logs.
 * If the result is 0, then it is safe to remove the older file.  (Since the new CL
 * inherited the old's dirty bitflags, getting a zero for any given bit in the anding
 * means that either the CF was clean in the old CL or it has been flushed since the
 * switch in the new.)
 */
public class CommitLog
{
    private static final int MAX_OUTSTANDING_REPLAY_COUNT = 1024;
    private static volatile int SEGMENT_SIZE = 128*1024*1024; // roll after log gets this big
    static String COMPRESSION_EXTENSION = ".z";

    private static final Logger logger = Logger.getLogger(CommitLog.class);

    public static CommitLog instance()
    {
        return CLHandle.instance;
    }

    private static class CLHandle
    {
        public static final CommitLog instance = new CommitLog();
    }

    private final Deque<CommitLogSegment> segments = new ArrayDeque<CommitLogSegment>();

    public static void setSegmentSize(int size)
    {
        SEGMENT_SIZE = size;
    }

    public int getSegmentCount()
    {
        return segments.size();
    }

    private final ICommitLogExecutorService executor;

    /**
     * param @ table - name of table for which we are maintaining
     *                 this commit log.
     * param @ recoverymode - is commit log being instantiated in
     *                        in recovery mode.
    */
    private CommitLog()
    {
        // all old segments are recovered and deleted before CommitLog is instantiated.
        // All we need to do is create a new one.
        int cfSize = Table.TableMetadata.getColumnFamilyCount();
        segments.add(new CommitLogSegment(cfSize));
        
        if (DatabaseDescriptor.getCommitLogSync() == DatabaseDescriptor.CommitLogSync.periodic)
        {
            executor = new PeriodicCommitLogExecutorService();
            final Callable syncer = new Callable()
            {
                public Object call() throws Exception
                {
                    sync();
                    return null;
                }
            };

            new Thread(new Runnable()
            {
                public void run()
                {
                    while (true)
                    {
                        try
                        {
                            executor.submit(syncer).get();
                            watchMaxCommitLogs();
                            Thread.sleep(DatabaseDescriptor.getCommitLogSyncPeriod());
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
            }, "PERIODIC-COMMIT-LOG-SYNCER").start();
        }
        else
        {
            executor = new BatchCommitLogExecutorService();
        }
    }

    public static void recover() throws IOException
    {
        recover(Long.MAX_VALUE);
    }
    
    public static void recover(long maxReplayTimestamp) throws IOException
    {
        String directory = DatabaseDescriptor.getLogFileLocation();
        File file = new File(directory);
        File[] files = file.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.matches("CommitLog-\\d+\\.log(\\.z)?");
            }
        });
        if (files.length == 0)
            return;

        Arrays.sort(files, new FileUtils.FileComparator());
        logger.info("Replaying " + StringUtils.join(files, ", "));
        recover(files,false,maxReplayTimestamp);
        
        archiveLogFiles(files);
        
        logger.info("Log replay complete");
    }

    /**
     * This is forced version of log replay. It is needed when data files 
     * are not in sync with commit log files headers, e.g. when youre replaying
     * shipped logs using file copies restored from backups.
     * @param maxReplayTimestamp 
     * 
     * @throws IOException
     */
    public static void forcedRecover(long maxReplayTimestamp) throws IOException
    {
        String directory = DatabaseDescriptor.getLogFileLocation();
        File file = new File(directory);
        File[] files = file.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                // throw out anything that starts with dot.
                return !name.matches("\\..*");
            }
        });
        if (files.length == 0)
            return;

        Arrays.sort(files, new FileUtils.FileComparator());
        logger.info("Forced replay " + StringUtils.join(files, ", "));
        recover(files,true,maxReplayTimestamp);
        
        FileUtils.delete(files);
        logger.info("Forced Log replay complete");
    }
    
    public static void recover(File[] clogs, boolean forced) throws IOException
    {
        recover(clogs,forced,Long.MAX_VALUE);
    }
    
    public static void recover(File[] clogs, boolean forced, long maxReplayTimestamp) throws IOException
    {
        Set<Table> tablesRecovered = new HashSet<Table>();
        List<Future<?>> futures = new ArrayList<Future<?>>();
        REPLAYLOOP:
        for (File file : clogs)
        {
            // empty log file - just removing it
            if (file.length()==0)
            {
                continue;
            }
            
            int bufferSize = (int)Math.min(file.length(), 32 * 1024 * 1024);
            BufferedRandomAccessFile reader = new BufferedRandomAccessFile(file.getAbsolutePath(), "r", bufferSize);

            try
            {
                CommitLogHeader clHeader = null;
                int replayPosition = 0;
                if (!forced)
                {
                    String headerPath = CommitLogHeader.getHeaderPathFromSegmentPath(file.getAbsolutePath());
                    try
                    {
                        clHeader = CommitLogHeader.readCommitLogHeader(headerPath);
                        replayPosition = CommitLogHeader.getLowestPosition(clHeader);
                    }
                    catch (IOException ioe)
                    {
                        logger.info(headerPath + " incomplete, missing or corrupt.  Everything is ok, don't panic.  CommitLog will be replayed from the beginning");
                        logger.debug("exception was", ioe);
                    }
                    
                    if (replayPosition < 0 || replayPosition > reader.length())
                    {
                        // replayPosition > reader.length() can happen if some data gets flushed before it is written to the commitlog
                        // (see https://issues.apache.org/jira/browse/CASSANDRA-2285)
                        logger.debug("skipping replay of fully-flushed "+ file);
                        continue;
                    }
                }

                /* seek to the lowest position where any CF has non-flushed data 
                 * if replay was forced - reading all records, regardless of commit log header 
                 */
                reader.seek(replayPosition);
                if (logger.isDebugEnabled())
                    logger.debug("Replaying " + file + " starting at " + replayPosition);

                if (forced)
                    logger.info("Replaying " + file + " starting at " + replayPosition);

                boolean logFileCompression = file.getName().endsWith(COMPRESSION_EXTENSION);
                if (logFileCompression && logger.isDebugEnabled())
                    logger.debug("Filename: " + file + " ends with \"" + COMPRESSION_EXTENSION + "\", expecting compression");

                /* read the logs populate RowMutation and apply */
                while (!reader.isEOF())
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Reading mutation at " + reader.getFilePointer());

                    long claimedCRC32;
                    byte[] bytes;
                    try
                    {
                        long length = reader.readLong();
                        // RowMutation must be at LEAST 10 bytes:
                        // 3 each for a non-empty Table and Key (including the 2-byte length from writeUTF), 4 bytes for column count.
                        // This prevents CRC by being fooled by special-case garbage in the file; see CASSANDRA-2128
                        if (length < 10 || length > Integer.MAX_VALUE)
                            break;
                        bytes = new byte[(int) length]; // readlong can throw EOFException too
                        reader.readFully(bytes);
                        claimedCRC32 = reader.readLong();
                    }
                    catch (EOFException e)
                    {
                        // last CL entry didn't get completely written.  that's ok.
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

                    if (logFileCompression) {
                        bytes = Snappy.uncompress(bytes);
                    }

                    ByteArrayInputStream bufIn = new ByteArrayInputStream(bytes);
                    
                    /* deserialize the commit log entry */
                    final RowMutation rm = RowMutation.serializer().deserialize(new DataInputStream(bufIn));
                    
                    if (maxReplayTimestamp<Long.MAX_VALUE)
                    {
                        // inspecting mutation if it has any column with timestamp value greater than max
                        boolean timestampLimitReached=false;
                        for (ColumnFamily cf : rm.getColumnFamilies()) 
                        {
                            if (cf.isMarkedForDelete() && cf.getMarkedForDeleteAt()>maxReplayTimestamp)
                            {
                                timestampLimitReached = true;
                                break;
                            }
                            
                            for (IColumn c : cf.getSortedColumns())
                            {
                                if ( (c.isMarkedForDelete() && c.getMarkedForDeleteAt()>maxReplayTimestamp) || c.timestamp()>maxReplayTimestamp)
                                {
                                    timestampLimitReached = true;
                                    break;
                                }
                            }
                        }
                        
                        if (timestampLimitReached)
                        {
                            logger.info("Stopped replay at "+file+", position "+reader.getFilePointer()+" - mutation "+rm+" has timestamp >"+maxReplayTimestamp);
                            break REPLAYLOOP;
                        }
                        
                    }
                    
                    if (logger.isDebugEnabled())
                        logger.debug(String.format("replaying mutation for %s.%s: %s",
                                                    rm.getTable(),
                                                    rm.key(),
                                                    "{" + StringUtils.join(rm.getColumnFamilies(), ", ") + "}"));
                    final Table table = Table.open(rm.getTable());
                    tablesRecovered.add(table);
                    final Collection<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>(rm.getColumnFamilies());
                    final long entryLocation = reader.getFilePointer();
                    Runnable runnable;

                    if (forced || clHeader == null)
                    {
                        runnable = new WrappedRunnable()
                        {
                            public void runMayThrow() throws IOException
                            {
                                if (!rm.isEmpty())
                                {
                                    Table.open(rm.getTable()).apply(rm, null, false);
                                }
                            }
                        };
                    } else {
                        final CommitLogHeader finalHeader = clHeader;
                        runnable = new WrappedRunnable()
                        {
                            public void runMayThrow() throws IOException
                            {
                                /* remove column families that have already been flushed before applying the rest */
                                for (ColumnFamily columnFamily : columnFamilies)
                                {
                                    int id = table.getColumnFamilyId(columnFamily.name());
                                    if (!finalHeader.isDirty(id) || entryLocation <= finalHeader.getPosition(id))
                                    {
                                        rm.removeColumnFamily(columnFamily);
                                    }
                                }
                                if (!rm.isEmpty())
                                {
                                    Table.open(rm.getTable()).apply(rm, null, false);
                                }
                            }
                        };
                    }
                    futures.add(StageManager.getStage(StageManager.MUTATION_STAGE).submit(runnable));
                    if (futures.size() > MAX_OUTSTANDING_REPLAY_COUNT)
                    {
                        FBUtilities.waitOnFutures(futures);
                        futures.clear();
                    }
                }
            }
            finally
            {
                reader.close();
                logger.info("Finished reading " + file);
            }
        }


        // wait for all the writes to finish on the mutation stage
        FBUtilities.waitOnFutures(futures);
        logger.debug("Finished waiting on mutations from recovery");

        // flush replayed tables
        futures.clear();
        for (Table table : tablesRecovered)
            futures.addAll(table.flush());
        FBUtilities.waitOnFutures(futures);
        logger.info("Recovery complete");
    }

    private CommitLogSegment currentSegment()
    {
        return segments.getLast();
    }
    
    public Future<CommitLogContext> getContext() 
    {
        Callable<CommitLogSegment.CommitLogContext> task = new Callable<CommitLogSegment.CommitLogContext>()
        {
            public CommitLogSegment.CommitLogContext call() throws Exception
            {
                return currentSegment().getContext();
            }
        };
//        try
//        {
            return executor.submit(task);
//        }
//        catch (InterruptedException e)
//        {
//            throw new RuntimeException(e);
//        }
//        catch (ExecutionException e)
//        {
//            throw new RuntimeException(e);
//        }
    }

    /*
     * Adds the specified row to the commit log. This method will reset the
     * file offset to what it is before the start of the operation in case
     * of any problems. This way we can assume that the subsequent commit log
     * entry will override the garbage left over by the previous write.
    */
    public void add(RowMutation rowMutation, Object serializedRow) 
    {
        executor.add(new LogRecordAdder(rowMutation, serializedRow));
    }

    /*
     * This is called on Memtable flush to add to the commit log
     * a token indicating that this column family has been flushed.
     * The bit flag associated with this column family is set in the
     * header and this is used to decide if the log file can be deleted.
    */
    public void discardCompletedSegments(final String tableName, final String cf, final CommitLogSegment.CommitLogContext context) 
    {
        Callable task = new Callable()
        {
            public Object call() throws IOException
            {
                int id = Table.open(tableName).getColumnFamilyId(cf);
                discardCompletedSegmentsInternal(context, id);
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

    /**
     * Delete log segments whose contents have been turned into SSTables. NOT threadsafe.
     *
     * param @ context The commitLog context .
     * param @ id id of the columnFamily being flushed to disk.
     *
    */
    private void discardCompletedSegmentsInternal(CommitLogSegment.CommitLogContext context, int id) 
    {
        if (logger.isDebugEnabled())
            logger.debug("discard completed log segments for " + context + ", column family " + id + ". CFIDs are " + Table.TableMetadata.getColumnFamilyIDString());

        /*
         * Loop through all the commit log files in the history. Now process
         * all files that are older than the one in the context. For each of
         * these files the header needs to modified by resetting the dirty
         * bit corresponding to the flushed CF.
        */
        Iterator<CommitLogSegment> iter = segments.iterator();
        while (iter.hasNext())
        {
            CommitLogSegment segment = iter.next();
            CommitLogHeader header = segment.getHeader();
            if (segment.equals(context.getSegment())) // is this segment grabbed at memtable flush ?
            {
                // Only unmark this segment if there were not write since the
                // ReplayPosition was grabbed.
                if (header.turnOffIfNotWritten(id, context.position)) {
                    maybeDiscardSegment(iter,segment,header);
                }
                
                break;
            }
            
            if (header.isDirty(id)) {
                header.turnOff(id);
    
                maybeDiscardSegment(iter, segment, header);
            }
        }
        
    }

    /**
     * For use from tests.
     */
    public Future<?> watchMaxCommitLogs()
    {
        if (DatabaseDescriptor.getMaxCommitLogSegmentsActive()>0) {
            final Callable maxCommitLogsWatcher = new Callable()
            {
                public Object call() throws Exception
                {
                    watchMaxCommitLogsInternal();
                    return null;
                }
            };


            return executor.submit(maxCommitLogsWatcher);
        }
        
        return null;
    }

    private void watchMaxCommitLogsInternal()
    {
        if (DatabaseDescriptor.getMaxCommitLogSegmentsActive()>0 && segments.size()>DatabaseDescriptor.getMaxCommitLogSegmentsActive()) {
            // trying to flush memtables, which marked dirty the oldest open segment
            CommitLogSegment oldestSegment = segments.getFirst();
            
            int cfid = oldestSegment.getHeader().getFirstDirtyCFId();
            
            assert cfid >=0 : "Commit Log Segment is clear, but open: "+oldestSegment+", header: "+oldestSegment.getHeader();
            
            final ColumnFamilyStore columnFamilyStore = Table.getColumnFamilyStore(cfid);
            
            // only submit flush, is this store is not currently flushing
            if (columnFamilyStore.getMemtablesPendingFlush().isEmpty()) {
                logger.info("Current open commit log segment count "+segments.size()+">"+DatabaseDescriptor.getMaxCommitLogSegmentsActive()+". Forcing flush of "+columnFamilyStore.columnFamily_+" to close "+oldestSegment);

                new Thread(
                        new WrappedRunnable()
                        {

                            @Override
                            protected void runMayThrow() throws Exception
                            {
                                columnFamilyStore.forceFlush();
                            }
                        },
                        "Flush submit of "+columnFamilyStore).start();
            }
        }
    }

    private void maybeDiscardSegment(Iterator<CommitLogSegment> iter,
            CommitLogSegment segment, CommitLogHeader header)
    {
        if ( header.isSafeToDelete() && segment!=currentSegment() )
        {

            if (DatabaseDescriptor.isLogArchiveActive())
            {
                logger.info("Archiving obsolete commit log:" + segment);
                segment.close();

                archiveLogfile(segment.getPath());
            }
            else
            {
                logger.info("Discarding obsolete commit log:" + segment);
                segment.close();
            }
            
            segment.submitDelete();
         
            // usually this will be the first (remaining) segment, but not always, if segment A contains
            // writes to a CF that is unflushed but is followed by segment B whose CFs are all flushed.
            iter.remove();
        }
        else
        {
            if (logger.isDebugEnabled())
                logger.debug("Not safe to delete commit log " + segment + "; dirty is " + header.dirtyString());

            try {
                segment.writeHeader();
            } catch (IOException e) {
                throw new FSWriteError(e);
            }
        }
    }
    
    private static void archiveLogFiles(File[] files) throws IOException
    {
        if (DatabaseDescriptor.isLogArchiveActive())
        {
            for (File file : files) {
                if (file.length()>0)
                {
                    logger.info("Archiving obsolete commit log:" + file);
                    archiveLogfile(file.getPath());
                }
            }

        }
        
        // MM: we dont archive .headers, because for archived files all CFs are clean
        // and anyway we'll want to replay all records in archived log
        for (File f : files)
        {
            FileUtils.delete(CommitLogHeader.getHeaderPathFromSegmentPath(f.getAbsolutePath())); // may not actually exist
            if (!f.delete())
                logger.error("Unable to remove " + f + "; you should remove it manually or next restart will replay it again (harmless, but time-consuming)");
        }

    }

    /**
     * @param oldLogFile
     */
    private static void archiveLogfile(String oldLogFile)
    {
        try {
            int lastSlash=oldLogFile.lastIndexOf( File.separator );
            String archivePath = DatabaseDescriptor.getLogArchiveDestination() + oldLogFile.substring(lastSlash);
            
            CLibrary.createHardLink(
                    new File( oldLogFile ),
                    new File( archivePath )
            );
            
            logger.info("Log file archived "+archivePath);
            
        } catch (IOException e) 
        {
            logger.warn("Cannot make hard link to "+oldLogFile, e);
        }
    }

    void sync() 
    {
        currentSegment().sync();
    }
    
    /**
     * for unit tests
     */
    public void resetUnsafe()
    {
        for (CommitLogSegment segment : segments)
            segment.close();
        segments.clear();
        int cfSize = Table.TableMetadata.getColumnFamilyCount();

        segments.add(new CommitLogSegment(cfSize));
    }

    // for tests mainly
    public int segmentsCount()
    {
        return segments.size();
    }


    public void forceNewSegment()
    {
        Callable task = new Callable()
        {
            public Object call() throws Exception
            {
                sync();
                segments.add(new CommitLogSegment(currentSegment().getHeader().getColumnFamilyCount()));
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

    // TODO this should be a Runnable since it doesn't actually return anything, but it's difficult to do that
    // without breaking the fragile CheaterFutureTask in BatchCLES.
    class LogRecordAdder implements Callable, Runnable
    {
        final RowMutation rowMutation;
        final Object serializedRow;

        LogRecordAdder(RowMutation rm, Object serializedRow)
        {
            this.rowMutation = rm;
            this.serializedRow = serializedRow;
        }

        public void run()
        {
            try
            {
                currentSegment().write(rowMutation, serializedRow);
                // roll log if necessary
                if (currentSegment().length() >= SEGMENT_SIZE)
                {
                    sync();
                    segments.add(new CommitLogSegment(currentSegment().getHeader().getColumnFamilyCount()));
                }
            }
            catch (IOException e)
            {
                throw new FSWriteError(e);
            }
        }

        public Object call() throws Exception
        {
            run();
            return null;
        }
    }
}
