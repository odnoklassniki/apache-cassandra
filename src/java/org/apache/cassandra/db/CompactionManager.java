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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.management.*;

import org.apache.log4j.Logger;
import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.collections.iterators.CollatingIterator;
import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.proc.IRowProcessor;
import org.apache.cassandra.db.proc.RemoveDeletedRowProcessor;
import org.apache.cassandra.db.proc.RowProcessorChain;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.*;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.AntiEntropyService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

public class CompactionManager implements CompactionManagerMBean
{
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.db:type=CompactionManager";
    private static final Logger logger = Logger.getLogger(CompactionManager.class);
    public static final CompactionManager instance;

    private int minimumCompactionThreshold = DatabaseDescriptor.getMinimumCompactionThreshold();
    private int maximumCompactionThreshold = DatabaseDescriptor.getMaximumCompactionThreshold();

    static
    {
        instance = new CompactionManager();
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_OBJECT_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private CompactionExecutor executor = new CompactionExecutor();
    private Map<ColumnFamilyStore, Integer> estimatedCompactions = new NonBlockingHashMap<ColumnFamilyStore, Integer>();

    /**
     * Call this whenever a compaction might be needed on the given columnfamily.
     * It's okay to over-call (within reason) since the compactions are single-threaded,
     * and if a call is unnecessary, it will just be no-oped in the bucketing phase.
     */
    public Future<Integer> submitMinorIfNeeded(final ColumnFamilyStore cfs)
    {
        Callable<Integer> callable = new Callable<Integer>()
        {
            public Integer call() throws IOException
            {
                if (minimumCompactionThreshold <= 0 || maximumCompactionThreshold <= 0)
                {
                    logger.debug("Compaction is currently disabled.");
                    return 0;
                }
                logger.debug("Checking to see if compaction of " + cfs.columnFamily_ + " would be useful");

                Set<List<SSTableReader>> buckets = getBuckets(
                        convertSSTablesToPairs(cfs.getSSTables()), 50L * 1024L * 1024L);
                updateEstimateFor(cfs, buckets);
                
                for (List<SSTableReader> sstables : buckets)
                {
                    if (sstables.size() >= minimumCompactionThreshold)
                    {
                        // if we have too many to compact all at once, compact older ones first -- this avoids
                        // re-compacting files we just created.
                        Collections.sort(sstables);
                        return doCompaction(cfs, sstables.subList(0, Math.min(sstables.size(), maximumCompactionThreshold)), getDefaultGcBefore(cfs));
                    }
                }
                return 0;
            }
        };
        return executor.submit(callable);
    }

    private void updateEstimateFor(ColumnFamilyStore cfs, Set<List<SSTableReader>> buckets)
    {
        int n = 0;
        for (List<SSTableReader> sstables : buckets)
        {
            if (sstables.size() >= minimumCompactionThreshold)
            {
                n += 1 + sstables.size() / (maximumCompactionThreshold - minimumCompactionThreshold);
            }
        }
        estimatedCompactions.put(cfs, n);
    }

    public Future<Object> submitCleanup(final ColumnFamilyStore cfStore)
    {
        Callable<Object> runnable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                if (!StorageService.instance.isInLocalRange(cfStore))
                {
                    // this column family is completely out of local node range.
                    // so we can just remove its files
                    doCleanupDelete( cfStore );
                } else
                {
                    CFMetaData cfMetaData = DatabaseDescriptor.getCFMetaData(cfStore.getTable().name, cfStore.getColumnFamilyName());
                    if (!cfMetaData.domainSplit) // domain split CF dont need cleanup: if its 1st key is in local range, every key in it is
                        doCleanupCompaction(cfStore);
                    else
                        logger.info("Skipping cleanup of "+cfStore.getColumnFamilyName()+" - its data do belong to this node");
                }
                return this;
            }
        };
        return executor.submit(runnable);
    }

    public Future<List<String>> submitAnticompaction(final ColumnFamilyStore cfStore, final Collection<Range> ranges, final InetAddress target)
    {
        Callable<List<String>> callable = new Callable<List<String>>()
        {
            public List<String> call() throws IOException
            {
                // we dont need to do anticompaction, if 
                // CF os domain split and its whole domain fits inside at least one of 
                // requested ranges
                CFMetaData cfMetaData = DatabaseDescriptor.getCFMetaData(cfStore.getTable().name, cfStore.getColumnFamilyName());
                Range cfRange = new Range(cfMetaData.domainMinToken,cfMetaData.domainMaxToken);
                if ( cfMetaData.domainSplit && Range.isRangeInRanges(cfRange, ranges))
                {
                    logger.debug(cfStore.getColumnFamilyName()+"' range "+cfRange+" contained fully in "+ranges);
                
                    return doLinkReaders( cfStore, cfStore.getSSTables(), target);
        
                } else
                {
                    if ( cfMetaData.domainSplit && !Range.isTokenInRanges(cfMetaData.domainMinToken, ranges) )
                    {
                        logger.debug(cfStore.getColumnFamilyName()+"' range "+cfRange+" is completely out of "+ranges);
                    
                        return Collections.emptyList(); // this CF is out of ranges completely
                    
                    } else
                        return doAntiCompaction(cfStore, cfStore.getSSTables(), ranges, target);
                }
            }
        };
        
        return executor.submit(callable);
    }

    public Future submitMajor(final ColumnFamilyStore cfStore)
    {
        return submitMajor(cfStore, 0, getDefaultGcBefore(cfStore));
    }

    public Future submitMajor(final ColumnFamilyStore cfStore, final long skip, final int gcBefore)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                Collection<SSTableReader> sstables;
                if (skip > 0)
                {
                    sstables = new ArrayList<SSTableReader>();
                    for (SSTableReader sstable : cfStore.getSSTables())
                    {
                        if (sstable.length() < skip * 1024L * 1024L * 1024L)
                        {
                            sstables.add(sstable);
                        }
                    }
                }
                else
                {
                    sstables = cfStore.getSSTables();
                }

                doCompaction(cfStore, sstables, gcBefore);
                return this;
            }
        };
        return executor.submit(callable);
    }

    public Future submitValidation(final ColumnFamilyStore cfStore, final AntiEntropyService.Validator validator)
    {
        Callable<Object> callable = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                doValidationCompaction(cfStore, validator);
                return this;
            }
        };
        return executor.submit(callable);
    }

    /**
     * Gets the minimum number of sstables in queue before compaction kicks off
     */
    public int getMinimumCompactionThreshold()
    {
        return minimumCompactionThreshold;
    }

    /**
     * Sets the minimum number of sstables in queue before compaction kicks off
     */
    public void setMinimumCompactionThreshold(int threshold)
    {
        minimumCompactionThreshold = threshold;
    }

    /**
     * Gets the maximum number of sstables in queue before compaction kicks off
     */
    public int getMaximumCompactionThreshold()
    {
        return maximumCompactionThreshold;
    }

    /**
     * Sets the maximum number of sstables in queue before compaction kicks off
     */
    public void setMaximumCompactionThreshold(int threshold)
    {
        maximumCompactionThreshold = threshold;
    }

    public void disableAutoCompaction()
    {
        minimumCompactionThreshold = 0;
        maximumCompactionThreshold = 0;
    }

    /**
     * For internal use and testing only.  The rest of the system should go through the submit* methods,
     * which are properly serialized.
     */
    int doCompaction(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, int gcBefore) throws IOException
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        Table table = cfs.getTable();
        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            table.snapshot("compact-" + cfs.columnFamily_);
        logger.info("Compacting [" + StringUtils.join(sstables, ",") + "]");
        String compactionFileLocation = cfs.getDataFileLocation(cfs.getExpectedCompactedFileSize(sstables));
        // If the compaction file path is null that means we have no space left for this compaction.
        // try again w/o the largest one.
        List<SSTableReader> smallerSSTables = new ArrayList<SSTableReader>(sstables);
        while (compactionFileLocation == null && smallerSSTables.size() > 1)
        {
            logger.warn("insufficient space to compact all requested files " + StringUtils.join(smallerSSTables, ", "));
            smallerSSTables.remove(cfs.getMaxSizeFile(smallerSSTables));
            compactionFileLocation = cfs.getDataFileLocation(cfs.getExpectedCompactedFileSize(smallerSSTables));
        }
        if (compactionFileLocation == null)
        {
            logger.error("insufficient space to compact even the two smallest files, aborting");
            return 0;
        }
        sstables = smallerSSTables;

        // new sstables from flush can be added during a compaction, but only the compaction can remove them,
        // so in our single-threaded compaction world this is a valid way of determining if we're compacting
        // all the sstables (that existed when we started)
        boolean major = cfs.isCompleteSSTables(sstables);

        long startTime = System.currentTimeMillis();
        long totalkeysWritten = 0;

        boolean columnBloom = cfs.metadata.bloomColumns;
        
        final long expectedBloomFilterSize = Math.max(DatabaseDescriptor.getIndexInterval(), SSTableReader.getApproximateKeyCount(sstables, columnBloom));
        if (logger.isDebugEnabled())
          logger.debug("Expected bloom filter size : " + expectedBloomFilterSize);

        IRowProcessor chain = new RowProcessorChain().add( new RemoveDeletedRowProcessor(gcBefore) ).addAll(cfs.metadata.rowProcessors).build();
        
        String newFilename = new File(compactionFileLocation, cfs.getTempSSTableFileName()).getAbsolutePath();
        CompactionWriterIterator ci = new CompactionWriterIterator(cfs, sstables, chain, major, newFilename, expectedBloomFilterSize ); 
        Iterator<CompactionIterator.CompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());

        executor.beginCompaction(cfs, ci);

        Map<DecoratedKey, SSTable.PositionSize> cachedKeys = new HashMap<DecoratedKey, SSTable.PositionSize>();
        boolean preheatKeyCache = Boolean.getBoolean("compaction_preheat_key_cache");

        try
        {
            if (!nni.hasNext())
            {
                // don't mark compacted in the finally block, since if there _is_ nondeleted data,
                // we need to sync it (via closeAndOpen) first, so there is no period during which
                // a crash could cause data loss.
                cfs.markCompacted(sstables);
                return 0;
            }

            
            while (nni.hasNext())
            {
                CompactionIterator.CompactedRow row = nni.next();
                totalkeysWritten++;

                if (row.rowSize > DatabaseDescriptor.getRowWarningThreshold())
                    logger.warn("Large row " + row.key.key + " in " + cfs.getColumnFamilyName() + " " +  row.rowSize + " bytes");
                cfs.addToCompactedRowStats(row.rowSize);

                if (preheatKeyCache)
                {
                    for (SSTableReader sstable : sstables)
                    {
                        if (sstable.getCachedPosition(row.key) != null)
                        {
                            cachedKeys.put(row.key, new SSTable.PositionSize(row.rowPosition, row.rowSize));
                            break;
                        }
                    }
                }
            }
        }
        finally
        {
            ci.close();
        }

        SSTableReader ssTable = ci.closeAndOpenReader();
        cfs.replaceCompactedSSTables(sstables, Arrays.asList(ssTable));
        for (Entry<DecoratedKey, SSTable.PositionSize> entry : cachedKeys.entrySet()) // empty if preheat is off
            ssTable.cacheKey(entry.getKey(), entry.getValue());
        submitMinorIfNeeded(cfs);

        String format = "Compacted to %s.  %d/%d bytes for %d keys.  Time: %dms";
        long dTime = System.currentTimeMillis() - startTime;
        logger.info(String.format(format, ssTable.getFilename() , SSTable.getTotalBytes(sstables), ssTable.length(), totalkeysWritten, dTime));
        return sstables.size();
    }

    private SSTableWriter antiCompactionHelper(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, Collection<Range> ranges, InetAddress target)
            throws IOException
    {
        Table table = cfs.getTable();
        logger.info("AntiCompacting [" + StringUtils.join(sstables, ",") + "]");
        // Calculate the expected compacted filesize
        long expectedRangeFileSize = cfs.getExpectedCompactedFileSize(sstables) / 2;
        String compactionFileLocation = cfs.getDataFileLocation(expectedRangeFileSize);
        if (compactionFileLocation == null)
        {
            throw new UnsupportedOperationException("disk full");
        }
        if (target != null)
        {
            // compacting for streaming: send to subdirectory
            compactionFileLocation = compactionFileLocation + File.separator + DatabaseDescriptor.STREAMING_SUBDIR;
        }

        long totalKeysWritten = 0;
        long startTime = System.currentTimeMillis();

        boolean columnBloom = cfs.metadata.bloomColumns;
        
        long expectedBloomFilterSize = Math.max(DatabaseDescriptor.getIndexInterval(), (SSTableReader.getApproximateKeyCount(sstables, columnBloom) / 2));
        if (logger.isDebugEnabled())
          logger.debug("Expected bloom filter size : " + expectedBloomFilterSize);

        FileUtils.createDirectory(compactionFileLocation);
        String newFilename = new File(compactionFileLocation, cfs.getTempSSTableFileName()).getAbsolutePath();

        AntiCompactionIterator ci = new AntiCompactionIterator(cfs, sstables, ranges, getDefaultGcBefore(cfs), cfs.isCompleteSSTables(sstables),newFilename, expectedBloomFilterSize);
        Iterator<CompactionIterator.CompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());
        executor.beginCompaction(cfs, ci);

        try
        {
            while (nni.hasNext())
            {
                CompactionIterator.CompactedRow row = nni.next();
                totalKeysWritten++;
            }
        }
        finally
        {
            ci.close();
        }
        
        if (ci.writer() != null) {
            List<String> filenames = ci.writer().getAllFilenames();
            String format = "AntiCompacted to %s.  %d/%d bytes for %d keys.  Time: %dms.";
            long dTime = System.currentTimeMillis() - startTime;
            long length = new File(ci.writer().getFilename()).length(); 
            logger.info(String.format(format, ci.writer().getFilename(), SSTable.getTotalBytes(sstables), length, totalKeysWritten, dTime));
        }
        
        return ci.writer();
    }

    /**
     * This function is used to do the anti compaction process.  It spits out a file which has keys
     * that belong to a given range. If the target is not specified it spits out the file as a compacted file with the
     * unnecessary ranges wiped out.
     *
     * @param cfs
     * @param sstables
     * @param ranges
     * @param target
     * @return
     * @throws java.io.IOException
     */
    private List<String> doAntiCompaction(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, Collection<Range> ranges, InetAddress target)
            throws IOException
    {
        List<String> filenames = new ArrayList<String>(SSTable.FILES_ON_DISK);
        SSTableWriter writer = antiCompactionHelper(cfs, sstables, ranges, target);
        if (writer != null)
        {
            writer.close();
            filenames = writer.getAllFilenames();
        }
        return filenames;
    }
    
    /**
     * This is alternative for anticompaction for domain split CFs. If such CF fits requested range completely
     * all we should do is to transfer all its sstables and wera done (no need to read and write new file - we
     * just making hardlinks to existing files)
     * 
     * @param cfStore
     * @param ssTables
     * @param target
     * @return
     */
    protected List<String> doLinkReaders(ColumnFamilyStore cfStore,
            Collection<SSTableReader> ssTables, InetAddress target)
    {
        try {
            int ssCount = ssTables.size();
            List<String> result = new ArrayList<String>(ssCount* SSTable.FILES_ON_DISK);
            for (SSTableReader reader : ssTables) 
            {
                List<String> allFilenames = reader.getAllFilenames();
                for (String ssfile : allFilenames) {
                    // hard link must be located on the same disk as original file do
                    File f = new File(ssfile);
                    String streamingDir = f.getParentFile().getAbsolutePath() +  File.separator + DatabaseDescriptor.STREAMING_SUBDIR;
                    FileUtils.createDirectory(streamingDir);
                    
                    File linkF= new File( streamingDir, f.getName() );
                    
                    CLibrary.createHardLink(f, linkF);
                    
                    if (logger.isDebugEnabled())
                        logger.debug("Linked "+f+" to "+linkF);
                    
                    result.add( linkF.getAbsolutePath() );
                    
                }
            }
            
            logger.info("Skipped anticompaction and hard-linked ["+StringUtils.join(ssTables, ",")+"] for transfer to "+target);
            return result;
        } catch (IOException e) {
            throw new UnsupportedOperationException("Cannot create hardlinks",e);
        }
    }

    

    /**
     * Like doAntiCompaction(), but returns an List of SSTableReaders instead of a list of filenames.
     * @throws java.io.IOException
     */
    private List<SSTableReader> doAntiCompactionReturnReaders(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, Collection<Range> ranges, InetAddress target)
            throws IOException
    {
        List<SSTableReader> results = new ArrayList<SSTableReader>(1);
        SSTableWriter writer = antiCompactionHelper(cfs, sstables, ranges, target);
        if (writer != null)
        {
            results.add(writer.closeAndOpenReader());
        }
        return results;
    }

    /**
     * This function goes over each file and removes the keys that the node is not responsible for
     * and only keeps keys that this node is responsible for.
     *
     * @throws IOException
     */
    private void doCleanupCompaction(ColumnFamilyStore cfs) throws IOException
    {
        Collection<SSTableReader> originalSSTables = cfs.getSSTables();
        List<SSTableReader> sstables = doAntiCompactionReturnReaders(cfs, originalSSTables, StorageService.instance.getLocalRanges(cfs.getTable().name), null);
        if (!sstables.isEmpty())
        {
            cfs.replaceCompactedSSTables(originalSSTables, sstables);
        }
    }

    /**
     * Cleaning up sstables by removing it.
     * 
     * @param cfStore
     * @throws IOException 
     */
    protected void doCleanupDelete(ColumnFamilyStore cfs) throws IOException
    {
        try {
            cfs.forceBlockingFlush();
        } catch (Exception e) {
            logger.error("Flush prior cleanup failed. Still continuing with cleanup",e);
        }
        Collection<SSTableReader> sstables = cfs.getSSTables();
        
        if (sstables.isEmpty())
            return;
        
        logger.info("Removing sstables ["+StringUtils.join(sstables, ",")+"] due to cleanup");
        
        cfs.markCompacted(sstables);
        
        
        
    }

    /**
     * Performs a readonly "compaction" of all sstables in order to validate complete rows,
     * but without writing the merge result
     */
    private void doValidationCompaction(ColumnFamilyStore cfs, AntiEntropyService.Validator validator) throws IOException
    {
        Collection<SSTableReader> sstables = cfs.getSSTables();
        CompactionIterator ci = new CompactionIterator(cfs, sstables,  new RemoveDeletedRowProcessor(getDefaultGcBefore(cfs)), true)
        {
            /* (non-Javadoc)
             * @see org.apache.cassandra.io.CompactionIterator#startRowWrite(org.apache.cassandra.db.DecoratedKey, int)
             */
            @Override
            protected CompactedRow startRowWrite(DecoratedKey key, int cfSize)
            {
                return new CompactedRow(key, new DataOutputBuffer(), 0l);
            }
            
            protected void finishRowWrite(CompactedRow compactedRow) {};
        };
        
        executor.beginCompaction(cfs, ci);
        try
        {
            Iterator<CompactionIterator.CompactedRow> nni = new FilterIterator(ci, PredicateUtils.notNullPredicate());

            // validate the CF as we iterate over it
            validator.prepare(cfs);
            while (nni.hasNext())
            {
                CompactionIterator.CompactedRow row = nni.next();
                validator.add(row);
            }
            validator.complete();
        }
        finally
        {
            ci.close();
        }
    }

    /*
    * Group files of similar size into buckets.
    */
    static <T> Set<List<T>> getBuckets(Iterable<Pair<T, Long>> files, long min)
    {
        // Sort the list in order to get deterministic results during the grouping below
        List<Pair<T, Long>> sortedFiles = new ArrayList<Pair<T, Long>>();
        for (Pair<T, Long> pair: files)
            sortedFiles.add(pair);

        Collections.sort(sortedFiles, new Comparator<Pair<T, Long>>()
        {
            public int compare(Pair<T, Long> p1, Pair<T, Long> p2)
            {
                return p1.right.compareTo(p2.right);
            }
        });

        Map<List<T>, Long> buckets = new HashMap<List<T>, Long>();

        for (Pair<T, Long> pair: sortedFiles)
        {
            long size = pair.right;

            boolean bFound = false;
            // look for a bucket containing similar-sized files:
            // group in the same bucket if it's w/in 50% of the average for this bucket,
            // or this file and the bucket are all considered "small" (less than `min`)
            for (Entry<List<T>, Long> entry : buckets.entrySet())
            {
                List<T> bucket = entry.getKey();
                long averageSize = entry.getValue();
                if ((size > (averageSize / 2) && size < (3 * averageSize) / 2)
                    || (size < min && averageSize < min))
                {
                    // remove and re-add because adding changes the hash
                    buckets.remove(bucket);
                    long totalSize = bucket.size() * averageSize;
                    averageSize = (totalSize + size) / (bucket.size() + 1);
                    bucket.add(pair.left);
                    buckets.put(bucket, averageSize);
                    bFound = true;
                    break;
                }
            }
            // no similar bucket found; put it in a new one
            if (!bFound)
            {
                ArrayList<T> bucket = new ArrayList<T>();
                bucket.add(pair.left);
                buckets.put(bucket, size);
            }
        }

        return buckets.keySet();
    }

    private static Collection<Pair<SSTableReader, Long>> convertSSTablesToPairs(Collection<SSTableReader> collection)
    {
        Collection<Pair<SSTableReader, Long>> tablePairs = new HashSet<Pair<SSTableReader, Long>>();
        for(SSTableReader table: collection)
            tablePairs.add(new Pair<SSTableReader, Long>(table, table.length()));
        return tablePairs;
    }

    public static int getDefaultGcBefore(ColumnFamilyStore cfs)
    {
        return (int) (System.currentTimeMillis() / 1000) - cfs.metadata.gcGraceSeconds;
    }

    private static class AntiCompactionIterator extends CompactionWriterIterator
    {
        private Set<SSTableScanner> scanners;

        public AntiCompactionIterator(ColumnFamilyStore cfStore, Collection<SSTableReader> sstables, Collection<Range> ranges, int gcBefore, boolean isMajor,
                String newFilename,
                long expectedBloomFilterSize)
                throws IOException
        {
            super(cfStore, getCollatedRangeIterator(sstables, ranges), new RemoveDeletedRowProcessor(gcBefore), isMajor, newFilename, expectedBloomFilterSize);
        }

        private static Iterator getCollatedRangeIterator(Collection<SSTableReader> sstables, final Collection<Range> ranges)
                throws IOException
        {
            org.apache.commons.collections.Predicate rangesPredicate = new org.apache.commons.collections.Predicate()
            {
                public boolean evaluate(Object row)
                {
                    return Range.isTokenInRanges(((IteratingRow)row).getKey().token, ranges);
                }
            };
            CollatingIterator iter = FBUtilities.<IteratingRow>getCollatingIterator();
            for (SSTableReader sstable : sstables)
            {
                SSTableScanner scanner = sstable.getScanner(FILE_BUFFER_SIZE);
                iter.addIterator(new FilterIterator(scanner, rangesPredicate));
            }
            return iter;
        }

        public Iterable<SSTableScanner> getScanners()
        {
            if (scanners == null)
            {
                scanners = new HashSet<SSTableScanner>();
                for (Object o : ((CollatingIterator)source).getIterators())
                {
                    scanners.add((SSTableScanner)((FilterIterator)o).getIterator());
                }
            }
            return scanners;
        }
        
        public SSTableWriter writer()
        {
            return writer;
        }
    }

    public void checkAllColumnFamilies() throws IOException
    {
        // perform estimates
        for (final ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            Runnable runnable = new Runnable()
            {
                public void run ()
                {
                    logger.debug("Estimating compactions for " + cfs.columnFamily_);

                    final Set<List<SSTableReader>> buckets =
                            getBuckets(convertSSTablesToPairs(cfs.getSSTables()), 50L * 1024L * 1024L);
                    updateEstimateFor(cfs, buckets);
                }
            };
            executor.submit(runnable);
        }

        // actually schedule compactions.  done in a second pass so all the estimates occur before we
        // bog down the executor in actual compactions.
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            submitMinorIfNeeded(cfs);
        }
    }
    
    public boolean waitForCompletion(int timeoutSecs)
    {
        try {
            executor.awaitTermination(timeoutSecs, TimeUnit.SECONDS);
            return executor.getActiveCount()==0;
        } catch (InterruptedException e) 
        {
            return true;
        }
    }

    private class CompactionExecutor extends DebuggableThreadPoolExecutor
    {
        private volatile ColumnFamilyStore cfs;
        private volatile CompactionIterator ci;

        public CompactionExecutor()
        {
            super("COMPACTION-POOL", DatabaseDescriptor.getCompactionPriority());
        }

        @Override
        public void afterExecute(Runnable r, Throwable t)
        {
            super.afterExecute(r, t);
            cfs = null;
            ci = null;
        }

        void beginCompaction(ColumnFamilyStore cfs, CompactionIterator ci)
        {
            this.cfs = cfs;
            this.ci = ci;
        }

        public String getColumnFamilyName()
        {
            return cfs == null ? null : cfs.getColumnFamilyName();
        }

        public Long getBytesTotal()
        {
            return ci == null ? null : ci.getTotalBytes();
        }

        public Long getBytesCompleted()
        {
            return ci == null ? null : ci.getBytesRead();
        }
        
        public boolean isMajor()
        {
            return ci == null ? false : ci.isMajor();
        }
        
        
    }

    public String getColumnFamilyInProgress()
    {
        return executor.getColumnFamilyName();
    }

    public Long getBytesTotalInProgress()
    {
        return executor.getBytesTotal();
    }

    public Long getBytesCompacted()
    {
        return executor.getBytesCompleted();
    }
    
    public boolean isMajorCompaction()
    {
        return executor.isMajor();
    }

    public int getPendingTasks()
    {
        int n = 0;
        for (Integer i : estimatedCompactions.values())
        {
            n += i;
        }
        return n;
    }
}
