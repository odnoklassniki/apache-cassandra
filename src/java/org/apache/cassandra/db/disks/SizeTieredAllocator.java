/*
 * @(#) SizeWiseAllocator.java
 * Created 22.04.2013 by oleg
 * (C) Odnoklassniki.ru
 */
package org.apache.cassandra.db.disks;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Keeps tracking of files sizes, ensuring equal number of files of the same size are placed to each of disks. In SizeTiered compaction strategy
 * size could attibute to generation of data.
 * 
 * This should act as better approach of balancing iops and used space across several data disks.
 * 
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public class SizeTieredAllocator extends AbstractDiskAllocator
{
    private static final Log log = LogFactory.getLog(SizeTieredAllocator.class);
    
    /**
     * @param dataFileDirs
     */
    public SizeTieredAllocator(String[] dataFileDirs)
    {
        super(dataFileDirs);
        
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.disks.DiskAllocator#getDataFileLocation(org.apache.cassandra.db.ColumnFamilyStore, long)
     */
    @Override
    public String getDataFileLocation(ColumnFamilyStore cfs, long estimatedSize)
    {
        return getDataFileLocation(cfs.getTable().name, estimatedSize);
    }
        
    public String getDataFileLocation(String table, long estimatedSize)
    {
        int tier = getTierNumber( estimatedSize );
        
        // now counting number of files of this tier for each disk
        int fileCounts[] = new int[dataDirectories.length];
        
        for (Pair<File, Long> pair : getSSTables()) {
            if (getTierNumber(pair.right)==tier) {
                int idx = diskIdx(pair.left);
                
                if (idx>=0)
                    fileCounts[idx]++;
            }
        }

        Pair<File,Integer>[] dirsAndCount = new Pair[dataDirectories.length];
        for (int i = 0; i < dataDirectories.length; i++) {
            dirsAndCount[i]=new Pair<File, Integer>(dataDirectories[i], fileCounts[i]);
        }

        // sorting disks with less files number first
        Arrays.sort(dirsAndCount,new Comparator<Pair<File,Integer>>()
        {

            @Override
            public int compare(Pair<File, Integer> o1, Pair<File, Integer> o2)
            {
                int r = o1.right.compareTo(o2.right);
                
                if ( r==0 ) {
                    // returning disk with more free space first
                    return o1.left.getUsableSpace() > o1.left.getUsableSpace() ? 1 : -1;
                }
                
                return r;
            }
        });
        

        for (Pair<File, Integer> pair : dirsAndCount) {
            if( enoughSpaceAvailable(estimatedSize, pair.left))
            {
                if (log.isDebugEnabled()) {
                    StringBuilder sb=new StringBuilder();
                    for (Pair<File, Integer> p : dirsAndCount) {
                        sb.append(p.left+","+p.right+","+FileUtils.stringifyFileSize( p.left.getUsableSpace() ));
                    }

                    log.debug("estimation:"+estimatedSize+", tier="+tier+", by disk counts "+sb+", choosen "+pair);

                }
                return getDataFileLocationForTable(pair.left, table);
            }
        }

        return null;
    }

    /**
     * @param left
     * @return
     */
    private int diskIdx(File sstable)
    {
        for (int i = 0; i < dataDirectories.length; i++) {
            File dir = dataDirectories[i];
            
            String dirPath = dir.getAbsolutePath()+File.separator;
            
            if ( sstable.getAbsolutePath().startsWith(dirPath) ) {
                return i;
            }
        }
        
        return -1;
    }

    protected Collection<Pair<File,Long>> getSSTables() {
        
        ArrayList<Pair<File,Long>> sstables =  new ArrayList<Pair<File,Long>>();
        
        for ( ColumnFamilyStore cfs : ColumnFamilyStore.all() ) {
            for (SSTableReader ssTableReader : cfs.getSSTables() ) {
                File f = new File(ssTableReader.getFilename());
                sstables.add( new Pair<File,Long>( f, f.length() ) );
            }
        }
        
        return sstables;
    }
    
    private int getTierNumber(long size) {
        
        long firstTierBoundary = firstTierBoundary();
        if (size<firstTierBoundary)
            return 0;
        
        return (int) (Math.log(size/firstTierBoundary)/Math.log(4));
    }

    /**
     * @return
     */
    protected long firstTierBoundary() {
        return DatabaseDescriptor.getMemtableThroughput() * tierMult() * 1024l * 1024l;
    }

    
    /**
     * @return multiplier to use when calculating tier boundaries
     */
    private int tierMult() {
        return CompactionManager.instance.getMinimumCompactionThreshold();
    }
    
}
