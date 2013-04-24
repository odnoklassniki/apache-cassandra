/*
 * @(#) SizeConservativeAllocator.java
 * Created 22.04.2013 by oleg
 * (C) Odnoklassniki.ru
 */
package org.apache.cassandra.db.disks;

import java.io.File;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Allocates disk space from disk with most free space (could be better for SSD, if you're not IO bound)
 * 
 * This algo is used by stock cassandra
 * 
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 */
public class SpaceFirstAllocator extends AbstractDiskAllocator
{

    /**
     * @param dataFileDirs
     */
    public SpaceFirstAllocator(String[] dataFileDirs)
    {
        super(dataFileDirs);
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.disks.DiskAllocator#getDataFileLocation(org.apache.cassandra.db.ColumnFamilyStore, long)
     */
    @Override
    public String getDataFileLocation(ColumnFamilyStore cfs, long estimatedSize)
    {
        long maxUsableSpace = 0;
        File maxUsableFile = null;
        
        for ( int i = 0 ; i < dataDirectories.length ; i++ )
        {
            File f = dataDirectories[i];
            if( enoughSpaceAvailable(estimatedSize, f) )
            {
                long usableSpace = f.getUsableSpace();
                if (usableSpace>maxUsableSpace) {
                    maxUsableSpace = usableSpace;
                    maxUsableFile = f;
                }
            }
        }
        
        return maxUsableFile!=null ? getDataFileLocationForTable(maxUsableFile, cfs.getTable().name) : null;
        
    }

}
