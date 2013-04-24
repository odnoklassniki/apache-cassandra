/*
 * @(#) SingleDirAllocator.java
 * Created 24.04.2013 by oleg
 * (C) Odnoklassniki.ru
 */
package org.apache.cassandra.db.disks;

import java.io.File;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * This is stub allocation policy for single data disk setups
 * 
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public class SingleDirAllocator extends AbstractDiskAllocator
{

    /**
     * @param dataFileDirs
     */
    public SingleDirAllocator(String[] dataFileDirs)
    {
        super(dataFileDirs);
        
        assert dataFileDirs.length == 1;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.db.disks.DiskAllocator#getDataFileLocation(org.apache.cassandra.db.ColumnFamilyStore, long)
     */
    @Override
    public String getDataFileLocation(ColumnFamilyStore cfs, long estimatedSize)
    {
        File f = dataDirectories[0];

        if( enoughSpaceAvailable(estimatedSize, f) )
        {
            return getDataFileLocationForTable(f, cfs.getTable().name);
        }
    
        return null;
    }

}
