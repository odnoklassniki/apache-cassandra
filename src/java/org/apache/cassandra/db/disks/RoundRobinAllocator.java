/*
 * @(#) RoundRobinAllocator.java
 * Created 22.04.2013 by oleg
 * (C) Odnoklassniki.ru
 */
package org.apache.cassandra.db.disks;

import java.io.File;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Having several disk directories allocates data in RR manner - i.e. for every requested file it will choose next disk in list.
 * 
 * (If disk has not enough space - it will be skipped)
 * 
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public class RoundRobinAllocator extends AbstractDiskAllocator implements DiskAllocator
{
    private int currentDir = 0;
    

    public RoundRobinAllocator(String[] dataFileDirs)
    {
        super(dataFileDirs);
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.db.disks.DiskAllocator#getDataFileLocation(org.apache.cassandra.db.ColumnFamilyStore, long)
     */
    @Override
    public synchronized String getDataFileLocation(ColumnFamilyStore cfs, long estimatedSize)
    {

        for ( int i = 0 ; i < dataDirectories.length ; i++ )
        {
            currentDir = ( currentDir + 1 ) % dataDirectories.length ;

            File f = dataDirectories[currentDir];
            if( enoughSpaceAvailable(estimatedSize, f) )
            {
                return getDataFileLocationForTable(f, cfs.getTable().name);
            }
        }
        
        return null;
    }
    

}
