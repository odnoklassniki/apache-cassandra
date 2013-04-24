/*
 * @(#) AbstractDiskAllocator.java
 * Created 22.04.2013 by oleg
 * (C) Odnoklassniki.ru
 */
package org.apache.cassandra.db.disks;

import java.io.File;

/**
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public abstract class AbstractDiskAllocator implements DiskAllocator
{

    protected final File[] dataDirectories;

    public AbstractDiskAllocator(String[] dataFileDirs)
    {
        dataDirectories = new File[dataFileDirs.length];
        
        for (int i = 0; i < dataFileDirs.length; i++) {
            String dir = dataFileDirs[i];
            dataDirectories[i] = new File(dir);
        }
    }

    protected boolean enoughSpaceAvailable(long estimatedSize, File pair)
    {
        // Load factor of 0.9 we do not want to use the entire disk that is too risky.
        return estimatedSize+estimatedSize/10 < pair.getUsableSpace();
    }

    public static String getDataFileLocationForTable(File dir, String table) {
        return new File(dir,table).getAbsolutePath();
    }

}