/*
 * @(#) DiskAllocator.java
 * Created 22.04.2013 by oleg
 * (C) Odnoklassniki.ru
 */
package org.apache.cassandra.db.disks;

import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Manages disk space across several disks.
 * 
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public interface DiskAllocator
{
    /**
     * @param cfs
     * @param estimatedSize
     * @return fully qualified name of disk file -Data.db component
     */
    String getDataFileLocation(ColumnFamilyStore cfs, long estimatedSize);

}
