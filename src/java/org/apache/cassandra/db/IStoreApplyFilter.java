/*
 * @(#) IStoreApplyListener.java
 * Created Dec 6, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.db;

/**
 * Can filter row mutations applied to local column family store
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 * @see ColumnFamilyStore#setStoreApplyListener(IStoreApplyListener)
 * @see Table#apply(RowMutation, Object, boolean)
 */
public interface IStoreApplyFilter
{
    /**
     * Called by store just before application of new data to local column family store.
     * Be warned, that same data can arrive several times due to hinted handoffs, RRs etc.
     * 
     * You can modify supplied column family data. If after modifications it is empty - whole
     * mutation will be skipped
     * 
     * @param key row key
     * @param data 
     * 
     */
    void filter(String key, ColumnFamily data); 

}
