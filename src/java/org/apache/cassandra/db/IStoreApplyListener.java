/*
 * @(#) IStoreApplyListener.java
 * Created Dec 6, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.db;

/**
 * Listener for new data on column family store
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 * @see ColumnFamilyStore#apply(String, ColumnFamily)
 */
public interface IStoreApplyListener
{
    /**
     * Called by store just before application of new data to local column family store.
     * Be warned, that same data can arrive several times due to hinted handoffs, RRs etc.
     * 
     * At the moment of this method invokation mutation is already written to commit log.
     * 
     * @param key row key
     * @param data 
     */
    void apply(String key, ColumnFamily data); 

}
