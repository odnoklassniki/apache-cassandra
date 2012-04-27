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
public interface IStoreApplyListener
{
    /**
     * Called by store just before application of new data to local column family store.
     * Beware: same data can arrive several times due to hinted handoffs, RRs etc.
     * 
     * BEWARE: You CANNOT modify supplied column family data - you'll break things badly. 
     * 
     * @param key row key
     * @param data 
     * 
     */
    boolean preapply(String key, ColumnFamily data); 
    
    /**
     * Called after changes was successfully applied to local CF store.
     * 
     * @param key
     * @param data
     */
    void applied(String key, ColumnFamily data);

}
