package org.apache.cassandra.cache;

import java.util.Set;

/**
 * @author roman.antipin
 *         Date: 17.06.13
 */
public interface Cache<K, V> {
    void put(K key, V value);

    V get(K key);

    V getInternal(K key);

    void remove(K key);

    int getCapacity();

    boolean isCapacitySetManually();

    void updateCapacity(int capacity);

    void setCapacity(int capacity);

    int getSize();

    long getHits();

    long getRequests();

    double getRecentHitRate();

    void clear();

    Set<K> getKeySet();
}
