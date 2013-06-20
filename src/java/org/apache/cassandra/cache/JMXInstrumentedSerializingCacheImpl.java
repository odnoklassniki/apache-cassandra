package org.apache.cassandra.cache;

import org.apache.cassandra.io.ICompactSerializer2;

/**
 * @author roman.antipin
 *         Date: 18.06.13
 */
public class JMXInstrumentedSerializingCacheImpl<K, V> extends InstrumentedSerializingCache<K, V> implements JMXInstrumentedCache<K,V>, JMXInstrumentedSerializingCacheImplMBean {
    public JMXInstrumentedSerializingCacheImpl(String table, String name, int capacity, ICompactSerializer2<V> serializer) {
        super(capacity, serializer);
        AbstractCache.registerMBean(this, table, name);
    }
}