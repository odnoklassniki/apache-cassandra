package org.apache.cassandra.cache;

import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;
import org.apache.cassandra.io.ICompactSerializer2;

import java.io.IOException;
import java.util.Set;

/**
 * @author roman.antipin
 *         Date: 14.06.13
 */
public class InstrumentedSerializingCache<K, V> implements Cache<K, V> {

    private final ICompactSerializer2<V> serializer;
    private final InstrumentedCache<K, MemoryWrapper> cache;


    public InstrumentedSerializingCache(int capacity, ICompactSerializer2<V> serializer) {
        this.cache = new InstrumentedCache<K, MemoryWrapper>(capacity, new ConcurrentLinkedHashMap.EvictionListener<K, MemoryWrapper>() {
            @Override
            public void onEviction(K k, MemoryWrapper memoryWrapper) {
                memoryWrapper.unreference();
            }
        });
        this.serializer = serializer;
    }

    private MemoryWrapper serialize(V value) {
        try {
            ByteArrayStreamOutput output = new ByteArrayStreamOutput();
            serializer.serialize(value, output);
            return new MemoryWrapper(output.getCount(), output.getBuffer());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (OutOfMemoryError e) {
            return null;
        }
    }

    private V deserialize(MemoryWrapper memory) {
        try {
            return serializer.deserialize(new ByteArrayStreamInput(memory));
        } catch (IOException e) {
            return null;
        }
    }

        @Override
    public void put(K key, V value) {
        MemoryWrapper mem = serialize(value);
        if (mem == null)
            return;

        MemoryWrapper old = cache.putInternal(key, mem);
        if (old != null) {
            old.unreference();
        }
    }

    @Override
    public V get(K key) {
        return getValue(cache.get(key));
    }

    @Override
    public V getInternal(K key) {
        return getValue(cache.getInternal(key));
    }

    private V getValue(MemoryWrapper memoryWrapper) {
        if (memoryWrapper == null) {
            return null;
        } else if (!memoryWrapper.reference())
            return null;
        try {
            return deserialize(memoryWrapper);
        } finally {
            memoryWrapper.unreference();
        }
    }

    @Override
    public void remove(K key) {
        MemoryWrapper memoryWrapper = cache.removeInternal(key);
        if (memoryWrapper != null) {
            memoryWrapper.unreference();
        }
    }

    @Override
    public void clear() {
        for (K key: cache.getKeySet()) {
            remove(key);
        }
        cache.clear();
    }

    @Override
    public int getCapacity() {
        return cache.getCapacity();
    }

    @Override
    public boolean isCapacitySetManually() {
        return cache.isCapacitySetManually();
    }

    @Override
    public void updateCapacity(int capacity) {
        cache.updateCapacity(capacity);
    }

    @Override
    public void setCapacity(int capacity) {
        cache.setCapacity(capacity);
    }

    @Override
    public int getSize() {
        return cache.getSize();
    }

    @Override
    public long getHits() {
        return cache.getHits();
    }

    @Override
    public long getRequests() {
        return cache.getRequests();
    }

    @Override
    public double getRecentHitRate() {
        return cache.getRecentHitRate();
    }

    @Override
    public Set<K> getKeySet() {
        return cache.getKeySet();
    }
}
