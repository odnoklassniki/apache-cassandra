package org.apache.cassandra.cache;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author roman.antipin
 *         Date: 14.06.13
 */
public class MemoryWrapper {

    static
    {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static final Unsafe unsafe;
    private static final long ARRAY_BASE_OFFSET = (long)unsafe.arrayBaseOffset(byte[].class);

    private final AtomicInteger references = new AtomicInteger(1);
    private long position;
    private final long size;

    public MemoryWrapper(long size, byte[] buffer) {
        this.size = size;
        position = unsafe.allocateMemory(size);
        unsafe.copyMemory(buffer, ARRAY_BASE_OFFSET, null, position, size);
    }

    public void free() {
        unsafe.freeMemory(position);
        position = 0;
    }

    public byte getByte(long offset) {
        return unsafe.getByte(position + offset);
    }

    public long getLong(long offset) {
        return unsafe.getLong(position + offset);
    }

    public int getInt(long offset) {
        return unsafe.getInt(position + offset);
    }

    public short getShort(long offset) {
        return unsafe.getShort(position + offset);
    }

    public char getChar(long offset) {
        return unsafe.getChar(position + offset);
    }

    public void getBytes(long offset, byte[] buffer, int bufferOffset, int count) {
        if (buffer == null) {
            throw new NullPointerException();
        } else if (bufferOffset < 0 || count < 0 || count > buffer.length - bufferOffset) {
            throw new IndexOutOfBoundsException();
        } else if (count == 0) {
            return;
        }
        unsafe.copyMemory(null, position + offset, buffer, bufferOffset + ARRAY_BASE_OFFSET, count);
    }

    public boolean reference() {
        while (true) {
            int n = references.get();
            if (n <= 0) {
                return false;
            } else if (references.compareAndSet(n, n + 1)) {
                return true;
            }
        }
    }

    public void unreference() {
        if (references.decrementAndGet() == 0) {
            free();
        }
    }

    public long getSize() {
        return size;
    }
}
