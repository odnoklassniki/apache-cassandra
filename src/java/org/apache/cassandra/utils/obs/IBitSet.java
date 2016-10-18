package org.apache.cassandra.utils.obs;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface IBitSet extends Closeable
{
    public long capacity();

    /**
     * @return number of whole 64 bit words this bitset occupies
     */
    public long sizeInWords();

    /**
     * Returns true or false for the specified bit index. The index should be
     * less than the capacity.
     */
    public boolean get(long index);

    /**
     * Sets the bit at the specified index. The index should be less than the
     * capacity.
     */
    public void set(long index);

    /**
     * clears the bit. The index should be less than the capacity.
     */
    public void clear(long index);

    public void close();
    
    public void serialize(DataOutput out) throws IOException;

    public void deserialize(DataInput out) throws IOException;

}