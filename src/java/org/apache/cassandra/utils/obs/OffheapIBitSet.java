/*
 * @(#) MappedFileBitSet.java
 * Created Sep 27, 2016 by oleg
 * (C) Odnoklassniki.ru
 */
package org.apache.cassandra.utils.obs;

import static one.nio.util.JavaInternals.unsafe;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import one.nio.mem.OffheapBitSet;

/**
 * This bitset maps a file into memory, making all operations with that file through unsafe. It also
 * has option to mlock it into memory, preventing its swap out from memory.
 * 
 * This kind of bitset is useful, if you have stored large (>2GB) bloom filter ( i.e. bitsets ) on disk and want to avoid
 * disk ops (loading all these files into memory) across server restarts.
 * 
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public class OffheapIBitSet extends OffheapBitSet implements IBitSet
{

    public OffheapIBitSet( long numBits )
    {
        super( numBits );
    }

    @Override
    public void set( long index )
    {
        super.unsafeSet( index );
    }
    
    @Override
    public boolean get( long index )
    {
        return super.unsafeGet( index );
    }
    
    @Override
    public long sizeInWords()
    {
        return capacity() >> 6;
    }

    @Override
    public void serialize( DataOutput out ) throws IOException
    {
        long size = capacity() / 8;
        long endaddr = baseAddr + size;

        for ( long waddr = baseAddr; waddr < endaddr; waddr += 8 ) {
            out.writeLong( unsafe.getLong( waddr ) );
        }
    }


    @Override
    public void deserialize( DataInput in ) throws IOException
    {
        long size = capacity() / 8;
        long endaddr = baseAddr + size;

        for ( long waddr = baseAddr; waddr < endaddr; waddr += 8 ) {
            unsafe.putLong( waddr, in.readLong() );
        }
    }
}
