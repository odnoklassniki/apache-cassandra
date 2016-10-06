/*
 * @(#) MappedFileBitSet.java
 * Created Sep 27, 2016 by oleg
 * (C) Odnoklassniki.ru
 */
package org.apache.cassandra.utils.obs;

import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;

import one.nio.mem.MappedFile;
import one.nio.mem.OffheapBitSet;
import one.nio.os.Mem;
import one.nio.serial.DataStream;

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
public class MappedFileBitSet extends OffheapBitSet implements IBitSet
{
    private final MappedFile mfile;
    
    /**
     * @param mode one of {@link MappedFile}.MAP_* constants
     */
    public MappedFileBitSet( String name, long filesize, int mode, long fileoffset) throws IOException
    {
        this( new MappedFile( name, filesize, mode ), fileoffset);
    }
    
    public MappedFileBitSet( MappedFile mfile,long fileoffset) throws IOException
    {
        super( mfile.getAddr() + fileoffset, mfile.getSize() - fileoffset, ByteOrder.BIG_ENDIAN );
        this.mfile = mfile;

        if ( ( ( this.mfile.getSize() - fileoffset ) & 0x7l ) != 0 ) {
            throw new IOException( "Can map only a multiple of 8 bytes" );
        }

        if ( ( fileoffset & 0x7l ) != 0 ) {
            throw new IOException( "File offset must be a multiple of 8" );
        }

        if ( Mem.IS_SUPPORTED ) {
            int err = Mem.mlock( mfile.getAddr(), this.mfile.getSize() );
            if ( err != 0 )
                throw new IOException( "mlock failed with :" + err );
        }
    }    
    
    @Override
    public void set( long index )
    {
        assert this.mfile.getMode() == MappedFile.MAP_RW ; // avoiding JVM crash if wrong mode is chosen
        
        super.set( index );
    }
    
    public RandomAccessFile getFile() {
        return mfile.getFile();
    }
    
    public MappedFile getMappedFile() {
        return mfile;
    }
    
    @Override
    public long sizeInWords()
    {
        return capacity() >> 6;
    }

    @Override
    public void serialize( DataOutput out ) throws IOException
    {
        assert out instanceof DataStream : "Memory mapped bitset can serialize only to direct output";
        // this does not need serialization. only sync to disk
        getMappedFile().sync();
        getMappedFile().makeReadonly();
    }


    @Override
    public DataOutput newDataOutput()
    {
        return getMappedFile().dataStream( ByteOrder.BIG_ENDIAN );
    }

    @Override
    public void close() 
    {
        getMappedFile().close();
    }
}
