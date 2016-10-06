/*
 * @(#) MMappedBloomFilterSerializer.java
 * Created Oct 3, 2016 by oleg
 * (C) Odnoklassniki.ru
 */
package org.apache.cassandra.utils;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.MappedFileBitSet;

import one.nio.mem.MappedFile;
import one.nio.serial.DataStream;

/**
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public class MappedFileBloomFilterSerializer extends BloomFilterWithElementCountSerializer
{
    private MappedFile mfile ;
    
    MappedFileBloomFilterSerializer( MappedFile mfile )
    {
        this.mfile = mfile;
    }

    @Override
    protected IBitSet deserializeBitSet( DataInput dis, long bitLength ) throws IOException
    {
        DataStream dd = (DataStream) dis;
        return new MappedFileBitSet( mfile, dd.count()  );
    }
}
