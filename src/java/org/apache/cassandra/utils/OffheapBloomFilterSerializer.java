/*
 * @(#) MMappedBloomFilterSerializer.java
 * Created Oct 3, 2016 by oleg
 * (C) Odnoklassniki.ru
 */
package org.apache.cassandra.utils;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.OffheapIBitSet;

/**
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public class OffheapBloomFilterSerializer extends BloomFilterWithElementCountSerializer
{
    @Override
    protected IBitSet deserializeBitSet( DataInput dis, long wordLength ) throws IOException
    {
        IBitSet bs = new OffheapIBitSet( wordLength << 6 );
        bs.deserialize( dis );
        return bs;
    }
}
