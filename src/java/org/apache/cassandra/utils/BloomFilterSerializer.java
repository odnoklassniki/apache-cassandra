package org.apache.cassandra.utils;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.OpenBitSet;

public class BloomFilterSerializer implements ICompactSerializer2<BloomFilter>
{

    protected void serializeHeader( BloomFilter bf, DataOutput dos ) throws IOException
    {
        long wordsLength = bf.bitset.sizeInWords();
        
        dos.writeInt(bf.getHashCount());
        if (wordsLength < Integer.MAX_VALUE ) {
            dos.writeInt( (int) wordsLength );
        } else {
            dos.writeInt(-1);
            dos.writeLong( wordsLength );
        }
    }

    public void serialize(BloomFilter bf, DataOutput dos) throws IOException
    {
        serializeHeader( bf, dos );

        bf.bitset.serialize( dos );
    }
    
    public BloomFilter deserialize(DataInput dis) throws IOException
    {
        int hashes = dis.readInt();
        long bitLength = dis.readInt();
        if (bitLength<0) {
            bitLength = dis.readLong();
        }
        
        IBitSet bs = deserializeBitSet( dis, bitLength );
        
        return new BloomFilter(hashes, bs);
    }

    protected IBitSet deserializeBitSet( DataInput dis, long bitLength ) throws IOException
    {
        OpenBitSet bs = new OpenBitSet( bitLength<< 6 );
        bs.deserialize( dis );
        return bs;
    }
    
    public long serializeSize(BloomFilter bf)
    {
        // padding to the closest long word boundary
        long words = bf.bitset.sizeInWords();

        return serializeSize( words ) ;
    }

    public long serializeSize( long words )
    {
        return headerSize( words ) + ( words << 3 );
    }
    
    public long headerSize( long words ) {
        return words < Integer.MAX_VALUE ? 4 + 4 : 4 + 4/*-1 marker*/ + 8 /* real num */;
    }

}


