/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;


import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DatabaseDescriptor.DiskAccessMode;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.obs.BitUtil;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.MappedFileBitSet;
import org.apache.cassandra.utils.obs.OpenBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import one.nio.mem.MappedFile;

public class BloomFilter extends Filter implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(BloomFilter.class);
    private static final int EXCESS = 20;
    static ICompactSerializer2<BloomFilter> serializer_ = new BloomFilterSerializer();
    static BloomFilterWithElementCountSerializer serializerWithEC_ = new BloomFilterWithElementCountSerializer();

    public IBitSet bitset;
    
    private long elementCount;

    BloomFilter(int hashes, IBitSet bs)
    {
        hashCount = hashes;
        bitset = bs;
    }

    BloomFilter(int hashes, IBitSet bs, long elementCount)
    {
        hashCount = hashes;
        bitset = bs;
        this.elementCount = elementCount;
    }
    
    /**
     * @param elementCount the elementCount to set
     */
    void setElementCount(long elementCount)
    {
        this.elementCount = elementCount;
    }

    public static ICompactSerializer2<BloomFilter> serializer()
    {
        return serializer_;
    }

    public static BloomFilterWithElementCountSerializer serializerForSSTable()
    {
        return serializerWithEC_ ;
    }
    
    @Override
    @VisibleForTesting
    ICompactSerializer2<BloomFilter> getSerializer()
    {
        return serializerWithEC_ ;
    }

    public static BloomFilter open(String filename) throws IOException
    {
        BloomFilter bf;
        if ( DatabaseDescriptor.getFilterAccessMode()==DiskAccessMode.standard ) {
            bf = openOnHeapBitSet( filename );
        } else {
            MappedFile mapped = new MappedFile( filename, 0, MappedFile.MAP_RO );
            bf = new MappedFileBloomFilterSerializer( mapped ).deserialize( mapped.dataStream( ByteOrder.BIG_ENDIAN ) );
        }

        assert bf.getElementCount()>0;
        return bf;
    }

    // temporary for cross check
    @VisibleForTesting
    public static BloomFilter openOnHeapBitSet( String filename ) throws FileNotFoundException, IOException
    {
        BloomFilter bf;
        FileInputStream fileInputStream = new FileInputStream(filename);
        DataInputStream stream = new DataInputStream(new BufferedInputStream(fileInputStream, 128*1024 ));
        try
        {
            bf = serializerForSSTable().deserialize(stream);
        }
        finally
        {
            CLibrary.trySkipCache( CLibrary.getfd(fileInputStream.getFD()) ,0,0);

            stream.close();
        }
        return bf;
    }

    /**
    * @return A BloomFilter with the lowest practical false positive probability
    * for the given number of elements.
    */
    public static BloomFilter getFilter(long numElements, int targetBucketsPerElem)
    {
        BloomCalculations.BloomSpecification spec = computeSpec( numElements, targetBucketsPerElem );
        return new BloomFilter(spec.K, new OpenBitSet(numElements * spec.bucketsPerElement + EXCESS));
    }

    private static BloomCalculations.BloomSpecification computeSpec( long numElements, int targetBucketsPerElem )
    {
        int maxBucketsPerElement = Math.max(1, BloomCalculations.maxBucketsPerElement(numElements));
        int bucketsPerElement = Math.min(targetBucketsPerElem, maxBucketsPerElement);
        if (bucketsPerElement < targetBucketsPerElem)
        {
            logger.warn(String.format("Cannot provide an optimal BloomFilter for %d elements (%d/%d buckets per element).",
                                      numElements, bucketsPerElement, targetBucketsPerElem));
        }
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
        return spec;
    }

    /**
    * @return The smallest BloomFilter that can provide the given false positive
    * probability rate for the given number of elements.
    *
    * Asserts that the given probability can be satisfied using this filter.
    * 
     * @throws IOException 
    */
    public static BloomFilter create( long numElements,int targetBucketsPerElem, String filename ) throws IOException
    {
        assert filename != null;
        BloomCalculations.BloomSpecification spec = computeSpec( numElements, targetBucketsPerElem );
        long bits = numElements * spec.bucketsPerElement + EXCESS;

        if ( DatabaseDescriptor.getFilterAccessMode() == DiskAccessMode.standard ) {
            return new BloomFilter( spec.K, new OpenBitSet( bits ) );
            
        } else {
            long words = BitUtil.bits2words( bits );
            long headerSize = serializerWithEC_.headerSize( words );
            long fileSize = serializerWithEC_.serializeSize( words );
            
            MappedFile mfile = new MappedFile( filename, fileSize, MappedFile.MAP_RW );
            
            return new BloomFilter( spec.K, new MappedFileBitSet( mfile, headerSize ) );
        }
    }
    
    public static BloomFilter create(long numElements, double maxFalsePosProbability, String filename) throws IOException
    {
        assert maxFalsePosProbability <= 1.0 : "Invalid probability";
        int bucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement, maxFalsePosProbability);
        long bits = numElements * spec.bucketsPerElement + EXCESS;

        if ( DatabaseDescriptor.getFilterAccessMode() == DiskAccessMode.standard ) {
            return new BloomFilter( spec.K, new OpenBitSet( bits ) );
            
        } else {
            long words = BitUtil.bits2words( bits );
            long headerSize = serializerWithEC_.headerSize( words );
            long fileSize = serializerWithEC_.serializeSize( words );
            
            MappedFile mfile = new MappedFile( filename, fileSize, MappedFile.MAP_RW );
            
            return new BloomFilter( spec.K, new MappedFileBitSet( mfile, headerSize ) );
        }
    }

    public long buckets()
    {
      return bitset.capacity();
    }
    
    @Override
    public synchronized void close() 
    {
        IBitSet closing = bitset;
        // so trying to use closed bloom filter will lead to 100% false positives, which is ok, for some (rare) outstanding requests
        // this is better then risk catching segfault for un-mapped bloom filters 
        bitset = always.bitset; 
        closing.close();
    }

    private long[] getHashBuckets(ByteBuffer key)
    {
        return BloomFilter.getHashBuckets(key, hashCount, buckets());
    }

    // Murmur is faster than an SHA-based approach and provides as-good collision
    // resistance.  The combinatorial generation approach described in
    // http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
    // does prove to work in actual tests, and is obviously faster
    // than performing further iterations of murmur.
    static long[] getHashBuckets(ByteBuffer b, int hashCount, long max)
    {
        long[] result = new long[hashCount];
        long hash1 = MurmurHash.hash64(b, b.position(), b.remaining(), 0L);
        long hash2 = MurmurHash.hash64(b, b.position(), b.remaining(), hash1);
        for (int i = 0; i < hashCount; ++i)
        {
            result[i] = Math.abs((hash1 + (long)i * hash2) % max);
        }
        return result;
    }

    @VisibleForTesting
    static long[] getHashBuckets(String key, int hashCount, long max)
    {
        return getHashBuckets(toByteBuffer(key), hashCount, max);
    }
    
    public void add(ByteBuffer key)
    {
        elementCount++;
        
        final long max = buckets();
        long hash1 = MurmurHash.hash64(key, key.position(), key.remaining(), 0L);
        final long hash2 = MurmurHash.hash64(key, key.position(), key.remaining(), hash1);
        for (int i = 0; i < hashCount; ++i)
        {
            long bucketIndex = Math.abs(hash1 % max);
            hash1+=hash2;
            bitset.set(bucketIndex);
        }
    }

    /**
     * @return the elementCount of this bloom filter
     */
    public long getElementCount()
    {
        return elementCount;
    }
    
    public boolean isPresent(ByteBuffer key)
    {
        final long max = buckets();
        long hash1 = MurmurHash.hash64(key, key.position(), key.remaining(), 0L);
        final long hash2 = MurmurHash.hash64(key, key.position(), key.remaining(), hash1);
        for (int i = 0; i < hashCount; ++i)
        {
            long bucketIndex = Math.abs(hash1 % max);
            if (!bitset.get(bucketIndex))
            {
                return false;
            }
            hash1+=hash2;
        }

        return true;
    }
    
    public void add(byte[] key)
    {
        elementCount++;
        
        final long max = buckets();
        long hash1 = MurmurHash.hash64u(key, 0, key.length, 0L);
        final long hash2 = MurmurHash.hash64u(key, 0, key.length, hash1);
        for (int i = 0; i < hashCount; ++i)
        {
            long bucketIndex = Math.abs(hash1 % max);
            hash1+=hash2;
            bitset.set(bucketIndex);
        }
    }
    
    public boolean isPresent(byte[] key)
    {
        final long max = buckets();
        long hash1 = MurmurHash.hash64u(key, 0, key.length, 0L);
        final long hash2 = MurmurHash.hash64u(key, 0, key.length, hash1);
        for (int i = 0; i < hashCount; ++i)
        {
            long bucketIndex = Math.abs(hash1 % max);
            if (!bitset.get(bucketIndex))
            {
                return false;
            }
            hash1+=hash2;
        }

        return true;
    }

    public void add(String key)
    {
        elementCount++;
        
        final long max = buckets();
        long hash1 = MurmurHash.hash64u(key, 0L);
        final long hash2 = MurmurHash.hash64u(key, hash1);
        for (int i = 0; i < hashCount; ++i)
        {
            long bucketIndex = Math.abs(hash1 % max);
            hash1+=hash2;
            bitset.set(bucketIndex);
        }
    }
    
    public void add(String key, byte[] column)
    {
        elementCount++;
        
        final long max = buckets();
        long hash1 = MurmurHash.hash64u(key, column, 0L);
        final long hash2 = MurmurHash.hash64u(key, column, hash1);
        for (int i = 0; i < hashCount; ++i)
        {
            long bucketIndex = Math.abs(hash1 % max);
            hash1+=hash2;
            bitset.set(bucketIndex);
        }
    }

    /**
     * 
     */
    public boolean isPresent(String key)
    {
        return isPresent(key, ByteBufferUtil.EMPTY_BYTES);
    }

    public boolean isPresent(String key, byte[] column)
    {
        final long max = buckets();
        long hash1 = MurmurHash.hash64u(key, column, 0L);
        final long hash2 = MurmurHash.hash64u(key, column, hash1);
        for (int i = 0; i < hashCount; ++i)
        {
            long bucketIndex = Math.abs(hash1 % max);
            if (!bitset.get(bucketIndex))
            {
                return false;
            }
            hash1+=hash2;
        }

        return true;

    }
    
    private static ByteBuffer toByteBuffer(String s)
    {
        int strLen=s.length()*2;
        ByteBuffer byteBuffer = ByteBuffer.allocate( strLen*2 );
        for (int i=s.length(),j=strLen;i-->0;)
        {
            char c = s.charAt(i);
            
            byte b1 = (byte) (c & 0xFF);
            byteBuffer.put(--j,b1);
            byte b2 = (byte) ( (c & 0xFF00) >>8);
            byteBuffer.put(--j,b2);
            
        }
        
        byteBuffer.limit(strLen).position(0);
        
        return byteBuffer;
    }
    
    private static final BloomFilter always;
    static {
        OpenBitSet set = new OpenBitSet(64);
        set.set(0, 64);
        always=new BloomFilter(1, set);
        
    }
    
    /** @return a BloomFilter that always returns a positive match, for testing */
    public static BloomFilter alwaysMatchingBloomFilter()
    {
        return always;
    }
    
}
