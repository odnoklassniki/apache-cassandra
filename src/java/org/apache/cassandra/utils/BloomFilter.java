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


import java.nio.ByteBuffer;

import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.obs.OpenBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BloomFilter extends Filter
{
    private static final Logger logger = LoggerFactory.getLogger(BloomFilter.class);
    private static final int EXCESS = 20;
    static ICompactSerializer2<BloomFilter> serializer_ = new BloomFilterSerializer();
    static ICompactSerializer2<BloomFilter> serializerWithEC_ = new BloomFilterWithElementCountSerializer();

    public OpenBitSet bitset;
    
    private ByteBuffer strBuffer;
    
    private long elementCount;

    BloomFilter(int hashes, OpenBitSet bs)
    {
        hashCount = hashes;
        bitset = bs;
    }

    BloomFilter(int hashes, OpenBitSet bs, long elementCount)
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

    public static ICompactSerializer2<BloomFilter> serializerForSSTable()
    {
        return serializerWithEC_;
    }

    private static OpenBitSet bucketsFor(long numElements, int bucketsPer)
    {
        long l = numElements * bucketsPer + EXCESS;
        
        long desiredNumBits = l/16;
        
        try {
            // adjusting desired bits to the closest power of 2 to reduce heap fragmentation and
            // chance of PromotionFailed CMS failure
            if ( (desiredNumBits & (desiredNumBits-1)) !=0 ) // only if it is not power of 2 yet
            {
                // http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
                desiredNumBits--;
                desiredNumBits |= desiredNumBits >> 1;
                desiredNumBits |= desiredNumBits >> 2;
                desiredNumBits |= desiredNumBits >> 4;
                desiredNumBits |= desiredNumBits >> 8;
                desiredNumBits |= desiredNumBits >> 16;
                desiredNumBits |= desiredNumBits >> 32;
                desiredNumBits++;
            }
            
            if (desiredNumBits>0)
            {
                long adjustedNumBits=l - (l & (desiredNumBits-1) ) + desiredNumBits;

                if (bucketsPer>4) // dont print it for every column
                    logger.info("Adjusting filter size from "+l+" to "+adjustedNumBits+", applying 1/16 roundup of "+desiredNumBits);

                return new OpenBitSet( Math.max(128,adjustedNumBits) );
            }
            
            if (bucketsPer>4) // dont print it for every column
                logger.info("Bloom size adjustment is 0 for "+l);

        } catch (Exception e) {
            logger.error("Cannot make bloom adjust for "+numElements+","+bucketsPer,e);
        }
        
        return new OpenBitSet( l );
    }

    /**
    * @return A BloomFilter with the lowest practical false positive probability
    * for the given number of elements.
    */
    public static BloomFilter getFilter(long numElements, int targetBucketsPerElem)
    {
        int maxBucketsPerElement = Math.max(1, BloomCalculations.maxBucketsPerElement(numElements));
        int bucketsPerElement = Math.min(targetBucketsPerElem, maxBucketsPerElement);
        if (bucketsPerElement < targetBucketsPerElem)
        {
            logger.warn(String.format("Cannot provide an optimal BloomFilter for %d elements (%d/%d buckets per element).",
                                      numElements, bucketsPerElement, targetBucketsPerElem));
        }
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
        return new BloomFilter(spec.K, bucketsFor(numElements, spec.bucketsPerElement));
    }

    /**
    * @return The smallest BloomFilter that can provide the given false positive
    * probability rate for the given number of elements.
    *
    * Asserts that the given probability can be satisfied using this filter.
    */
    public static BloomFilter getFilter(long numElements, double maxFalsePosProbability)
    {
        assert maxFalsePosProbability <= 1.0 : "Invalid probability";
        int bucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement, maxFalsePosProbability);
        return new BloomFilter(spec.K, bucketsFor(numElements, spec.bucketsPerElement));
    }

    public int buckets()
    {
      return (int) bitset.size();
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

    static long[] getHashBuckets(String key, int hashCount, long max)
    {
        return getHashBuckets(toByteBuffer(key, null), hashCount, max);
    }
    
    public void add(ByteBuffer key)
    {
        elementCount++;
        
        for (long bucketIndex : getHashBuckets(key))
        {
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
      for (long bucketIndex : getHashBuckets(key))
      {
          if (!bitset.get(bucketIndex))
          {
              return false;
          }
      }
      return true;
    }
    
    public void add(byte[] key)
    {
        add(ByteBuffer.wrap(key));
    }
    
    public boolean isPresent(byte[] key)
    {
        return isPresent(ByteBuffer.wrap(key));
    }

    public void add(String key)
    {
        add(toBB(key));
    }
    
    /**
     * 
     */
    public boolean isPresent(String key)
    {
        // cannot use shared byte buffer here, because calls here are highly concurrent
        return isPresent(toByteBuffer(key,null));
    }
    
    private ByteBuffer toBB(String s)
    {
        return this.strBuffer=toByteBuffer(s,strBuffer);
    }
    
    public static ByteBuffer toByteBuffer(String s, ByteBuffer byteBuffer)
    {
        int strLen=s.length()*2;
        if (byteBuffer==null || byteBuffer.capacity()<strLen)
        {
            byteBuffer=ByteBuffer.allocate( Math.max(strLen*2,512) );
        }
        else
            byteBuffer.clear();
        
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

    public void clear()
    {
        bitset.clear(0, bitset.size());
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
