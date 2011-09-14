/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.BloomFilterWriter;
import org.junit.Before;
import org.junit.Test;

public class BloomFilterTest
{
    public BloomFilter bf;

    public BloomFilterTest()
    {
        bf = BloomFilter.getFilter(FilterTest.ELEMENTS, FilterTest.MAX_FAILURE_RATE);
    }

    @Before
    public void clear()
    {
        bf.clear();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testBloomLimits1()
    {
        int maxBuckets = BloomCalculations.probs.length - 1;
        int maxK = BloomCalculations.probs[maxBuckets].length - 1;

        // possible
        BloomCalculations.computeBloomSpec(maxBuckets, BloomCalculations.probs[maxBuckets][maxK]);

        // impossible, throws
        BloomCalculations.computeBloomSpec(maxBuckets, BloomCalculations.probs[maxBuckets][maxK] / 2);
    }

    @Test
    public void testOne()
    {
        bf.add("a");
        assert bf.isPresent("a");
        assert !bf.isPresent("b");
    }

    @Test
    public void testFalsePositivesInt()
    {
        FilterTest.testFalsePositives(bf, FilterTest.intKeys(), FilterTest.randomKeys2());
    }

    @Test
    public void testFalsePositivesRandom()
    {
        FilterTest.testFalsePositives(bf, FilterTest.randomKeys(), FilterTest.randomKeys2());
    }

    @Test
    public void testWords()
    {
        if (KeyGenerator.WordGenerator.WORDS == 0)
        {
            return;
        }
        BloomFilter bf2 = BloomFilter.getFilter(KeyGenerator.WordGenerator.WORDS / 2, FilterTest.MAX_FAILURE_RATE);
        int skipEven = KeyGenerator.WordGenerator.WORDS % 2 == 0 ? 0 : 2;
        FilterTest.testFalsePositives(bf2,
                                      new KeyGenerator.WordGenerator(skipEven, 2),
                                      new KeyGenerator.WordGenerator(1, 2));
    }

    @Test
    public void testSerialize() throws IOException
    {
        FilterTest.testSerialize(bf);
    }

    @Test
    public void testBloom() throws Exception
    {
        BloomFilterWriter bw=new BloomFilterWriter("/tmp/ram/filter.db", 100000, true);
        
        RandomPartitioner p = new RandomPartitioner();

        for (int i=1;i<100000;i++)
        {
            DecoratedKey<?> key = p.decorateKey(Integer.toHexString(i));
            bw.add(key, FBUtilities.toByteArray(i+2) );
        }
        
        BloomFilter built = bw.build();
        
        for (int i=1;i<100000;i++)
        {
            DecoratedKey<?> key = p.decorateKey(Integer.toHexString(i));
            assert mayPresent(built,key, FBUtilities.toByteArray(i+2)) : "Did not found:"+i;
        }

        BloomFilter read;
        DataInputStream stream = new DataInputStream(new FileInputStream("/tmp/ram/filter.db"));
        try
        {
            read = BloomFilter.serializerForSSTable().deserialize(stream);
        }
        finally
        {
            stream.close();
        }
        
        for (int i=1;i<100000;i++)
        {
            DecoratedKey<?> key = p.decorateKey(Integer.toHexString(i));
            assert mayPresent(read,key, FBUtilities.toByteArray(i+2));
        }

    }

    public boolean mayPresent(BloomFilter bf, DecoratedKey key, byte[] name)
    {
        int capacity = key.key.length()*2+name.length;
        ByteBuffer bb = ByteBuffer.allocate(capacity);
        
        // this is fuckup:
        bb=BloomFilter.toByteBuffer(key.key, bb);
        
        bb.position(bb.limit()).limit(capacity);
        
        bb.put(name);
        
        assert bb.remaining() == 0;
        
        // this was fuckup:
        boolean present = bf.isPresent((ByteBuffer) bb.flip());

        return present;
    }
    
    /* TODO move these into a nightly suite (they take 5-10 minutes each) 
    @Test
    // run with -mx1G
    public void testBigInt() {
        int size = 100 * 1000 * 1000;
        bf = BloomFilter.getFilter(size, FilterTest.spec.bucketsPerElement);
        FilterTest.testFalsePositives(bf,
                                      new KeyGenerator.IntGenerator(size),
                                      new KeyGenerator.IntGenerator(size, size * 2));
    }

    @Test
    public void testBigRandom() {
        int size = 100 * 1000 * 1000;
        bf = BloomFilter.getFilter(size, FilterTest.spec.bucketsPerElement);
        FilterTest.testFalsePositives(bf,
                                      new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                                      new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
    }

    @Test
    public void timeit() {
        int size = 300 * FilterTest.ELEMENTS;
        bf = BloomFilter.getFilter(size, FilterTest.spec.bucketsPerElement);
        for (int i = 0; i < 10; i++) {
            FilterTest.testFalsePositives(bf,
                                          new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                                          new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
            bf.clear();
        }
    }
    
    */
}
