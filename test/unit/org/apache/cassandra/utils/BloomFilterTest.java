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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DatabaseDescriptor.DiskAccessMode;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.BloomFilterWriter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.obs.MappedFileBitSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BloomFilterTest
{
    public BloomFilter bf;

    public BloomFilterTest()
    {
    }

    @Before
    public void clear() throws IOException
    {
        new File("/tmp/ram/filter1.db").delete();
        bf = BloomFilter.create(FilterTest.ELEMENTS, FilterTest.MAX_FAILURE_RATE, "/tmp/ram/filter1.db");
    }
    
    @After
    public void close() {
        if ( bf != null ) {
            bf.close();
            bf = null;
            new File("/tmp/ram/filter1.db").delete();
        }
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
    public void testWords() throws IOException
    {
        if (KeyGenerator.WordGenerator.WORDS == 0)
        {
            return;
        }
        BloomFilter bf2 = BloomFilter.create(KeyGenerator.WordGenerator.WORDS / 2, FilterTest.MAX_FAILURE_RATE,"/tmp/filter.db");
        int skipEven = KeyGenerator.WordGenerator.WORDS % 2 == 0 ? 0 : 2;
        FilterTest.testFalsePositives(bf2,
                                      new KeyGenerator.WordGenerator(skipEven, 2),
                                      new KeyGenerator.WordGenerator(1, 2));
    }

    @Test
    public void testSerialize() throws IOException
    {
        BloomFilter bf = BloomFilter.getFilter( FilterTest.ELEMENTS, 15 );
        bf.add("a");
        DataOutputBuffer out = new DataOutputBuffer();
        bf.getSerializer().serialize((BloomFilter) bf, out);
        
        ByteArrayInputStream in = new ByteArrayInputStream(out.getData(), 0, out.getLength());
        Filter f2 = bf.getSerializer().deserialize(new DataInputStream(in));
        
        assert f2.isPresent("a");
        assert !f2.isPresent("b");
    }

    @Test
    public void testBloom() throws Exception
    {
        BloomFilterWriter bw=new BloomFilterWriter("/tmp/ram/filter.db", 100000, 100000, true);
        
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
            assert built.isPresent(key.key,FBUtilities.toByteArray(i+2)) : "Did not found:"+i;
        }

        BloomFilter read = BloomFilter.open("/tmp/ram/filter.db");
        
        for (int i=1;i<100000;i++)
        {
            DecoratedKey<?> key = p.decorateKey(Integer.toHexString(i));
            assert read.isPresent(key.key,FBUtilities.toByteArray(i+2));
        }
        
        DatabaseDescriptor.setFilterAccessMode( DiskAccessMode.mmap );

        read = BloomFilter.open("/tmp/ram/filter.db");
        
        assert read.bitset instanceof MappedFileBitSet;
        
        for (int i=1;i<100000;i++)
        {
            DecoratedKey<?> key = p.decorateKey(Integer.toHexString(i));
            assert read.isPresent(key.key,FBUtilities.toByteArray(i+2));
        }

        DatabaseDescriptor.setFilterAccessMode( DiskAccessMode.standard );

    }
    
    /* TODO move these into a nightly suite (they take 5-10 minutes each)  
    @Test
    // run with -mx1G
    public void testBigInt() throws IOException {
        int size = 100 * 1000 * 1000;
        new File("/tmp/ram/filter2.db").delete();
        bf = BloomFilter.create(size, FilterTest.spec.bucketsPerElement,"/tmp/ram/filter2.db");
        FilterTest.testFalsePositives(bf,
                                      new KeyGenerator.IntGenerator(size),
                                      new KeyGenerator.IntGenerator(size, size * 2));
        bf.close();
        new File("/tmp/ram/filter2.db").delete();
    }

    @Test
    public void testBigRandom() throws IOException {
        int size = 100 * 1000 * 1000;
        new File("/tmp/ram/filter2.db").delete();
        bf = BloomFilter.create(size, FilterTest.spec.bucketsPerElement,"/tmp/ram/filter2.db");
        FilterTest.testFalsePositives(bf,
                                      new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                                      new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
        bf.close();
        new File("/tmp/ram/filter2.db").delete();
    }

    @Test
    public void timeit() throws IOException {
        int size = 300 * FilterTest.ELEMENTS;
        for (int i = 0; i < 10; i++) {
            bf = BloomFilter.create(size, FilterTest.spec.bucketsPerElement,"/tmp/ram/filter2.db");
            FilterTest.testFalsePositives(bf,
                                          new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                                          new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
            bf.close();
            new File("/tmp/ram/filter2.db").delete();
        }
    }
    
    */
}
