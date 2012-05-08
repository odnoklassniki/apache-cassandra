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
package org.apache.cassandra.db;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.ArrayUtils;
import static org.junit.Assert.assertNull;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.WrappedRunnable;

import java.net.InetAddress;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.CollatingOrderPreservingPartitioner;
import org.apache.cassandra.db.filter.ColumnsMayExistQueryFilter;
import org.apache.cassandra.db.filter.FastRowMayExistQueryFilter;
import org.apache.cassandra.db.filter.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.ColumnsMayExistQueryFilter.ColumnCollector;
import org.apache.cassandra.io.SSTableReader;

public class ColumnFamilyStoreTest extends CleanupHelper
{
    /**
     * @author Oleg Anastasyev<oa@hq.one.lv>
     *
     */
    private final class ColumnCollectorImplementation implements
            ColumnCollector
    {
        int c=0;

        @Override
        public boolean isCollected(byte[] name)
        {
            return c>0 && Arrays.equals(name,"Column1".getBytes());
        }

        @Override
        public void collect(byte[] name)
        {
            assert Arrays.equals(name,"Column1".getBytes()) : "Collected "+new String(name);
            c++;
        }

        /**
         * @return the c
         */
        public int getC()
        {
            return c;
        }
    }

    static byte[] bytes1, bytes2;

    static
    {
        Random random = new Random();
        bytes1 = new byte[1024];
        bytes2 = new byte[128];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);
    }

    @Test
    public void testGetColumnWithWrongBF() throws IOException, ExecutionException, InterruptedException
    {
        List<RowMutation> rms = new LinkedList<RowMutation>();
        RowMutation rm;
        rm = new RowMutation("Keyspace1", "key1");
        rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), "asdf".getBytes(), 0);
        rm.add(new QueryPath("Standard1", null, "Column2".getBytes()), "asdf".getBytes(), 0);
        rms.add(rm);
        ColumnFamilyStore store = Util.writeColumnFamily(rms);

        Table table = Table.open("Keyspace1");
        List<SSTableReader> ssTables = table.getAllSSTablesOnDisk();
        assertEquals(1, ssTables.size());
        ssTables.get(0).forceBloomFilterFailures();
        ColumnFamily cf = store.getColumnFamily(new IdentityQueryFilter("key2", new QueryPath("Standard1", null, "Column1".getBytes())));
        assertNull(cf);
    }

    @Test
    public void testEmptyRow() throws Exception
    {
        Table table = Table.open("Keyspace1");
        final ColumnFamilyStore store = table.getColumnFamilyStore("Standard2");
        RowMutation rm;

        rm = new RowMutation("Keyspace1", "key1");
        rm.delete(new QueryPath("Standard2", null, null), System.currentTimeMillis());
        rm.apply();

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                SliceQueryFilter sliceFilter = new SliceQueryFilter("key1", new QueryPath("Standard2", null, null), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, 1);
                ColumnFamily cf = store.getColumnFamily(sliceFilter);
                assert cf.isMarkedForDelete();
                assert cf.getColumnsMap().isEmpty();

                NamesQueryFilter namesFilter = new NamesQueryFilter("key1", new QueryPath("Standard2", null, null), "a".getBytes());
                cf = store.getColumnFamily(namesFilter);
                assert cf.isMarkedForDelete();
                assert cf.getColumnsMap().isEmpty();
            }
        };

        TableTest.reTest(store, r);
    }

    @Test
    public void testFastRowMayExist() throws Exception
    {
        final ColumnFamilyStore store = insert("row777");

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                FastRowMayExistQueryFilter filter = new FastRowMayExistQueryFilter("row777", new QueryPath("Standard1", null, null));
                ColumnFamily cf = store.getColumnFamily(filter);
                assert filter.mayExist();
                assert cf == null;

                filter = new FastRowMayExistQueryFilter("row778", new QueryPath("Standard1", null, null));
                cf = store.getColumnFamily(filter);
                assert !filter.mayExist();
                assert cf == null;

            }
        };

        TableTest.reTest(store, r);
    }

    @Test
    public void testColumnsMayExist() throws Exception
    {
        final ColumnFamilyStore store = insertc("row7779");

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                ColumnCollectorImplementation cc = new ColumnCollectorImplementation();
                
                ColumnsMayExistQueryFilter filter = new ColumnsMayExistQueryFilter("row7779", new QueryPath("Standard1c"),
                        Arrays.asList(new byte[][] {"Column1".getBytes(),"Column3".getBytes(),"Column4".getBytes()}), cc, 3);
                ColumnFamily cf = store.getColumnFamily(filter);
                assert cc.getC() == 1;
                assert cf == null;

                cc = new ColumnCollectorImplementation();
                filter = new ColumnsMayExistQueryFilter("row7779", new QueryPath("Standard1c"),
                        Arrays.asList(new byte[][] {"Column1".getBytes(),"Column3".getBytes(),"Column4".getBytes()}), cc, 0);
                cf = store.getColumnFamily(filter);
                assert cc.getC() == 0;
                assert cf == null;

                cc = new ColumnCollectorImplementation();
                filter = new ColumnsMayExistQueryFilter("row7779", new QueryPath("Standard1c"),
                        Arrays.asList(new byte[][] {"Column1".getBytes(),"Column1".getBytes(),"Column4".getBytes()}), cc, 3);
                cf = store.getColumnFamily(filter);
                assert cc.getC() == 1 :" c="+cc.getC();
                assert cf == null;

                cc = new ColumnCollectorImplementation();
                filter = new ColumnsMayExistQueryFilter("row7780", new QueryPath("Standard1c"),
                        Arrays.asList(new byte[][] {"Column1".getBytes(),"Column3".getBytes(),"Column4".getBytes()}), cc, 3);
                cf = store.getColumnFamily(filter);
                assert cc.getC() == 0;
                assert cf == null;

            }
        };

        TableTest.reTest(store, r);
    }

    @Test
    public void testListener() throws Exception
    {
        final ColumnFamilyStore store = insert("row7777");
        final AtomicInteger i = new AtomicInteger();

        store.setListener(new IStoreApplyListener()
        {
            
            @Override
            public boolean preapply(String key, ColumnFamily data)
            {
                assert key.equals("row7778");
                assert Arrays.equals(data.getColumn("Column1".getBytes()).value(),"asdf".getBytes());
                
                i.incrementAndGet();
                
                return true;
            }
            
        });

        assert i.get() == 0;
        
        insert("row7778");

        assert i.get() == 1;

        insert("row7778");

        assert i.get() == 2;
        
        store.setListener(null);

        insert("row7778");

        assert i.get() == 2;
    }

    /**
     * Writes out a bunch of keys into an SSTable, then runs anticompaction on a range.
     * Checks to see if anticompaction returns true.
     */
    private void testAntiCompaction(String columnFamilyName, int insertsPerTable) throws IOException, ExecutionException, InterruptedException
    {
        List<RowMutation> rms = new ArrayList<RowMutation>();
        for (int j = 0; j < insertsPerTable; j++)
        {
            String key = String.valueOf(j);
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath(columnFamilyName, null, "0".getBytes()), new byte[0], j);
            rms.add(rm);
        }
        ColumnFamilyStore store = Util.writeColumnFamily(rms);

        List<Range> ranges  = new ArrayList<Range>();
        IPartitioner partitioner = new CollatingOrderPreservingPartitioner();
        Range r = new Range(partitioner.getToken("0"), partitioner.getToken("zzzzzzz"));
        ranges.add(r);

        List<String> fileList = CompactionManager.instance.submitAnticompaction(store, ranges, InetAddress.getByName("127.0.0.1")).get();
        assert fileList.size() >= 1;
    }

    @Test
    public void testAntiCompaction1() throws IOException, ExecutionException, InterruptedException
    {
        testAntiCompaction("Standard1", 100);
    }

    private RangeSliceReply getRangeSlice(ColumnFamilyStore cfs, Token start, Token end) throws IOException, ExecutionException, InterruptedException
    {
        return cfs.getRangeSlice(ArrayUtils.EMPTY_BYTE_ARRAY,
                                 new Range(start, end),
                                 10,
                                 null,
                                 Arrays.asList("asdf".getBytes()));
    }

    private void assertKeys(List<Row> rows, List<String> keys) throws UnsupportedEncodingException
    {
        assertEquals(keys.size(), rows.size());
        for (int i = 0; i < keys.size(); i++)
        {
            assertEquals(keys.get(i), rows.get(i).key);
        }
    }

    @Test
    public void testSkipStartKey() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = insert("key1", "key2");
        IPartitioner p = StorageService.getPartitioner();

        RangeSliceReply result = getRangeSlice(cfs, p.getToken("key1"), p.getToken("key2"));
        assertKeys(result.rows, Arrays.asList("key2"));
    }

    private ColumnFamilyStore insert(String... keys) throws IOException, ExecutionException, InterruptedException
    {
        List<RowMutation> rms = new LinkedList<RowMutation>();
        RowMutation rm;
        for (String key : keys)
        {
            rm = new RowMutation("Keyspace2", key);
            rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), "asdf".getBytes(), 0);
            rms.add(rm);
        }

        return Util.writeColumnFamily(rms);
    }

    private ColumnFamilyStore insertc(String... keys) throws IOException, ExecutionException, InterruptedException
    {
        List<RowMutation> rms = new LinkedList<RowMutation>();
        RowMutation rm;
        for (String key : keys)
        {
            rm = new RowMutation("Keyspace2", key);
            rm.add(new QueryPath("Standard1c", null, "Column1".getBytes()), "asdf".getBytes(), 0);
            rms.add(rm);
        }

        return Util.writeColumnFamily(rms);
    }

}
