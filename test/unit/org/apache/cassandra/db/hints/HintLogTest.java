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
package org.apache.cassandra.db.hints;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

public class HintLogTest extends CleanupHelper
{
    @Test
    public void testCleanup() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        HintLog.setSegmentSize(128*1024);
        HintLog.setSyncPeiod(100);
        HintLog.setHintDelivery(false);

        DatabaseDescriptor.setHintedHandoffManager("hintlog");
        
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        unregisterBean(mbs);
        HintLog hl = new HintLog();
        unregisterBean(mbs);
        
        InetAddress local = InetAddress.getByName("127.0.0.1");
        assert hl.getSegmentCount(local) == 1;

        Iterator<byte[]> iterator = hl.getHintsToDeliver(local);
        assert !iterator.hasNext();

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store1 = table.getColumnFamilyStore("Standard1");
        ColumnFamilyStore store2 = table.getColumnFamilyStore("Standard2");
        RowMutation rm;

        // add data.  use relatively large values to force quick segment creation since we have a low flush threshold in the test config.
        byte[] value = new byte[1024];
        rm = new RowMutation("Keyspace1", "key1");
        rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), value, 0);
        rm.add(new QueryPath("Standard2", null, "Column1".getBytes()), value, 0);
        for (int i = 0; i < 1290; i++)
        {
            value[0]=(byte) (i % 127);
            
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream( bos );
            
            RowMutation.serializer().serialize(rm, dos);
            
            hl.add(local, bos.toByteArray() );
        }
        
        hl.forceNewSegment(local);
     
        int segmentCount = hl.getSegmentCount(local);
        assert segmentCount > 1;
        
        iterator = hl.getHintsToDeliver(local);
        assert iterator.hasNext();
        
        assert segmentCount == hl.getSegmentCount(local) : ""+segmentCount+","+hl.instance().getSegmentCount(local) ;
        
        int ii=0;
        while (iterator.hasNext())
        {
            byte[] bb = iterator.next();
            RowMutation r = RowMutation.serializer().deserialize(new DataInputStream(new ByteArrayInputStream(bb)));
            
            assert r.getTable().equals("Keyspace1");
            assert r.key().equals("key1");
            assert r.getColumnFamilies().iterator().next().getColumnsMap().values().iterator().next().value()[0]==(byte) (ii % 127);
            assert r.getColumnFamilies().iterator().next().getColumnsMap().values().iterator().next().value()[0]==(byte) (ii % 127);
            ii++;
            
            iterator.remove();
        }
        
        assert ii == 1290;

        // after all hints served all hint logs must be removed
        assert hl.getSegmentCount(local) == 1;

        // testing how it reads files from disk
        for (int i = 1290; i < 3*1290; i++)
        {
            value[0]=(byte) (i % 127);
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream( bos );
            
            RowMutation.serializer().serialize(rm, dos);
            
            hl.add(local, bos.toByteArray() );
        }

        segmentCount = hl.forceNewSegment(local).size();
        
        
        // now it will reread from disk

        
        unregisterBean(mbs);
        hl = new HintLog();
        mbs = ManagementFactory.getPlatformMBeanServer();
        unregisterBean(mbs);
        assert hl.getSegmentCount(local) == segmentCount + 1;

        iterator = hl.getHintsToDeliver(local);
        assert iterator.hasNext();
        
        assert segmentCount + 1 == hl.getSegmentCount(local)  : ""+segmentCount+","+hl.getSegmentCount(local) ;
        
        ii=1290;
        while (iterator.hasNext())
        {
            byte[] bb = iterator.next();
            RowMutation r = RowMutation.serializer().deserialize(new DataInputStream(new ByteArrayInputStream(bb)));
            
            assert r.getTable().equals("Keyspace1");
            assert r.key().equals("key1");
            byte b = r.getColumnFamilies().iterator().next().getColumnsMap().values().iterator().next().value()[0];
            assert b==(byte) (ii % 127) : ""+b+","+ ii%127 +" - "+ii;
            byte d = r.getColumnFamilies().iterator().next().getColumnsMap().values().iterator().next().value()[0];
            assert d==(byte) (ii % 127) : ""+d+","+ ii%127 +" - "+ii;
            ii++;
            
            iterator.remove();
        }
        
        assert ii == 3*1290;
        
        assert hl.getSegmentCount(local) == 1;
    }

    private void unregisterBean(MBeanServer mbs)
    {
        try
        {
            mbs.unregisterMBean(new ObjectName("org.apache.cassandra.db:type=Hintlog"));
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void test2() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        testCleanup();
    }
    
    public void main(String[] args)
    {
        
        for (int i=0;i<100000;i++)
        {
            cas();
            lbq();
        }

        long start1 = System.nanoTime();
        for (int i=10000000;i-->0;)
        {
            cas();
        }
        
        long end1 = System.nanoTime();
        
        long start2 = System.nanoTime();
        for (int i=10000000;i-->0;)
        {
            lbq();
        }
        
        long end2 = System.nanoTime();
        
        System.out.println("CAS: "+(end1-start1)/1000000+", LBQ: "+ (end2-start2)/1000000);
        
    }



    private void lbq()
    {
        LinkedBlockingQueue<Object> messages = new LinkedBlockingQueue<Object>();
        
        messages.add("akjdhakd kajdhad kajdh");
        messages.size();
        messages.add("akjdhakd kajdhad kajdh");
        messages.size();
        messages.add("akjdhakd kajdhad kajdh");
        messages.size();
    }

    private static byte[] b1 = {1,1,1,1};
    private static byte[] b2 = {1,1,1,2};
    private static byte[] b3 = {1,1,1,3};
    
    public static final int toInt(byte[] b) {
        return b[0]<<24 | (b[1]&0xff)<<16 | (b[2]&0xff)<<8 | (b[3]&0xff);
    }
    
    int c=1;

    private void cas()
    {
        AtomicIntegerArray ia = new AtomicIntegerArray(4);

        ia.set(c++, FBUtilities.byteArrayToInt(b1));
        ia.set(c++, FBUtilities.byteArrayToInt(b2));
        ia.set(c++, FBUtilities.byteArrayToInt(b3));

        ia.decrementAndGet(0);
        for (int i=4, a = FBUtilities.byteArrayToInt(b1);i-->1 && !ia.compareAndSet(i, a, 0););
        ia.decrementAndGet(0);
        for (int i=4, a = FBUtilities.byteArrayToInt(b2);i-->1 && !ia.compareAndSet(i, a, 0););
        ia.decrementAndGet(0);
        for (int i=4, a = FBUtilities.byteArrayToInt(b3);i-->1 && !ia.compareAndSet(i, a, 0););
    }
    
}
