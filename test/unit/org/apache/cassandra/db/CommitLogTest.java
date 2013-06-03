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

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegment.CommitLogContext;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class CommitLogTest extends CleanupHelper
{
    @Test
    public void testCleanup() throws IOException, ExecutionException, InterruptedException
    {
        assert CommitLog.instance().getSegmentCount() == 1;
        CommitLog.setSegmentSize(1000);
        
        DatabaseDescriptor.setMaxCommitLogSegmentsActive(0);

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store1 = table.getColumnFamilyStore("Standard1");
        ColumnFamilyStore store2 = table.getColumnFamilyStore("Standard2");
        RowMutation rm;
        byte[] value = new byte[501];

        // add data.  use relatively large values to force quick segment creation since we have a low flush threshold in the test config.
        for (int i = 0; i < 10; i++)
        {
            rm = new RowMutation("Keyspace1", "key1");
            rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), value, 0);
            rm.add(new QueryPath("Standard2", null, "Column1".getBytes()), value, 0);
            rm.apply();
        }
        assert CommitLog.instance().getSegmentCount() > 1;

        // nothing should get removed after flushing just Standard1
        store1.forceBlockingFlush();
        assert CommitLog.instance().getSegmentCount() > 1;

        // after flushing Standard2 we should be able to clean out all segments
        store2.forceBlockingFlush();
        assert CommitLog.instance().getSegmentCount() == 1;
    }

    @Test
    public void testRecoveryWithPartiallyWrittenHeader() throws Exception
    {
        File tmpFile = File.createTempFile("testRecoveryWithPartiallyWrittenHeaderTestFile", null);
        tmpFile.deleteOnExit();
        OutputStream out = new FileOutputStream(tmpFile);
        out.write(new byte[6]);
        //statics make it annoying to test things correctly
        CommitLog.instance().recover(new File[] {tmpFile},false); //CASSANDRA-1119 throws on failure
    }
    
    @Test
    public void testDontDeleteIfDirty() throws Exception
    {
        CommitLog.instance().resetUnsafe();
        CommitLog.instance().setSegmentSize(128 * 1024 * 1024);
        // Roughly 32 MB mutation
        RowMutation rm = new RowMutation("Keyspace1", "k");
        rm.add(new QueryPath("Standard1", null, "c1".getBytes()), new byte[32 * 1024 * 1024], 0);
        DataOutputBuffer buf = serialize(rm);

        // Adding it 5 times
        CommitLog.instance().add(rm,buf);
        CommitLog.instance().add(rm,buf);
        CommitLog.instance().add(rm,buf);
        CommitLog.instance().add(rm,buf);
        CommitLog.instance().add(rm,buf);

        // Adding new mutation on another CF
        RowMutation rm2 = new RowMutation("Keyspace1", "k");
        rm2.add(new QueryPath("Standard2", null, "c1".getBytes()), new byte[4], 0);
        DataOutputBuffer buf2 = serialize(rm2);
        CommitLog.instance().add(rm2,buf2);

        assert CommitLog.instance().segmentsCount() == 2 : "Expecting 2 segments, got " + CommitLog.instance().segmentsCount();
        
        Table table = Table.open(rm2.getTable());
        String cf = rm2.getColumnFamilies().iterator().next().name();
        CommitLog.instance().discardCompletedSegments(rm2.getTable(), cf, CommitLog.instance().getContext().get());

        // Assert we still have both our segment
        assert CommitLog.instance().segmentsCount() == 2 : "Expecting 2 segments, got " + CommitLog.instance().segmentsCount();
    }

    private DataOutputBuffer serialize(RowMutation rm2) throws IOException
    {
        DataOutputBuffer buf2 = new DataOutputBuffer();
        RowMutation.serializer().serialize(rm2, buf2);
        return buf2;
    }

    @Test
    public void testDeleteIfNotDirty() throws Exception
    {
        CommitLog.instance().resetUnsafe();
        CommitLog.instance().setSegmentSize(128 * 1024 * 1024);
        // Roughly 32 MB mutation
        RowMutation rm = new RowMutation("Keyspace1", "k");
        rm.add(new QueryPath("Standard1", null, "c1".getBytes()), new byte[32 * 1024 * 1024], 0);

        // Adding it twice (won't change segment)
        DataOutputBuffer buffer = serialize(rm);
        CommitLog.instance().add(rm,buffer);
        CommitLog.instance().add(rm,buffer);

        assert CommitLog.instance().segmentsCount() == 1 : "Expecting 1 segment, got " + CommitLog.instance().segmentsCount();

        // "Flush": this won't delete anything
        Table table = Table.open(rm.getTable());
        String cf = rm.getColumnFamilies().iterator().next().name();
        CommitLog.instance().discardCompletedSegments(rm.getTable(), cf, CommitLog.instance().getContext().get());

        assert CommitLog.instance().segmentsCount() == 1 : "Expecting 1 segment, got " + CommitLog.instance().segmentsCount();

        // Adding new mutation on another CF so that a new segment is created
        RowMutation rm2 = new RowMutation("Keyspace1", "k");
        rm2.add(new QueryPath("Standard2", null, "c1".getBytes()), new byte[64 * 1024 * 1024], 0);
        DataOutputBuffer buffer2 = serialize(rm2);
        CommitLog.instance().add(rm2,buffer2);
        CommitLog.instance().add(rm2,buffer2);

        assert CommitLog.instance().segmentsCount() == 2 : "Expecting 2 segments, got " + CommitLog.instance().segmentsCount();


        // "Flush" second cf: The first segment should be deleted since we
        // didn't write anything on cf1 since last flush (and we flush cf2)

        table = Table.open(rm2.getTable());
        cf = rm2.getColumnFamilies().iterator().next().name();
        CommitLog.instance().discardCompletedSegments(rm2.getTable(), cf, CommitLog.instance().getContext().get());

        // Assert we still have both our segment
        assert CommitLog.instance().segmentsCount() == 1 : "Expecting 1 segment, got " + CommitLog.instance().segmentsCount();
    }

    @Test
    public void testForceFlush() throws IOException, ExecutionException, InterruptedException
    {
        assert CommitLog.instance().getSegmentCount() == 1;
        CommitLog.setSegmentSize(1000);
        
        DatabaseDescriptor.setMaxCommitLogSegmentsActive(4);

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store1 = table.getColumnFamilyStore("Standard1");
        ColumnFamilyStore store2 = table.getColumnFamilyStore("Standard2");
        RowMutation rm;
        byte[] value = new byte[501];

        // add data.  use relatively large values to force quick segment creation since we have a low flush threshold in the test config.
        for (int i = 0; i < 10; i++)
        {
            rm = new RowMutation("Keyspace1", "key1");
            rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), value, 0);
            rm.add(new QueryPath("Standard2", null, "Column1".getBytes()), value, 0);
            rm.apply();
        }
        assert CommitLog.instance().getSegmentCount() > 1;

        // nothing should get removed after flushing just Standard1
        store1.forceBlockingFlush();
        assert CommitLog.instance().getSegmentCount() > 1;

        CommitLog.instance().watchMaxCommitLogs().get();
        
        // wait while store2 flushes by itself
        long ct = System.currentTimeMillis();
        while (!store2.getMemtablesPendingFlush().isEmpty()) {
            Thread.sleep(100);
            
            if (ct+2000<System.currentTimeMillis()) {
                assert false : "Flush timeout";
            }
        }

        // after flushing Standard2 we should be able to clean out all segments
        assert CommitLog.instance().getSegmentCount() == 1;
    }
    
}
