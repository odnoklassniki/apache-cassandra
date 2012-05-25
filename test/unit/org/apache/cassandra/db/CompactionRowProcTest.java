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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.proc.TestRowProcessor;
import org.apache.cassandra.io.SSTableReader;
import org.junit.Test;

public class CompactionRowProcTest extends CleanupHelper
{
    public static final String TABLE1 = "Keyspace1";
    public static final String TABLE2 = "Keyspace2";

    @Test
    public void testMajorCompactionPurge() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(TABLE1);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

        String key = "key1";
        RowMutation rm;
        
        TestRowProcessor.count = 0;
        TestRowProcessor.active = true;

        // inserts
        rm = new RowMutation(TABLE1, key);
        for (int i = 0; i < 10; i++)
        {
            rm.add(new QueryPath(cfName, null, String.valueOf(i).getBytes()), new byte[0], 0);
        }
        rm.apply();
        assert TestRowProcessor.count==0;
        cfs.forceBlockingFlush();
        assert TestRowProcessor.count==0;

        // inserts
        rm = new RowMutation(TABLE1, key);
        for (int i = 0; i < 10; i++)
        {
            rm.add(new QueryPath(cfName, null, String.valueOf(i).getBytes()), new byte[0], 0);
        }
        rm.apply();
        TestRowProcessor.shouldProcessIncomplete = true;
        cfs.forceBlockingFlush();
        assert TestRowProcessor.count==3; // 2 procs in chain, one is doing +=1, another +=2
        
        // inserts
        rm = new RowMutation(TABLE1, key);
        for (int i = 0; i < 10; i++)
        {
            rm.add(new QueryPath(cfName, null, String.valueOf(i).getBytes()), new byte[0], 0);
        }
        rm.apply();
        cfs.forceBlockingFlush();

        TestRowProcessor.count = 0;
        CompactionManager.instance.doCompaction(cfs, Collections.singleton(cfs.getSSTables().iterator().next()), 0);
        assert TestRowProcessor.count==0; // 2 procs in chain, one is doing +=1, another +=2
        
        TestRowProcessor.count = 0;
        TestRowProcessor.shouldProcessUnchanged = true;
        CompactionManager.instance.doCompaction(cfs, Collections.singleton(cfs.getSSTables().iterator().next()), 0);
        assert TestRowProcessor.count==3; // 2 procs in chain, one is doing +=1, another +=2

        TestRowProcessor.count = 0;
        TestRowProcessor.shouldProcessUnchanged = false;
        TestRowProcessor.shouldProcessIncomplete = true;
        Iterator<SSTableReader> iterator = cfs.getSSTables().iterator();
        CompactionManager.instance.doCompaction(cfs, Arrays.asList(iterator.next(),iterator.next()), 0);
        assert TestRowProcessor.count==3; // 2 procs in chain, one is doing +=1, another +=2

        TestRowProcessor.count = 0;
        TestRowProcessor.shouldProcessUnchanged = false;
        TestRowProcessor.shouldProcessIncomplete = false;
        iterator = cfs.getSSTables().iterator();
        CompactionManager.instance.doCompaction(cfs, Arrays.asList(iterator.next(),iterator.next()), 0);
        assert TestRowProcessor.count==3; // 2 procs in chain, one is doing +=1, another +=2

        TestRowProcessor.active = false;
    }

}
