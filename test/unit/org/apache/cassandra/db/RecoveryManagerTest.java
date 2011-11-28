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
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.db.TableTest.assertColumns;

public class RecoveryManagerTest extends CleanupHelper
{
    @Test
    public void testNothing() throws IOException {
        // TODO nothing to recover
        CommitLog.recover();
    }

    @Test
    public void testOne() throws IOException, ExecutionException, InterruptedException
    {
        Table table1 = Table.open("Keyspace1");
        Table table2 = Table.open("Keyspace2");

        RowMutation rm;
        ColumnFamily cf;

        rm = new RowMutation("Keyspace1", "keymulti");
        cf = ColumnFamily.create("Keyspace1", "Standard1");
        cf.addColumn(column("col1", "val1", 1L));
        rm.add(cf);
        rm.apply();

        rm = new RowMutation("Keyspace2", "keymulti");
        cf = ColumnFamily.create("Keyspace2", "Standard3");
        cf.addColumn(column("col2", "val2", 1L));
        rm.add(cf);
        rm.apply();

        table1.getColumnFamilyStore("Standard1").clearUnsafe();
        table2.getColumnFamilyStore("Standard3").clearUnsafe();

        CommitLog.recover();

        assertColumns(table1.get("keymulti", "Standard1"), "col1");
        assertColumns(table2.get("keymulti", "Standard3"), "col2");
    }
    
    @Test
    public void testPartialReplay() throws IOException, ExecutionException, InterruptedException
    {

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store1 = table.getColumnFamilyStore("Standard1");
        ColumnFamilyStore store2 = table.getColumnFamilyStore("Standard2");

        genCommits(store1, store2);
        
        // now replaying with max timestamp = 5. only 5 mutations must be applied

        CommitLog.instance().forcedRecover(5);
        
        ColumnFamily cf1 = store1.getColumnFamily(new IdentityQueryFilter("key33", new QueryPath("Standard1")));

        for (int i = 1; i < 10; i++)
        {
            IColumn c = cf1.getColumn(("Column33"+i).getBytes());
            if (i>5)
                assert c == null;
            if (i<=5)
                assert c.timestamp()==i;
        }
        
        ColumnFamily cf2 = store2.getColumnFamily(new IdentityQueryFilter("key33", new QueryPath("Standard2")));
        assert cf2.getColumn("Column33".getBytes()).timestamp()==5;

        store1 = table.getColumnFamilyStore("Standard1");
        store2 = table.getColumnFamilyStore("Standard2");
        genCommits(store1, store2);
        
        // now replaying with max timestamp = 0. no mutations must be applied

        CommitLog.instance().forcedRecover(0);

        cf1 = store1.getColumnFamily(new IdentityQueryFilter("key33", new QueryPath("Standard1")));
        for (int i = 1; i < 10; i++)
        {
            IColumn c = cf1.getColumn(("Column33"+i).getBytes());
            
            assert c == null : c.toString();
        }

        cf2 = store2.getColumnFamily(new IdentityQueryFilter("key33", new QueryPath("Standard2")));
        assert cf2.getColumn("Column33".getBytes()).timestamp() == 0l;

        genCommits(store1, store2);

        // now replaying with no max timestamp. all mutations must be there

        CommitLog.instance().forcedRecover(Long.MAX_VALUE);

        cf1 = store1.getColumnFamily(new IdentityQueryFilter("key33", new QueryPath("Standard1")));
        for (int i = 1; i < 10; i++)
        {
            IColumn c = cf1.getColumn(("Column33"+i).getBytes());
            
            assert c.timestamp()==i;
        }

        cf2 = store2.getColumnFamily(new IdentityQueryFilter("key33", new QueryPath("Standard2")));
        assert cf2.getColumn("Column33".getBytes()).timestamp() == 9;
        
    }

    private void genCommits(ColumnFamilyStore store1, ColumnFamilyStore store2)
            throws IOException
    {
        store1.clearUnsafe();
        store2.clearUnsafe();
        CommitLog.instance().resetUnsafe();
        
        assert CommitLog.instance().getSegmentCount() == 1;
        CommitLog.setSegmentSize(10000000);
        
        RowMutation rm;
        byte[] value = new byte[501];

        // add data.  use relatively large values to force quick segment creation since we have a low flush threshold in the test config.
        for (int i = 0; i < 10; i++)
        {
            rm = new RowMutation("Keyspace1", "key33");
            rm.add(new QueryPath("Standard1", null, ("Column33"+i).getBytes()), value, i);
            rm.add(new QueryPath("Standard2", null, "Column33".getBytes()), value, i);
            rm.apply();
        }
        assert CommitLog.instance().getSegmentCount() == 1;

        CommitLog.instance().forceNewSegment();

        store1.clearUnsafe();
        store2.clearUnsafe();
    }
    
 
}
