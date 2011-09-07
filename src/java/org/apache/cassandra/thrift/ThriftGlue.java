package org.apache.cassandra.thrift;
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


import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.utils.ByteBufferUtil;


public class ThriftGlue
{
    private static ColumnOrSuperColumn createColumnOrSuperColumn(Column col, SuperColumn scol)
    {
        ColumnOrSuperColumn ret = new ColumnOrSuperColumn();
        ret.setColumn(col);
        ret.setSuper_column(scol);
        return ret;
    }

    public static ColumnOrSuperColumn createColumnOrSuperColumn_Column(Column col)
    {
        return createColumnOrSuperColumn(col, null);
    }

    public static ColumnOrSuperColumn createColumnOrSuperColumn_SuperColumn(SuperColumn scol)
    {
        return createColumnOrSuperColumn(null, scol);
    }

    public static ColumnParent createColumnParent(String columnFamily, byte[] super_column)
    {
        ColumnParent ret = new ColumnParent(columnFamily);
        ret.setSuper_column(super_column);
        return ret;
    }

    public static ColumnParent createColumnParent(String columnFamily, ByteBuffer super_column)
    {
        ColumnParent ret = new ColumnParent(columnFamily);
        ret.setSuper_column(super_column);
        return ret;
    }

    public static ColumnPath createColumnPath(String columnFamily, byte[] superColumnName, byte[] columnName)
    {
        ColumnPath ret = new ColumnPath(columnFamily);
        ret.setSuper_column(superColumnName);
        ret.setColumn(columnName);
        return ret;
    }

    public static ColumnPath createColumnPath(String columnFamily, ByteBuffer superColumnName, ByteBuffer columnName)
    {
        ColumnPath ret = new ColumnPath(columnFamily);
        ret.setSuper_column(superColumnName);
        ret.setColumn(columnName);
        return ret;
    }

    public static SlicePredicate createSlicePredicate(List<ByteBuffer> columns, SliceRange range)
    {
        SlicePredicate ret = new SlicePredicate();
        ret.setColumn_names(columns);
        ret.setSlice_range(range);
        return ret;
    }
    
    public static Column column(byte[] name, byte[] value, long timestamp)
    {
        return new Column(ByteBuffer.wrap(name), ByteBuffer.wrap(value), timestamp);
    }
    
    public static byte[] columnPath(ColumnPath columnPath)
    {
        return columnPath.isSetColumn() ? columnPath.getColumn() : columnPath.getSuper_column();
    }
    
    public static SliceRange sliceRange(byte[] start, byte[] finish, boolean reverse, int count)
    {
        return new SliceRange(ByteBuffer.wrap(start), ByteBuffer.wrap(finish), false, 0);
    }
    
    public static SuperColumn superColumn(byte[] name, List<Column> columns)
    {
        return new SuperColumn(ByteBuffer.wrap(name), columns);
    }
    
    public static List<byte[]> toBytes(List<ByteBuffer> list)
    {
        if (list==null)
            return null;
        
        return new TransformingList<ByteBuffer, byte[]>(list, new Transformer<ByteBuffer, byte[]>()
        {
            /* (non-Javadoc)
             * @see org.apache.cassandra.thrift.Transformer#transform(java.lang.Object)
             */
            @Override
            public byte[] transform(ByteBuffer o)
            {
                return ByteBufferUtil.getArray(o);
            }
        });
    }
    
}
