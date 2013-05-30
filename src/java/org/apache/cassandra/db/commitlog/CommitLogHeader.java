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

package org.apache.cassandra.db.commitlog;

import java.io.*;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.BitSetSerializer;

public class CommitLogHeader
{
    public static String getHeaderPathFromSegment(CommitLogSegment segment)
    {
        return getHeaderPathFromSegmentPath(segment.getPath());
    }

    public static String getHeaderPathFromSegmentPath(String segmentPath)
    {
        return segmentPath + ".header";
    }

    public static boolean possibleCommitLogHeaderFile(String filename)
    {
        return filename.matches("CommitLog-\\d+.log.header");
    }
    

    private static CommitLogHeaderSerializer serializer = new CommitLogHeaderSerializer();

    static CommitLogHeaderSerializer serializer()
    {
        return serializer;
    }
        
    static int getLowestPosition(CommitLogHeader clHeader)
    {
        int minPosition = Integer.MAX_VALUE;
        for (int position : clHeader.cfDirtiedAt)
        {
            if ( position < minPosition && position >= 0)
            {
                minPosition = position;
            }
        }
        
        if(minPosition == Integer.MAX_VALUE)
            minPosition = 0;
        return minPosition;
    }

    private BitSet dirty; // columnfamilies with un-flushed data in this CommitLog
    private int[] cfDirtiedAt; // position at which each CF was last flushed
    private int[] cfLastWriteAt; // position at which last write was done per CF
    
    CommitLogHeader(int size)
    {
        dirty = new BitSet(size);
        cfDirtiedAt = new int[size];
        Arrays.fill(cfDirtiedAt, -1);
        cfLastWriteAt = new int[size];
        Arrays.fill(cfLastWriteAt, -1);
    }
    
    /*
     * This ctor is used while deserializing. This ctor
     * also builds an index of position to column family
     * Id.
    */
    CommitLogHeader(BitSet dirty, int[] cfDirtiedAt)
    {
        this.dirty = dirty;
        this.cfDirtiedAt = cfDirtiedAt;
    }
        
    boolean isDirty(int index)
    {
        return dirty.get(index);
    } 
    
    int getPosition(int index)
    {
        return cfDirtiedAt[index];
    }
    
    /**
     * Turns on dirty flag for specific CF, if neccessary or just renews last written position
     * 
     * @param index
     * @param position
     * @return true - if dirty flag was really turned on (consider write of the header to disk)
     */
    boolean turnOn(int index, long position)
    {
        cfLastWriteAt[index] = (int) position;

        if (!isDirty(index))
        {
            dirty.set(index);
            cfDirtiedAt[index] = (int) position;
            
            return true;
        }
        
        return false;
    }

    void turnOff(int index)
    {
        dirty.set(index, false);
        cfDirtiedAt[index] = -1;
    }

    /**
     * Turn the dirty bit off only if there has been no write since the flush
     * position was grabbed.
     * 
     * @param index cf id
     * @param position flush position grabbed at memtable flush
     * 
     * @return true, if made changes to header
     */
    boolean turnOffIfNotWritten(int index, long position)
    {
        if (isDirty(index) && cfLastWriteAt[index] < position) {
            turnOff(index);
            
            return true;
        }
        
        return false;
    }

    boolean isSafeToDelete() 
    {
        return dirty.isEmpty();
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder("");
        sb.append("CLH(dirty={");
        for ( int i = 0; i < dirty.size(); ++i )
        {
            if (dirty.get(i))
            {
                sb.append(Table.TableMetadata.getColumnFamilyName(i)).append(", ");
            }
        }
        sb.append("}, flushed={");
        for (int i = 0; i < cfDirtiedAt.length; i++)
        {
            sb.append(Table.TableMetadata.getColumnFamilyName(i)).append(": ").append(cfDirtiedAt[i]).append(", ");
        }
        sb.append("})");
        return sb.toString();
    }

    public String dirtyString()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < dirty.length(); i++)
        {
            if (dirty.get(i))
            {
                sb.append(Table.TableMetadata.getColumnFamilyName(i)).append(", ");
            }
        }
        return sb.toString();
    }

    public Map<Integer, Integer> dirtyCFs()
    {
        HashMap<Integer,Integer> map = new HashMap<Integer, Integer>();
        for ( int i = 0; i < dirty.size(); ++i )
        {
            if (dirty.get(i))
            {
                map.put(i,cfDirtiedAt[i]);
            }
        }
        return map;
    }
    
    public int getFirstDirtyCFId() {
        return dirty.nextSetBit(0);
    }


    static void writeCommitLogHeader(CommitLogHeader header, String headerFile) throws IOException
    {
        DataOutputStream out = null;
        try
        {
            /*
             * FileOutputStream doesn't sync on flush/close.
             * As headers are "optional" now there is no reason to sync it.
             * This provides nearly double the performance of BRAF, more under heavey load.
             */
            out = new DataOutputStream(new FileOutputStream(headerFile));
            serializer.serialize(header, out);
        }
        finally
        {
            if (out != null)
                out.close();
        }
    }

    public static CommitLogHeader readCommitLogHeader(String headerFile) throws IOException
    {
        DataInputStream reader = null;
        try
        {
            reader = new DataInputStream(new BufferedInputStream(new FileInputStream(headerFile)));
            return serializer.deserialize(reader);
        }
        finally
        {
            if (reader!=null)
                reader.close();
        }
    }

//    static CommitLogHeader readCommitLogHeader(BufferedRandomAccessFile logReader) throws IOException
//    {
//        int size = (int)logReader.readLong();
//        byte[] bytes = new byte[size];
//        logReader.read(bytes);
//        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
//        return serializer().deserialize(new DataInputStream(byteStream));
//    }

    public int getColumnFamilyCount()
    {
        return cfDirtiedAt.length;
    }

    static class CommitLogHeaderSerializer implements ICompactSerializer<CommitLogHeader>
    {
        public void serialize(CommitLogHeader clHeader, DataOutputStream dos) throws IOException
        {
            BitSetSerializer.serialize(clHeader.dirty, dos);
            dos.writeInt(clHeader.cfDirtiedAt.length);
            for (int position : clHeader.cfDirtiedAt)
            {
                dos.writeInt(position);
            }
        }

        public CommitLogHeader deserialize(DataInputStream dis) throws IOException
        {
            BitSet bitFlags = BitSetSerializer.deserialize(dis);
            int[] position = new int[dis.readInt()];
            for (int i = 0; i < position.length; ++i)
            {
                position[i] = dis.readInt();
            }
            return new CommitLogHeader(bitFlags, position);
        }
    }
}
