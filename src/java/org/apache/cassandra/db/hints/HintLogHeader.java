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

package org.apache.cassandra.db.hints;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.cassandra.io.ICompactSerializer;

class HintLogHeader
{
    public static String getHeaderPathFromSegment(HintLogSegment segment)
    {
        return getHeaderPathFromSegmentPath(segment.getPath());
    }

    public static String getHeaderPathFromSegmentPath(String segmentPath)
    {
        return segmentPath + ".header";
    }

    private static HintLogHeaderSerializer serializer = new HintLogHeaderSerializer();

    static HintLogHeaderSerializer serializer()
    {
        return serializer;
    }
        
    static long getLowestPosition(HintLogHeader clHeader)
    {
        return clHeader.replayedAt;
    }

    private long replayedAt; // position till which hints was successfully replayed to endpoint
    
    HintLogHeader()
    {
        replayedAt = 0L;
    }
    
    /*
     * This ctor is used while deserializing. 
    */
    HintLogHeader(long replayedPosition)
    {
        this.replayedAt = replayedPosition;
    }
        
    long getPosition()
    {
        return replayedAt;
    }
    
    void setReplayedPosition(long position)
    {
        replayedAt = position;
    }

    static void writeCommitLogHeader(HintLogHeader header, String headerFile) throws IOException
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

    static HintLogHeader readCommitLogHeader(String headerFile) throws IOException
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

    static class HintLogHeaderSerializer implements ICompactSerializer<HintLogHeader>
    {
        public void serialize(HintLogHeader clHeader, DataOutputStream dos) throws IOException
        {
            dos.writeLong(clHeader.replayedAt);
        }

        public HintLogHeader deserialize(DataInputStream dis) throws IOException
        {
            long  position = dis.readLong();
            return new HintLogHeader(position);
        }
    }
}
