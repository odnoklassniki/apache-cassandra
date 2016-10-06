package org.apache.cassandra.io;
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


import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.FBUtilities;

public class SSTableWriter extends SSTable
{
    private static Logger logger = Logger.getLogger(SSTableWriter.class);

    private BufferedRandomAccessFile dataFile;
    private BufferedRandomAccessFile indexFile;
    private DecoratedKey lastWrittenKey;
    private BloomFilterWriter bfw;
    
    

    public SSTableWriter(String filename, long keyCount, long columnCount, IPartitioner partitioner) throws IOException
    {
        super(filename, partitioner);
        indexSummary = new IndexSummary();
        dataFile = new BufferedRandomAccessFile(path, "rw", (int)(DatabaseDescriptor.getFlushDataBufferSizeInMB() * 1024 * 1024));
        dataFile.setSkipCache(true);
        indexFile = new BufferedRandomAccessFile(indexFilename(), "rw", (int)(DatabaseDescriptor.getFlushIndexBufferSizeInMB() * 1024 * 1024));
        
        boolean bloomColumns = DatabaseDescriptor.getBloomColumns(getTableName(), getColumnFamilyName());
        
        bfw = new BloomFilterWriter(filterFilename(),  keyCount, columnCount, bloomColumns);
    }

    public SSTableWriter(String filename, long keyCount, long columnCount, IPartitioner partitioner, boolean columnBloom) throws IOException
    {
        super(filename, partitioner);
        indexSummary = new IndexSummary();
        dataFile = new BufferedRandomAccessFile(path, "rw", (int)(DatabaseDescriptor.getFlushDataBufferSizeInMB() * 1024 * 1024));
        dataFile.setSkipCache(true);
        indexFile = new BufferedRandomAccessFile(indexFilename(), "rw", (int)(DatabaseDescriptor.getFlushIndexBufferSizeInMB() * 1024 * 1024));
        
        bfw = new BloomFilterWriter(filterFilename(), keyCount, columnCount, columnBloom );
    }

    private long beforeAppend(DecoratedKey decoratedKey) throws IOException
    {
        if (decoratedKey == null)
        {
            throw new IOException("Keys must not be null.");
        }
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) > 0)
        {
            logger.info("Last written key : " + lastWrittenKey);
            logger.info("Current key : " + decoratedKey);
            logger.info("Writing into file " + path);
            throw new IOException("Keys must be written in ascending order.");
        }
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    long afterAppend(DecoratedKey decoratedKey, long dataPosition) throws IOException
    {
        String diskKey = partitioner.convertToDiskFormat(decoratedKey);
        bfw.add(decoratedKey);
        lastWrittenKey = decoratedKey;
        long indexPosition = indexFile.getFilePointer();
        indexFile.writeUTF(diskKey);
        indexFile.writeLong(dataPosition);
        if (logger.isTraceEnabled())
            logger.trace("wrote " + decoratedKey + " at " + dataPosition);
        if (logger.isTraceEnabled())
            logger.trace("wrote index of " + decoratedKey + " at " + indexPosition);

        long rowSize = dataFile.getFilePointer() - dataPosition;
        indexSummary.maybeAddEntry(decoratedKey, dataPosition, rowSize, indexPosition, indexFile.getFilePointer());
        
        return rowSize;
    }

    // TODO make this take a DataOutputStream and wrap the byte[] version to combine them
    public void append(DecoratedKey decoratedKey, DataOutputBuffer buffer) throws IOException
    {
        int length = buffer.getLength();
        long currentPosition = startAppend(decoratedKey, length);
        dataFile.write(buffer.getData(), 0, length);
        afterAppend(decoratedKey, currentPosition);
    }
    
    long startAppend(DecoratedKey decoratedKey, int cfLength) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        dataFile.writeUTF(partitioner.convertToDiskFormat(decoratedKey));
        assert cfLength > 0;
        dataFile.writeInt(cfLength);

        return currentPosition;
    }
    
    DataOutput getRowOutput()
    {
        return dataFile;
    }
    

    public void append(DecoratedKey decoratedKey, byte[] value) throws IOException
    {
        int length = value.length;
        long currentPosition = startAppend(decoratedKey, length);
        dataFile.write(value);
        afterAppend(decoratedKey, currentPosition);
    }
    
    public BloomFilterWriter getBloomFilterWriter()
    {
        return bfw;
    }

    /**
     * Renames temporary SSTable files to valid data, index, and bloom filter files
     */
    public void close() throws IOException
    {
        // filter
        bfw.build();

        // index
        indexFile.getChannel().force(true);
        indexFile.close();

        // main data
        dataFile.close(); // calls force

        indexPath = rename(indexFilename());
        rename(filterFilename());
        path = rename(path); // important to do this last since index & filter file names are derived from it

        indexSummary.complete();
    }
    
    /**
     * Renames temporary SSTable files to valid data, index, and bloom filter files and returns an SSTableReader
     */
    public SSTableReader closeAndOpenReader() throws IOException
    {
        this.close();
        return new SSTableReader(path, partitioner, indexSummary, bfw.getFilter());
    }

    static String rename(String tmpFilename)
    {
        String filename = tmpFilename.replace("-" + SSTable.TEMPFILE_MARKER, "");
        try
        {
            FBUtilities.renameWithConfirm(tmpFilename, filename);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return filename;
    }

    public long getFilePointer()
    {
        return dataFile.getFilePointer();
    }
    
    public static SSTableReader renameAndOpen(String dataFileName) throws IOException
    {
        SSTableWriter.rename(indexFilename(dataFileName));
        SSTableWriter.rename(filterFilename(dataFileName));
        dataFileName = SSTableWriter.rename(dataFileName);
        return SSTableReader.open(dataFileName);
    }

}
