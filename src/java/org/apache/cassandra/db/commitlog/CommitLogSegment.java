package org.apache.cassandra.db.commitlog;
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


import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DatabaseDescriptor.CommitLogSync;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.FSReadError;
import org.apache.cassandra.db.FSWriteError;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.DeletionService;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.log4j.Logger;
import org.xerial.snappy.Snappy;

public class CommitLogSegment
{
    private static final Logger logger = Logger.getLogger(CommitLogSegment.class);

    private final BufferedRandomAccessFile logWriter;
    private final CommitLogHeader header;

    // right after creation of a new commit log segment there is a flood of
    // column families with requests to turn on dirty flag in it.
    // this leads to many write header requests issued in first couple of seconds;
    // this lead to hiccups in commit logging thread, and, as a consequence to mutation acceptance rate.
    
    // to workaround this, we delay 1st write (and creation) of .header file for commit log period. this is ok, because if
    // there are no .header file - all CFs are considered dirty and will be anyway replayed if node crashed before 1st write of header file
    // this does not work, if you requested batched commit log mode (for batched commit mode, you dont care about write performance, right ?)
    private long  delayWriteUntil;

    public CommitLogSegment(int cfCount)
    {
        this.header = new CommitLogHeader(cfCount);
        long now = System.currentTimeMillis();
        String logFile = DatabaseDescriptor.getLogFileLocation() + File.separator + "CommitLog-" + now + ".log";
        
        // add special file extension, if compression is enabled
        if (DatabaseDescriptor.isLogFileCompression()) {
            logFile += CommitLog.COMPRESSION_EXTENSION;
        }

        logger.info("Creating new commitlog segment " + logFile);

        try
        {
            logWriter = createWriter(logFile);
            logWriter.setSkipCache(true);

            if (DatabaseDescriptor.getCommitLogSync() == CommitLogSync.periodic) {
                delayWriteUntil = now + DatabaseDescriptor.getCommitLogSyncPeriod();
            } else {
                delayWriteUntil = 0;
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e);
        }
    }

    public void writeHeader() throws IOException
    {
        if (delayWriteUntil==0 || delayWriteUntil<System.currentTimeMillis()) {
            String headerFile = getHeaderPath();
            if (delayWriteUntil!=0) {
                logger.info("Creating new commitlog segment header " + headerFile);
            }
            CommitLogHeader.writeCommitLogHeader(header, headerFile );

            delayWriteUntil = 0;
        }
    }
    
    private boolean isDelayedHeaderWritePending() {
        return delayWriteUntil!=0;
    }

    private static BufferedRandomAccessFile createWriter(String file) throws IOException
    {
        return new BufferedRandomAccessFile(file, "rw", 128 * 1024);
    }

    public CommitLogSegment.CommitLogContext write(RowMutation rowMutation, Object serializedRow) throws IOException
    {
        long currentPosition = -1L;
        try
        {
            currentPosition = logWriter.getFilePointer();
            CommitLogSegment.CommitLogContext cLogCtx = new CommitLogSegment.CommitLogContext(currentPosition);
            Table table = Table.open(rowMutation.getTable());

            // update header
            boolean writePending = false;
            for (ColumnFamily columnFamily : rowMutation.getColumnFamilies())
            {
                int id = table.getColumnFamilyId(columnFamily.name());
                writePending|=header.turnOn(id, logWriter.getFilePointer());
            }
            
            if (writePending)
                writeHeader();

            // write mutation, w/ checksum
            Checksum checkum = new CRC32();
            if (serializedRow instanceof DataOutputBuffer)
            {
                DataOutputBuffer buffer = (DataOutputBuffer) serializedRow;

                if (DatabaseDescriptor.isLogFileCompression()) {
                    // apply compression
                    byte[] compressed = new byte[Snappy.maxCompressedLength(buffer.getLength())];
                    int compressedSize = Snappy.compress(buffer.getData(), 0, buffer.getLength(), compressed, 0);
                    
                    logWriter.writeLong(compressedSize);
                    logWriter.write(compressed, 0, compressedSize);
                    checkum.update(compressed, 0, compressedSize);
                } else {
                    logWriter.writeLong(buffer.getLength());
                    logWriter.write(buffer.getData(), 0, buffer.getLength());
                    checkum.update(buffer.getData(), 0, buffer.getLength());
                }
            }
            else
            {
                assert serializedRow instanceof byte[];
                byte[] bytes = (byte[]) serializedRow;

                if (DatabaseDescriptor.isLogFileCompression()) {
                    // apply compression
                    byte[] compressed = new byte[Snappy.maxCompressedLength(bytes.length)];
                    int compressedSize = Snappy.compress(bytes, 0, bytes.length, compressed, 0);
    
                    logWriter.writeLong(compressedSize);
                    logWriter.write(compressed, 0, compressedSize);
                    checkum.update(compressed, 0, compressedSize);
                } else {
                    logWriter.writeLong(bytes.length);
                    logWriter.write(bytes);
                    checkum.update(bytes, 0, bytes.length);                    
                }
            }
            logWriter.writeLong(checkum.getValue());

            return cLogCtx;
        }
        catch (IOException e)
        {
            if (currentPosition != -1)
                logWriter.seek(currentPosition);
            throw e;
        }
    }

    public void sync() 
    {
        
        try {
            logWriter.sync();

            if (isDelayedHeaderWritePending())
                writeHeader();
            
        } catch (IOException e) {
            throw new FSWriteError(e);
        }
    }

    public CommitLogContext getContext()
    {
        return new CommitLogContext(logWriter.getFilePointer());
    }

    public CommitLogHeader getHeader()
    {
        return header;
    }

    public String getPath()
    {
        return logWriter.getPath();
    }
    
    public String getHeaderPath()                                                                                                                                                                                 
    {                                                                                                                                                                                                             
        return CommitLogHeader.getHeaderPathFromSegment(this);                                                                                                                                                    
    }                                                                                                                                                                                                             

    public long length()
    {
        try
        {
            return logWriter.length();
        }
        catch (IOException e)
        {
            throw new FSReadError(e);
        }
    }

    public void close()
    {
        try
        {
            logWriter.close();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e);
        }
        
    }
    
    public void submitDelete() {
        if (!isDelayedHeaderWritePending()) {
            DeletionService.submitDelete(getHeaderPath());
        }
        DeletionService.submitDelete(getPath());
    }

    @Override
    public String toString()
    {
        return "CommitLogSegment(" + logWriter.getPath() + ')';
    }

    public class CommitLogContext
    {
        public final long position;

        public CommitLogContext(long position)
        {
            assert position >= 0;
            this.position = position;
        }

        public CommitLogSegment getSegment()
        {
            return CommitLogSegment.this;
        }

        @Override
        public String toString()
        {
            return "CommitLogContext(" +
                   "file='" + logWriter.getPath() + '\'' +
                   ", position=" + position +
                   ')';
        }
    }

    /**
     * @param name
     * @return
     */
    public static boolean possibleCommitLogFile(String filename)
    {
        return filename.matches("CommitLog-\\d+\\.log(\\.z)?");
    }


}
