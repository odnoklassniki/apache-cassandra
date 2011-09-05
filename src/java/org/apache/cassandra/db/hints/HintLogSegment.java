package org.apache.cassandra.db.hints;
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
import java.net.InetAddress;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.io.DeletionService;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.log4j.Logger;

/**
 * Hints log segment stores all hinted writes for single endpoint. 
 * Hint log will swicth to new file as soon as hints delivery is started and segment reached its size constraint
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class HintLogSegment
{
    private static final Logger logger = Logger.getLogger(HintLogSegment.class);

    /**
     * This is null until a write is done
     */
    private BufferedRandomAccessFile logWriter;
    private HintLogHeader header;
    
    /**
     * Endpoint, for which this segment stores hinted mutations
     */
    private InetAddress endpoint;
    
    /**
     * File this context stores data in
     */
    private String file;
    
    public HintLogSegment(InetAddress endpoint)
    {
        this.header = new HintLogHeader();
        
        this.endpoint = endpoint;

    }
    
    HintLogSegment(InetAddress endpoint, String logFile) 
    {
        logger.info("Opening hint log segment " + logFile);

        String headerPath = HintLogHeader.getHeaderPathFromSegmentPath(logFile);
        try
        {
            this.header = HintLogHeader.readCommitLogHeader(headerPath);
        }
        catch (IOException ioe)
        {
            this.header = new HintLogHeader();
            logger.info(headerPath + " incomplete, missing or corrupt.  Everything is ok, don't panic.  HintLog will be replayed from the beginning");
            logger.debug("exception was", ioe);
        }

        this.endpoint = endpoint;
        this.file = logFile;
    }

    private void createWriter() throws IOError
    {
        assert isEmpty();
        
        this.file = DatabaseDescriptor.getHintLogDirectory() + File.separator + "Hints-" + endpoint.getHostAddress() + "-" + System.currentTimeMillis() + ".log";

        logger.info("Creating new hint log segment " + file);
        try
        {
            logWriter = createWriter(file);
            logWriter.setSkipCache(true);
            
            writeHeader();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public void writeHeader() throws IOException
    {
        HintLogHeader.writeCommitLogHeader(header, getHeaderPath() );
    }

    private static BufferedRandomAccessFile createWriter(String file) throws IOException
    {
        return new BufferedRandomAccessFile(file, "rw", 128 * 1024);
    }

    public BufferedRandomAccessFile createReader() throws IOException
    {
        return new BufferedRandomAccessFile(file, "r", 128 * 1024);
    }

    public void write(byte[] serializedRow) throws IOException
    {
        if (logWriter == null)
        {
            assert isEmpty();
            
            createWriter();
        }
        
        long currentPosition = -1L;
        try
        {
            currentPosition = logWriter.getFilePointer();

            // write mutation, w/ checksum
            // TODO ensure do we really want this instanceof here specfically for hints here
            Checksum checkum = new CRC32();

            logWriter.writeLong(serializedRow.length);
            logWriter.write(serializedRow);
            checkum.update(serializedRow, 0, serializedRow.length);
            
            logWriter.writeLong(checkum.getValue());

        }
        catch (IOException e)
        {
            if (currentPosition != -1)
                logWriter.seek(currentPosition);
            throw e;
        }
    }

    public void sync() throws IOException
    {
        if (logWriter!=null)
            logWriter.sync();

    }

    public HintLogHeader getHeader()
    {
        return header;
    }
    
    public boolean isFullyReplayed()
    {
        assert file != null;
        
        return new File(file).length() <=header.getPosition();
    }
    
    public boolean isEmpty()
    {
        return file==null ;
    }
    
    public String getPath()
    {
        return file;
    }
    
    public String getHeaderPath()                                                                                                                                                                                 
    {                                                                                                                                                                                                             
        return HintLogHeader.getHeaderPathFromSegment(this);                                                                                                                                                    
    }                                                                                                                                                                                                             

    public long length()
    {
        if (file==null)
            return 0l;
        
        if (logWriter!=null)
            try {
                return logWriter.length();
            } catch (IOException e) {
                throw new IOError(e);
            }
        
        return new File(file).length();
    }

    public void close()
    {
        if (logWriter==null)
            return;
        
        try
        {
            logWriter.close();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        
    }
    
    public void delete()
    {
        assert getPath() != null;
        
        DeletionService.submitDelete(getHeaderPath());
        DeletionService.submitDelete(getPath());
    }

    @Override
    public String toString()
    {
        return "HintLogSegment(" + (isEmpty() ? "empty" : file ) + ')';
    }

    /**
     * @return
     */
    public InetAddress getEndpoint()
    {
        return endpoint;
    }

}
