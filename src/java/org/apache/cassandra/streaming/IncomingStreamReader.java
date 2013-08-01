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

package org.apache.cassandra.streaming;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;

public class IncomingStreamReader
{
    private static Logger logger = Logger.getLogger(IncomingStreamReader.class);
    private PendingFile pendingFile;
    private CompletedFileStatus streamStatus;
    private SocketChannel socketChannel;
    
    private static final int MEGABIT_BYTES = 1024*1024/8;
    

    public IncomingStreamReader(SocketChannel socketChannel)
    {
        this.socketChannel = socketChannel;
        InetSocketAddress remoteAddress = (InetSocketAddress)socketChannel.socket().getRemoteSocketAddress();
        pendingFile = StreamInManager.getStreamContext(remoteAddress.getAddress());
        StreamInManager.activeStreams.put(remoteAddress.getAddress(), pendingFile);
        assert pendingFile != null;
        streamStatus = StreamInManager.getStreamStatus(remoteAddress.getAddress());
        assert streamStatus != null;
        
        
        String fileLocation = StreamInManager.getFileLocation(remoteAddress.getAddress(), pendingFile.getNewName());
        if (fileLocation == null){
            String[] pieces = FBUtilities.strip(pendingFile.getNewName(), "-");
            try {
                List<PendingFile> incomingFiles = StreamInManager.getIncomingFiles(remoteAddress.getAddress());
                
                long expectedDataFileSize = Math.max(pendingFile.getExpectedBytes(),  getExpectedDataFileSize(pendingFile.getNewName(), incomingFiles));
                fileLocation = DatabaseDescriptor.getDataFileLocation(Table.open(pendingFile.getTable()).getColumnFamilyStore(pieces[0]), expectedDataFileSize);
            } catch (IOException e) {
               throw new IllegalStateException("Can't open table "+pendingFile.getTable(), e);
            }
            StreamInManager.setFileLocation(remoteAddress.getAddress(), pendingFile.getNewName(), fileLocation);
            if (logger.isDebugEnabled())
                logger.debug("Registered location for  " + pendingFile.getNewName()+", at "+ fileLocation);
            
        }
        
        
        File sourceFile = new File( pendingFile.getSourceFile() );
        String[] piece = FBUtilities.strip(sourceFile.getName(), "-");
        String typeOfFile = piece[2];

        
        String newFileName = pendingFile.getNewName().replace("Data.db", typeOfFile);
        pendingFile.setTargetFile(fileLocation + File.separator + newFileName);
        
        
    }

    private long getExpectedDataFileSize(String newName, List<PendingFile> incomingFiles) {
        //просто выберем макс размер
        long max = 0;
        for (PendingFile pendingFile : incomingFiles) {
            if (pendingFile.getNewName().equals(newName)){
                if (pendingFile.getExpectedBytes() > max){
                    max = pendingFile.getExpectedBytes();
                }
            }
        }
        return max;
    }

    public void read() throws IOException
    {
        logger.debug("Receiving stream");
        InetSocketAddress remoteAddress = (InetSocketAddress)socketChannel.socket().getRemoteSocketAddress();
        
        File targetFile = new File(pendingFile.getTargetFile());
        
        if (logger.isDebugEnabled())
            logger.debug("Creating file for " + pendingFile.getSourceFile()+", at "+targetFile);
          
        
        FileOutputStream fos = new FileOutputStream(targetFile, true);
        FileChannel fc = fos.getChannel();
        
        long bytesRead = 0;
        try
        {
            long rateControlledBytes = 0;
            while (bytesRead < pendingFile.getExpectedBytes()) {
                bytesRead += fc.transferFrom(socketChannel, bytesRead, MEGABIT_BYTES);
                pendingFile.update(bytesRead);
                
                while (bytesRead-rateControlledBytes > MEGABIT_BYTES)
                {
                    // we received megabit. make pause, if we received it too fast
                    StreamingService.instance.getStreamRateControl().control();
                    rateControlledBytes+=MEGABIT_BYTES;
                }
            }
            logger.debug("Receiving stream: finished reading chunk, awaiting more");
        }
        catch (IOException ex)
        {
            /* Ask the source node to re-stream this file. */
            streamStatus.setAction(CompletedFileStatus.StreamCompletionAction.STREAM);
            handleStreamCompletion(remoteAddress.getAddress());
            /* Delete the orphaned file. */
            targetFile.delete();
            logger.debug("Receiving stream: recovering from IO error");
            throw ex;
        }
        finally
        {
            StreamInManager.activeStreams.remove(remoteAddress.getAddress(), pendingFile);
        }

        if (bytesRead == pendingFile.getExpectedBytes())
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Removing stream context " + pendingFile);
            }
            fc.close();
            handleStreamCompletion(remoteAddress.getAddress());
        }
    }

    private void handleStreamCompletion(InetAddress remoteHost) throws IOException
    {
        /*
         * Streaming is complete. If all the data that has to be received inform the sender via
         * the stream completion callback so that the source may perform the requisite cleanup.
        */
        IStreamComplete streamComplete = StreamInManager.getStreamCompletionHandler(remoteHost);
        if (streamComplete != null)
            streamComplete.onStreamCompletion(remoteHost, pendingFile, streamStatus);
    }
}
