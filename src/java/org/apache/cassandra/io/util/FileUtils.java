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

package org.apache.cassandra.io.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.RateControl;
import org.apache.log4j.Logger;


public class FileUtils
{
    private static Logger logger_ = Logger.getLogger(FileUtils.class);
    private static final DecimalFormat df_ = new DecimalFormat("#.##");
    private static final double kb_ = 1024d;
    private static final double mb_ = 1024*1024d;
    private static final double gb_ = 1024*1024*1024d;
    private static final double tb_ = 1024*1024*1024*1024d;

    public static void deleteWithConfirm(File file) throws IOException
    {
        assert file.exists() : "attempted to delete non-existing file " + file.getName();
        if (logger_.isDebugEnabled())
            logger_.debug("Deleting " + file.getName());
        if (!file.delete())
        {
            throw new IOException("Failed to delete " + file.getAbsolutePath());
        }
    }

    public static class FileComparator implements Comparator<File>
    {
        public int compare(File f, File f2)
        {
            return (int)(f.lastModified() - f2.lastModified());
        }
    }

    public static void createDirectory(String directory) throws IOException
    {
        File file = new File(directory);
        if (!file.exists())
        {
            if (!file.mkdirs())
            {
                throw new IOException("unable to mkdirs " + directory);
            }
        }
    }

    public static void createFile(String directory) throws IOException
    {
        File file = new File(directory);
        if ( !file.exists() )
            file.createNewFile();
    }

    public static boolean isExists(String filename) throws IOException
    {
        File file = new File(filename);
        return file.exists();
    }

    public static boolean delete(String file)
    {
        File f = new File(file);
        return f.delete();
    }

    public static boolean delete(List<String> files) throws IOException
    {
        boolean bVal = true;
        for ( int i = 0; i < files.size(); ++i )
        {
            String file = files.get(i);
            bVal = delete(file);
            if (bVal)
            {
            	if (logger_.isDebugEnabled())
            	  logger_.debug("Deleted file " + file);
                files.remove(i);
            }
        }
        return bVal;
    }

    public static void delete(File[] files) throws IOException
    {
        for ( File file : files )
        {
            file.delete();
        }
    }

    public static String stringifyFileSize(double value)
    {
        double d = 0d;
        if ( value >= tb_ )
        {
            d = value / tb_;
            String val = df_.format(d);
            return val + " TB";
        }
        else if ( value >= gb_ )
        {
            d = value / gb_;
            String val = df_.format(d);
            return val + " GB";
        }
        else if ( value >= mb_ )
        {
            d = value / mb_;
            String val = df_.format(d);
            return val + " MB";
        }
        else if ( value >= kb_ )
        {
            d = value / kb_;
            String val = df_.format(d);
            return val + " KB";
        }
        else
        {       
            String val = df_.format(value);
            return val + " bytes";
        }        
    }
    
    /**
     * Deletes all files and subdirectories under "dir".
     * @param dir Directory to be deleted
     * @throws IOException if any part of the tree cannot be deleted
     */
    public static void deleteDir(File dir) throws IOException
    {
        if (dir.isDirectory())
        {
            for (String aChildren : dir.list())
                deleteDir(new File(dir, aChildren));
        }

        // The directory is now empty so now it can be smoked
        deleteWithConfirm(dir);
    }
    
    /**
     * Copies one file to another, skippong posix cache and limiting speed of copy.
     * 
     * @param from
     * @param to
     * @param chunkSize chunk size in bytes
     * @param chunksPerSec limit speed to no more than this number of chunks per second
     * @throws IOException
     */
    public static long copyFileSkippingCache(File from, File to, int chunkSize, int chunksPerSec) throws IOException
    {
        long length = from.length();
        
        RateControl rateControl = new RateControl(chunksPerSec);
        
        if (!to.getParentFile().exists())
        {
            to.getParentFile().mkdirs();
        }
        
        FileInputStream in = new FileInputStream(from);
        FileOutputStream out = new FileOutputStream(to);
        
        try {
        
            FileChannel cin = in.getChannel(), cout = out.getChannel();
            int fdin = CLibrary.getfd(in.getFD()), fdout = CLibrary.getfd(out.getFD());
            
            long copied = 0;

            for (long position=0;position<length;position+=chunkSize)
            {
                long transferred = cout.transferFrom(cin, position, chunkSize);
                
                copied += transferred;

                CLibrary.trySkipCache(fdin, position, chunkSize);
                CLibrary.trySkipCache(fdout, position, chunkSize);

                rateControl.control();
            }
            
            return copied;
        } finally {
            in.close();
            out.close();
        }
        
    }
    
    public static long copyDir(File fromDir, File toDir, int chunkSize, int chunksPerSec) throws IOException
    {
        long copied = 0;
        
        for (String children : fromDir.list())
        {
            File from = new File(fromDir,children);
            File to = new File(toDir,children);
            
            if (from.isDirectory())
            {
                copied += copyDir(from, to, chunkSize, chunksPerSec);
            } else
            {
                copied += copyFileSkippingCache(from, to, chunkSize, chunksPerSec);
            }
        }
        
        return copied;
    }
}
