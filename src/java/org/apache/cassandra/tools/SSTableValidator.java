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

package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.io.IteratingRow;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.SSTableScanner;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

/**
 * Reads SSTables in launch directory. If broken SSTable found prints info about it.
 * To use launch from directory with SSTables (usually /mnt/db/{DBNAME}/Lists): SSTableValidator.sh /one/db/{DBNAME}/
 */
public class SSTableValidator {
    private static int INPUT_FILE_BUFFER_SIZE = 8 * 1024 * 1024;
    
    public static void main(String[] args) throws IOException {
        File directory = new File(".");
        Collection<File> files = FileUtils.listFiles(directory, new WildcardFileFilter("*Data.db"), FalseFileFilter.FALSE);
        System.out.println("Found " + files.size() + " in " + directory.getAbsolutePath());
        
        System.setProperty("storage-config", args[0]);

        for (File file : files) {
            try {
                SSTableReader reader = SSTableReader.open(file.getCanonicalPath());
                SSTableScanner scanner = reader.getScanner(INPUT_FILE_BUFFER_SIZE);
                while (scanner.hasNext()) {
                    IteratingRow row = scanner.next();
                    ColumnFamily cf = row.getColumnFamily();
                    if (cf.isSuper()) {
                        for (Iterator<IColumn> iter = cf.getSortedColumns().iterator(); iter.hasNext(); ) {
                            serializeColumns(iter.next().getSubColumns());
                        }
                    }
                    else {
                        serializeColumns(cf.getSortedColumns());
                    }
                }
                System.out.println(file.getName() + " is ok");
            }
            catch (OutOfMemoryError oom) {
                System.err.println("ERROR: Out of memory on " + file.getName());
            } 
            catch (Throwable e) {
                if (e.getCause() instanceof UTFDataFormatException) {
                    System.err.println(file.getName() + " is corrupt! File size " + FileUtils.byteCountToDisplaySize(file.length()));
                } else {
                    System.err.println("Unexpected error on " + file.getName() + ":" + e.getMessage());
                    e.printStackTrace(System.err);
                }
            }
        }
        System.exit(0);
    }
    
    private static void serializeColumns(Collection<IColumn> cols) {
        for (Iterator<IColumn> iter = cols.iterator(); iter.hasNext(); iter.next());
    }
}
