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

import org.apache.cassandra.io.IteratingRow;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.SSTableScanner;
import org.apache.cassandra.io.SSTableWriter;

/**
 * Reads SSTable pointed by 2nd parameter and recovers all accessible records to directory passed as 3rd param. 
 * For example: SSTableRepair.sh /one/db/{DBNAME}/ Lists\ListByRef_8b-192-Data.db .
 */
public class SSTableRepair {
    private static int INPUT_FILE_BUFFER_SIZE = 8 * 1024 * 1024;
    
    public static void main(String[] args) throws IOException {
        File file = new File(args[1]);
        System.setProperty("storage-config", args[0]);

        SSTableReader reader = SSTableReader.open(file.getCanonicalPath());
        if (reader.isColumnBloom()) {
            System.err.println("SSTable recovery with Bloom filter not supported");
            System.exit(0);
            return;
        }
        
        String newName = new File(args[2], file.getName()).getCanonicalPath();
        System.out.println("Repairing " + file.getCanonicalPath() + " into " + newName);
        SSTableWriter writer = new SSTableWriter(newName, 0L, reader.getPartitioner(), false);
        
        SSTableScanner scanner = reader.getScanner(INPUT_FILE_BUFFER_SIZE);
        int good = 0, bad = 0;
        while (scanner.hasNext()) {
            try {
                IteratingRow row = scanner.next();
                byte[] data = new byte[row.getDataSize()];
                row.getDataInput().readFully(data);
                writer.append(row.getKey(), data);
                ++good;
            }
            catch (Throwable e) {
                ++bad;
                if (e.getCause() instanceof UTFDataFormatException) {
                    System.err.println(e.getCause().getMessage() + " - "+ good + " stopping traverse");
                    break;
                }
            }
            if (good + bad > 0 && (good + bad) % 100 == 0) {
                System.out.println("So far " + good + " good rows and " + bad + " bad rows");
            }
        }
        System.out.println(good + " recoverable rows recoverd");
        writer.close();
        System.exit(0);
    }
}
