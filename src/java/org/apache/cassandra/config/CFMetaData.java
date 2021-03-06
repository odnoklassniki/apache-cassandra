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

package org.apache.cassandra.config;

import java.util.List;
import java.util.Properties;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.proc.IRowProcessor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public final class CFMetaData
{
    public final static double DEFAULT_KEY_CACHE_SIZE = 200000;
    public final static double DEFAULT_ROW_CACHE_SIZE = 0.0;

    public final String tableName;            // name of table which has this column family
    public final String cfName;               // name of the column family
    public final String columnType;           // type: super, standard, etc.
    public final AbstractType comparator;       // name sorted, time stamp sorted etc.
    public final AbstractType subcolumnComparator; // like comparator, for supercolumns
    public final String comment; // for humans only
    public final double rowCacheSize; // default 0
    public final double keyCacheSize; // default 0.01
    public final int rowCacheSavePeriodInSeconds; //default 0 (off)
    public final int keyCacheSavePeriodInSeconds; //default 0 (off)
    
    /** MM: do we want column to be checked against bloom filter **/
    public final boolean bloomColumns ;
    /** MM: is this column family split by domain **/
    public final boolean domainSplit;
    /** MM: if split by domain - name of CF without domain postfix **/
    public final String  domainCFName;
    /** MM: domain value of the minimum key, if split by domain (domain + NUL char) **/
    public final Token domainMinToken, domainMaxToken;
    
    public final int gcGraceSeconds;
    
    /** MM: row processor descriptors **/
    public final List<Pair<Class<? extends IRowProcessor>,Properties>> rowProcessors;
    
    CFMetaData(String tableName, String cfName, String columnType, AbstractType comparator, AbstractType subcolumnComparator,
               boolean bloomColumns,
               String comment, double rowCacheSize, double keyCacheSize, int rowCacheSavePeriodInSeconds, int keyCacheSavePeriodInSeconds,
               boolean domainSplit, String domainCFName, Token domainMin, Token domainMax,
               int gcGraceSeconds,
               List<Pair<Class<? extends IRowProcessor>,Properties>> rowProcClasses
               )
    {
        this.tableName = tableName;
        this.cfName = cfName;
        this.columnType = columnType;
        this.comparator = comparator;
        this.subcolumnComparator = subcolumnComparator;
        this.bloomColumns = bloomColumns;
        this.comment = comment;
        this.rowCacheSize = rowCacheSize;
        this.keyCacheSize = keyCacheSize;
        this.rowCacheSavePeriodInSeconds = rowCacheSavePeriodInSeconds;
        this.keyCacheSavePeriodInSeconds = keyCacheSavePeriodInSeconds;
        this.domainSplit = domainSplit;
        this.domainCFName = domainCFName;
        this.domainMinToken = domainMin;
        this.domainMaxToken = domainMax;
        
        this.gcGraceSeconds = gcGraceSeconds;
        
        this.rowProcessors = rowProcClasses;
    }

    // a quick and dirty pretty printer for describing the column family...
    public String pretty()
    {
        return tableName + "." + cfName + "\n"
               + "Column Family Type: " + columnType + "\n"
               + "Columns Sorted By: " + comparator + "\n";
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof CFMetaData))
            return false;
        CFMetaData other = (CFMetaData)obj;
        return other.tableName.equals(tableName)
                && other.cfName.equals(cfName)
                && other.columnType.equals(columnType)
                && other.comparator.equals(comparator)
                && FBUtilities.equals(other.subcolumnComparator, subcolumnComparator)
                && other.bloomColumns==bloomColumns
                && FBUtilities.equals(other.comment, comment)
                && other.rowCacheSize == rowCacheSize
                && other.keyCacheSize == keyCacheSize
                && other.rowCacheSavePeriodInSeconds == rowCacheSavePeriodInSeconds
                && other.keyCacheSavePeriodInSeconds == keyCacheSavePeriodInSeconds
                && other.domainSplit == domainSplit
                && other.domainCFName.equals(domainCFName)
                && other.domainMinToken.compareTo( domainMinToken )==0;
    }

}
