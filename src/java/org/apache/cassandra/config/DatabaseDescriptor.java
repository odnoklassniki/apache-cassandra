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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.*;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.log4j.Logger;

import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.disks.AbstractDiskAllocator;
import org.apache.cassandra.db.disks.DiskAllocator;
import org.apache.cassandra.db.disks.RoundRobinAllocator;
import org.apache.cassandra.db.disks.SingleDirAllocator;
import org.apache.cassandra.db.disks.SizeTieredAllocator;
import org.apache.cassandra.db.disks.SpaceFirstAllocator;
import org.apache.cassandra.db.hints.HintLog;
import org.apache.cassandra.db.hints.HintLogHandoffManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.proc.IRowProcessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.OdklDomainPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndPointSnitch;
import org.apache.cassandra.locator.RackAwareOdklEvenStrategy;
import org.apache.cassandra.locator.RackAwareOdklStrategy;
import org.apache.cassandra.maint.CleanArchivedLogsTask;
import org.apache.cassandra.maint.CleanOldSnapshotsTask;
import org.apache.cassandra.maint.ClusterSnapshotTask;
import org.apache.cassandra.maint.MaintenanceTask;
import org.apache.cassandra.maint.MaintenanceTaskManager;
import org.apache.cassandra.maint.MajorCompactionTask;
import org.apache.cassandra.maint.RackAwareMajorCompactionTask;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.XMLUtils;
import org.w3c.dom.DOMException;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class DatabaseDescriptor
{
    private static Logger logger = Logger.getLogger(DatabaseDescriptor.class);
    public static final String STREAMING_SUBDIR = "stream";

    // don't capitalize these; we need them to match what's in the config file for CLS.valueOf to parse
    public static enum CommitLogSync {
        periodic,
        batch
    }

    public static enum DiskAccessMode {
        auto,
        mmap,
        mmap_index_only,
        mmap_random,
        standard,
    }

    public static final String random = "RANDOM";
    public static final String ophf = "OPHF";
    private static int storagePort = 7000;
    private static int thriftPort = 9160;
    private static boolean thriftFramed = false;
    private static InetAddress listenAddress; // leave null so we can fall through to getLocalHost
    private static InetAddress thriftAddress;
    private static String clusterName = "Test";
    private static long rpcTimeoutInMillis = 2000;
    private static int phiConvictThreshold = 8;
    private static Set<InetAddress> seeds = new HashSet<InetAddress>();
    /* Keeps the list of data file directories */
    private static String[] dataFileDirectories;
    /** MM: disk allocation policy **/
    private static DiskAllocator dataFileAllocator;
    private static String logFileDirectory;
    /** MM: where to ship commit logs for backup **/
    private static String logFileArchiveDestination;
    /** MM: where to write hint logs for later delivery **/
    private static String hintLogDirectory;
    /** MM: where to transfer snapshots for archiving **/
    private static String dataArchiveDirectory;
    /** MM: speed of data archive, MBytes/sec **/
    private static int    dataArchiveThrottle = 10;
    /** MM: datacenter:rack location of this endpoint **/
    private static String location;
    /** MM: allowed locations for all endpoints in cluster. null if not configured = dont check and allow all **/
    private static Set<String> allowedLocations = null;

    private static String savedCachesDirectory;
    private static int consistencyThreads = 4; // not configurable
    private static int concurrentReaders = 8;
    private static int concurrentWriters = 32;

    private static double flushDataBufferSizeInMB = 32;
    private static double flushIndexBufferSizeInMB = 8;
    private static int slicedReadBufferSizeInKB = 64;

    static Map<String, KSMetaData> tables = new HashMap<String, KSMetaData>();
    private static int bmtThreshold = 256;
    /* if this a row exceeds this threshold, we issue warnings during compaction */
    private static long rowWarningThreshold = 512 * 1024 * 1024;

    /* Hashing strategy Random or OPHF */
    private static IPartitioner partitioner;

    /* if the size of columns or super-columns are more than this, indexing will kick in */
    private static int columnIndexSizeInKB;
    /* Number of minutes to keep a memtable in memory */
    private static int memtableLifetimeMs = 60 * 60 * 1000;
    /* Size of the memtable in memory before it is dumped */
    private static int memtableThroughput = 64;
    /* Number of objects in millions in the memtable before it is dumped */
    private static double memtableOperations = 0.1;
    /* 
     * This parameter enables or disables consistency checks. 
     * If set to false the read repairs are disable for very
     * high throughput on reads but at the cost of consistency.
    */
    private static float doConsistencyCheck = 1.0f;
    
    /**
     * MM: Should weak and quorum reads be submitted in parallel to all natural endpoints and
     * served as soon as 1st data response arrived.
     * This differs from default behaviour, which reads data from local node, requesting digests only from
     * remotes. This leads to read response spikes if local node is slow at the moment.
     */
    private static boolean parallelReads = true;
    
    /* Job Jar Location */
    private static String jobJarFileLocation;
    /* Address where to run the job tracker */
    private static String jobTrackerHost;    

    // the path qualified config file (storage-conf.xml) name
    private static URI configFileURI;
    /* initial token in the ring */
    private static String initialToken = null;

    private static CommitLogSync commitLogSync;
    private static double commitLogSyncBatchMS;
    private static int commitLogSyncPeriodMS;
    private static int maxCommitLogSegmentsActive=4;

    private static DiskAccessMode diskAccessMode;
    private static DiskAccessMode indexAccessMode;
    private static boolean diskRandomHint = false;

    private static boolean snapshotBeforeCompaction;
    private static boolean autoBootstrap = false;
    
    private static Class<? extends HintedHandOffManager> hintedHandoffManager = null;

    private static IAuthenticator authenticator = new AllowAllAuthenticator();

    private static int indexinterval = 128;
    
    public enum RpcServerTypes { sync, hsha };
    
    private static RpcServerTypes rpcServerType = RpcServerTypes.sync; 
    private static int rpcThreads ;
    
    /**
     * MM: Limit stream in tasks to use no more than 600 mbits by default.
     * This is ok for bootstrapping node, but you should limit it more when
     * decomissioning
     */
    private static int streamInMBits = 600;
    
    public static int thriftMaxMessageLengthMB = 16;
    public static int thriftFramedTransportSizeMB = 15;

    private final static String STORAGE_CONF_FILE = "storage-conf.xml";

    public static final int DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS = 0;
    public static final int DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS = 0;

    public static File getSerializedRowCachePath(String ksName, String cfName)
    {
        return new File(savedCachesDirectory + File.separator + ksName + "-" + cfName + "-RowCache");
    }

    public static File getSerializedKeyCachePath(String ksName, String cfName)
    {
        return new File(savedCachesDirectory + File.separator + ksName + "-" + cfName + "-KeyCache");
    }

    public static int getCompactionPriority()
    {
        String priorityString = System.getProperty("cassandra.compaction.priority");
        return priorityString == null ? Thread.NORM_PRIORITY : Integer.parseInt(priorityString);
    }

    /**
     * Try the storage-config system property, and then inspect the classpath.
     */
    static URI getStorageConfigURI()
    {
	String confdir = System.getProperty("storage-config");
	if (confdir != null) {
	    String scp = confdir + File.separator + STORAGE_CONF_FILE;
	    File scpf = new File(scp);
	    if (scpf.exists()) {
		return scpf.toURI();
	    }
	}

	// try the classpath
	ClassLoader loader = DatabaseDescriptor.class.getClassLoader();
	URL scpurl = loader.getResource(STORAGE_CONF_FILE);
	if (scpurl != null) {
	    String s = scpurl.toString();
	    URI u;
	    try {
		u = new URI(s);
	    }
	    catch (java.net.URISyntaxException e)
	    {
		throw new RuntimeException(e);
	    }
	    return u;
	}

	throw new RuntimeException("Cannot locate " + STORAGE_CONF_FILE + " via storage-config system property or classpath lookup.");
    }

    static
    {
        try
        {
	    configFileURI = getStorageConfigURI();

	    if (logger.isDebugEnabled())
                logger.debug("Loading settings from " + configFileURI);
            XMLUtils xmlUtils = new XMLUtils(configFileURI);

            /* Cluster Name */
            clusterName = xmlUtils.getNodeValue("/Storage/ClusterName");

            String syncRaw = xmlUtils.getNodeValue("/Storage/CommitLogSync");
            try
            {
                commitLogSync = CommitLogSync.valueOf(syncRaw);
            }
            catch (IllegalArgumentException e)
            {
                throw new ConfigurationException("CommitLogSync must be either 'periodic' or 'batch'");
            }
            if (commitLogSync == null)
            {
                throw new ConfigurationException("Missing required directive CommitLogSync");
            }
            else if (commitLogSync == CommitLogSync.batch)
            {
                try
                {
                    commitLogSyncBatchMS = Double.valueOf(xmlUtils.getNodeValue("/Storage/CommitLogSyncBatchWindowInMS"));
                }
                catch (Exception e)
                {
                    throw new ConfigurationException("Unrecognized value for CommitLogSyncBatchWindowInMS.  Double expected.");
                }
                if (xmlUtils.getNodeValue("/Storage/CommitLogSyncPeriodInMS") != null)
                {
                    throw new ConfigurationException("Batch sync specified, but CommitLogSyncPeriodInMS found.  Only specify CommitLogSyncBatchWindowInMS when using batch sync.");
                }
                logger.debug("Syncing log with a batch window of " + commitLogSyncBatchMS);
            }
            else
            {
                assert commitLogSync == CommitLogSync.periodic;
                try
                {
                    commitLogSyncPeriodMS = Integer.valueOf(xmlUtils.getNodeValue("/Storage/CommitLogSyncPeriodInMS"));
                }
                catch (Exception e)
                {
                    throw new ConfigurationException("Unrecognized value for CommitLogSyncPeriodInMS.  Integer expected.");
                }
                if (xmlUtils.getNodeValue("/Storage/CommitLogSyncBatchWindowInMS") != null)
                {
                    throw new ConfigurationException("Periodic sync specified, but CommitLogSyncBatchWindowInMS found.  Only specify CommitLogSyncPeriodInMS when using periodic sync.");
                }
                logger.debug("Syncing log with a period of " + commitLogSyncPeriodMS);
            }
            
            if (xmlUtils.getNodeValue("/Storage/CommitLogActiveSegments") != null ) {
                try
                {
                    maxCommitLogSegmentsActive = Integer.valueOf(xmlUtils.getNodeValue("/Storage/CommitLogActiveSegments"));
                }
                catch (Exception e)
                {
                    throw new ConfigurationException("Unrecognized value for CommitLogActiveSegments.  Integer expected.");
                }
                
                if (maxCommitLogSegmentsActive<0)
                    throw new ConfigurationException("Unrecognized value for CommitLogActiveSegments. Non negative value expected.");
                
            }

            String modeRaw = xmlUtils.getNodeValue("/Storage/DiskAccessMode");
            try
            {
                diskAccessMode = DiskAccessMode.valueOf(modeRaw);
            }
            catch (IllegalArgumentException e)
            {
                throw new ConfigurationException("DiskAccessMode must be either 'auto', 'mmap', 'mmap_index_only', or 'standard'");
            }

            /* evaluate the DiskAccessMode conf directive, which also affects indexAccessMode selection */
            if (diskAccessMode == DiskAccessMode.auto)
            {
                diskAccessMode = System.getProperty("os.arch").contains("64") ? DiskAccessMode.mmap : DiskAccessMode.standard;
                indexAccessMode = diskAccessMode;
                logger.info("DiskAccessMode 'auto' determined to be " + diskAccessMode + ", indexAccessMode is " + indexAccessMode );
            }
            else if (diskAccessMode == DiskAccessMode.mmap_index_only)
            {
                diskAccessMode = DiskAccessMode.standard;
                indexAccessMode = DiskAccessMode.mmap;
                logger.info("DiskAccessMode is " + diskAccessMode + ", indexAccessMode is " + indexAccessMode );
            }
            else if (diskAccessMode == DiskAccessMode.mmap_random)
            {
                diskAccessMode = DiskAccessMode.mmap;
                indexAccessMode = DiskAccessMode.mmap;
                diskRandomHint = true;
                logger.info("DiskAccessMode is " + diskAccessMode + ", indexAccessMode is " + indexAccessMode + " with random hint (no readahead)" );
            }
            else
            {
                indexAccessMode = diskAccessMode;
                logger.info("DiskAccessMode is " + diskAccessMode + ", indexAccessMode is " + indexAccessMode );
            }

            /* Authentication and authorization backend, implementing IAuthenticator */
            String authenticatorClassName = xmlUtils.getNodeValue("/Storage/Authenticator");
            if (authenticatorClassName != null)
            {
                try
                {
                    Class cls = Class.forName(authenticatorClassName);
                    authenticator = (IAuthenticator) cls.getConstructor().newInstance();
                }
                catch (ClassNotFoundException e)
                {
                    throw new ConfigurationException("Invalid authenticator class " + authenticatorClassName);
                }
            }
            
            /* Hashing strategy */
            String partitionerClassName = xmlUtils.getNodeValue("/Storage/Partitioner");
            if (partitionerClassName == null)
            {
                throw new ConfigurationException("Missing partitioner directive /Storage/Partitioner");
            }
            try
            {
                partitioner = FBUtilities.newPartitioner(partitionerClassName);
            }
            catch (Exception e)
            {
                throw new ConfigurationException("Invalid partitioner class " + partitionerClassName);
            }

            /* JobTracker address */
            jobTrackerHost = xmlUtils.getNodeValue("/Storage/JobTrackerHost");

            /* Job Jar file location */
            jobJarFileLocation = xmlUtils.getNodeValue("/Storage/JobJarFileLocation");

            String gcGrace = xmlUtils.getNodeValue("/Storage/GCGraceSeconds");
            /* time to wait before garbage collecting tombstones (deletion markers) */
            int gcGraceInSeconds = 10 * 24 * 3600; // 10 days

            if ( gcGrace != null )
                gcGraceInSeconds = Integer.parseInt(gcGrace);

            initialToken = xmlUtils.getNodeValue("/Storage/InitialToken");
            
            location = xmlUtils.getNodeValue("/Storage/Location");
            
            if (location !=null && !location.contains(":"))
            {
                throw new ConfigurationException("Invalid Location  - all locations must be of form DC:RACK");
            }
            
            String allowedLocationsString = xmlUtils.getNodeValue("/Storage/AllowedLocations");
            if (allowedLocationsString!=null)
            {
                String[] allLocations = allowedLocationsString.split(",");
                allowedLocations = new HashSet<String>(allLocations.length);
                
                for (String l : allLocations) {
                    if (!l.contains(":"))
                    {
                        throw new ConfigurationException("Invalid AllowedLocations  - all locations must be of form DC:RACK");
                    }
                    
                    allowedLocations.add( l.trim() );
                }
                
                if (getLocation() !=null && !allowedLocations.contains(getLocation()))
                {
                    throw new ConfigurationException("Invalid Location - it is not in AllowedLocations list");
                }
            }

            /* RPC Timeout */
            String rpcTimeout = xmlUtils.getNodeValue("/Storage/RpcTimeoutInMillis");
            if ( rpcTimeout != null )
                rpcTimeoutInMillis = Integer.parseInt(rpcTimeout);

            /* RPC Server Type */
            String rpcST = xmlUtils.getNodeValue("/Storage/RpcServerType");
            if ( rpcST != null )
            {
                try {
                    rpcServerType = RpcServerTypes.valueOf(rpcST);
                } catch (IllegalArgumentException e)
                {
                    throw new ConfigurationException("Invalid RpcServerType="+rpcST+". sync and hsha are supported");
                }
            }

            /* phi convict threshold for FailureDetector */
            String phiThreshold = xmlUtils.getNodeValue("/Storage/PhiConvictThreshold");
            if ( phiThreshold != null )
                    phiConvictThreshold = Integer.parseInt(phiThreshold);

            if (phiConvictThreshold < 5 || phiConvictThreshold > 16)
            {
                throw new ConfigurationException("PhiConvictThreshold must be between 5 and 16");
            }
            
            /* Thread per pool */
            String rawReaders = xmlUtils.getNodeValue("/Storage/ConcurrentReads");
            if (rawReaders != null)
            {
                concurrentReaders = Integer.parseInt(rawReaders);
            }
            if (concurrentReaders < 2)
            {
                throw new ConfigurationException("ConcurrentReads must be at least 2");
            }

            String rawWriters = xmlUtils.getNodeValue("/Storage/ConcurrentWrites");
            if (rawWriters != null)
            {
                concurrentWriters = Integer.parseInt(rawWriters);
            }
            if (concurrentWriters < 2)
            {
                throw new ConfigurationException("ConcurrentWrites must be at least 2");
            }

            /** Min and max rpc threads allowed **/
            String rpcMinT = xmlUtils.getNodeValue("/Storage/RpcThreads");
            if ( rpcMinT != null )
                rpcThreads = Integer.parseInt(rpcMinT);
            else
            {
                // the default is number of concurrent reads and writes we can support + some slack
                rpcThreads = concurrentReaders + concurrentWriters;
                rpcThreads += rpcThreads/10;
            }

            String rawFlushData = xmlUtils.getNodeValue("/Storage/FlushDataBufferSizeInMB");
            if (rawFlushData != null)
            {
                flushDataBufferSizeInMB = Double.parseDouble(rawFlushData);
            }
            String rawFlushIndex = xmlUtils.getNodeValue("/Storage/FlushIndexBufferSizeInMB");
            if (rawFlushIndex != null)
            {
                flushIndexBufferSizeInMB = Double.parseDouble(rawFlushIndex);
            }

            String rawSlicedBuffer = xmlUtils.getNodeValue("/Storage/SlicedBufferSizeInKB");
            if (rawSlicedBuffer != null)
            {
                slicedReadBufferSizeInKB = Integer.parseInt(rawSlicedBuffer);
            }

            String bmtThresh = xmlUtils.getNodeValue("/Storage/BinaryMemtableThroughputInMB");
            if (bmtThresh != null)
            {
                bmtThreshold = Integer.parseInt(bmtThresh);
            }

            /* TCP port on which the storage system listens */
            String port = xmlUtils.getNodeValue("/Storage/StoragePort");
            if ( port != null )
                storagePort = Integer.parseInt(port);

            /* Local IP or hostname to bind services to */
            String listenAddr = xmlUtils.getNodeValue("/Storage/ListenAddress");
            if (listenAddr != null)
            {
                if (listenAddr.equals("0.0.0.0"))
                    throw new ConfigurationException("ListenAddress must be a single interface.  See http://wiki.apache.org/cassandra/FAQ#cant_listen_on_ip_any");
                try
                {
                    listenAddress = InetAddress.getByName(listenAddr);
                }
                catch (UnknownHostException e)
                {
                    throw new ConfigurationException("Unknown ListenAddress '" + listenAddr + "'");
                }
            }

            /* Local IP or hostname to bind thrift server to */
            String thriftAddr = xmlUtils.getNodeValue("/Storage/ThriftAddress");
            if ( thriftAddr != null )
                thriftAddress = InetAddress.getByName(thriftAddr);

            /* get the thrift port from conf file */
            port = xmlUtils.getNodeValue("/Storage/ThriftPort");
            if (port != null)
                thriftPort = Integer.parseInt(port);

            /* Framed (Thrift) transport (default to "no") */
            String framedRaw = xmlUtils.getNodeValue("/Storage/ThriftFramedTransport");
            if (framedRaw != null)
            {
                if (framedRaw.equalsIgnoreCase("true") || framedRaw.equalsIgnoreCase("false"))
                {
                    thriftFramed = Boolean.valueOf(framedRaw);
                }
                else
                {
                    throw new ConfigurationException("Unrecognized value for ThriftFramedTransport.  Use 'true' or 'false'.");
                }
            }

            /* snapshot-before-compaction.  defaults to false */
            String sbc = xmlUtils.getNodeValue("/Storage/SnapshotBeforeCompaction");
            if (sbc != null)
            {
                if (sbc.equalsIgnoreCase("true") || sbc.equalsIgnoreCase("false"))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("setting snapshotBeforeCompaction to " + sbc);
                    snapshotBeforeCompaction = Boolean.valueOf(sbc);
                }
                else
                {
                    throw new ConfigurationException("Unrecognized value for SnapshotBeforeCompaction.  Use 'true' or 'false'.");
                }
            }

            /* snapshot-before-compaction.  defaults to false */
            String autoBootstr = xmlUtils.getNodeValue("/Storage/AutoBootstrap");
            if (autoBootstr != null)
            {
                if (autoBootstr.equalsIgnoreCase("true") || autoBootstr.equalsIgnoreCase("false"))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("setting autoBootstrap to " + autoBootstr);
                    autoBootstrap = Boolean.valueOf(autoBootstr);
                }
                else
                {
                    throw new ConfigurationException("Unrecognized value for AutoBootstrap.  Use 'true' or 'false'.");
                }
            }

            /* Number of days to keep the memtable around w/o flushing */
            String lifetime = xmlUtils.getNodeValue("/Storage/MemtableFlushAfterMinutes");
            if (lifetime != null)
                memtableLifetimeMs = Integer.parseInt(lifetime) * 60 * 1000;

            /* Size of the memtable in memory in MB before it is dumped */
            String memtableSize = xmlUtils.getNodeValue("/Storage/MemtableThroughputInMB");
            if ( memtableSize != null )
                memtableThroughput = Integer.parseInt(memtableSize);
            /* Number of objects in millions in the memtable before it is dumped */
            String memtableObjectCount = xmlUtils.getNodeValue("/Storage/MemtableOperationsInMillions");
            if ( memtableObjectCount != null )
                memtableOperations = Double.parseDouble(memtableObjectCount);
            if (memtableOperations <= 0)
            {
                throw new ConfigurationException("Memtable object count must be a positive double");
            }

            String streamInLimit = xmlUtils.getNodeValue("/Storage/StreamInLimit");
            if ( streamInLimit != null )
                streamInMBits = Integer.parseInt(streamInLimit);
            
            /* This parameter enables or disables consistency checks.
             * If set to false the read repairs are disable for very
             * high throughput on reads but at the cost of consistency.*/
            String doConsistency = xmlUtils.getNodeValue("/Storage/DoConsistencyChecksBoolean");
            if ( doConsistency != null )
                doConsistencyCheck = Boolean.parseBoolean(doConsistency) ? 1.0f : 0.0f;

            /* read the size at which we should do column indexes */
            String columnIndexSize = xmlUtils.getNodeValue("/Storage/ColumnIndexSizeInKB");
            if(columnIndexSize == null)
            {
                columnIndexSizeInKB = 64;
            }
            else
            {
                columnIndexSizeInKB = Integer.parseInt(columnIndexSize);
            }

            String rowWarning = xmlUtils.getNodeValue("/Storage/RowWarningThresholdInMB");
            if (rowWarning != null)
            {
                rowWarningThreshold = Long.parseLong(rowWarning) * 1024 * 1024;
                if (rowWarningThreshold <= 0)
                    throw new ConfigurationException("Row warning threshold must be a positive integer");
            }
            /* data file and commit log directories. they get created later, when they're needed. */
            dataFileDirectories = xmlUtils.getNodeValues("/Storage/DataFileDirectories/DataFileDirectory");
            logFileDirectory = xmlUtils.getNodeValue("/Storage/CommitLogDirectory");
            savedCachesDirectory = xmlUtils.getNodeValue("/Storage/SavedCachesDirectory");
            dataArchiveDirectory = xmlUtils.getNodeValue("/Storage/DataArchiveDirectory");
            
            String dataArThrottleString = xmlUtils.getNodeValue("/Storage/DataArchiveThrottle");
            if (dataArThrottleString != null)
            {
                dataArchiveThrottle = Integer.parseInt(dataArThrottleString);
            }

            for (String datadir : dataFileDirectories)
            {
                if (datadir.equals(logFileDirectory))
                    throw new ConfigurationException("CommitLogDirectory must not be the same as any DataFileDirectory");
            }

            String fileAllocationPolicy = xmlUtils.getNodeValue("/Storage/DataFileDirectories/Allocation");
            if (dataFileDirectories.length==1) {
                dataFileAllocator = new SingleDirAllocator(dataFileDirectories);
                if (fileAllocationPolicy!=null)
                    logger.warn("Disk space allocator policy configuration "+fileAllocationPolicy+" is not applicable for single data dir setup");
            } else {
                
                if (fileAllocationPolicy==null || fileAllocationPolicy.equalsIgnoreCase("roundrobin")) {
                    dataFileAllocator = new RoundRobinAllocator(dataFileDirectories);
                } else if (fileAllocationPolicy.equalsIgnoreCase("spacefirst")) {
                    dataFileAllocator = new SpaceFirstAllocator(dataFileDirectories);
                } else if (fileAllocationPolicy.equalsIgnoreCase("sizetiered")) {
                    dataFileAllocator = new SizeTieredAllocator(dataFileDirectories);
                } else {
                    throw new ConfigurationException("Invalid data directories allocator "+fileAllocationPolicy);
                }
                
                logger.info("Multiple data directories allocator is set to "+dataFileAllocator.getClass().getSimpleName());
            }
            

            String activateShipping = xmlUtils.getNodeValue("/Storage/CommitLogArchive");

            if (activateShipping !=null)
            {
                if (Boolean.valueOf(activateShipping))
                {
                    logFileArchiveDestination = logFileDirectory+File.separator+".archived";
                    
                    logger.warn("Commit logs archiving activated to "+logFileArchiveDestination+". You must backup them to archive location and remove by yourself to prevent commit log disk overflow");
                }
            }
            
            /* threshold after which commit log should be rotated. */
            String value = xmlUtils.getNodeValue("/Storage/CommitLogRotationThresholdInMB");
            if ( value != null)
                CommitLog.setSegmentSize(Integer.parseInt(value) * 1024 * 1024);

            hintLogDirectory = xmlUtils.getNodeValue("/Storage/HintLogDirectory");
            if (hintLogDirectory==null)
                hintLogDirectory = logFileDirectory + File.separatorChar + ".hints";

            /* should Hinted Handoff be on? */
            String hintedHandOffStr = xmlUtils.getNodeValue("/Storage/HintedHandoffEnabled");
            if (hintedHandOffStr != null)
            {
                setHintedHandoffManager(hintedHandOffStr);
            } else
            {
                hintedHandoffManager = HintedHandOffManager.class;
                
            }
            

            String v = xmlUtils.getNodeValue("/Storage/CommitLogRotationThresholdInMB");
            if ( v != null)
                HintLog.setSegmentSize(Integer.parseInt(v) * 1024 * 1024);

            if (logger.isDebugEnabled())
                logger.debug("setting hintedHandoffManager to " + hintedHandoffManager);
            

            String indexIntervalStr = xmlUtils.getNodeValue("/Storage/IndexInterval");
            if (indexIntervalStr != null)
            {
                indexinterval = Integer.parseInt(indexIntervalStr);
                if (indexinterval <= 0)
                    throw new ConfigurationException("Index Interval must be a positive, non-zero integer.");
            }

            readTablesFromXml(gcGraceInSeconds);
            if (tables.isEmpty())
                throw new ConfigurationException("No keyspaces configured");

            // Hardcoded system tables
            KSMetaData systemMeta = new KSMetaData(Table.SYSTEM_TABLE, null, -1, null);
            tables.put(Table.SYSTEM_TABLE, systemMeta);
            systemMeta.cfMetaData.put(SystemTable.STATUS_CF, new CFMetaData(Table.SYSTEM_TABLE,
                                                                            SystemTable.STATUS_CF,
                                                                            "Standard",
                                                                            new BytesType(),
                                                                            null,
                                                                            false,
                                                                            "persistent metadata for the local node",
                                                                            0.0,
                                                                            0.01,
                                                                            DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS,
                                                                            DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS,
                                                                            false, 
                                                                            SystemTable.STATUS_CF,
                                                                            null,null,
                                                                            0,
                                                                            null
                                                                            ));

            systemMeta.cfMetaData.put(HintedHandOffManager.HINTS_CF, new CFMetaData(Table.SYSTEM_TABLE,
                                                                                    HintedHandOffManager.HINTS_CF,
                                                                                    "Super",
                                                                                    new BytesType(),
                                                                                    new BytesType(),
                                                                                    false,
                                                                                    "hinted handoff data",
                                                                                    0.0,
                                                                                    0.01,
                                                                                    DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS,
                                                                                    DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS,
                                                                                    false, 
                                                                                    HintedHandOffManager.HINTS_CF,
                                                                                    null,null,
                                                                                    0,
                                                                                    null
                                                                                    ));

            // Configured local storages
            readLocalStoragesFromXml();
            
            /* Load the seeds for node contact points */
            String[] seedsxml = xmlUtils.getNodeValues("/Storage/Seeds/Seed");
            if (seedsxml.length <= 0)
            {
                throw new ConfigurationException("A minimum of one seed is required.");
            }
            for (String seedString : seedsxml)
            {
                seeds.add(InetAddress.getByName(seedString));
            }
            
            /* Init maintenance manager **/
            readMaintenanceManagerConfig(xmlUtils);
        }
        catch (UnknownHostException e)
        {
            logger.error("Fatal error: " + e.getMessage());
            System.err.println("Unable to start with unknown hosts configured.  Use IP addresses instead of hostnames.");
            System.exit(2);
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal error: " + e.getMessage());
            System.err.println("Bad configuration; unable to start server");
            System.exit(1);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void setHintedHandoffManager(String hintedHandOffStr)
            throws ConfigurationException
    {
        if (hintedHandOffStr.equalsIgnoreCase("hintlog"))
        {
            hintedHandoffManager = HintLogHandoffManager.class;

            logger.info("HintLog Handoff activated. Will write hint logs to "+hintLogDirectory);

        }
        else if (hintedHandOffStr.equalsIgnoreCase("false"))
            hintedHandoffManager = null;
        else if (hintedHandOffStr.equalsIgnoreCase("true"))
        {
            hintedHandoffManager = HintLogHandoffManager.class;

            logger.info("HintLog Handoff activated. Will write hint logs to "+hintLogDirectory);
            
        } 
        else
            if (hintedHandOffStr.equalsIgnoreCase("stock"))
            {
                hintedHandoffManager = HintLogHandoffManager.class;

                logger.info("Stock cassandra HintedHandoff activated. Hope you know what are you doing - its slow and ugly");

            }
            else
                throw new ConfigurationException("Unrecognized value for HintedHandoff.  Use 'true','false' or 'hintlog'.");

        HintedHandOffManager.setInstance(hintedHandoffManager);
    }

    /**
     * @param xmlUtils
     * @throws XPathExpressionException 
     */
    private static void readMaintenanceManagerConfig(XMLUtils xmlUtils) throws XPathExpressionException
    {
        String windowStart = xmlUtils.getNodeValue("/Storage/Maintenance/TimeWindow/Start");
        String windowEnd = xmlUtils.getNodeValue("/Storage/Maintenance/TimeWindow/End");
        
        if (windowStart==null || windowEnd==null )
            return;

        List<MaintenanceTask> tasks = new ArrayList<MaintenanceTask>();
        String daysBack = xmlUtils.getNodeValue("/Storage/Maintenance/Tasks/CleanOldSnapshots");
        if (daysBack!=null)
        {
            tasks.add(new CleanOldSnapshotsTask(Integer.parseInt(daysBack)));
        }

        daysBack = xmlUtils.getNodeValue("/Storage/Maintenance/Tasks/CleanArchivedLogs");
        if (daysBack!=null)
        {
            tasks.add(new CleanArchivedLogsTask(Integer.parseInt(daysBack)));
        }

        String tag = xmlUtils.getNodeValue("/Storage/Maintenance/Tasks/ClusterSnapshot");
        if (tag!=null)
        {
            tasks.add(new ClusterSnapshotTask(tag));
        }

        String spareNodes = xmlUtils.getNodeValue("/Storage/Maintenance/Tasks/MajorCompaction");
        if (spareNodes!=null)
        {
            if ( RackAwareOdklEvenStrategy.class.isAssignableFrom( getReplicaPlacementStrategyClass( getNonSystemTables().get(0) )) )
            {
                tasks.add(new RackAwareMajorCompactionTask(Integer.parseInt(spareNodes)));
            } else
                if ( RackAwareOdklStrategy.class.isAssignableFrom( getReplicaPlacementStrategyClass( getNonSystemTables().get(0) )) )
                {
                    tasks.add(new RackAwareMajorCompactionTask(Integer.parseInt(spareNodes)));
                } else
                {
                    tasks.add(new MajorCompactionTask(Integer.parseInt(spareNodes)));
                }
        }
        
        if (tasks.size()==0)
            return;
        
        MaintenanceTaskManager.init(tasks, windowStart, windowEnd);
    }
    
    private static void readTablesFromXml(int gcGraceInSeconds) throws ConfigurationException
    {
        XMLUtils xmlUtils = null;
        try
        {
            xmlUtils = new XMLUtils(configFileURI);
        }
        catch (ParserConfigurationException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (SAXException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }

            /* Read the table related stuff from config */
        try
        {
            NodeList tablesxml = xmlUtils.getRequestedNodeList("/Storage/Keyspaces/Keyspace");
            int size = tablesxml.getLength();
            for ( int i = 0; i < size; ++i )
            {
                String value = null;
                Node table = tablesxml.item(i);

                /* parsing out the table ksName */
                String ksName = XMLUtils.getAttributeValue(table, "Name");
                if (ksName == null)
                {
                    throw new ConfigurationException("Table name attribute is required");
                }
                if (ksName.equalsIgnoreCase(Table.SYSTEM_TABLE))
                {
                    throw new ConfigurationException("'system' is a reserved table name for Cassandra internals");
                }

                /* See which replica placement strategy to use */
                String replicaPlacementStrategyClassName = xmlUtils.getNodeValue("/Storage/Keyspaces/Keyspace[@Name='" + ksName + "']/ReplicaPlacementStrategy");
                if (replicaPlacementStrategyClassName == null)
                {
                    throw new ConfigurationException("Missing replicaplacementstrategy directive for " + ksName);
                }
                Class<? extends AbstractReplicationStrategy> repStratClass = null;
                try
                {
                    repStratClass = (Class<? extends AbstractReplicationStrategy>) Class.forName(replicaPlacementStrategyClassName);
                }
                catch (ClassNotFoundException e)
                {
                    throw new ConfigurationException("Invalid replicaplacementstrategy class " + replicaPlacementStrategyClassName);
                }

                /* Data replication factor */
                String replicationFactor = xmlUtils.getNodeValue("/Storage/Keyspaces/Keyspace[@Name='" + ksName + "']/ReplicationFactor");
                int repFact = -1;
                if (replicationFactor == null)
                    throw new ConfigurationException("Missing replicationfactor directory for keyspace " + ksName);
                else
                {
                    repFact = Integer.parseInt(replicationFactor);
                }

                /* end point snitch */
                String endPointSnitchClassName = xmlUtils.getNodeValue("/Storage/Keyspaces/Keyspace[@Name='" + ksName + "']/EndPointSnitch");
                if (endPointSnitchClassName == null)
                {
                    throw new ConfigurationException("Missing endpointsnitch directive for keyspace " + ksName);
                }
                IEndPointSnitch epSnitch = null;
                try
                {
                    Class cls = Class.forName(endPointSnitchClassName);
                    IEndPointSnitch snitch = (IEndPointSnitch)cls.getConstructor().newInstance();
                    if (Boolean.getBoolean("cassandra.dynamic_snitch"))
                        epSnitch = new DynamicEndpointSnitch(snitch);
                    else
                        epSnitch = snitch;
                }
                catch (ClassNotFoundException e)
                {
                    throw new ConfigurationException("Not found endpointsnitch class " + endPointSnitchClassName , e);
                }
                catch (NoSuchMethodException e)
                {
                    throw new ConfigurationException("No constructor for endpointsnitch class " + endPointSnitchClassName , e);
                }
                catch (InstantiationException e)
                {
                    logger.error("Unable to instantiate endpointsnitch class " + endPointSnitchClassName , e);
                    throw new ConfigurationException("Unable to instantiate endpointsnitch class " + endPointSnitchClassName , e);
                }
                catch (IllegalAccessException e)
                {
                    logger.error("Unable to instantiate endpointsnitch class " + endPointSnitchClassName , e);
                    throw new ConfigurationException("Unable to instantiate endpointsnitch class " + endPointSnitchClassName , e);
                }
                catch (InvocationTargetException e)
                {
                    logger.error("Unable to instantiate endpointsnitch class " + endPointSnitchClassName , e);
                    throw new ConfigurationException("Unable to instantiate endpointsnitch class " + endPointSnitchClassName , e);
                }
                
                String xqlTable = "/Storage/Keyspaces/Keyspace[@Name='" + ksName + "']/";

                KSMetaData meta = new KSMetaData(ksName, repStratClass, repFact, epSnitch);

                readColumnFamiliesFromXml(
                        xmlUtils,
                        ksName,
                        xqlTable,
                        meta,
                        gcGraceInSeconds
                        );

                tables.put(meta.name, meta);
            }
            
            
        }
        catch (XPathExpressionException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (TransformerException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    private static void readColumnFamiliesFromXml(XMLUtils xmlUtils, String ksName,
            String xqlTable, KSMetaData meta, int gcGraceInSeconds)
            throws TransformerException, ConfigurationException,
            XPathExpressionException
    {
        NodeList columnFamilies = xmlUtils.getRequestedNodeList(xqlTable + "ColumnFamily");
        String value;
        int size2 = columnFamilies.getLength();

        for ( int j = 0; j < size2; ++j )
        {
            Node columnFamily = columnFamilies.item(j);
            String tableName = ksName;
            String cfName = XMLUtils.getAttributeValue(columnFamily, "Name");
            if (cfName == null)
            {
                throw new ConfigurationException("ColumnFamily name attribute is required");
            }
            if (cfName.contains("-"))
            {
                throw new ConfigurationException("ColumnFamily names cannot contain hyphens");
            }
            String xqlCF = xqlTable + "ColumnFamily[@Name='" + cfName + "']/";

            // Parse out the column type
            String rawColumnType = XMLUtils.getAttributeValue(columnFamily, "ColumnType");
            String columnType = ColumnFamily.getColumnType(rawColumnType);
            if (columnType == null)
            {
                throw new ConfigurationException("ColumnFamily " + cfName + " has invalid type " + rawColumnType);
            }

            if (XMLUtils.getAttributeValue(columnFamily, "ColumnSort") != null)
            {
                throw new ConfigurationException("ColumnSort is no longer an accepted attribute.  Use CompareWith instead.");
            }

            // Parse out the column comparator
            AbstractType comparator = getComparator(columnFamily, "CompareWith");
            AbstractType subcolumnComparator = null;
            if (columnType.equals("Super"))
            {
                subcolumnComparator = getComparator(columnFamily, "CompareSubcolumnsWith");
            }
            else if (XMLUtils.getAttributeValue(columnFamily, "CompareSubcolumnsWith") != null)
            {
                throw new ConfigurationException("CompareSubcolumnsWith is only a valid attribute on super columnfamilies (not regular columnfamily " + cfName + ")");
            }

            double keyCacheSize = CFMetaData.DEFAULT_KEY_CACHE_SIZE;
            if ((value = XMLUtils.getAttributeValue(columnFamily, "KeysCachedFraction")) != null)
            {
                keyCacheSize = Double.valueOf(value);
                // TODO: KeysCachedFraction deprecated: remove in 1.0
                logger.warn("KeysCachedFraction is deprecated: use KeysCached instead.");
            }
            if ((value = XMLUtils.getAttributeValue(columnFamily, "KeysCached")) != null)
            {
                keyCacheSize = FBUtilities.parseDoubleOrPercent(value);
            }

            double rowCacheSize = CFMetaData.DEFAULT_ROW_CACHE_SIZE;
            if ((value = XMLUtils.getAttributeValue(columnFamily, "RowsCached")) != null)
            {
                rowCacheSize = FBUtilities.parseDoubleOrPercent(value);
            }
            
            // MM: parse out bloom columns for this CF
            boolean bloomColumns = false;
            if ((value = XMLUtils.getAttributeValue(columnFamily, "BloomColumns")) != null)
            {
                bloomColumns = Boolean.valueOf(value);
                if (bloomColumns)
                {
                    if (columnType.equals("Super"))
                    {
                        throw new ConfigurationException("bloomColumns mode is not implemented for super columns");
                    }

                    logger.info("Column level bloom filter in on for "+cfName);
                }
            }                    
            // MM: parse out domain split for this CF
            boolean splitByDomain = false;
            if ((value = XMLUtils.getAttributeValue(columnFamily, "SplitByDomain")) != null)
            {
                splitByDomain = Boolean.valueOf(value);
                if (splitByDomain)
                {
                    if (cfName.contains("_"))
                    {
                        throw new ConfigurationException("ColumnFamily split by domain names cannot contain underscores");
                    }
                    if (! (DatabaseDescriptor.getPartitioner() instanceof OdklDomainPartitioner) )
                    {
                        throw new ConfigurationException("SplitByDomain mode is possible only with OdklDomainPartitioner");
                    }

                    logger.info("Splitting "+cfName+" by domain of keys");
                }
            }                    

            // Parse out user-specified logical names for the various dimensions
            // of a the column family from the config.
            String comment = xmlUtils.getNodeValue(xqlCF + "Comment");
            
            // insert it into the table dictionary.
            String rowCacheSavePeriodString = XMLUtils.getAttributeValue(columnFamily, "RowCacheSavePeriodInSeconds");
            String keyCacheSavePeriodString = XMLUtils.getAttributeValue(columnFamily, "KeyCacheSavePeriodInSeconds");
            int rowCacheSavePeriod = keyCacheSavePeriodString != null ? Integer.valueOf(keyCacheSavePeriodString) : DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS;
            int keyCacheSavePeriod = rowCacheSavePeriodString != null ? Integer.valueOf(rowCacheSavePeriodString) : DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS;
            
            // Configuration of row processors:
            // <RowProcessor class=ClassName parameter1="" />
            NodeList rowProcessorsString = xmlUtils.getRequestedNodeList(xqlCF+"RowProcessor");
            ArrayList<Pair<Class<? extends IRowProcessor>, Properties>> processors = null;
            for (int i=0;i<rowProcessorsString.getLength();i++)
            {
               Node procNode = rowProcessorsString.item(i); 
               
               String procClassString =  XMLUtils.getAttributeValue(procNode, "class");
               if (procClassString.indexOf('.')<0)
               {
                   procClassString= IRowProcessor.class.getPackage().getName()+'.'+procClassString+"RowProcessor";
               }
               
               try {
                   Class<? extends IRowProcessor> procClass = Class.forName(procClassString).asSubclass(IRowProcessor.class);

                   // trying to create instance
                   procClass.newInstance();

                   Properties procProps = new Properties();
                   for ( int ai=0;ai<procNode.getAttributes().getLength();ai++)
                   {
                       Node attr = procNode.getAttributes().item(ai);
                       procProps.put(attr.getNodeName(), attr.getNodeValue());
                   }

                   if (processors==null) processors = new ArrayList<Pair<Class<? extends IRowProcessor>,Properties>>();
                   processors.add( new Pair<Class<? extends IRowProcessor>, Properties>(procClass, procProps));
               } catch (Exception e) {
                   throw new ConfigurationException("Cannot configure row processor "+procClassString, e);
               }
            }
            
            if (splitByDomain)
            {
                // generating CFs postfixed with _{0-255}
                for (int domain =0;domain<256;domain++)
                {
                    Token domainToken = getPartitioner().getToken(Integer.toHexString(domain));
                    String postfix='_'+domainToken.toString();
                    domainToken = getPartitioner().getToken(domainToken.toString()+((char)0));
                    Token domainMax = domain==255 ? getPartitioner().getToken(Integer.toHexString(0)) : getPartitioner().getToken(Integer.toHexString(domain+1));
                    meta.cfMetaData.put(cfName+postfix, new CFMetaData(tableName, cfName+postfix, columnType, comparator, subcolumnComparator, bloomColumns, comment, rowCacheSize, keyCacheSize, keyCacheSavePeriod, rowCacheSavePeriod, true,cfName, domainToken,domainMax,gcGraceInSeconds,processors));
                }
            }
            else
            {
                meta.cfMetaData.put(cfName, new CFMetaData(tableName, cfName, columnType, comparator, subcolumnComparator, bloomColumns, comment, rowCacheSize, keyCacheSize, keyCacheSavePeriod, rowCacheSavePeriod, false,cfName,null,null,gcGraceInSeconds,processors));
            }
        }
        
    }
    
    private static void readLocalStoragesFromXml() throws ConfigurationException
    {
        XMLUtils xmlUtils = null;
        try
        {
            xmlUtils = new XMLUtils(configFileURI);
        }
        catch (ParserConfigurationException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (SAXException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }

        /* Read the table related stuff from config */
        try
        {
                
                String xqlTable = "/Storage/LocalStores/";

                KSMetaData meta = tables.get(Table.SYSTEM_TABLE);

                readColumnFamiliesFromXml(
                        xmlUtils,
                        Table.SYSTEM_TABLE,
                        xqlTable,
                        meta,
                        0); // for system storages gc grace is always 0 - they are not distributed, so thombstones are useless

        }
        catch (XPathExpressionException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (TransformerException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public static IAuthenticator getAuthenticator()
    {
        return authenticator;
    }

    public static boolean isThriftFramed()
    {
        return thriftFramed;
    }

    private static AbstractType getComparator(Node columnFamily, String attr) throws ConfigurationException
    {
        String compareWith = null;
        try
        {
            compareWith = XMLUtils.getAttributeValue(columnFamily, attr);
        }
        catch (TransformerException e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }

        try
        {
            return FBUtilities.getComparator(compareWith);
        }
        catch (Exception e)
        {
            ConfigurationException ex = new ConfigurationException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    /**
     * Creates all storage-related directories.
     * @throws IOException when a disk problem is encountered.
     */
    public static void createAllDirectories() throws IOException
    {
        try {
            if (dataFileDirectories.length == 0)
            {
                throw new ConfigurationException("At least one DataFileDirectory must be specified");
            }
            
            for ( String dataFileDirectory : dataFileDirectories )
                FileUtils.createDirectory(dataFileDirectory);
            
            if (logFileDirectory == null)
            {
                throw new ConfigurationException("CommitLogDirectory must be specified");
            }
            FileUtils.createDirectory(logFileDirectory);
            
            if (logFileArchiveDestination !=null )
            {
                FileUtils.createDirectory(logFileArchiveDestination);
            }   
            if (savedCachesDirectory == null)
            {
                throw new ConfigurationException("SavedCachesDirectory must be specified");
            }

            FileUtils.createDirectory(savedCachesDirectory);
            
            if (hintLogDirectory !=null )
            {
                FileUtils.createDirectory(hintLogDirectory);
            }   
            
            if (dataArchiveDirectory != null)
            {
                FileUtils.createDirectory(dataArchiveDirectory);
            }
        }
        catch (ConfigurationException ex) {
            logger.error("Fatal error: " + ex.getMessage());
            System.err.println("Bad configuration; unable to start server");
            System.exit(1);
        }
        /* make sure we have a directory for each table */
        for (String dataFile : dataFileDirectories)
        {
            FileUtils.createDirectory(dataFile + File.separator + Table.SYSTEM_TABLE);
            for (String table : tables.keySet())
            {
                String oneDir = dataFile + File.separator + table;
                FileUtils.createDirectory(oneDir);
                File streamingDir = new File(oneDir, STREAMING_SUBDIR);
                if (streamingDir.exists())
                    FileUtils.deleteDir(streamingDir);
            }
        }
    }

    /**
     * Create the metadata tables. This table has information about
     * the table name and the column families that make up the table.
     * Each column family also has an associated ID which is an int.
    */
    // TODO duplicating data b/t tablemetadata and CFMetaData is confusing and error-prone
    public static void storeMetadata() throws IOException
    {
        int cfId = 0;
        Set<String> tableset = tables.keySet();

        for (String table : tableset)
        {
            Table.TableMetadata tmetadata = Table.TableMetadata.instance(table);
            if (tmetadata.isEmpty())
            {
                tmetadata = Table.TableMetadata.instance(table);
                /* Column families associated with this table */
                Map<String, CFMetaData> columnFamilies = tables.get(table).cfMetaData;

                for (String columnFamily : columnFamilies.keySet())
                {
                    tmetadata.add(columnFamily, cfId++, DatabaseDescriptor.getColumnType(table, columnFamily));
                }
            }
        }
    }

    public static IPartitioner getPartitioner()
    {
        return partitioner;
    }

    public static IEndPointSnitch getEndPointSnitch(String table)
    {
        return tables.get(table).epSnitch;
    }

    public static Class<? extends AbstractReplicationStrategy> getReplicaPlacementStrategyClass(String table)
    {
        return tables.get(table).repStratClass;
    }
    
    public static String getJobTrackerAddress()
    {
        return jobTrackerHost;
    }
    
    public static int getColumnIndexSize()
    {
    	return columnIndexSizeInKB * 1024;
    }

    public static int getMemtableLifetimeMS()
    {
      return memtableLifetimeMs;
    }

    public static String getInitialToken()
    {
      return initialToken;
    }
    
    public static String getLocation()
    {
        return System.getProperty("cassandra.location", location );
    }
    
    public static Set<String> getAllowedLocations()
    {
        return allowedLocations;
    }
  
    public static String getReplaceToken()
    {
        return System.getProperty("cassandra.replace_token", null);
    }

    public static int getMemtableThroughput()
    {
      return memtableThroughput;
    }
    
    /**
     * MM: allow experiments with memtable throughput at runtime
     * @param memtableThroughput the memtableThroughput to set
     */
    public static void setMemtableThroughput(int memtableThroughput)
    {
        DatabaseDescriptor.memtableThroughput = memtableThroughput;
    }

    public static double getMemtableOperations()
    {
      return memtableOperations;
    }
    
    /**
     * MM: allow experiments with memtable operations at runtime
     * @param memtableOperations the memtableOperations to set
     */
    public static void setMemtableOperations(double memtableOperations)
    {
        DatabaseDescriptor.memtableOperations = memtableOperations;
    }
    
    private static Random consistencyRandom = new Random();

    public static boolean getConsistencyCheck()
    {
        if (doConsistencyCheck==1.0f)
            return true;

        if (doConsistencyCheck==0.0f)
            return false;

        return consistencyRandom.nextFloat() < doConsistencyCheck;
    }
    
    public static void setConsistencyCheckProbability( float p )
    {
        doConsistencyCheck = p;
    }

    public static float getConsistencyCheckProbability( )
    {
        return doConsistencyCheck ;
    }
    
    public static boolean getParallelReads()
    {
        return parallelReads;
    }
    
    /**
     * @param parallelWeakRead the parallelWeakRead to set
     */
    public static void setParallelReads(boolean parallelWeakRead)
    {
        parallelReads = parallelWeakRead;
    }

    public static String getClusterName()
    {
        return clusterName;
    }

    public static String getConfigFileName() {
        return configFileURI.toString();
    }

    public static String getJobJarLocation()
    {
        return jobJarFileLocation;
    }
    
    public static Map<String, CFMetaData> getTableMetaData(String tableName)
    {
        assert tableName != null;
        KSMetaData ksm = tables.get(tableName);
        assert ksm != null;
        return Collections.unmodifiableMap(ksm.cfMetaData);
    }

    /*
     * Given a table name & column family name, get the column family
     * meta data. If the table name or column family name is not valid
     * this function returns null.
     */
    public static CFMetaData getCFMetaData(String tableName, String cfName)
    {
        assert tableName != null;
        KSMetaData ksm = tables.get(tableName);
        if (ksm == null)
            return null;
        return ksm.cfMetaData.get(cfName);
    }
    
    public static String getColumnType(String tableName, String cfName)
    {
        assert tableName != null;
        CFMetaData cfMetaData = getCFMetaData(tableName, cfName);
        
        if (cfMetaData == null)
            return null;
        return cfMetaData.columnType;
    }

    public static Set<String> getTables()
    {
        return tables.keySet();
    }

    public static List<String> getNonSystemTables()
    {
        List<String> tableslist = new ArrayList<String>(tables.keySet());
        tableslist.remove(Table.SYSTEM_TABLE);
        return Collections.unmodifiableList(tableslist);
    }

    public static int getStoragePort()
    {
        return storagePort;
    }

    public static int getThriftPort()
    {
        return thriftPort;
    }

    public static int getReplicationFactor(String table)
    {
        return tables.get(table).replicationFactor;
    }

    public static long getRpcTimeout()
    {
        return rpcTimeoutInMillis;
    }
    
    public static RpcServerTypes getRpcServerType()
    {
        return rpcServerType;
    }

    public static int getRpcThreads()
    {
        return rpcThreads;
    }
    
    /**
     * @return the thriftFramedTransportSizeMB
     */
    public static int getThriftFramedTransportSize()
    {
        return thriftFramedTransportSizeMB * 1024 * 1024;
    }
    
    /**
     * @return the thriftMaxMessageLengthMB
     */
    public static int getThriftMaxMessageLength()
    {
        return thriftMaxMessageLengthMB * 1024 * 1024;
    }
    
    public static int getStreamInMBits()
    {
        return streamInMBits;
    }
    
    public static void setStreamInMBits(int newMBits)
    {
        streamInMBits = newMBits;
    }

    public static int getPhiConvictThreshold()
    {
        return phiConvictThreshold;
    }

    public static int getConsistencyThreads()
    {
        return consistencyThreads;
    }

    public static int getConcurrentReaders()
    {
        return concurrentReaders;
    }

    public static int getConcurrentWriters()
    {
        return concurrentWriters;
    }

    public static long getRowWarningThreshold()
    {
        return rowWarningThreshold;
    }
    
    public static String[] getAllDataFileLocations()
    {
        return dataFileDirectories;
    }

    /**
     * Get a list of data directories for a given table
     * 
     * @param table name of the table.
     * 
     * @return an array of path to the data directories. 
     */
    public static String[] getAllDataFileLocationsForTable(String table)
    {
        String[] tableLocations = new String[dataFileDirectories.length];

        for (int i = 0; i < dataFileDirectories.length; i++)
        {
            tableLocations[i] = AbstractDiskAllocator.getDataFileLocationForTable( new File( dataFileDirectories[i] ), table );
        }

        return tableLocations;
    }
    
    public static String getLogFileLocation()
    {
        return logFileDirectory;
    }
    
    public static String getLogArchiveDestination()
    {
        return logFileArchiveDestination;
    }
    
    public static String getMerkleTreeArchiveDestination()
    {
        return logFileDirectory+File.separatorChar+".tmp";
    }
    
    public static boolean isLogArchiveActive()
    {
        return logFileArchiveDestination != null;
    }

    public static Set<InetAddress> getSeeds()
    {
        return seeds;
    }

    public static String getColumnFamilyType(String tableName, String cfName)
    {
        assert tableName != null;
        String cfType = getColumnType(tableName, cfName);
        if ( cfType == null )
            cfType = "Standard";
    	return cfType;
    }

    /*
     * Loop through all the disks to see which disk has the max free space
     * return the disk with max free space for compactions. If the size of the expected
     * compacted file is greater than the max disk space available return null, we cannot
     * do compaction in this case.
     */
    public static String getDataFileLocation(ColumnFamilyStore store, long expectedCompactedFileSize)
    {
        return dataFileAllocator.getDataFileLocation(store, expectedCompactedFileSize);
    }
    
    public static File getDataArchiveFileLocationForSnapshot(String table)
    {
        assert dataArchiveDirectory !=null ;
        
        File f = new File(dataArchiveDirectory + File.separator + table);
        
        if (!f.exists())
            f.mkdirs();
        
        return f;
    }
    
    public static boolean isDataArchiveEnabled()
    {
        return dataArchiveDirectory!=null;
    }
    
    public static int getDataArchiveThrottle()
    {
        return dataArchiveThrottle;
    }

    public static AbstractType getComparator(String tableName, String cfName)
    {
        assert tableName != null;
        CFMetaData cfmd = getCFMetaData(tableName, cfName);
        if (cfmd == null)
            throw new NullPointerException("Unknown ColumnFamily " + cfName + " in keyspace " + tableName);
        return cfmd.comparator;
    }

    public static AbstractType getSubComparator(String tableName, String cfName)
    {
        assert tableName != null;
        return getCFMetaData(tableName, cfName).subcolumnComparator;
    }

    public static boolean getBloomColumns(String tableName, String cfName)
    {
        assert tableName != null;
        CFMetaData cfMetaData = getCFMetaData(tableName, cfName);
        return cfMetaData==null ? false : cfMetaData.bloomColumns;
    }

    /**
     * @return The absolute number of keys that should be cached per table.
     */
    public static int getKeysCachedFor(String tableName, String columnFamilyName, long expectedKeys)
    {
        CFMetaData cfm = getCFMetaData(tableName, columnFamilyName);
        double v = (cfm == null) ? CFMetaData.DEFAULT_KEY_CACHE_SIZE : cfm.keyCacheSize;
        return (int)Math.min(FBUtilities.absoluteFromFraction(v, expectedKeys), Integer.MAX_VALUE);
    }

    /**
     * @return The absolute number of rows that should be cached for the columnfamily.
     */
    public static int getRowsCachedFor(String tableName, String columnFamilyName, long expectedRows)
    {
        CFMetaData cfm = getCFMetaData(tableName, columnFamilyName);
        double v = (cfm == null) ? CFMetaData.DEFAULT_ROW_CACHE_SIZE : cfm.rowCacheSize;
        return (int)Math.min(FBUtilities.absoluteFromFraction(v, expectedRows), Integer.MAX_VALUE);
    }

    public static InetAddress getListenAddress()
    {
        return listenAddress;
    }
    
    public static InetAddress getThriftAddress()
    {
        return thriftAddress;
    }

    public static double getCommitLogSyncBatchWindow()
    {
        return commitLogSyncBatchMS;
    }

    public static int getCommitLogSyncPeriod() {
        return commitLogSyncPeriodMS;
    }

    public static int getMaxCommitLogSegmentsActive() {
        return maxCommitLogSegmentsActive;
    }

    public static void setMaxCommitLogSegmentsActive(int c) {
        maxCommitLogSegmentsActive=c;
    }

    public static CommitLogSync getCommitLogSync()
    {
        return commitLogSync;
    }

    public static DiskAccessMode getDiskAccessMode()
    {
        return diskAccessMode;
    }
    
    public static boolean isDiskRandomHintEnabled()
    {
        return diskRandomHint;
    }

    public static DiskAccessMode getIndexAccessMode()
    {
        return indexAccessMode;
    }

    public static double getFlushDataBufferSizeInMB()
    {
        return flushDataBufferSizeInMB;
    }

    public static double getFlushIndexBufferSizeInMB()
    {
        return flushIndexBufferSizeInMB;
    }

    public static int getIndexedReadBufferSizeInKB()
    {
        return columnIndexSizeInKB;
    }

    public static int getSlicedReadBufferSizeInKB()
    {
        return slicedReadBufferSizeInKB;
    }

    public static int getBMTThreshold()
    {
        return bmtThreshold;
    }

    public static boolean isSnapshotBeforeCompaction()
    {
        return snapshotBeforeCompaction;
    }

    public static boolean isAutoBootstrap()
    {
        return autoBootstrap || getReplaceToken()!=null;
    }

    public static boolean hintedHandoffEnabled()
    {
        return hintedHandoffManager != null;
    }
    
    public static String getHintLogDirectory()
    {
        return hintLogDirectory;
    }

    public static int getIndexInterval()
    {
        return indexinterval;
    }
}
