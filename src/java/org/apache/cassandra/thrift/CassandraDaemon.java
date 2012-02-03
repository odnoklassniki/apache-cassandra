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

package org.apache.cassandra.thrift;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.JMXConfigurableThreadPoolExecutor;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DatabaseDescriptor.RpcServerTypes;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.FSReadError;
import org.apache.cassandra.db.FSWriteError;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.maint.MaintenanceTaskManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

/**
 * This class supports two methods for creating a Cassandra node daemon, 
 * invoking the class's main method, and using the jsvc wrapper from 
 * commons-daemon, (for more information on using this class with the 
 * jsvc wrapper, see the 
 * <a href="http://commons.apache.org/daemon/jsvc.html">Commons Daemon</a>
 * documentation).
 */

public class CassandraDaemon
{
    private static Logger logger = Logger.getLogger(CassandraDaemon.class);
    private TServer serverEngine;

    public void setup() throws IOException, TTransportException
    {
        // log4j
        String file = System.getProperty("storage-config") + File.separator + "log4j.properties";
        PropertyConfigurator.configure(file);

        setupInternals();

        setupServer();

        setupThrift();
        
        // start maintenance
        if (MaintenanceTaskManager.isConfigured())
            MaintenanceTaskManager.instance.start();
    }

    protected void setupThrift()
    {
        // init thrift server
        int listenPort = DatabaseDescriptor.getThriftPort();
        InetAddress listenAddr = DatabaseDescriptor.getThriftAddress();
        
        /* 
         * If ThriftAddress was left completely unconfigured, then assume
         * the same default as ListenAddress
         */
        if (listenAddr == null)
            listenAddr = FBUtilities.getLocalAddress();
        
        initThrift(listenAddr, listenPort);
    }

    protected void setupServer() throws IOException
    {
        // start server internals
        try
        {
            StorageService.instance.initServer();
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal error: " + e.getMessage());
            System.err.println("Bad configuration; unable to start server");
            System.exit(1);
        }
    }

    protected void setupInternals() throws IOException
    {
        CLibrary.tryMlockall();

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            private volatile boolean shutdown = false;
            
            public void uncaughtException(Thread t, Throwable e)
            {
                logger.error("Uncaught exception in thread " + t, e);
                if (e instanceof OutOfMemoryError)
                {
                    System.exit(100);
                }
                if (e instanceof FSReadError || e instanceof FSWriteError)
                {
                    synchronized (this)
                    {
                        if (shutdown)
                            return;
                        
                        logger.error("Filesystem errors detected, shutting down gossip and rpc server");
                        stop();
                        try {
                            Thread.sleep(DatabaseDescriptor.getRpcTimeout()*2);
                        } catch (InterruptedException e1) {
                            logger.error("Hm",e1);
                        }
                        
                        Gossiper.instance.stop();
                        shutdown=true;
                    }
                }
            }
        });
        
        // check the system table for mismatched partitioner.
        try
        {
            SystemTable.checkHealth();
        }
        catch (IOException e)
        {
            logger.error("Fatal exception during initialization", e);
            System.exit(100);
        }
        
        // initialize keyspaces
        for (String table : DatabaseDescriptor.getTables())
        {
            if (logger.isDebugEnabled())
                logger.debug("opening keyspace " + table);
            Table.open(table);
        }

        // replay the log if necessary and check for compaction candidates
        CommitLog.recover();
        CompactionManager.instance.checkAllColumnFamilies();
    }
    
    private void initThrift(InetAddress listenAddr, int listenPort)
    {
        // now we start listening for clients
        final CassandraServer cassandraServer = new CassandraServer();
        Cassandra.Processor processor = new Cassandra.Processor(cassandraServer);

        // Transport
        logger.info(String.format("Binding thrift service to %s:%s", listenAddr, listenPort));

        // Protocol factory
        TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(true, true, DatabaseDescriptor.getThriftMaxMessageLength());

        // Transport factory
        int tFramedTransportSize = DatabaseDescriptor.getThriftFramedTransportSize();
        TTransportFactory inTransportFactory = new TFramedTransport.Factory(tFramedTransportSize);
        TTransportFactory outTransportFactory = new TFramedTransport.Factory(tFramedTransportSize);
        logger.info(String.format("Using TFramedTransport with a max frame size of %d bytes.", tFramedTransportSize));

        if (DatabaseDescriptor.getRpcServerType()==RpcServerTypes.sync)
        {                
            TServerTransport serverTransport;
            try
            {
                serverTransport = new TServerSocket(new InetSocketAddress(listenAddr, listenPort));
            } 
            catch (TTransportException e)
            {
                throw new RuntimeException(String.format("Unable to create thrift socket to %s:%s", listenAddr, listenPort), e);
            }
            // ThreadPool Server and will be invocation per connection basis...
            TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport)
            .minWorkerThreads(DatabaseDescriptor.getRpcThreads())
            .maxWorkerThreads(Integer.MAX_VALUE)
            .inputTransportFactory(inTransportFactory)
            .outputTransportFactory(outTransportFactory)
            .inputProtocolFactory(tProtocolFactory)
            .outputProtocolFactory(tProtocolFactory)
            .processor(processor);
            ExecutorService executorService = new JMXEnabledThreadPoolExecutor(serverArgs.minWorkerThreads, serverArgs.maxWorkerThreads,
                    60,
                    TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>(),
                    new NamedThreadFactory("RPC-Thread")
                    )
            {
                @Override
                public void afterExecute(Runnable r, Throwable t)
                {
                    super.afterExecute(r, t);
                    cassandraServer.logout();
                }
            };

            serverEngine = new CustomTThreadPoolServer(serverArgs, executorService);
            logger.info(String.format("Using synchronous/threadpool thrift server on %s : %s", listenAddr, listenPort));
        }
        else
        {
            // async hsha server init
            TNonblockingServerTransport serverTransport;
            try
            {
                serverTransport = new TNonblockingServerSocket(new InetSocketAddress(listenAddr, listenPort));
            } 
            catch (TTransportException e)
            {
                throw new RuntimeException(String.format("Unable to create thrift socket to %s:%s", listenAddr, listenPort), e);
            }

            // This is NIO selector service but the invocation will be Multi-Threaded with the Executor service.
            ExecutorService executorService = new JMXConfigurableThreadPoolExecutor(DatabaseDescriptor.getRpcThreads(),
                    DatabaseDescriptor.getRpcThreads(),
                    DatabaseDescriptor.getRpcTimeout(), 
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(), 
                    new NamedThreadFactory("RPC-Thread"));

            TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport).inputTransportFactory(inTransportFactory)
                    .outputTransportFactory(outTransportFactory)
                    .inputProtocolFactory(tProtocolFactory)
                    .outputProtocolFactory(tProtocolFactory)
                    .processor(processor);
            logger.info(String.format("Using custom half-sync/half-async thrift server on %s : %s with %d processing threads", listenAddr, listenPort, DatabaseDescriptor.getRpcThreads()));
            // Check for available processors in the system which will be equal to the IO Threads.
            serverEngine = new CustomTHsHaServer(serverArgs, executorService, Runtime.getRuntime().availableProcessors());
        }
    }

    /** hook for JSVC */
    public void init(String[] args) throws IOException, TTransportException
    {  
        setup();
    }

    /** hook for JSVC */
    public void start()
    {
        logger.info("Cassandra thrift server starting up...");
        serverEngine.serve();
    }

    /** hook for JSVC */
    public void stop()
    {
        // this doesn't entirely shut down Cassandra, just the Thrift server.
        // jsvc takes care of taking the rest down
        logger.info("Cassandra thrift server shutting down...");
        serverEngine.stop();
        
        if (MaintenanceTaskManager.isConfigured())
            MaintenanceTaskManager.instance.stop();
    }
    
    
    /** hook for JSVC */
    public void destroy()
    {
        // this is supposed to "destroy any object created in init", but
        // StorageService et al. are crash-only, so we no-op here.
    }
    
    public static void main(String[] args)
    {
     
        CassandraDaemon daemon = new CassandraDaemon();
        String pidFile = System.getProperty("cassandra-pidfile");
        
        try
        {   
            daemon.setup();

            if (pidFile != null)
            {
                new File(pidFile).deleteOnExit();
            }

            if (System.getProperty("cassandra-foreground") == null)
            {
                System.out.close();
                System.err.close();
            }

            daemon.start();
        }
        catch (Throwable e)
        {
            String msg = "Exception encountered during startup.";
            logger.error(msg, e);

            // try to warn user on stdout too, if we haven't already detached
            System.out.println(msg);
            e.printStackTrace();

            System.exit(3);
        }
    }
}
