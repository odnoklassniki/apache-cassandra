/*
 * @(#) SystemArchitectureAspect.java
 * Created Dec 23, 2010 by oleg
 * (C) ONE, SIA
 */
package odkl.cassandra.stat;

import java.net.InetAddress;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.CompactionIterator;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

/**
 * Defines all logging pointcuts for cassandra to collect stats to one-log
 * 
 * NOTE:
 * To enable these aspects you should list em in META-INF/aop.xml <aspects> list
 * and run java with -javaagent:/path/to/aspectjweaver.jar 
 * 
 * @author Oleg Anastasyev<oa@one.lv>
 */
@Aspect
public abstract class SystemArchitectureAspect
{
    public static String OP_LOGGER_NAME = "stats.cassandra.server";
    public static String MSG_LOGGER_NAME = "msg.cassandra.server";
    
    @Pointcut("call( public org.apache.cassandra.utils.LatencyTracker.new() )")
    public void latencyTrackerPointcut() {}

    // local store statistic points
    @Pointcut("set( org.apache.cassandra.utils.LatencyTracker org.apache.cassandra.db.ColumnFamilyStore.read* )")
    public void readLatencyTrackerPointcut() {}
    @Pointcut("set( org.apache.cassandra.utils.LatencyTracker org.apache.cassandra.db.ColumnFamilyStore.write* )")
    public void writeLatencyTrackerPointcut() {}

    @Pointcut("execution( org.apache.cassandra.db.ColumnFamilyStore.new(String,..) )")
    public void cfsPointcut() {}

    @Pointcut("within( odkl.cassandra.stat.* )")
    public void withinStatPointcut() {}

    // proxy (coodinator node) statistic points
    @Pointcut("set( org.apache.cassandra.utils.LatencyTracker org.apache.cassandra.service.StorageProxy.range* )")
    public void proxyRangeLatencyTrackerPointcut() {}
    // how long node waited for write to be acked by all endpoints
    // to decide on hint write
    @Pointcut("set( org.apache.cassandra.utils.LatencyTracker org.apache.cassandra.service.StorageProxy.hint* )")
    public void proxyHintLatencyTrackerPointcut() {}

    // READ.ONE calls
    @Pointcut("execution( * org.apache.cassandra.service.StorageProxy.weak*(..) ) ")
    public void proxyWeakReadPointcut() {}
    // READ QUORUM and more calls
    @Pointcut("execution( * org.apache.cassandra.service.StorageProxy.strong*(..,org.apache.cassandra.thrift.ConsistencyLevel) ) && args(..,cl) ")
    public void proxyStrongReadPointcut(ConsistencyLevel cl) {}

    // WRITE.ONE calls
    @Pointcut("execution( * org.apache.cassandra.service.StorageProxy.mutate(..) ) ")
    public void proxyWriteOnePointcut() {}
    // WRITE.QUORUM and more calls
    @Pointcut("execution( * org.apache.cassandra.service.StorageProxy.mutateBlocking(..,org.apache.cassandra.thrift.ConsistencyLevel) ) && args(..,cl) ")
    public void proxyWriteQuorumPointcut(ConsistencyLevel cl) {}
    // RANGE calls
    @Pointcut("execution( * org.apache.cassandra.service.StorageProxy.getRangeSlice(..,org.apache.cassandra.thrift.ConsistencyLevel) ) && args(..,cl) ")
    public void proxyRangePointcut(ConsistencyLevel cl) {}
    // when read repair happens this method is called
    @Pointcut("execution( * org.apache.cassandra.service.StorageProxy.countReadRepair() )")
    public void proxyRRPointcut() {}

    // HH activity
    // store of hint at coordinator node
    @Pointcut("execution( * org.apache.cassandra.db.HintedHandOffManager+.storeHint(java.net.InetAddress,..) ) && args(endpoint,..)")
    public void hintStorePointcut(InetAddress endpoint) {}

    // hint delivery
    @Pointcut("( execution( * org.apache.cassandra.db.HintedHandOffManager+.sendMessage(java.net.InetAddress,..) ) " +
    		  "|| execution( * org.apache.cassandra.db.HintedHandOffManager+.deliverHint(java.net.InetAddress,..) ) ) " +
    		  "&& args(endpoint,..)")
    public void hintDeliveryPointcut(InetAddress endpoint) {}
 
    
    // start and stop of cassandra service. this is neccessary for statistics collection daemons starting and stopping to mixin their init and stop code
    @Pointcut("execution( * org.apache.cassandra.thrift.CassandraDaemon.setup(..) ) ")
    public void cassandraStart() {}
    @Pointcut("execution( * org.apache.cassandra.thrift.CassandraDaemon.stop(..) ) ")
    public void cassandraStop() {}
    
    /**
     * Called when any message just read from channel was dropped due to rpc timeout.
     * @param verb kind of message
     */
    @Pointcut("execution( * org.apache.cassandra.net.MessagingService.incrementDroppedMessages(org.apache.cassandra.service.StorageService.Verb) ) && args(verb)")
    public void droppedMessage(StorageService.Verb verb) {}
    
    @Pointcut("execution( * org.apache.cassandra.db.CompactionManager.CompactionExecutor.beginCompaction(org.apache.cassandra.db.ColumnFamilyStore,org.apache.cassandra.io.CompactionIterator) ) && args(cfs,ci)")
    public void compactionStartedPointcut(ColumnFamilyStore cfs, CompactionIterator ci) {}
    @Pointcut("execution( * org.apache.cassandra.db.CompactionManager.CompactionExecutor.afterExecute(..) )")
    public void compactionCompletedPointcut() {}
}
