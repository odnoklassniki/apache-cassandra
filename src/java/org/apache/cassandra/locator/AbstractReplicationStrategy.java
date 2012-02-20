/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * This class contains a helper method that will be used by
 * all abstraction that implement the IReplicaPlacementStrategy
 * interface.
*/
public abstract class AbstractReplicationStrategy
{
    protected static final Logger logger_ = Logger.getLogger(AbstractReplicationStrategy.class);

    protected TokenMetadata tokenMetadata_;
    protected final IEndPointSnitch snitch_;

    AbstractReplicationStrategy(TokenMetadata tokenMetadata, IEndPointSnitch snitch)
    {
        tokenMetadata_ = tokenMetadata;
        snitch_ = snitch;
        
        tokenMetadata_.register(this);
    }

    private final Map<Pair<String, Token>, ArrayList<InetAddress>> cachedEndpoints = new NonBlockingHashMap<Pair<String, Token>, ArrayList<InetAddress>>();

    public ArrayList<InetAddress> getCachedEndpoints(String table, Token t)
    {
        return cachedEndpoints.get(new Pair<String, Token>(table,t));
    }

    public void cacheEndpoint(String table, Token t, ArrayList<InetAddress> addr)
    {
        cachedEndpoints.put(new Pair<String, Token>(table,t), addr);
    }

    public void clearEndpointCache()
    {
        logger_.debug("clearing cached endpoints");
        cachedEndpoints.clear();
    }

    /**
     * get the (possibly cached) endpoints that should store the given Token.
     * Note that while the endpoints are conceptually a Set (no duplicates will be included),
     * we return a List to avoid an extra allocation when sorting by proximity later
     * @param searchPosition the position the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public ArrayList<InetAddress> getNaturalEndpoints(Token token, TokenMetadata metadata, String table)
    {
        Token keyToken = TokenMetadata.firstToken(metadata.sortedTokens(), token);
        ArrayList<InetAddress> endpoints = getCachedEndpoints(table,keyToken);
        if (endpoints == null)
        {
            TokenMetadata tokenMetadataClone = metadata.cloneOnlyTokenMap();
            keyToken = TokenMetadata.firstToken(tokenMetadataClone.sortedTokens(), token);
            endpoints = new ArrayList<InetAddress>(calculateNaturalEndpoints(token, tokenMetadataClone, table));
            cacheEndpoint(table,keyToken, endpoints);
        }

        return new ArrayList<InetAddress>(endpoints);
    }

    /**
     * calculate the natural endpoints for the given token
     *
     * @see #getNaturalEndpoints(org.apache.cassandra.dht.RingPosition)
     *
     * @param searchToken the token the natural endpoints are requested for
     * @return a copy of the natural endpoints for the given token
     */
    public abstract ArrayList<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata, String table);
    
    public WriteResponseHandler getWriteResponseHandler(int blockFor, int endpointCount, ConsistencyLevel consistency_level, String table)
    {
        return new WriteResponseHandler(blockFor, endpointCount, table);
    }

    public ArrayList<InetAddress> getNaturalEndpoints(Token token, String table)
    {
        return getNaturalEndpoints(token, tokenMetadata_, table);
    }
    
    /**
     * returns multimap of {live destination: ultimate targets}, where if target is not the same
     * as the destination, it is a "hinted" write, and will need to be sent to
     * the ultimate target when it becomes alive again.
     */
    public Multimap<InetAddress, InetAddress> getHintedEndpoints(String table, Collection<InetAddress> targets)
    {
        Multimap<InetAddress, InetAddress> map = HashMultimap.create(targets.size(), 1);

        IEndPointSnitch endPointSnitch = DatabaseDescriptor.getEndPointSnitch(table);

        // first, add the live endpoints
        for (InetAddress ep : targets)
        {
            if (FailureDetector.instance.isAlive(ep))
                map.put(ep, ep);
        }

        // if everything was alive or we're not doing HH on this keyspace, stop with just the live nodes
        if (map.size() == targets.size() || !StorageProxy.isHintedHandoffEnabled())
            return map;

        // assign dead endpoints to be hinted to the closest live one, or to the local node
        // (since it is trivially the closest) if none are alive.  This way, the cost of doing
        // a hint is only adding the hint header, rather than doing a full extra write, if any
        // destination nodes are alive.
        //
        // we do a 2nd pass on targets instead of using temporary storage,
        // to optimize for the common case (everything was alive).
        InetAddress localAddress = FBUtilities.getLocalAddress();
        for (InetAddress ep : targets)
        {
            if (map.containsKey(ep))
                continue;

            InetAddress destination = map.isEmpty()
                                    ? localAddress
                                    : endPointSnitch.getSortedListByProximity(localAddress, map.keySet()).get(0);
            map.put(destination, ep);
        }

        return map;
    }

    /**
     * write endpoints may be different from read endpoints, because read endpoints only need care about the
     * "natural" nodes for a token, but write endpoints also need to account for nodes that are bootstrapping
     * into the ring, and write data there too so that they stay up to date during the bootstrap process.
     * Thus, this method may return more nodes than the Replication Factor.
     *
     * If possible, will return the same collection it was passed, for efficiency.
     *
     * Only ReplicationStrategy should care about this method (higher level users should only ask for Hinted).
     * todo: this method should be moved into TokenMetadata.
     */
    public Collection<InetAddress> getWriteEndpoints(Token token, String table, Collection<InetAddress> naturalEndpoints)
    {
        if (tokenMetadata_.getPendingRanges(table).isEmpty())
            return naturalEndpoints;

        List<InetAddress> endpoints = new ArrayList<InetAddress>(naturalEndpoints);

        for (Map.Entry<Range, Collection<InetAddress>> entry : tokenMetadata_.getPendingRanges(table).entrySet())
        {
            if (entry.getKey().contains(token))
            {
                endpoints.addAll(entry.getValue());
            }
        }

        return endpoints;
    }

    /*
     NOTE: this is pretty inefficient. also the inverse (getRangeAddresses) below.
     this is fine as long as we don't use this on any critical path.
     (fixing this would probably require merging tokenmetadata into replicationstrategy, so we could cache/invalidate cleanly.)
     */
    public ListMultimap<InetAddress, Range> getAddressRanges(TokenMetadata metadata, String table)
    {
        ListMultimap<InetAddress, Range> map = ArrayListMultimap.create();

        for (Token token : metadata.sortedTokens())
        {
            Range range = metadata.getPrimaryRangeFor(token);
            for (InetAddress ep : calculateNaturalEndpoints(token, metadata, table))
            {
                map.put(ep, range);
            }
        }

        return map;
    }

    public ListMultimap<Range, InetAddress> getRangeAddresses(TokenMetadata metadata, String table)
    {
        ListMultimap<Range, InetAddress> map = ArrayListMultimap.create();

        for (Token token : metadata.sortedTokens())
        {
            Range range = metadata.getPrimaryRangeFor(token);
            for (InetAddress ep : calculateNaturalEndpoints(token, metadata, table))
            {
                map.put(range, ep);
            }
        }

        return map;
    }

    public ListMultimap<InetAddress, Range> getAddressRanges(String table)
    {
        return getAddressRanges(tokenMetadata_, table);
    }

    public Collection<Range> getPendingAddressRanges(TokenMetadata metadata, Token pendingToken, InetAddress pendingAddress, String table)
    {
        TokenMetadata temp = metadata.cloneOnlyTokenMap();
        temp.updateNormalToken(pendingToken, pendingAddress);
        return getAddressRanges(temp, table).get(pendingAddress);
    }

}
