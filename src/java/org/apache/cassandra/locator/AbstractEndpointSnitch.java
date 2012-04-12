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
import java.util.*;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public abstract class AbstractEndpointSnitch implements IEndPointSnitch
{
    /**
     * names of app states used in gossip to advertise
     * location of an endpoint
     */
    protected static final String APPSTATE_DC="DC";
    protected static final String APPSTATE_RACK="RACK";
    protected static final String APPSTATE_HOSTNAME="HOST-NAME";
    
    public abstract int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2);

    /**
     * Sorts the <tt>Collection</tt> of node addresses by proximity to the given address
     * @param address the address to sort by proximity to
     * @param unsortedAddress the nodes to sort
     * @return a new sorted <tt>List</tt>
     */
    public List<InetAddress> getSortedListByProximity(InetAddress address, Collection<InetAddress> unsortedAddress)
    {
        List<InetAddress> preferred = new ArrayList<InetAddress>(unsortedAddress);
        sortByProximity(address, preferred);
        return preferred;
    }

    /**
     * Sorts the <tt>List</tt> of node addresses, in-place, by proximity to the given address
     * @param address the address to sort the proximity by
     * @param addresses the nodes to sort
     * @return 
     */
    public List<InetAddress> sortByProximity(final InetAddress address, List<InetAddress> addresses)
    {
        Collections.sort(addresses, new Comparator<InetAddress>()
        {
            public int compare(InetAddress a1, InetAddress a2)
            {
                return compareEndpoints(address, a1, a2);
            }
        });
        
        return addresses;
    }

    public void gossiperStarting()
    {
        InetAddress localAddress = FBUtilities.getLocalAddress();
        
        Gossiper.instance.addLocalApplicationState(APPSTATE_HOSTNAME, new ApplicationState(FBUtilities.getLocalName()));
        Gossiper.instance.addLocalApplicationState(APPSTATE_DC, new ApplicationState( getDatacenter(localAddress)));
        Gossiper.instance.addLocalApplicationState(APPSTATE_RACK, new ApplicationState( getRack(localAddress)));
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.locator.IEndPointSnitch#getRack(java.net.InetAddress)
     */
    @Override
    public String getRack(InetAddress endpoint)
    {
        if (FBUtilities.getLocalAddress().equals(endpoint))
            return getLocalRack();
        
        ApplicationState applicationState = Gossiper.instance.getEndPointStateForEndPoint(endpoint).getApplicationState(APPSTATE_RACK);
        if (applicationState==null)
            return null; // no information about rack in gossip. old endpoint ?
        
        return applicationState.getValue();
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.locator.IEndPointSnitch#getDatacenter(java.net.InetAddress)
     */
    @Override
    public String getDatacenter(InetAddress endpoint)
    {
        if (FBUtilities.getLocalAddress().equals(endpoint))
            return getLocalDatacenter();
        
        ApplicationState applicationState = Gossiper.instance.getEndPointStateForEndPoint(endpoint).getApplicationState(APPSTATE_DC);
        if (applicationState==null)
            return null; // no information about datacenter in gossip. old endpoint ?
        
        return applicationState.getValue();
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.locator.IEndPointSnitch#getEndpointName(java.net.InetAddress)
     */
    @Override
    public String getEndpointName(InetAddress endpoint)
    {
        if (FBUtilities.getLocalAddress().equals(endpoint))
            return FBUtilities.getLocalName();
        
        ApplicationState applicationState = Gossiper.instance.getEndPointStateForEndPoint(endpoint).getApplicationState(APPSTATE_HOSTNAME);
        if (applicationState==null)
            return endpoint.getHostName(); // no information about datacenter in gossip. old endpoint ?
        
        return applicationState.getValue();
    }
    
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.locator.IEndPointSnitch#isOnSameRack(java.net.InetAddress, java.net.InetAddress)
     */
    @Override
    public boolean isOnSameRack(InetAddress a1, InetAddress a2)
    {
        return getRack(a1).equals(getRack(a2));
    }
    
    public boolean isInSameDataCenter(InetAddress a1, InetAddress a2) 
    {
        return getDatacenter(a1).equals(getDatacenter(a2));
    };
    
    
}
