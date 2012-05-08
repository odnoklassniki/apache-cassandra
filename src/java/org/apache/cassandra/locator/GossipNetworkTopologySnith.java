/*
 * @(#) GossipNetworkTopologySnith.java
 * Created Apr 10, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndPointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndPointStateChangeSubscriber;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CopyOnWriteMap;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This snitch takes local node location information from cassandra.location system property of the form
 * DatacenterName:RackName
 * 
 * It learns about other endpoints location information using application states distributed by
 * other nodes in gossip.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class GossipNetworkTopologySnith extends AbstractNetworkTopologySnitch implements IEndPointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(GossipNetworkTopologySnith.class);

    private static final Map<InetAddress, String[]> endpointMap = new CopyOnWriteMap<InetAddress, String[]>();

    protected static volatile String[] defaultDCRack = null;

    /**
     * 
     */
    public GossipNetworkTopologySnith()
    {
        if (DatabaseDescriptor.getAllowedLocations()==null)
        {
            throw new RuntimeException("Allowed locations must be configured for GossipNetworkTopologySnitch");
        }
        
        putEndpoints(Collections.singletonMap(FBUtilities.getLocalAddress(), DatabaseDescriptor.getLocation().split(":")));
    }
    
    /**
     * Return the data center for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of data center
     */
    public String getDatacenter(InetAddress endpoint)
    {
        return getEndpointInfo(endpoint)[0];
    }

    /**
     * Return the rack for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of rack
     */
    public String getRack(InetAddress endpoint)
    {
        return getEndpointInfo(endpoint)[1];
    }

    /**
     * Get the raw information about an end point
     *
     * @param endpoint endpoint to process
     * @return a array of string with the first index being the data center and the second being the rack
     */
    public String[] getEndpointInfo(InetAddress endpoint)
    {
        String[] value = endpointMap.get(endpoint);
        if (value == null)
        {
            logger.debug("Could not find end point information for {}, will use default", endpoint);
            if ( defaultDCRack == null )
                throw new RuntimeException("Could not find topology info for "+endpoint+": It is not in cassandra-topology.properties and there is no default");
            
            return defaultDCRack;
        }
        return value;
    }

    /**
     * @param reloadedMap
     * @return
     */
    private String printTopology(Map<InetAddress, String[]> reloadedMap)
    {
        StringBuilder sb = new StringBuilder();
        
        for (Entry<InetAddress, String[]> a : reloadedMap.entrySet())
        {
            sb.append(a.getKey()+" => "+ Arrays.toString(a.getValue()));
        }
        
        return sb.toString();
    }

    Set<String> getConfiguredRacks()
    {
        TreeSet<String> racks = new TreeSet<String>();
        
        for (String info : DatabaseDescriptor.getAllowedLocations()) {
            racks.add(info.split(":")[1]);
        }
        
        if (defaultDCRack!=null)
            racks.add(defaultDCRack[1]);
        
        return racks;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.locator.AbstractEndpointSnitch#getLocalDatacenter()
     */
    @Override
    public String getLocalDatacenter()
    {
        String dc = DatabaseDescriptor.getLocation();

        return dc == null ? getEndpointInfo(FBUtilities.getLocalAddress())[0] : dc.split(":")[0];
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.locator.AbstractEndpointSnitch#getLocalRack()
     */
    @Override
    public String getLocalRack()
    {
        String dc = DatabaseDescriptor.getLocation();

        return dc == null ? getEndpointInfo(FBUtilities.getLocalAddress())[0] : dc.split(":")[1];
    }

    /**
     * @param endpointMap the endpointMap to set
     */
    public void putEndpoints(Map<InetAddress, String[]> endp)
    {
        endpointMap.putAll(endp);

        logger.info("set network topology {}", printTopology( endpointMap ));

    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.locator.AbstractEndpointSnitch#gossiperStarting()
     */
    @Override
    public void gossiperStarting()
    {
        Gossiper.instance.register(this);

        super.gossiperStarting();
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.gms.IEndPointStateChangeSubscriber#onJoin(java.net.InetAddress, org.apache.cassandra.gms.EndPointState)
     */
    @Override
    public void onJoin(InetAddress endpoint, EndPointState epState)
    {
        ApplicationState stateDC = epState.getApplicationState(APPSTATE_DC);
        ApplicationState stateRack = epState.getApplicationState(APPSTATE_RACK);
        
        String[] location = new String[2] ;
        
        if (stateDC!=null)
            location[0] = stateDC.getValue();
        if (stateRack!=null)
            location[1] = stateRack.getValue();
        
        
        if (location[0] == null || location[1] == null) {
            location[0] = getDatacenter(endpoint);
            location[1] = getRack(endpoint);
        }
        
        if (location[0] == null || location[1] == null) {
            logger.error("Cannot determine datacenter and rack location of "+endpoint+". On rack aware replication strategies secondary replicas will be broken for this endpoint");
            
            blockFromJoin(endpoint, epState);
            
            return;
        }
        
        if (DatabaseDescriptor.getAllowedLocations()!=null && !DatabaseDescriptor.getAllowedLocations().contains(location[0]+":"+location[1]) )
        {
            logger.error("Location of "+endpoint+"["+location[0]+":"+location[1]+"] is not in allowed locations list.");
            
            blockFromJoin(endpoint,epState);
            
            return;
        }
        
        putEndpoints(Collections.singletonMap(endpoint, location));
    }

    private void blockFromJoin(InetAddress endpoint, EndPointState epState)
    {
        logger.info("Blocking "+endpoint+" from joining ring");
        epState.getApplicationStateMap().put(StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_HIBERNATE+StorageService.Delimiter+"true"));
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.gms.IEndPointStateChangeSubscriber#onChange(java.net.InetAddress, java.lang.String, org.apache.cassandra.gms.ApplicationState)
     */
    @Override
    public void onChange(InetAddress endpoint, String stateName,
            ApplicationState state)
    {
        if (stateName.equals(APPSTATE_DC) || stateName.equals(APPSTATE_RACK))
            onJoin(endpoint, Gossiper.instance.getEndPointStateForEndPoint(endpoint));
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.gms.IEndPointStateChangeSubscriber#onAlive(java.net.InetAddress, org.apache.cassandra.gms.EndPointState)
     */
    @Override
    public void onAlive(InetAddress endpoint, EndPointState state)
    {
        onJoin(endpoint, state);
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.gms.IEndPointStateChangeSubscriber#onDead(java.net.InetAddress, org.apache.cassandra.gms.EndPointState)
     */
    @Override
    public void onDead(InetAddress endpoint, EndPointState state)
    {
        
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.gms.IEndPointStateChangeSubscriber#onRemove(java.net.InetAddress)
     */
    @Override
    public void onRemove(InetAddress endpoint)
    {
        // cannot remove info here - because node can be temporary in dead state
    }
}
