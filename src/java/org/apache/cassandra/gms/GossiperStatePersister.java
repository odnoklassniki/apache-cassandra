/*
 * @(#) GossiperStatePersister.java
 * Created 28.05.2013 by oleg
 * (C) Odnoklassniki.ru
 */
package org.apache.cassandra.gms;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;

/**
 * Persists gossiper state to system table, so it will be prepopulated on startup
 * 
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public class GossiperStatePersister implements IEndPointStateChangeSubscriber
{
    private static Logger logger_ = Logger.getLogger(Gossiper.class);

    /* (non-Javadoc)
     * @see org.apache.cassandra.gms.IEndPointStateChangeSubscriber#onJoin(java.net.InetAddress, org.apache.cassandra.gms.EndPointState)
     */
    @Override
    public void onJoin(InetAddress endpoint, EndPointState epState)
    {
        persistEndpointState(endpoint);
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.gms.IEndPointStateChangeSubscriber#onChange(java.net.InetAddress, java.lang.String, org.apache.cassandra.gms.ApplicationState)
     */
    @Override
    public void onChange(InetAddress endpoint, String stateName,
            ApplicationState state)
    {
        persistEndpointState(endpoint);
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.gms.IEndPointStateChangeSubscriber#onAlive(java.net.InetAddress, org.apache.cassandra.gms.EndPointState)
     */
    @Override
    public void onAlive(InetAddress endpoint, EndPointState state)
    {
        persistEndpointState(endpoint);
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.gms.IEndPointStateChangeSubscriber#onDead(java.net.InetAddress, org.apache.cassandra.gms.EndPointState)
     */
    @Override
    public void onDead(InetAddress endpoint, EndPointState state)
    {
        persistEndpointState(endpoint);
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.gms.IEndPointStateChangeSubscriber#onRemove(java.net.InetAddress)
     */
    @Override
    public void onRemove(InetAddress endpoint)
    {
        SystemTable.removeEndpointState(endpoint);
        if (logger_.isDebugEnabled())
            logger_.debug("Removed endpoint "+endpoint);
    }

    private void persistEndpointState(InetAddress ep) {
        
        EndPointState endpointState = Gossiper.instance.getEndPointStateForEndPoint(ep);
        
        int generation = endpointState.getHeartBeatState().getGeneration(); 
        int version = endpointState.getHeartBeatState().getHeartBeatVersion();

        assert !ep.equals(FBUtilities.getLocalAddress());
        
        DataOutputBuffer buffer = new DataOutputBuffer();
        
        if (logger_.isDebugEnabled())
            logger_.debug("Persisting gossip state to system table for "+endpointState.toString(ep));
        
        try {
            EndPointState.serializer().serialize(endpointState, buffer);
            SystemTable.updateEndpointState(ep, buffer.getData(), generation, version);
        } catch (IOException e) {
            logger_.error("Cannot serialize endpointstate for "+endpointState.toString(ep)+". Will not persist.",e);
        }
        
    }

}
