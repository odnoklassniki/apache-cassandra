/*
 * @(#) RackAwareOdklEvenStrategy.java
 * Created Feb 15, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.OdklDomainPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.StringToken;
import org.apache.cassandra.dht.Token;

/**
 * In addition to even (re)distribution of replicas across all cluster nodes, it ensures no replicas of the same key hit
 * the endpoint in the same rack.
 * 
 * It uses {@link IEndPointSnitch} to get information about location of nodes.
 * 
 * This stategy requires uniq racks number to match replication factor. 
 * 
 * It works by making subrings from ring for every uniq rack. Each replica will be placed including only nodes from
 * one individual subring. Replica #0 (AKA master) is placed to 1st subring, replice #2 to 2nd subring and so on.
 * 
 * For example, imagine cluster with RF=2 and ring
 * 
 * token endpoint rack
 * 0     127.0.0.0 RACK0
 * 1     127.0.0.1 RACK1
 * 2     127.0.0.2 RACK0
 * 3     127.0.0.3 RACK1
 * 
 * 2 rings will be extracted. 
 * 1st:
 * token endpoint rack
 * 0     127.0.0.0 RACK0
 * 2     127.0.0.2 RACK0
 * 
 * and 2nd
 * token endpoint rack
 * 1     127.0.0.1 RACK1
 * 3     127.0.0.3 RACK1
 * 
 * so when placing row with key = 0, first replica accprding to std cassandra ring algo will be placed to node
 * 127.0.0.0 and second replica will be placed to node 127.0.0.1 (because considering only 2nd subring we see 
 * min token == 1, with wrapping range (3,1], which includes rows with token == 0
 * 
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class RackAwareOdklEvenStrategy extends OdklEvenStrategy
{
    public RackAwareOdklEvenStrategy(TokenMetadata tokenMetadata, IEndPointSnitch snitch) throws ConfigurationException
    {
        super(tokenMetadata,snitch);
        
        if (! (snitch instanceof AbstractNetworkTopologySnitch) )
            throw new ConfigurationException("Invalid EndPoint snith configured for this replication strategy. You must select one with network topology information");
        
        if (snitch instanceof PropertyFileSnitch) {
            PropertyFileSnitch pfs = (PropertyFileSnitch) snitch;

            validate(pfs);
        }
    }

    void validate(PropertyFileSnitch pfs)
            throws ConfigurationException
    {
        Set<String> racks = pfs.getConfiguredRacks();

        logger_.info("RackAwareOdklEvenStrategy (re)configuring with the following known racks: "+racks);

        for (String table : DatabaseDescriptor.getNonSystemTables())
        {
            int rf = DatabaseDescriptor.getReplicationFactor(table);
            if (rf != racks.size())
            {
                throw new ConfigurationException("Number of unique racks in cassandra-topology.properties must match replication factor of "+table);
            }
        }
    }

    public ArrayList<InetAddress> calculateNaturalEndpoints(Token keyToken,
            TokenMetadata metadata, String table)
    {
        int replicas = DatabaseDescriptor.getReplicationFactor(table);
        ArrayList<InetAddress> endpoints = new ArrayList<InetAddress>(replicas);

        do
        {
            List<Token> tokens = getReplicaTokens(keyToken, metadata, endpoints.size());
            if (tokens.isEmpty())
                return endpoints;

            int domain = Integer.parseInt( keyToken.toString(), 16 ) & 0xFF;
            Token t = TokenMetadata.firstToken(tokens, keyToken);

            InetAddress endPoint = metadata.getEndPoint(t);

            endpoints.add(endPoint);
    
            domain = shuffle( domain );
            keyToken = odklPartitioner.toStringToken(domain);
            
        } while (endpoints.size() < replicas);

        return endpoints;
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.locator.OdklEvenStrategy#getReplicaTokens(org.apache.cassandra.dht.Token, org.apache.cassandra.locator.TokenMetadata, int)
     */
    @Override
    protected List<Token> getReplicaTokens(Token keyToken,
            TokenMetadata metadata, int replica)
    {
        // searching for the rack we should collect endpoint tokens for
        List<Token> sortedTokens = metadata.sortedTokens();
        String rack = null;
        
        if (snitch_ instanceof PropertyFileSnitch)
        {
            Iterator<String> racks = ((PropertyFileSnitch)snitch_).getConfiguredRacks().iterator();
            
            for (int i=0;i<replica && racks.hasNext();i++)
                racks.next();
            
            if (racks.hasNext())
                rack = racks.next();
            
        } else {
            Set<String> racks;
            racks = new HashSet<String>();
            for (Token t : sortedTokens)
            {
                rack = snitch_.getRack(metadata.getEndPoint(t));

                if (racks.size()>=replica)
                    break;

                racks.add(rack);
                rack = null;
            }
        }
        
        if (rack == null)
        {
            throw new IllegalStateException("Cannot find rack to store replica number "+replica+". This typically means error in configuration. Number of uniq racks is probably less than RF or you did not configured endpoint snith to prvide replica information");
        }
        
        ArrayList<Token> rc = new ArrayList<Token>(sortedTokens.size());
        for (Token t : sortedTokens)
        {
            if (snitch_.getRack(metadata.getEndPoint(t)).equals(rack))
                rc.add(t);
        }        
        
        return rc;
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.locator.AbstractReplicationStrategy#clearEndpointCache()
     */
    @Override
    public void clearEndpointCache()
    {
        try {
            if (snitch_ instanceof PropertyFileSnitch) {
                PropertyFileSnitch pfs = (PropertyFileSnitch) snitch_;

                validate(pfs);
            }
            
            super.clearEndpointCache();
            
        } catch (ConfigurationException e) {
            logger_.error("Cannot reconfigure: "+e);
        }
    }
    
/*    
    public static void main(String[] args)
    {
        try {
            TokenMetadata meta = new TokenMetadata();
            
            OdklDomainPartitioner pp = new OdklDomainPartitioner();
            Properties topology = new Properties();
            
            for (int i=0;i<255;i++)
            {
                meta.updateNormalToken( pp.toStringToken(i)  , InetAddress.getByName("127.0.0."+i) );
                
                topology.put("127.0.0."+i, "DC1:RAC"+i % 3);
            }
          
//          int e=0;
//          meta.updateNormalToken( new StringToken("00") , InetAddress.getByName("127.0.0."+e++) );
//          meta.updateNormalToken( new StringToken("15") , InetAddress.getByName("127.0.0."+e++) );
//          meta.updateNormalToken( new StringToken("2a") , InetAddress.getByName("127.0.0."+e++) );
//          meta.updateNormalToken( new StringToken("40") , InetAddress.getByName("127.0.0."+e++) );
//          meta.updateNormalToken( new StringToken("55") , InetAddress.getByName("127.0.0."+e++) );
//          meta.updateNormalToken( new StringToken("6a") , InetAddress.getByName("127.0.0."+e++) );
//          meta.updateNormalToken( new StringToken("80") , InetAddress.getByName("127.0.0."+e++) );
//          meta.updateNormalToken( new StringToken("95") , InetAddress.getByName("127.0.0."+e++) );
//          meta.updateNormalToken( new StringToken("aa") , InetAddress.getByName("127.0.0."+e++) );
//          meta.updateNormalToken( new StringToken("c0") , InetAddress.getByName("127.0.0."+e++) );
//          meta.updateNormalToken( new StringToken("d5") , InetAddress.getByName("127.0.0."+e++) );
//          meta.updateNormalToken( new StringToken("ea") , InetAddress.getByName("127.0.0."+e++) );
//            
//          for (int i=0;i<12;i++)
//          {
//              topology.put("127.0.0."+i, "DC1:RAC"+i % 3);
//          }
//            
            RackAwareOdklEvenStrategy o = new RackAwareOdklEvenStrategy(new TokenMetadata(), new PropertyFileSnitch(topology)) {
                void validate(PropertyFileSnitch pfs)
                        throws ConfigurationException
                {
                }
                
//                protected int shuffle(int domain)
//                {
//                    return domain;
//                }
            };
            
            TreeMap<InetAddress, AtomicInteger> cc = new TreeMap<InetAddress, AtomicInteger>(new Comparator<InetAddress>()
            {
                @Override
                public int compare(InetAddress o1, InetAddress o2)
                {
                    return new Integer(o1.getAddress()[3] & 0xFF).compareTo(new Integer(o2.getAddress()[3] & 0xFF));
                }
                
            });
            
            for (int i=0;i<256;i++)
            {
                List<InetAddress> endpoints = o.getNaturalEndpoints(pp.toStringToken(i), meta, null);
                
                System.out.println(i+" => "+Arrays.toString(endpoints.toArray()));
                
                for (InetAddress end : endpoints) {
                    AtomicInteger c = cc.get(end);
                    if (c==null)
                        cc.put(end,new AtomicInteger(1));
                    else
                        c.incrementAndGet();
                }
            }
            
            for (int i=0;i<256;i++)
            {
                List<InetAddress> endpoints = o.getNaturalEndpoints(pp.toStringToken(i), meta, null);
                
                System.out.println(i+" => "+Arrays.toString(endpoints.toArray()));
                
            }

            System.out.println("Per endpoint Counters:");
            
            for (java.util.Map.Entry<InetAddress, AtomicInteger> i : cc.entrySet()) {
                System.out.println(i.getKey().toString()+"="+i.getValue());
            }

            System.out.println("Range Addresses:");
            System.out.println(o.getRangeAddresses(meta, "aaa"));
            System.out.println("Address Ranges:");
            System.out.println(o.getAddressRanges(meta, "aaa"));
            
            System.out.println("Range");
            System.out.println(o.getRangeAddresses(meta, "aaa").get(new Range(new StringToken("00"), new StringToken("02"),pp)));
            
        } catch (Exception e) {
            e.printStackTrace();
        }
     
    }
    */
}