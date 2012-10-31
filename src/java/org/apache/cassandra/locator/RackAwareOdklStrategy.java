/*
 * @(#) RackAwareOdklStrategy.java
 * Created May 9, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.locator;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.OdklDomainPartitioner;
import org.apache.cassandra.dht.StringToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

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
 * for the 1st subring domain value will be used as is.
 * for the 2nd, it will be shuffled according to {@link #shuffle(int)} to 0x55
 * 
 * so when placing row with key = 0, first replica accprding to std cassandra ring algo will be placed to node
 * 127.0.0.0 and second replica will be placed to node 127.0.0.1 (because considering only 2nd subring we see 
 * min token == 1, with wrapping range (3,1], which includes rows with token == 0x55
 * 
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class RackAwareOdklStrategy extends OdklEvenStrategy
{
    public RackAwareOdklStrategy(TokenMetadata tokenMetadata, IEndPointSnitch snitch) throws ConfigurationException
    {
        super(tokenMetadata,snitch);
        
        if (! (snitch instanceof AbstractNetworkTopologySnitch) )
            throw new ConfigurationException("Invalid EndPoint snith configured for this replication strategy. You must select one with network topology information");
        
        if (snitch instanceof GossipNetworkTopologySnith) {
            GossipNetworkTopologySnith pfs = (GossipNetworkTopologySnith) snitch;

            validate(pfs);
        }
    }

    void validate(GossipNetworkTopologySnith pfs)
            throws ConfigurationException
    {
        Set<String> racks = pfs.getConfiguredRacks();

        logger_.info("RackAwareOdklStrategy (re)configuring with the following known racks: "+racks);

        for (String table : DatabaseDescriptor.getNonSystemTables())
        {
            int rf = DatabaseDescriptor.getReplicationFactor(table);
            if (rf != racks.size())
            {
                throw new ConfigurationException("Number of unique racks in AllowedLocations must match replication factor of "+table);
            }
        }
    }

    protected ArrayList<InetAddress> doCalculateEndpoints(Token keyToken,
            TokenMetadata metadata, String table)
    {
        int replicas = DatabaseDescriptor.getReplicationFactor(table);
        ArrayList<InetAddress> endpoints = new ArrayList<InetAddress>(replicas);

        List<Token> tokens = metadata.sortedTokens();
        if (tokens.isEmpty())
            return endpoints;
        
        String[] racks = ringRacks(metadata, tokens).toArray(new String[replicas]);
        
        StringToken[] rackDomain = new StringToken[replicas];
        
        int domain = Integer.parseInt( keyToken.toString().substring(0,2), 16 ) & 0xFF;

        // starting from different index each time to make calls even distributed
        // across replicas in each rack
        int rackIndex = domain % replicas; 
        
        for (int i=0;i<racks.length;i++) {
                rackDomain[i] = odklPartitioner.toStringToken(domain,keyToken.toString()) ;
            domain = shuffle( domain );
        }
        
        do
        {
            String rack = racks[rackIndex];
            tokens = getReplicaTokens(metadata,rack);
            
            assert tokens.size()>0 : "No nodes in ring found for rack "+rack+". This replication strategy requires you to have nodes for all racks you configured in AllowedLocations";
            
            keyToken = rackDomain[rackIndex];
    
            Token t = TokenMetadata.firstToken(tokens, keyToken);

            InetAddress endPoint = metadata.getEndPoint(t);

            endpoints.add(endPoint);
            
            rackIndex = (rackIndex+1) % replicas;
            
        } while (endpoints.size() < replicas);

        return endpoints;
    }
    
    public NavigableSet<String> ringRacks(TokenMetadata metadata, List<Token> sortedTokens)
    {
        if (snitch_ instanceof GossipNetworkTopologySnith)
        {
            return ((GossipNetworkTopologySnith)snitch_).getConfiguredRacks();
            
        } else {
            TreeSet<String> racks = new TreeSet<String>();
            for (Token t : sortedTokens)
            {
                racks.add(snitch_.getRack(metadata.getEndPoint(t)));
            }
            
            return racks;
        }
        
    }
    
    public String getRack(InetAddress endp)
    {
        return snitch_.getRack(endp);
    }

    protected List<Token> getReplicaTokens(TokenMetadata metadata, String rack)
    {
        
        List<Token> sortedTokens = metadata.sortedTokens();
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
            if (snitch_ instanceof GossipNetworkTopologySnith) {
                GossipNetworkTopologySnith pfs = (GossipNetworkTopologySnith) snitch_;

                validate(pfs);
            }
            
            super.clearEndpointCache();
            
        } catch (ConfigurationException e) {
            logger_.error("Cannot reconfigure: "+e);
        }
    }

    public static void main(String[] args)
    {
        try {
            TokenMetadata meta = new TokenMetadata();
            
            OdklDomainPartitioner pp = new OdklDomainPartitioner();
            Properties topology = new Properties();
            
//            for (int i=0;i<255;i++)
//            {
//                meta.updateNormalToken( pp.toStringToken(i)  , InetAddress.getByName("127.0.0."+i) );
//                
//                topology.put("127.0.0."+i, "DC1:RAC"+i % 3);
//            }
          
            int e=0;
//            meta.updateNormalToken( new StringToken("00") , InetAddress.getByName("192.168.36.86") );
//            meta.updateNormalToken( new StringToken("2a") , InetAddress.getByName("192.168.36.87") );
//            meta.updateNormalToken( new StringToken("55") , InetAddress.getByName("192.168.36.88") );
//            meta.updateNormalToken( new StringToken("80") , InetAddress.getByName("192.168.36.89") );
//            meta.updateNormalToken( new StringToken("aa") , InetAddress.getByName("192.168.10.247") );
//            meta.updateNormalToken( new StringToken("d5") , InetAddress.getByName("192.168.10.248") );
//            
//            
//
//            topology.put("192.168.36.86","DL:RACK1");
//            topology.put("192.168.36.87","DL:RACK2");
//            topology.put("192.168.36.88","DL:RACK3");
//            topology.put("192.168.36.89","DL:RACK1");
//
//            topology.put("192.168.10.247","DL:RACK2");
//            topology.put("192.168.10.248","DL:RACK3");

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
            
//          meta.updateNormalToken( new StringToken("00") , InetAddress.getByName("192.168.38.112") );
//          meta.updateNormalToken( new StringToken("15") , InetAddress.getByName("192.168.11.244") );
//          meta.updateNormalToken( new StringToken("2a") , InetAddress.getByName("192.168.48.169") );
//          meta.updateNormalToken( new StringToken("40") , InetAddress.getByName("192.168.38.113") );
//          meta.updateNormalToken( new StringToken("55") , InetAddress.getByName("192.168.11.245") );
//          meta.updateNormalToken( new StringToken("6a") , InetAddress.getByName("192.168.48.170") );
//          meta.updateNormalToken( new StringToken("80") , InetAddress.getByName("192.168.38.114") );
//          meta.updateNormalToken( new StringToken("95") , InetAddress.getByName("192.168.11.246") );
//          meta.updateNormalToken( new StringToken("aa") , InetAddress.getByName("192.168.48.171") );
//          meta.updateNormalToken( new StringToken("c0") , InetAddress.getByName("192.168.38.115") );
//          meta.updateNormalToken( new StringToken("d5") , InetAddress.getByName("192.168.11.247") );
//          meta.updateNormalToken( new StringToken("ea") , InetAddress.getByName("192.168.48.172") );
//
          topology.put("192.168.38.112","DL:DL"    );        
          topology.put("192.168.11.244","M100:M100");        
          topology.put("192.168.48.169","KV:KV"    );        
          topology.put("192.168.38.113","DL:DL"    );        
          topology.put("192.168.11.245","M100:M100");        
          topology.put("192.168.48.170","KV:KV"    );        
          topology.put("192.168.38.114","DL:DL"    );        
          topology.put("192.168.11.246","M100:M100");        
          topology.put("192.168.48.171","KV:KV"     );       
          topology.put("192.168.38.115","DL:DL"    );        
          topology.put("192.168.11.247","M100:M100");        
          topology.put("192.168.48.172","KV:KV"    );        
                                                   
          meta.updateNormalToken( new StringToken("0a") , InetAddress.getByName("192.168.49.135") );
          meta.updateNormalToken( new StringToken("20") , InetAddress.getByName("192.168.38.227") );
          meta.updateNormalToken( new StringToken("35") , InetAddress.getByName("192.168.12.76") ); 
          meta.updateNormalToken( new StringToken("4a") , InetAddress.getByName("192.168.49.136") );
          meta.updateNormalToken( new StringToken("60") , InetAddress.getByName("192.168.38.228") );
          meta.updateNormalToken( new StringToken("75") , InetAddress.getByName("192.168.12.77") ); 
          meta.updateNormalToken( new StringToken("8a") , InetAddress.getByName("192.168.49.137") );
          meta.updateNormalToken( new StringToken("a0") , InetAddress.getByName("192.168.38.229") );
          meta.updateNormalToken( new StringToken("b5") , InetAddress.getByName("192.168.12.78") ); 
          meta.updateNormalToken( new StringToken("ca") , InetAddress.getByName("192.168.49.138") );
          meta.updateNormalToken( new StringToken("e0") , InetAddress.getByName("192.168.38.230") );
          meta.updateNormalToken( new StringToken("f5") , InetAddress.getByName("192.168.12.79") ); 
                                                                                                
          topology.put("192.168.49.135","KV:KV"     );
          topology.put("192.168.38.227","DL:DL"     );
          topology.put("192.168.12.76","M100:M100" );
          topology.put("192.168.49.136","KV:KV"     );
          topology.put("192.168.38.228","DL:DL"     );
          topology.put("192.168.12.77","M100:M100" );
          topology.put("192.168.49.137","KV:KV"     );
          topology.put("192.168.38.229","DL:DL"     );
          topology.put("192.168.12.78","M100:M100" );
          topology.put("192.168.49.138","KV:KV"     );
          topology.put("192.168.38.230","DL:DL"     );
          topology.put("192.168.12.79","M100:M100" );

//          for (int i=0;i<12;i++)
//          {
//              topology.put("127.0.0."+i, "DC1:RAC"+i % 3);
//          }
            
            RackAwareOdklStrategy o = new RackAwareOdklStrategy(new TokenMetadata(), new PropertyFileSnitch(topology)) {
                void validate(GossipNetworkTopologySnith pfs)
                        throws ConfigurationException
                {
                }
                
//                protected int shuffle(int domain)
//                {
//                    return super.shuffle( super.shuffle(domain) );
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
            
            int startd=0, endd=256;
            
            Multimap<InetAddress, String> endpDomains = ArrayListMultimap.create();
            
            for (int i=startd;i<endd;i++)
            {
                List<InetAddress> endpoints = o.getNaturalEndpoints(pp.toStringToken(i,"000"), meta, "Likes");
                
                System.out.println( Integer.toHexString(i)+" => "+Arrays.toString(endpoints.toArray()));
                
                for (InetAddress end : endpoints) {
                    AtomicInteger c = cc.get(end);
                    if (c==null)
                        cc.put(end,new AtomicInteger(1));
                    else
                        c.incrementAndGet();
                    
                    endpDomains.put(end, Integer.toHexString(i));
                }
            }
            
            System.out.println("Per endpoint Counters:");
            
            for (InetAddress i : endpDomains.asMap().keySet()) {
                System.out.println(i.toString()+"="+endpDomains.get(i).size()+"   "+endpDomains.get(i));
            }

            
            for (InetAddress i : endpDomains.asMap().keySet()) {
                generateCopyScript(i,endpDomains.get(i),(String)topology.get(i.getHostAddress()));
                generateCheckScript(i,endpDomains.get(i),(String)topology.get(i.getHostAddress()));
            }

            String[][] bootstraps =
                {
//                    {"d5","192.168.10.248"},
//                    {"80","192.168.36.89"},
//                    {"2a","192.168.36.87"},
                    
//                    {"0a","192.168.49.135"},
//                    {"4a","192.168.49.136"},
//                    {"8a","192.168.49.137"},
//                    {"ca","192.168.49.138"},
//                    
//                    {"f5","192.168.12.79"},
//                    {"b5","192.168.12.78"},
//                    {"75","192.168.12.77"},
//                    {"35","192.168.12.76"},
//                    
//                    {"e0","192.168.38.230"},
//                    {"a0","192.168.38.229"},
//                    {"60","192.168.38.228"},
//                    {"20","192.168.38.227"},
                    
                  {"00","192.168.38.112"},
                  {"15","192.168.11.244"},
                  {"2a","192.168.48.169"},
                  {"40","192.168.38.113"},
                  {"55","192.168.11.245"},
                  {"6a","192.168.48.170"},
                  {"80","192.168.38.114"},
                  {"95","192.168.11.246"},
                  {"aa","192.168.48.171"},
                  {"c0","192.168.38.115"},
                  {"d5","192.168.11.247"},
                  {"ea","192.168.48.172"},

                };
            
            TokenMetadata btm = meta;
            for (String[] task : bootstraps) {
                btm = testBootstrap(pp, o, btm, task[0], task[1]);
            }

            for (Object failed : topology.keySet())
            {
                System.out.println("Calls when primary fail:"+failed);
                cc.clear();
                for (int i=0;i<256;i++)
                {
                    InetAddress[] endpoints = o.getNaturalEndpoints(pp.toStringToken(i,"000"), meta, "Likes").toArray(new InetAddress[3]);

                    if (endpoints[0].getHostAddress().equals(failed))
                        endpoints[0]=null;
                    else
                        continue;

                    InetAddress end=testChooseEndpoint(i, endpoints, 0);
                    
                    if (end.equals(testChooseEndpoint(i, endpoints, 1)))
                        System.out.println("Dup choosen: "+i+"=>"+end);
                    
                    if (topology.get(failed).equals(topology.get(end.getHostAddress()) ) )
                        continue; // same rack

                    AtomicInteger c = cc.get(end);
                    if (c==null)
                        cc.put(end,new AtomicInteger(1));
                    else
                        c.incrementAndGet();

                }
                for (java.util.Map.Entry<InetAddress, AtomicInteger> i : cc.entrySet()) {
                    System.out.println(i.getKey().toString()+"="+i.getValue());
                }
            }

            for (Object failed : new HashSet( topology.values() ))
            {
                System.out.println("Calls when rack fail:"+failed);
                cc.clear();
                for (int i=0;i<256;i++)
                {
                    InetAddress[] endpoints = o.getNaturalEndpoints(pp.toStringToken(i,"000"), meta, "Likes").toArray(new InetAddress[3]);

                    if ( topology.get( endpoints[0].getHostAddress() ).equals(failed))
                        endpoints[0]=null;
                    else
                        continue;
                    
                    InetAddress[] copyOf = Arrays.copyOf(endpoints,3);
                    
//                    copyOf[1] = testChooseEndpoint(i, endpoints, 0);
//                    copyOf[2] = testChooseEndpoint(i, endpoints, 1);

                    InetAddress end=testChooseEndpoint(i, copyOf, 0);
                    
                    if (end.equals(testChooseEndpoint(i, copyOf, 1)))
                        System.out.println("Dup choosen: "+i+"=>"+end);
                    
                    AtomicInteger c = cc.get(end);
                    if (c==null)
                        cc.put(end,new AtomicInteger(1));
                    else
                        c.incrementAndGet();

                }
                for (java.util.Map.Entry<InetAddress, AtomicInteger> i : cc.entrySet()) {
                    System.out.println(i.getKey().toString()+"="+i.getValue());
                }
            }
            System.out.println("Calls when 2 fail:");
            cc.clear();
            for (int i=startd;i<endd;i++)
            {
                InetAddress[] endpoints = o.getNaturalEndpoints(pp.toStringToken(i,"000"), meta, "Likes").toArray(new InetAddress[3]);
                
                endpoints[0]=null;
                endpoints[1]=null;
                
                InetAddress end=testChooseEndpoint(i, endpoints, 0);

                AtomicInteger c = cc.get(end);
                if (c==null)
                    cc.put(end,new AtomicInteger(1));
                else
                    c.incrementAndGet();
                
            }

            for (java.util.Map.Entry<InetAddress, AtomicInteger> i : cc.entrySet()) {
                System.out.println(i.getKey().toString()+"="+i.getValue());
            }
            
//            System.out.println("Address ranges:"+o.getAddressRanges(meta,null).asMap().size());
//            for (Entry<InetAddress, Collection<Range>> en : o.getAddressRanges(meta,null).asMap().entrySet()) {
//                System.out.println(en.getKey()+" => "+en.getValue());
//            }
//            
//            System.out.println("Range addresses:"+o.getRangeAddresses(meta,null).asMap().size());
//            for (Entry<Range, Collection<InetAddress>> en : new TreeMap<Range, Collection<InetAddress>>( o.getRangeAddresses(meta,null).asMap() ).entrySet()) {
//                System.out.println(en.getKey()+" => "+en.getValue());
//
//                StringToken stringToken = new StringToken( en.getKey().left.toString()+"01091" );
//                Set<InetAddress> s1= new HashSet<InetAddress>(o.calculateNaturalEndpoints( stringToken, meta, null)), s2=new HashSet<InetAddress>(en.getValue());
//                if (!s1.equals(s2))
//                    System.out.println("OILOLO: "+s1+" != "+s2+" "+en.getKey().contains(stringToken));
//            }
//
        } catch (Exception e) {
            e.printStackTrace();
        }
     
    }
    
    /**
     * @param i
     * @param collection
     * @param object
     */
    private static void generateCopyScript(InetAddress i,
            Collection<String> domains, String rack)
    {
        try {
            PrintWriter script = new PrintWriter( new FileOutputStream("/tmp/ram/"+rack.split(":")[0]+"-"+i.getHostAddress()+".sh",true) );
            
            script.format("echo Copying data to %s... \n", i.getHostAddress());
            
            int c=1;
            
            for (String domain : domains) {
                if (domain.length()<2)
                    domain="0"+domain;
                
                script.format("echo ... domain %s, which is %d of %d\n", domain,c++,domains.size());
                script.format("scp -B -q -c arcfour /mnt/db/Likes/*_%s* %s:/mnt/db/Likes/\n\n", domain,i.getHostAddress());
            }
            
            script.format("echo Finished with %s... \n", i.getHostAddress());
            script.close();
            
            script = new PrintWriter( new FileOutputStream("/tmp/ram/"+rack.split(":")[0]+".sh",true) );
            script.format("./%s-%s.sh &\n",rack.split(":")[0],i.getHostAddress());
            script.format("scp -B -q -c arcfour check-%s.sh %s:/mnt/\n\n",i.getHostAddress(),i.getHostAddress());
            
            script.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param i
     * @param collection
     * @param object
     */
    private static void generateCheckScript(InetAddress i,
            Collection<String> domains, String rack)
    {
        try {
            PrintWriter script = new PrintWriter( new FileOutputStream("/tmp/ram/check-"+i.getHostAddress()+".sh",true) );
            
            
            int c=1;
            
            for (String domain : domains) {
                if (domain.length()<2)
                    domain="0"+domain;
                
                script.format("echo 'domain %s check (must divide to 3)': `ls -1 /mnt/db/Likes/*_%s* | grep -v Compacted | wc | awk '{print $1}'`\n", domain, domain, domain);
            }
            
            script.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static InetAddress testChooseEndpoint(int partition,InetAddress[] endpoints,int tryCount)
    {
        int cycle = 0;
        
        InetAddress endpoint=null;
        
        while (endpoint == null && ++cycle<endpoints.length)
        {
            int index = ( cycle + tryCount + sh( sh( partition ) ) ) % (endpoints.length-1);
            
            endpoint = endpoints[1+index];
        }
        
        return endpoint;
        
    }

    private static int sh(int domain)
    {
        // these special bit patterns need special cure
        switch (domain) 
        { 
            case 0: return 0x55;
            case 0x55: return 0xFF;
            case 0xAA: return 0;
            case 0xFF: return 0xAA;
        }
        
        // others work good with the following
        return  ( (domain >> 1) | (domain & 1) << 7 );
    }
    
    private static TokenMetadata testBootstrap(OdklDomainPartitioner pp, RackAwareOdklStrategy o, TokenMetadata meta, String token, String endpoint) throws Exception
    {
        Multimap<InetAddress, String> beforeB = domainMM(pp, o, meta);

        TokenMetadata newMeta = meta.cloneOnlyTokenMap();
        
        newMeta.updateNormalToken(new StringToken(token), InetAddress.getByName(endpoint));
        
        Multimap<InetAddress, String> afterB = domainMM(pp, o, newMeta);
        
        System.out.println("After bootstrap of "+endpoint);
        
        for (InetAddress i : afterB.asMap().keySet()) {
            System.out.println(i.toString()+"="+afterB.get(i).size()+"   "+afterB.get(i));
        }
        
        // now validating 
        for (InetAddress endp : beforeB.keySet()) 
        {
            Set<String> beforeDomains = new TreeSet<String>(beforeB.get(endp));
            Set<String> afterDomains = new TreeSet<String>(afterB.get(endp));

            // after bootstrap on new node no domains must be added to old nodes
            afterDomains.removeAll(beforeDomains);
            if (afterDomains.size()>0)
            {
                System.out.println("ERROR WHEN ADDING "+endpoint+" on "+endp+" added domains "+afterDomains);
            }
        }
        
        return newMeta;
    }

    private static Multimap<InetAddress, String> domainMM(OdklDomainPartitioner pp, RackAwareOdklStrategy o, TokenMetadata meta)
    {
        Multimap<InetAddress, String> endpDomains = ArrayListMultimap.create();
        
        o.clearEndpointCache();
        
        int startd=0, endd=256;
        
        for (int i=startd;i<endd;i++)
        {
            List<InetAddress> endpoints = o.getNaturalEndpoints(pp.toStringToken(i,"000"), meta, "Likes");
            
            for (InetAddress end : endpoints) {
                endpDomains.put(end, Integer.toHexString(i));
            }
        }

        return endpDomains;
    }
}
