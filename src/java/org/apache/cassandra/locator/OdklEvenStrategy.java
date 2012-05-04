/*
 * @(#) RackAwareEvenReplicationStrategy.java
 * Created Feb 15, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.OdklDomainPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.StringToken;
import org.apache.cassandra.dht.Token;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * Unlike other replica strategies, which replicate all data from one endpoint to single another endpoint, this
 * replica strategy redistributes keys from one primary natural endpoint to several endpoints. 
 * 
 * So, if some of endpoints is down additional load of serving client requests, writes, etc will hit all other
 * healthy endpoints in cluster, but not 1 (when RF=2) or 2 (when RF=3) selected nodes. This way impact of 
 * single endpoint failure is way smaller comparing to other replication strategies.
 * 
 * Bootstrap, leave operations will pull/push data from all cluster nodes at once, not overloading 
 * individual endpoints, which happen to have close token value in ring.
 * 
 * This strategy does not awares of rack information. 
 * 
 * @see RackAwareOdklEvenStrategy for rack aware implementation
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class OdklEvenStrategy extends
        AbstractReplicationStrategy
{
    protected OdklDomainPartitioner odklPartitioner;
    
    public OdklEvenStrategy(TokenMetadata tokenMetadata, IEndPointSnitch snitch) throws ConfigurationException
    {
        super(tokenMetadata, snitch);

        try {
            odklPartitioner = (OdklDomainPartitioner) DatabaseDescriptor.getPartitioner();
        } catch (ClassCastException e)
        {
            throw new ConfigurationException("Only OdklDomainPartitioner is supported by this replication strategy");
        }
        
//        odklPartitioner = new OdklDomainPartitioner();
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.locator.AbstractReplicationStrategy#getNaturalEndpoints(org.apache.cassandra.dht.Token, org.apache.cassandra.locator.TokenMetadata, java.lang.String)
     */
    @Override
    public ArrayList<InetAddress> getNaturalEndpoints(Token token,
            TokenMetadata metadata, String table)
    {
        // for odkl style we considering only odkl domain, which consists of first 2 chars of token
        Token keyToken = keyToken(token);
        ArrayList<InetAddress> endpoints = getCachedEndpoints(table,keyToken);
        if (endpoints == null)
        {
            TokenMetadata tokenMetadataClone = metadata.cloneOnlyTokenMap();
            endpoints = new ArrayList<InetAddress>(doCalculateEndpoints(keyToken, tokenMetadataClone, table));
            cacheEndpoint(table,keyToken, endpoints);
        }

        return new ArrayList<InetAddress>(endpoints);
    }

    private StringToken keyToken(Token token)
    {
        String tokenString = token.toString();
        
        if (tokenString.length()<=2)
            tokenString = prevDomain(tokenString);
        
        return new StringToken( tokenString.substring(0, 2)+"0" );
    }

    /**
     * @param tokenString
     * @return
     */
    private String prevDomain(String tokenString)
    {
        int domain = Integer.parseInt( tokenString, 16 );
        
        if (domain==0)
            domain=0xff;
        else
            domain--;
        
        return odklPartitioner.toStringToken(domain).toString();
    }

    /* (non-Javadoc)
     * @see org.apache.cassandra.locator.AbstractReplicationStrategy#calculateNaturalEndpoints(org.apache.cassandra.dht.Token, org.apache.cassandra.locator.TokenMetadata, java.lang.String)
     */
    @Override
    public final ArrayList<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata, String table)
    {
        return doCalculateEndpoints( keyToken(token) , metadata, table);
    }

    protected ArrayList<InetAddress> doCalculateEndpoints(Token keyToken,
            TokenMetadata metadata, String table)
    {
        int replicas = DatabaseDescriptor.getReplicationFactor(table);
        ArrayList<InetAddress> endpoints = new ArrayList<InetAddress>(replicas);

        do
        {
            List<Token> tokens = getReplicaTokens(keyToken, metadata, endpoints.size());
            if (tokens.isEmpty())
                return endpoints;

            String keyTokenString = keyToken.toString();
            int domain = Integer.parseInt( keyTokenString.substring(0,2), 16 ) & 0xFF;
            Iterator<Token> iter = TokenMetadata.ringIterator(tokens, keyToken, false);
            do
            {
                InetAddress endPoint = metadata.getEndPoint(iter.next());
                
                if (endpoints.contains(endPoint))
                    endpoints.add(metadata.getEndPoint(iter.next()));
                else
                    endpoints.add(endPoint);
                
            } while (endpoints.size() < replicas && domain == shuffle( domain ));

            domain = shuffle( domain );
            
            keyToken = odklPartitioner.toStringToken(domain,keyTokenString);
            
        } while (endpoints.size() < replicas);


        return endpoints;
    }

    protected List<Token> getReplicaTokens(Token keyToken, TokenMetadata metadata, int replica)
    {
        // in rack unaware strategy each replica has same allowed token list
        return metadata.sortedTokens();
    }
    
    protected int shuffle(int domain)
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
    

    /*
     * Overrriden here, because even distribution dont operate in ranges for sencondary replicas, but,
     * instead works on individual domains
    */
   public ListMultimap<InetAddress, Range> getAddressRanges(TokenMetadata metadata, String table)
   {
       ListMultimap<InetAddress, Range> map = ArrayListMultimap.create();

       for (int i=0;i<256;i++)
       {
           Range range = toRange(i);
           for (InetAddress ep : calculateNaturalEndpoints(range.right, metadata, table))
           {
               map.put(ep, range);
           }
       }

       return map;
   }

   public ListMultimap<Range, InetAddress> getRangeAddresses(TokenMetadata metadata, String table)
   {
       ListMultimap<Range, InetAddress> map = ArrayListMultimap.create();

       for (int i=0;i<256;i++)
       {
           Range range = toRange(i);
           for (InetAddress ep : calculateNaturalEndpoints(range.right, metadata, table))
           {
               map.put(range, ep);
           }
       }

       return map;
   }

   private Range toRange(int i)
   {
       StringToken token = odklPartitioner.toStringToken(i);
       Range range = new Range( token, odklPartitioner.toStringToken( i==255?0:i+1 ), odklPartitioner);
       return range;
   }

   /**
    * Overriden here to have more performant impl
    */
   public Collection<InetAddress> getWriteEndpoints(Token token, String table, Collection<InetAddress> naturalEndpoints)
   {
       if (tokenMetadata_.getPendingRanges(table).isEmpty())
           return naturalEndpoints;

       Collection<InetAddress> pending = tokenMetadata_.getPendingRanges(table).get(toRange( Integer.parseInt( keyToken(token).toString() ,16 ) ) );
       
       if (pending == null || pending.isEmpty())
           return naturalEndpoints;

       List<InetAddress> endpoints = new ArrayList<InetAddress>(naturalEndpoints.size() + pending.size() );
       
       endpoints.addAll(naturalEndpoints);
       endpoints.addAll(pending);
       
       return endpoints;
   }
/*
   public static void main(String[] args)
    {
        try {
            OdklEvenStrategy o = new OdklEvenStrategy(null, null) {
                protected java.util.List<Token> getReplicaTokens(Token keyToken, TokenMetadata metadata, int replica) 
                {
                    ArrayList<Token> tokens = new ArrayList<Token>(256);
                    
                    for (int i=0;i<256;i++)
                    {
                        tokens.add( odklPartitioner.toStringToken(i) );
                    }
                    
                    return tokens;
                };
            };
            
            TokenMetadata meta = new TokenMetadata();
            
            OdklDomainPartitioner pp = new OdklDomainPartitioner();
            
            for (int i=0;i<256;i++)
            {
                meta.updateNormalToken( pp.toStringToken(i)  , InetAddress.getByName("127.0.0."+i) );
            }
            
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
                List<InetAddress> endpoints = o.calculateNaturalEndpoints(pp.toStringToken(i), meta, null);
                
                System.out.println(i+" => "+Arrays.toString(endpoints.toArray()));
                
                for (InetAddress end : endpoints) {
                    AtomicInteger c = cc.get(end);
                    if (c==null)
                        cc.put(end,new AtomicInteger(1));
                    else
                        c.incrementAndGet();
                }
            }
            
            System.out.println("Per endpoint Counters:");
            
            for (Entry<InetAddress, AtomicInteger> i : cc.entrySet()) {
                if (i.getValue().get()!=3)
                {
                    System.out.println(i.getKey().toString()+"="+i.getValue());
                }
            }
            
            System.out.println("Address ranges:"+o.getAddressRanges(meta,null).asMap().size());
            for (Entry<InetAddress, Collection<Range>> e : o.getAddressRanges(meta,null).asMap().entrySet()) {
                System.out.println(e.getKey()+" => "+e.getValue());
            }
            
            System.out.println("Range addresses:"+o.getRangeAddresses(meta,null).asMap().size());
            for (Entry<Range, Collection<InetAddress>> e : new TreeMap<Range, Collection<InetAddress>>( o.getRangeAddresses(meta,null).asMap() ).entrySet()) {
                System.out.println(e.getKey()+" => "+e.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    */
}
