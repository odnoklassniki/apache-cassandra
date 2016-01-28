/*
 * @(#) OdklDomainPartitioner.java
 * Created 21.06.2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;

/**
 * This partitioner expects to find keys as long numbers in form of hex strings, in which last 2 chars represent odnoklassniki domain byte.
 * Say, you have key 168efd9e, so partitioning will performed by last byte of this string, which is 0x9e.
 * 
 * As an internal representation it transforms key to token by placing last 2 chars first in {@link StringToken}
 * 
 * So token of id=168efd9e will be 9e168eafd.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class OdklDomainPartitioner extends OrderPreservingPartitioner
{
    
    public static final StringToken MINIMUM = new StringToken("00");
    
    private static final Logger logger = Logger.getLogger(OdklDomainPartitioner.class);

    /* (non-Javadoc)
     * @see org.apache.cassandra.dht.OrderPreservingPartitioner#getMinimumToken()
     */
    @Override
    public StringToken getMinimumToken()
    {
        return MINIMUM;
    }
    
    private boolean isHexDigit(char c)
    {
        return Character.isDigit(c) || (c>='a' && c<='f');
    }
    
    protected StringToken toStringToken(String key)
    {
        key=key.toLowerCase();
        
        assert key.length()>0 : "Size of key is minimum single digit to partition odnoklassniki style";
        
        if (key.length()<2) {
            if (isHexDigit(key.charAt(0)))
                return new StringToken('0'+key);

            if (logger.isDebugEnabled())
                logger.debug("Last 2 chars of key must be hex digits, but in "+key+" they're not. This is ok for system table. reverting to OrderedPartitioner");
            
            return new StringToken(key);
        }
        
        if (key.length()==2 && isHexDigit(key.charAt(0)) && isHexDigit(key.charAt(1)))
        {
            return new StringToken(key);
        }
        
        StringBuilder token = new StringBuilder(key.length()).append(key.substring(key.length()-2));
        if ( !isHexDigit(token.charAt(0)) || !isHexDigit(token.charAt(1)) )
        {
            if (logger.isDebugEnabled())
                logger.debug("Last 2 chars of key must be hex digits, but in "+key+" they're not. This is ok for system table. reverting to OrderedPartitioner");
            return new StringToken(key);
        }
        
        token.append(key,0,key.length()-2);
        
        return new StringToken(token.toString());
    }
    
    public DecoratedKey<StringToken> decorateKey(String key)
    {
        return new DecoratedKey<StringToken>(toStringToken(key), key);
    }
    
    public DecoratedKey<StringToken> convertFromDiskFormat(String key)
    {
        return new DecoratedKey<StringToken>(toStringToken(key), key);
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.dht.OrderPreservingPartitioner#getRandomToken()
     */
    @Override
    public StringToken getRandomToken()
    {
        byte[] rnd = new byte[1];
        new Random().nextBytes(rnd);
        
        return new StringToken(FBUtilities.bytesToHex(rnd));
    }
    
    /* (non-Javadoc)
     * @see org.apache.cassandra.dht.OrderPreservingPartitioner#getToken(java.lang.String)
     */
    @Override
    public StringToken getToken(String key)
    {
        return toStringToken(key);
    }
    
    public StringToken toStringToken(int domain)
    {
        StringBuilder sb = new StringBuilder(2);
        if (domain<0x10)
            sb.append('0');
        
        return new StringToken( sb.append(Integer.toHexString(domain)).toString() );
    }

    public StringToken toStringToken(int domain, String tail)
    {
        if (tail.length()<=2)
            return toStringToken(domain);
        
        StringBuilder sb = new StringBuilder(tail.length());
        if (domain<0x10)
            sb.append('0');
        
        return new StringToken( sb.append(Integer.toHexString(domain)).append(tail, 2,tail.length()).toString() );
    }
    
    /**
     * Validates, that token is hex digit 0-255, lower case letters only
     * 
     * @param initialToken
     * @throws ConfigurationException if something is wrong
     */
    @Override
    public void validateToken(Token nodeToken) throws ConfigurationException
    {
        String initialToken = nodeToken.toString();
        
        if (initialToken==null)
            throw new ConfigurationException("Initial node token must be specified explicitly");
        
        if (initialToken.length()!=2)
            throw new ConfigurationException("Node token must be 2 char hex digit  : "+initialToken);
        
        try {
            int token = Integer.parseInt(initialToken, 16);
            if (token<0 || token>255)
                throw new ConfigurationException("Node token must be hex digit 0-255 : "+initialToken);
            
            if (!initialToken.toLowerCase().equals(initialToken))
                throw new ConfigurationException("Node token must be hex digit. Only lower case letters allowed : "+initialToken);
                
        } catch (NumberFormatException e) {
            throw new ConfigurationException("Node token must be hex digit : "+initialToken);
        }
    }

    /**
     * Ownership calc for odkl partitioner is much simpler. There are only 256 distinct domains, so ownership is just domaincount/256 %
     */
    @Override
    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        
        TokenMetadata tm = StorageService.instance.getTokenMetadata();
        
        Map<InetAddress,Integer> rangesPerEndpoint = new HashMap<InetAddress,Integer>();
        float totalCount = 0;
        
        for (String table : DatabaseDescriptor.getNonSystemTables() ) {
            AbstractReplicationStrategy strategy = StorageService.getReplicationStrategy(tm, table);
            
            for (int domain=0;domain<256;domain++) {
                ArrayList<InetAddress> endpoints = strategy.getNaturalEndpoints(toStringToken(domain, "1"), table);
                for (InetAddress p : endpoints) {
                    Integer count = rangesPerEndpoint.get(p);
                    if (count==null)
                        count=1;
                    else 
                        count+=1;
                    rangesPerEndpoint.put(p,count);
                    totalCount ++;
                }
            }
        }
        
        Map<Token, Float> alltokens = new HashMap<Token, Float>();
        for ( Entry<InetAddress, Integer> en : rangesPerEndpoint.entrySet() ) {
            if (tm.isMember(en.getKey())) {
                alltokens.put( tm.getToken(en.getKey()), en.getValue() / totalCount );
            }
        }

        return alltokens;
    }

}
