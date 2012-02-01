/*
 * @(#) SimpleReadResponseResolver.java
 * Created Jan 31, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.service;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.net.Message;
import org.apache.log4j.Logger;

/**
 * Fast and simple read resolver object when you dont need all those NBH.
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class SimpleReadResponseResolver
{

    protected static Logger logger_ = Logger.getLogger(ReadResponseResolver.class);
    protected final String table;
    protected final String key;

    /**
     * 
     */
    public SimpleReadResponseResolver(String table, String key)
    {
        this.table = table;
        this.key = key;
    }

    public Row resolve(List<ColumnFamily> versions, List<InetAddress> endPoints)
    {
        ColumnFamily resolved;
        if (versions.size() > 1)
        {
            resolved = ReadResponseResolver.resolveSuperset(versions);
            
            if (logger_.isDebugEnabled())
                logger_.debug("versions merged");

            ReadResponseResolver.maybeScheduleRepairs(resolved, table, key, versions, endPoints);
        }
        else
        {
            resolved = versions.get(0);
        }
    
    	return new Row(key, resolved);
    }

    public Row resolve(List<ColumnFamily> versions)
    {
        ColumnFamily resolved;
        if (versions.size() > 1)
        {
            resolved = ReadResponseResolver.resolveSuperset(versions);
            
            if (logger_.isDebugEnabled())
                logger_.debug("versions merged");

        }
        else
        {
            resolved = versions.get(0);
        }
        
        return new Row(key, resolved);
    }
    
    public ReadResponse parseResponse(Message message) throws IOException
    {
        byte[] body = message.getMessageBody();
        ByteArrayInputStream bufIn = new ByteArrayInputStream(body);

        ReadResponse result = ReadResponse.serializer().deserialize(new DataInputStream(bufIn));
        return result;
    }

    public void maybeScheduleRepairs(ColumnFamily resolved, List<ColumnFamily> versions, List<InetAddress> endPoints)
    {
        ReadResponseResolver.maybeScheduleRepairs(resolved, table, key, versions, endPoints);
    }

}