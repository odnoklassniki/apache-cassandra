/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.locator;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ResourceWatcher;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to determine if two IP's are in the same datacenter or on the same rack.
 * <p/>
 * Based on a properties file in the following format:
 *
 * 10.0.0.13=DC1:RAC2
 * 10.21.119.14=DC3:RAC2
 * 10.20.114.15=DC2:RAC2
 * default=DC1:r1
 */
public class PropertyFileSnitch extends AbstractNetworkTopologySnitch
{
    private static final Logger logger = LoggerFactory.getLogger(PropertyFileSnitch.class);

    private static final String RACK_PROPERTY_FILENAME = "cassandra-topology.properties";

    private static volatile Map<InetAddress, String[]> endpointMap;
    private static volatile String[] defaultDCRack;

    public PropertyFileSnitch(Properties properties) throws ConfigurationException
    {
        HashMap<InetAddress, String[]> reloadedMap = new HashMap<InetAddress, String[]>();
        
        loadConfiguration(reloadedMap, properties);

        logger.info("set network topology {}", printTopology( reloadedMap ));
        endpointMap = reloadedMap;
    }
    
    /**
     * @param reloadedMap
     * @return
     */
    private String printTopology(HashMap<InetAddress, String[]> reloadedMap)
    {
        StringBuilder sb = new StringBuilder();
        
        for (Entry<InetAddress, String[]> a : reloadedMap.entrySet())
        {
            sb.append(a.getKey()+" => "+ Arrays.toString(a.getValue()));
        }
        
        return sb.toString();
    }

    public PropertyFileSnitch() throws ConfigurationException
    {
        reloadConfiguration();
        try
        {
            String filename=resourceToFile(RACK_PROPERTY_FILENAME);
            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow() throws ConfigurationException
                {
                    reloadConfiguration();
                    StorageService.instance.getTokenMetadata().invalidateCaches();
                }
            };
            ResourceWatcher.watch(filename, runnable, 60 * 1000);
        }
        catch (ConfigurationException ex)
        {
            logger.warn(RACK_PROPERTY_FILENAME + " found, but does not look like a plain file. Will not watch it for changes");
        }
    }

    public static String resourceToFile(String filename) throws ConfigurationException
    {
        ClassLoader loader = PropertyFileSnitch.class.getClassLoader();
        URL scpurl = loader.getResource(filename);
        if (scpurl == null)
            throw new ConfigurationException("unable to locate " + filename);

        return scpurl.getFile();
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
    
    Set<String> getConfiguredRacks()
    {
        TreeSet<String> racks = new TreeSet<String>();
        
        for (String[] info : endpointMap.values()) {
            racks.add(info[1]);
        }
        
        if (defaultDCRack!=null)
            racks.add(defaultDCRack[1]);
        
        return racks;
    }

    public void reloadConfiguration() throws ConfigurationException
    {
        HashMap<InetAddress, String[]> reloadedMap = new HashMap<InetAddress, String[]>();

        Properties properties = new Properties();
        InputStream stream = null;
        try
        {
            stream = getClass().getClassLoader().getResourceAsStream(RACK_PROPERTY_FILENAME);
            properties.load(stream);
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Unable to read " + RACK_PROPERTY_FILENAME);
        }
        finally
        {
            if (stream!=null)
                try {
                    stream.close();
                } catch (IOException e) {
                }
        }

        loadConfiguration(reloadedMap, properties);

        logger.info("loaded network topology {}", printTopology(reloadedMap) );
        endpointMap = reloadedMap;
    }

    private void loadConfiguration(HashMap<InetAddress, String[]> reloadedMap,
            Properties properties) throws ConfigurationException
    {
        for (Map.Entry<Object, Object> entry : properties.entrySet())
        {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();

            if (key.equals("default"))
            {
                String[] newDefault = value.split(":");
                if (newDefault.length < 2)
                    newDefault = new String[] { "default", "default" };
                defaultDCRack = newDefault;
            }
            else
            {
                InetAddress host;
                String hostString = key.replace("/", "");
                try
                {
                    host = InetAddress.getByName(hostString);
                }
                catch (UnknownHostException e)
                {
                    throw new ConfigurationException("Unknown host " + hostString);
                }
                String[] token = value.split(":");
                if (token.length < 2)
                    token = new String[] { "default", "default" };
                reloadedMap.put(host, token);
            }
        }
    }
}
