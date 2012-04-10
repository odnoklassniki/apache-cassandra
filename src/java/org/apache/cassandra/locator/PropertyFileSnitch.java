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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ResourceWatcher;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to determine if two IP's are in the same datacenter or on the same rack.
 * 
 * <p/>
 * Additionally to info available in {@link GossipNetworkTopologySnith} it reads a properties file in the following format:
 *
 * 10.0.0.13=DC1:RAC2
 * 10.21.119.14=DC3:RAC2
 * 10.20.114.15=DC2:RAC2
 * default=DC1:r1
 * 
 * and supplies this additional information to global state
 */
public class PropertyFileSnitch extends GossipNetworkTopologySnith
{
    private static final Logger logger = LoggerFactory.getLogger(PropertyFileSnitch.class);

    private static final String RACK_PROPERTY_FILENAME = "cassandra-topology.properties";

    public PropertyFileSnitch(Properties properties) throws ConfigurationException
    {
        HashMap<InetAddress, String[]> reloadedMap = new HashMap<InetAddress, String[]>();
        
        loadConfiguration(reloadedMap, properties);

        super.putEndpoints( reloadedMap );
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

        putEndpoints( reloadedMap );
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
