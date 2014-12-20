/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.stream.conf;

import org.apache.commons.configuration.*;

import java.net.URL;

/**
 * Configuration
 */
public class StreamConfiguration extends CompositeConfiguration {

    private static final String ZK_SESSION_TIMEOUT_MS = "zk.session.timeout.ms";

    public StreamConfiguration() {
        super();
        addConfiguration(new SystemConfiguration());
    }

    /**
     * Load configuration from a given <i>url</i>.
     *
     * @param url url to load configuration from.
     * @throws ConfigurationException
     * @return stream configuration
     */
    public StreamConfiguration loadConf(URL url) throws ConfigurationException {
        Configuration loadedConf = new PropertiesConfiguration(url);
        addConfiguration(loadedConf);
        return this;
    }

    /**
     * Load configuration other configuration object <i>conf</i>.
     *
     * @param conf other configuration object.
     * @return stream configuration.
     */
    public StreamConfiguration loadConf(Configuration conf) {
        addConfiguration(conf);
        return this;
    }

    /**
     * Validate if current configuration is valid.
     *
     * @throws ConfigurationException
     */
    public void validate() throws ConfigurationException {
        // no-op
    }

    /**
     * Get ZooKeeper Session Timeout In Millis.
     *
     * @return zookeeper session timeout in millis.
     */
    public int getZkSessionTimeoutMs() {
        return getInt(ZK_SESSION_TIMEOUT_MS, 30000);
    }

    /**
     * Set ZooKeeper Session Timeout In Millis.
     *
     * @param zkSessionTimeoutMs zookeeper session timeout in millis
     * @return stream configuration
     */
    public StreamConfiguration setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
        setProperty(ZK_SESSION_TIMEOUT_MS, zkSessionTimeoutMs);
        return this;
    }

}
