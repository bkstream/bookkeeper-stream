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
package org.apache.bookkeeper.stream.resolver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.bookkeeper.stream.conf.StreamConfiguration;
import org.apache.bookkeeper.stream.exceptions.MetadataException;
import org.apache.bookkeeper.stream.exceptions.StreamException;
import org.apache.bookkeeper.stream.exceptions.ZKException;
import org.apache.bookkeeper.stream.metadata.StreamFactoryMetadata;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * ZooKeeper Based Resolver
 */
public class ZKStreamResolver extends StreamResolver {

    private static final Logger logger = LoggerFactory.getLogger(ZKStreamResolver.class);

    private final String zkServers;
    private final StreamConfiguration conf;
    private ZooKeeper zk = null;

    ZKStreamResolver(String zkServers, StreamConfiguration conf) {
        this.zkServers = zkServers;
        this.conf = conf;
    }

    @VisibleForTesting
    ZooKeeper getZk() throws StreamException, InterruptedException {
        if (null == this.zk) {
            try {
                this.zk = ZooKeeperClient.createConnectedZooKeeper(zkServers, conf.getZkSessionTimeoutMs());
            } catch (KeeperException e) {
                throw new ZKException(e.code().intValue(), "Failed to create zookeeper client to " + zkServers);
            } catch (IOException e) {
                throw new MetadataException("Encountered IO Exception on creating zookeeper client to "
                        + zkServers, e);
            }
        }
        return this.zk;
    }

    @Override
    public StreamFactoryMetadata resolve(URI uri)
            throws StreamException, InterruptedException {
        String zkPath = uri.getPath();
        PathUtils.validatePath(zkPath);
        String[] parts = StringUtils.split(zkPath, '/');
        Preconditions.checkNotNull(parts, "Null zookeeper path to resolve");
        Preconditions.checkArgument(0 != parts.length,
                "Invalid zookeeper path to resolve : " + zkPath);

        for (int i = parts.length; i >= 0; i--) {
            String zkPathToResolve = "/" + StringUtils.join(parts, '/', 0, i);
            StreamFactoryMetadata metadata = resolve(zkPathToResolve);
            if (null != metadata) {
                return metadata;
            }
        }
        throw new MetadataException("No stream factory metadata found under uri " + uri);
    }

    private StreamFactoryMetadata resolve(String zkPath)
            throws StreamException, InterruptedException {
        byte[] data;
        try {
            data = getZk().getData(zkPath, false, new Stat());
        } catch (KeeperException.NoNodeException nne) {
            return null;
        } catch (KeeperException e) {
            throw new ZKException(e.code().intValue(),
                    "Failed to resolve stream factory metadata from " + zkPath);
        }
        if (null == data || 0 == data.length) {
            return null;
        }
        try {
            return StreamFactoryMetadata.deserialize(data);
        } catch (StreamException e) {
            logger.warn("Invalid stream factory metadata found in {}", zkPath, e);
        }
        return null;
    }

    @Override
    public void bind(StreamFactoryMetadata metadata, URI uri)
            throws StreamException, InterruptedException {
        String zkPath = uri.getPath();
        byte[] data = metadata.serialize();
        try {
            ZkUtils.createFullPathOptimistic(getZk(), zkPath, data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException nee) {
            try {
                getZk().setData(zkPath, data, -1);
            } catch (KeeperException e) {
                throw new ZKException(e.code().intValue(),
                        "Failed to bind stream factory metadata to zookeeper path " + zkPath);
            }
        } catch (KeeperException e) {
            throw new ZKException(e.code().intValue(),
                    "Failed to create zookeeper path " + zkPath + " to bind metadata");
        }
    }

    @Override
    public void unbind(URI uri) throws StreamException, InterruptedException {
        String zkPath = uri.getPath();
        byte[] data = new byte[0];
        try {
            getZk().setData(zkPath, data, -1);
        } catch (KeeperException.NoNodeException nne) {
            return;
        } catch (KeeperException e) {
            throw new ZKException(e.code().intValue(),
                    "Failed to unbind stream factory metadata in zookeeper path " + zkPath);
        }
    }

    @Override
    public void close() {
        if (null != zk) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                logger.warn("Interrupted on closing zookeeper client to {} : ", zkServers, e);
                Thread.currentThread().interrupt();
            }
        }
    }
}
