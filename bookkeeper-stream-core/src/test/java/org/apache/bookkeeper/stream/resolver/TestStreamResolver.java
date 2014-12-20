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

import org.apache.bookkeeper.stream.conf.StreamConfiguration;
import org.apache.bookkeeper.stream.exceptions.MetadataException;
import org.apache.bookkeeper.stream.metadata.StreamFactoryMetadata;
import org.apache.bookkeeper.stream.proto.DataFormats.BKStreamFactoryMetadataFormat;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamFactoryMetadataFormat.Type;
import org.apache.bookkeeper.stream.test.ZooKeeperClusterTestCase;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.*;

/**
 * Test Case for {@link org.apache.bookkeeper.stream.resolver.StreamResolver}
 */
public class TestStreamResolver extends ZooKeeperClusterTestCase {

    private static final String zkLedgersPath = "/ledgers";

    private final StreamConfiguration conf = new StreamConfiguration();

    private StreamFactoryMetadata buildStreamFactoryMetadata() {
        return buildStreamFactoryMetadata(zkLedgersPath);
    }

    private StreamFactoryMetadata buildStreamFactoryMetadata(String zkLedgersPath) {
        String zkServers = zkUtil.getZooKeeperConnectString();
        BKStreamFactoryMetadataFormat.Builder bkFormat = BKStreamFactoryMetadataFormat.newBuilder()
                .setSZkServers(zkServers).setBkZkServers(zkServers).setBkLedgersPath(zkLedgersPath);
        StreamFactoryMetadata.Builder builder = StreamFactoryMetadata.newBuilder();
        builder.getFormatBuilder().setType(Type.BK).setBkFormat(bkFormat);
        return builder.build();
    }

    private URI createURI(String zkPath) {
        return URI.create("bk://zk!" + zkUtil.getZooKeeperConnectString() + zkPath);
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testNullURI() throws Exception {
        StreamResolver.of(conf, null);
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testUnknownScheme() throws Exception {
        URI uri = URI.create("unknown://zk!127.0.0.1:2181/path/to/stream");
        StreamResolver.of(conf, uri);
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testUnknownResolver() throws Exception {
        URI uri = URI.create("bk://unknown!127.0.0.1:2181/path/to/stream");
        StreamResolver.of(conf, uri);
    }

    @Test(timeout = 60000)
    public void testBindUnbind() throws Exception {
        URI uri = createURI("/test-bind-unbind");
        StreamFactoryMetadata metadata = buildStreamFactoryMetadata();

        StreamResolver resolver = StreamResolver.of(conf, uri);
        try {
            try {
                resolver.resolve(uri);
                fail("Should fail on resolving " + uri + " if no metadata bound");
            } catch (MetadataException me) {
                // expected
            }
            // bind metadata to unexisted path
            resolver.bind(metadata, uri);
            // resolve metadata
            StreamFactoryMetadata resolvedMetadata = resolver.resolve(uri);
            assertEquals("Metadata should be same after resolving", resolvedMetadata, metadata);
            // unbind metadata
            resolver.unbind(uri);
            // resolve should fail after unbind
            try {
                resolver.resolve(uri);
                fail("Should fail on resolving " + uri + " after unbind");
            } catch (MetadataException me) {
                // expected
            }
        } finally {
            resolver.close();
        }
    }

    @Test(timeout = 60000, expected = MetadataException.class)
    public void testResolveUnexistedPath() throws Exception {
        URI uri = createURI("/unexisted/path");
        StreamResolver resolver = StreamResolver.of(conf, uri);
        try {
            resolver.resolve(uri);
        } finally {
            resolver.close();
        }
    }

    @Test(timeout = 60000, expected = MetadataException.class)
    public void testResolveExistedUnboundPath() throws Exception {
        String zkPath = "/existed/unbound/path";
        URI uri = createURI(zkPath);
        StreamResolver resolver = StreamResolver.of(conf, uri);
        ZKStreamResolver zkResolver = (ZKStreamResolver) resolver;
        ZkUtils.createFullPathOptimistic(zkResolver.getZk(), zkPath, new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        try {
            resolver.resolve(uri);
        } finally {
            resolver.close();
        }
    }

    @Test(timeout = 60000)
    public void testRecursiveResolve() throws Exception {
        StreamFactoryMetadata metadata1 = buildStreamFactoryMetadata("/ledgers1");
        StreamFactoryMetadata metadata2 = buildStreamFactoryMetadata("/ledgers2");

        String zkPath = "/recursive/resolver";
        URI uri = createURI(zkPath);
        StreamResolver resolver = StreamResolver.of(conf, uri);
        ZKStreamResolver zkResolver = (ZKStreamResolver) resolver;
        try {
            resolver.bind(metadata1, createURI(zkPath + "/parent"));
            resolver.bind(metadata2, createURI(zkPath + "/parent/child"));
            assertEquals("Resolve /parent should be same as metadata1",
                    metadata1, resolver.resolve(createURI(zkPath + "/parent")));
            assertEquals("Resolve /parent/child should be same as metadata2",
                    metadata2, resolver.resolve(createURI(zkPath + "/parent/child")));
            assertEquals("Resolve /parent/child/unknown should be same as metadata2",
                    metadata2, resolver.resolve(createURI(zkPath + "/parent/child/unexisted")));
            ZkUtils.createFullPathOptimistic(zkResolver.getZk(), zkPath + "/parent/child/unbound", new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            assertEquals("Resolve /parent/child/unbound should be same as metadata2",
                    metadata2, resolver.resolve(createURI(zkPath + "/parent/child/unbound")));
        } finally {
            resolver.close();
        }
    }
}
