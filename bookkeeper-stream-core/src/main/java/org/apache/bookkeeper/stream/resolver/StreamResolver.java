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

import com.google.common.base.Preconditions;
import org.apache.bookkeeper.stream.conf.StreamConfiguration;
import org.apache.bookkeeper.stream.exceptions.StreamException;
import org.apache.bookkeeper.stream.metadata.StreamFactoryMetadata;

import java.net.URI;

/**
 * Resolver to resolve the factory for streams.
 */
public abstract class StreamResolver {

    static final String SCHEME_BK = "bk";

    /**
     * Resolve given <i>uri</i> to fetch the stream factory metadata.
     *
     * @param uri stream uri
     * @return stream factory metadata
     * @throws StreamException
     * @throws InterruptedException
     */
    public abstract StreamFactoryMetadata resolve(URI uri)
            throws StreamException, InterruptedException;

    /**
     * Bind stream factory <i>metadata</i> to given <i>uri</i>.
     *
     * @param metadata stream factory metadata
     * @param uri stream uri
     * @throws StreamException
     * @throws InterruptedException
     */
    public abstract void bind(StreamFactoryMetadata metadata, URI uri)
            throws StreamException, InterruptedException;

    /**
     * Unbind stream factory metadata bound to given <i>uri</i>.
     *
     * @param uri stream uri
     * @throws StreamException
     * @throws InterruptedException
     */
    public abstract void unbind(URI uri)
            throws StreamException, InterruptedException;

    /**
     * Close resolver.
     */
    public abstract void close();

    /**
     * Resolve current <i>uri</i> to fetch the stream factory metadata.
     *
     * <p>
     *     Currently only support bk://zk!(zkservers)/path/to/stream
     * </p>
     * Currently
     *
     * @param conf stream configuration
     * @param uri stream uri
     * @return stream factory metadata
     * @throws java.lang.IllegalArgumentException if provided uri is illegal
     * @throws StreamException
     */
    public static StreamResolver of(StreamConfiguration conf, URI uri)
            throws StreamException {
        // validate uri
        Preconditions.checkNotNull(uri, "URI is null");
        Preconditions.checkArgument(SCHEME_BK.equalsIgnoreCase(uri.getScheme()),
                "Unknown scheme " + uri.getScheme() + ", currently only support bk://");
        String authority = uri.getAuthority();
        Preconditions.checkArgument(authority.startsWith("zk!"),
                "Unknown resolver " + authority + ", currently only support 'zk!' resolver");

        // initialize the zk resolver
        String zkServers = authority.substring(3);

        return new ZKStreamResolver(zkServers, conf);
    }
}
