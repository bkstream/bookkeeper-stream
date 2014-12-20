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
package org.apache.bookkeeper.stream.metadata;

import org.apache.bookkeeper.stream.exceptions.MetadataException;
import org.apache.bookkeeper.stream.proto.DataFormats.BKStreamFactoryMetadataFormat;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamFactoryMetadataFormat.Type;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test Case for {@link org.apache.bookkeeper.stream.metadata.StreamFactoryMetadata}
 */
public class TestStreamFactoryMetadata {

    private static final String zkServers = "127.0.0.1:2181";
    private static final String zkLedgersPath = "/ledgers";

    private static StreamFactoryMetadata buildStreamFactoryMetadata() {
        BKStreamFactoryMetadataFormat.Builder bkFormat = BKStreamFactoryMetadataFormat.newBuilder()
                .setSZkServers(zkServers).setBkZkServers(zkServers).setBkLedgersPath(zkLedgersPath);
        StreamFactoryMetadata.Builder builder = StreamFactoryMetadata.newBuilder();
        builder.getFormatBuilder().setType(Type.BK).setBkFormat(bkFormat);
        return builder.build();
    }

    @Test(timeout = 60000, expected = MetadataException.class)
    public void testEmptyStreamFactoryMetadata() throws Exception {
        StreamFactoryMetadata.deserialize(new byte[0]);
    }

    @Test(timeout = 60000, expected = MetadataException.class)
    public void testCorruptedStreamFactoryMetadata() throws Exception {
        byte[] data = buildStreamFactoryMetadata().serialize();
        // corrupt data
        byte[] corruptedData = new byte[data.length - 3];
        System.arraycopy(data, 0, corruptedData, 0, corruptedData.length);
        StreamFactoryMetadata.deserialize(corruptedData);
    }

    @Test(timeout = 60000)
    public void testSerializeDeserialize() throws Exception {
        StreamFactoryMetadata metadata = buildStreamFactoryMetadata();
        byte[] data = metadata.serialize();
        StreamFactoryMetadata deserializedMetadata =
                StreamFactoryMetadata.deserialize(data);
        assertEquals("Deserialized metadata should equal to original metadata",
                metadata, deserializedMetadata);
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testMissingStreamFactoryType() throws Exception {
        StreamFactoryMetadata.newBuilder().build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testMissingBKStreamFactoryMetadataFormat() throws Exception {
        StreamFactoryMetadata.Builder builder = StreamFactoryMetadata.newBuilder();
        builder.getFormatBuilder().setType(Type.BK);
        builder.build();
    }

}
