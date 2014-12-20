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
package org.apache.bookkeeper.stream.segment;

import org.apache.bookkeeper.stream.exceptions.MetadataException;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamSegmentsMetadataFormat;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test Case for {@link org.apache.bookkeeper.stream.segment.SegmentsMetadata}
 */
public class TestSegmentsMetadata {

    @Test(timeout = 60000)
    public void testEmptySegmentsMetadata() throws Exception {
        StreamSegmentsMetadataFormat format =
                SegmentsMetadata.deserializeSegmentsFormat(new byte[0]);
        assertFalse(format.hasMaxSegmentId());
    }

    @Test(timeout = 60000, expected = MetadataException.class)
    public void testCorruptedSegmentsMetadata() throws Exception {
        StreamSegmentsMetadataFormat segmentsMetadata =
                StreamSegmentsMetadataFormat.newBuilder()
                        .setMaxSegmentId(1L).build();
        byte[] data = SegmentsMetadata.serializeSegmentsFormat(segmentsMetadata);
        byte[] corruptedData = new byte[data.length - 5];
        System.arraycopy(data, 0, corruptedData, 0, corruptedData.length);
        SegmentsMetadata.deserializeSegmentsFormat(corruptedData);
    }

    @Test(timeout = 60000)
    public void testSerializeSegmentsFormat() throws Exception {
        StreamSegmentsMetadataFormat format =
                StreamSegmentsMetadataFormat.newBuilder()
                        .setMaxSegmentId(1L).build();
        byte[] data = SegmentsMetadata.serializeSegmentsFormat(format);
        StreamSegmentsMetadataFormat dFormat =
                SegmentsMetadata.deserializeSegmentsFormat(data);
        assertEquals(format, dFormat);
    }
}
