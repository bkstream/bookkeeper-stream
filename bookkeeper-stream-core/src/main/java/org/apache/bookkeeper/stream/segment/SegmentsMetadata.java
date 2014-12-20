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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.TextFormat;
import com.google.protobuf.UninitializedMessageException;
import org.apache.bookkeeper.stream.exceptions.MetadataException;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamSegmentsMetadataFormat;

import java.util.Map;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Stream Metadata Container
 */
public class SegmentsMetadata {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(SegmentsMetadata metadata) {
        return new Builder(metadata);
    }

    public static class Builder {

        private StreamSegmentsMetadataFormat segmentsFormat;
        private Map<String, StreamSegmentMetadata> segments;

        private Builder() {}

        private Builder(SegmentsMetadata segmentsMetadata) {
            this.segmentsFormat = segmentsMetadata.segmentsFormat;
            this.segments = Maps.newHashMap(segmentsMetadata.segments);
        }

        public Builder setStreamSegmentsMetadataFormat(StreamSegmentsMetadataFormat segmentsFormat) {
            this.segmentsFormat = segmentsFormat;
            return this;
        }

        public Builder addSegment(StreamSegmentMetadata metadata) {
            segments.put(metadata.getSegmentName(), metadata);
            return this;
        }

        public Builder removeSegment(StreamSegmentMetadata metadata) {
            segments.remove(metadata.getSegmentName());
            return this;
        }

        public SegmentsMetadata build() {
            return new SegmentsMetadata(segmentsFormat,
                    ImmutableMap.copyOf(segments));
        }

    }

    private final StreamSegmentsMetadataFormat segmentsFormat;
    private final ImmutableMap<String, StreamSegmentMetadata> segments;

    private SegmentsMetadata(StreamSegmentsMetadataFormat segmentsFormat,
                             ImmutableMap<String, StreamSegmentMetadata> segments) {
        Preconditions.checkNotNull(segmentsFormat, "Stream Segments Metadata is Null");
        this.segmentsFormat = segmentsFormat;
        this.segments = segments;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(segmentsFormat, segments);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SegmentsMetadata)) {
            return false;
        }
        SegmentsMetadata that = (SegmentsMetadata) obj;
        return Objects.equal(segmentsFormat, that.segmentsFormat) &&
                Objects.equal(segments, that.segments);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(meta=").append(segmentsFormat).append(", segments=[")
                .append(segments).append("])");
        return sb.toString();
    }

    public byte[] serializeSegmentsFormat() {
        return serializeSegmentsFormat(segmentsFormat);
    }

    static byte[] serializeSegmentsFormat(StreamSegmentsMetadataFormat segmentsFormat) {
        return TextFormat.printToString(segmentsFormat).getBytes(UTF_8);
    }

    public static StreamSegmentsMetadataFormat deserializeSegmentsFormat(byte[] data)
            throws MetadataException {
        String metadataStr = new String(data, UTF_8);
        StreamSegmentsMetadataFormat.Builder builder = StreamSegmentsMetadataFormat.newBuilder();
        try {
            TextFormat.merge(metadataStr, builder);
            return builder.build();
        } catch (UninitializedMessageException ume) {
            throw new MetadataException("Invalid segments metadata : " + metadataStr, ume);
        } catch (TextFormat.ParseException pe) {
            throw new MetadataException("Failed to parse segments metadata : " + metadataStr, pe);
        }
    }
}
