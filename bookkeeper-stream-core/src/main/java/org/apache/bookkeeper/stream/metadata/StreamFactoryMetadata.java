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

import com.google.common.base.Preconditions;
import com.google.protobuf.TextFormat;
import org.apache.bookkeeper.stream.exceptions.MetadataException;
import org.apache.bookkeeper.stream.exceptions.StreamException;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamFactoryMetadataFormat;

import static com.google.common.base.Charsets.UTF_8;

/**
 * This class encapsulates stream factory metadata.
 */
public class StreamFactoryMetadata {

    public static Builder newBuilder() {
        return new Builder(StreamFactoryMetadataFormat.newBuilder());
    }

    public static class Builder {

        private final StreamFactoryMetadataFormat.Builder builder;

        private Builder(StreamFactoryMetadataFormat.Builder builder) {
            this.builder = builder;
        }

        public StreamFactoryMetadataFormat.Builder getFormatBuilder() {
            return this.builder;
        }

        public StreamFactoryMetadata build() {
            StreamFactoryMetadataFormat format = this.builder.build();
            return new StreamFactoryMetadata(format);
        }

    }

    private final StreamFactoryMetadataFormat format;

    private static void validateFormat(StreamFactoryMetadataFormat format) throws IllegalArgumentException {
        Preconditions.checkArgument(format.hasType(), "Missing stream factory type");
        if (StreamFactoryMetadataFormat.Type.BK == format.getType()) {
            Preconditions.checkArgument(format.hasBkFormat(), "Missing bookkeeper stream factory type");
        }
    }

    private StreamFactoryMetadata(StreamFactoryMetadataFormat format) {
        Preconditions.checkNotNull(format, "Stream Factory Metadata is Null");
        validateFormat(format);
        this.format = format;
    }

    public byte[] serialize() {
        return TextFormat.printToString(format).getBytes(UTF_8);
    }

    public static StreamFactoryMetadata deserialize(byte[] data) throws StreamException {
        String metadataStr = new String(data, UTF_8);
        StreamFactoryMetadataFormat.Builder builder = StreamFactoryMetadataFormat.newBuilder();
        try {
            TextFormat.merge(metadataStr, builder);
            StreamFactoryMetadataFormat metadata = builder.build();
            return new StreamFactoryMetadata(metadata);
        } catch (TextFormat.ParseException pe) {
            throw new MetadataException("Failed to parse factory metadata : " + metadataStr, pe);
        } catch (IllegalArgumentException iae) {
            throw new MetadataException("Invalid stream factory metadata : " + metadataStr, iae);
        }
    }

    @Override
    public int hashCode() {
        return format.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StreamFactoryMetadata)) {
            return false;
        }
        StreamFactoryMetadata that = (StreamFactoryMetadata) obj;
        return format.equals(that.format);
    }

    @Override
    public String toString() {
        return format.toString();
    }
}
