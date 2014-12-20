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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.protobuf.TextFormat;
import com.google.protobuf.UninitializedMessageException;
import org.apache.bookkeeper.stream.SSN;
import org.apache.bookkeeper.stream.exceptions.MetadataException;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamSegmentMetadataFormat;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamSegmentMetadataFormat.State;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamSegmentMetadataFormat.TruncationState;

import java.util.Comparator;

import static com.google.common.base.Charsets.UTF_8;

/**
 * This class encapsulates all the stream segment metadata that is persistently stored
 * in metadata store.
 */
public class StreamSegmentMetadata {

    private final static int VERSION0 = 0;
    private final static int CUR_VERSION = VERSION0;

    public final static String PREFIX_INPROGRESS = "segment_inprogress_";
    public final static String PREFIX_COMPLETED  = "segment_completed_";

    public static String segmentName(long segmentId, boolean inprogress) {
        if (inprogress) {
            return String.format("%s_%018d", PREFIX_INPROGRESS, segmentId);
        } else {
            return String.format("%s_%018d", PREFIX_COMPLETED, segmentId);
        }
    }

    public static String segmentNameWithTimestamp(long segmentId, boolean inprogress) {
        long ts = System.currentTimeMillis();
        if (inprogress) {
            return String.format("%s_%018d_%018d", PREFIX_INPROGRESS, segmentId, ts);
        } else {
            return String.format("%s_%018d_%018d", PREFIX_COMPLETED, segmentId, ts);
        }
    }

    public static Builder newBuilder() {
        return newBuilder(CUR_VERSION);
    }

    public static Builder newBuilder(int version) {
        return new Builder(version);
    }

    public static class Builder {

        private int version;
        private String segmentName;
        private StreamSegmentMetadataFormat.Builder segmentBuilder;

        private Builder(int version) {
            this.version = version;
        }

        public Builder setSegmentName(String segmentName) {
            this.segmentName = segmentName;
            return this;
        }

        public Builder setStreamSegmentMetadataFormatBuilder(StreamSegmentMetadataFormat.Builder builder) {
            this.segmentBuilder = builder;
            return this;
        }

        public StreamSegmentMetadata build() {
            Preconditions.checkNotNull(segmentName, "Segment Name is Null");
            Preconditions.checkNotNull(segmentBuilder, "Segment Format Builder is Null");
            StreamSegmentMetadataFormat format = this.segmentBuilder.setVersion(version).build();
            return new StreamSegmentMetadata(segmentName, format);
        }
    }

    private static void validateFormat(StreamSegmentMetadataFormat format) throws IllegalArgumentException {
        Preconditions.checkArgument(format.getVersion() == VERSION0, "Unknown stream segment version");
        Preconditions.checkArgument(format.hasSegmentId(), "Missing segment id");
        Preconditions.checkArgument(format.hasLedgerId(), "Missing ledger id");
        Preconditions.checkArgument(format.hasState(), "Missing segment state");
        Preconditions.checkArgument(format.hasCTime(), "Missing segment creation time");
        Preconditions.checkArgument(format.hasMTime(), "Missing segment modification time");
        if (StreamSegmentMetadataFormat.State.COMPLETED == format.getState()) {
            Preconditions.checkArgument(format.hasLastSSN(), "Missing last stream sequence number");
            Preconditions.checkArgument(format.hasCompletionTime(), "Missing stream segment completion time");
            Preconditions.checkArgument(format.hasRecordCount(), "Missing record count");
            Preconditions.checkArgument(format.getLastSSN().getSegmentId() == format.getSegmentId(),
                    "Invalid last ssn found in segment " + format.getSegmentId());
            Preconditions.checkArgument(format.getRecordCount() >= 0, "Negative record count");
        }
        if (format.hasTruncatedSSN()) {
            Preconditions.checkArgument(format.getTruncatedSSN().getSegmentId() == format.getSegmentId(),
                    "Invalid truncated ssn found in segment " + format.getSegmentId());
            if (format.hasLastSSN()) {
                Preconditions.checkArgument(SSN.notGreaterThan(format.getTruncatedSSN(), format.getLastSSN()),
                        "Truncated SSN should be not greater than Last SSN");
            }
        }
    }

    public static final Comparator<StreamSegmentMetadata> COMPARATOR =
            new Comparator<StreamSegmentMetadata>() {
                @Override
                public int compare(StreamSegmentMetadata o1, StreamSegmentMetadata o2) {
                    StreamSegmentMetadataFormat f1 = o1.format;
                    StreamSegmentMetadataFormat f2 = o2.format;
                    if (f1.getSegmentId() < f2.getSegmentId()) {
                        return -1;
                    } else if (f1.getSegmentId() == f2.getSegmentId()) {
                        return 0;
                    } else {
                        return 1;
                    }
                }
            };

    public static final Comparator<StreamSegmentMetadata> DESC_COMPARATOR =
            new Comparator<StreamSegmentMetadata>() {
                @Override
                public int compare(StreamSegmentMetadata o1, StreamSegmentMetadata o2) {
                    StreamSegmentMetadataFormat f1 = o1.format;
                    StreamSegmentMetadataFormat f2 = o2.format;
                    if (f1.getSegmentId() > f2.getSegmentId()) {
                        return -1;
                    } else if (f1.getSegmentId() == f2.getSegmentId()) {
                        return 0;
                    } else {
                        return 1;
                    }
                }
            };

    private final String segmentName;
    private final StreamSegmentMetadataFormat format;

    private StreamSegmentMetadata(String segmentName,
            StreamSegmentMetadataFormat format) {
        Preconditions.checkNotNull(segmentName, "Stream Segment Name is Null");
        Preconditions.checkNotNull(format, "Stream Segment Metadata is Null");
        validateFormat(format);
        this.segmentName = segmentName;
        this.format = format;
    }

    public StreamSegmentMetadata complete(SSN lastSSN, int recordCount) {
        Preconditions.checkArgument(isInprogress(),
                "Only support complete an inprogress segment");
        long completionTime = System.currentTimeMillis();
        Builder builder = newBuilder(this.format.getVersion());
        StreamSegmentMetadataFormat.Builder formatBuilder =
                StreamSegmentMetadataFormat.newBuilder(this.format)
                        .setLastSSN(lastSSN.toProtoSSN())
                        .setRecordCount(recordCount)
                        .setCompletionTime(completionTime)
                        .setMTime(completionTime)
                        .setState(State.COMPLETED);
        builder.setSegmentName(segmentNameWithTimestamp(format.getSegmentId(), false));
        builder.setStreamSegmentMetadataFormatBuilder(formatBuilder);
        return builder.build();
    }

    public StreamSegmentMetadata setTruncationState(TruncationState truncationState,
                                                    Optional<SSN> truncatedSSN) {
        long mTime = System.currentTimeMillis();
        Builder builder = newBuilder(this.format.getVersion());
        StreamSegmentMetadataFormat.Builder formatBuilder =
                StreamSegmentMetadataFormat.newBuilder(this.format)
                        .setTruncationState(truncationState)
                        .setMTime(mTime);
        if (TruncationState.NONE == truncationState) {
            Preconditions.checkArgument(!truncatedSSN.isPresent(),
                    "Truncate SSN should not be present if set truncation state to NONE");
            formatBuilder.clearTruncatedSSN();
        } else if (TruncationState.PARTIAL == truncationState) {
            Preconditions.checkArgument(truncatedSSN.isPresent(),
                    "Truncate SSN should be present if set truncation state to PARTIAL");
            formatBuilder.setTruncatedSSN(truncatedSSN.get().toProtoSSN());
        } else {
            if (isInprogress()) {
                throw new IllegalArgumentException("Couldn't fully truncate an inprogress segment");
            } else {
                Preconditions.checkArgument(!truncatedSSN.isPresent(),
                        "Truncate SSN should be not present if set truncation state to FULL");
                formatBuilder.clearTruncatedSSN();
            }
        }
        builder.setSegmentName(segmentNameWithTimestamp(format.getSegmentId(), isInprogress()));
        builder.setStreamSegmentMetadataFormatBuilder(formatBuilder);
        return builder.build();
    }

    public byte[] serialize() {
        return TextFormat.printToString(format).getBytes(UTF_8);
    }

    public static StreamSegmentMetadata deserialize(String segmentName, byte[] data)
            throws MetadataException {
        String metadataStr = new String(data, UTF_8);
        StreamSegmentMetadataFormat.Builder builder = StreamSegmentMetadataFormat.newBuilder();
        try {
            TextFormat.merge(metadataStr, builder);
            return new StreamSegmentMetadata(segmentName, builder.build());
        } catch (UninitializedMessageException ume) {
            throw new MetadataException("Invalid stream segment metadata : " + metadataStr, ume);
        } catch (TextFormat.ParseException pe) {
            throw new MetadataException("Failed to parse stream segment metadata : " + metadataStr, pe);
        } catch (IllegalArgumentException iae) {
            throw new MetadataException("Invalid stream segment metadata : name = " + segmentName
                    + ", msg = " + metadataStr, iae);
        }
    }

    public String getSegmentName() {
        return segmentName;
    }

    public StreamSegmentMetadataFormat getSegmentFormat() {
        return format;
    }

    public boolean isInprogress() {
        return State.INPROGRESS == format.getState();
    }

    public boolean isCompleted() {
        return State.COMPLETED == format.getState();
    }

    @Override
    public int hashCode() {
        return format.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StreamSegmentMetadata)) {
            return false;
        }
        StreamSegmentMetadata that = (StreamSegmentMetadata) obj;
        return format.equals(that.format);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[name=").append(segmentName)
                .append(", metadata='").append(format).append("']");
        return sb.toString();
    }
}
