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

import com.google.common.base.Optional;
import org.apache.bookkeeper.stream.SSN;
import org.apache.bookkeeper.stream.exceptions.MetadataException;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamSegmentMetadataFormat;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamSegmentMetadataFormat.State;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamSegmentMetadataFormat.TruncationState;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test Case for {@link org.apache.bookkeeper.stream.metadata.StreamSegmentMetadata}
 */
public class TestStreamSegmentMetadata {

    private static StreamSegmentMetadata buildInprogressStreamSegment(long segmentId) {
        long curTime = System.currentTimeMillis();
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(segmentId)
                        .setLedgerId(segmentId)
                        .setState(State.INPROGRESS)
                        .setCTime(curTime)
                        .setMTime(curTime);
        return StreamSegmentMetadata.newBuilder()
                .setSegmentName(StreamSegmentMetadata.segmentName(segmentId, true))
                .setStreamSegmentMetadataFormatBuilder(segmentBuilder).build();
    }

    private static StreamSegmentMetadata buildCompletedStreamSegment(long segmentId, SSN lastSSN, long recordCount) {
        long curTime = System.currentTimeMillis();
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(segmentId)
                        .setLedgerId(segmentId)
                        .setState(State.COMPLETED)
                        .setCTime(curTime)
                        .setMTime(curTime)
                        .setCompletionTime(curTime)
                        .setLastSSN(lastSSN.toProtoSSN())
                        .setRecordCount(recordCount);
        return StreamSegmentMetadata.newBuilder()
                .setSegmentName(StreamSegmentMetadata.segmentName(segmentId, false))
                .setStreamSegmentMetadataFormatBuilder(segmentBuilder).build();
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testBuilderMissingSegmentName() throws Exception {
        StreamSegmentMetadata.newBuilder().build();
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testBuilderMissingSegmentFormatBuilder() throws Exception {
        StreamSegmentMetadata.newBuilder().setSegmentName("missing-format-builder").build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testFormatMissingSegmentId() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder();
        StreamSegmentMetadata.newBuilder()
                .setSegmentName("missing-segment-id")
                .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                .build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testFormatMissingLedgerId() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(1L);
        StreamSegmentMetadata.newBuilder()
                .setSegmentName("missing-ledger-id")
                .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                .build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testFormatMissingState() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(1L)
                        .setLedgerId(1L);
        StreamSegmentMetadata.newBuilder()
                .setSegmentName("missing-state")
                .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                .build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testFormatMissingCTime() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(1L)
                        .setLedgerId(1L)
                        .setState(State.INPROGRESS);
        StreamSegmentMetadata.newBuilder()
                .setSegmentName("missing-ctime")
                .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                .build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testFormatMissingMTime() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(1L)
                        .setLedgerId(1L)
                        .setState(State.INPROGRESS)
                        .setCTime(System.currentTimeMillis());
        StreamSegmentMetadata.newBuilder()
                .setSegmentName("missing-mtime")
                .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                .build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testFormatMissingLastSSN() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(1L)
                        .setLedgerId(1L)
                        .setState(State.COMPLETED)
                        .setCTime(System.currentTimeMillis())
                        .setMTime(System.currentTimeMillis());
        StreamSegmentMetadata.newBuilder()
                .setSegmentName("missing-last-ssn")
                .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                .build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testFormatMissingCompletionTime() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(1L)
                        .setLedgerId(1L)
                        .setState(State.COMPLETED)
                        .setCTime(System.currentTimeMillis())
                        .setMTime(System.currentTimeMillis())
                        .setLastSSN(SSN.of(1L, 0L, 0L).toProtoSSN());
        StreamSegmentMetadata.newBuilder()
                .setSegmentName("missing-completion-time")
                .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                .build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testFormatMissingRecordCount() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(1L)
                        .setLedgerId(1L)
                        .setState(State.COMPLETED)
                        .setCTime(System.currentTimeMillis())
                        .setMTime(System.currentTimeMillis())
                        .setLastSSN(SSN.of(1L, 0L, 0L).toProtoSSN())
                        .setCompletionTime(System.currentTimeMillis());
        StreamSegmentMetadata.newBuilder()
                .setSegmentName("missing-record-count")
                .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                .build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testFormatInvalidLastSSN() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(1L)
                        .setLedgerId(1L)
                        .setState(State.COMPLETED)
                        .setCTime(System.currentTimeMillis())
                        .setMTime(System.currentTimeMillis())
                        .setLastSSN(SSN.of(0L, 0L, 0L).toProtoSSN())
                        .setCompletionTime(System.currentTimeMillis());
        StreamSegmentMetadata.newBuilder()
                .setSegmentName("invalid-last-ssn")
                .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                .build();
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testCompleteCompletedSegment() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(1L)
                        .setLedgerId(1L)
                        .setState(State.COMPLETED)
                        .setCTime(System.currentTimeMillis())
                        .setMTime(System.currentTimeMillis())
                        .setLastSSN(SSN.of(1L, 0L, 0L).toProtoSSN())
                        .setCompletionTime(System.currentTimeMillis())
                        .setRecordCount(0L);
        StreamSegmentMetadata segmentMetadata =
                StreamSegmentMetadata.newBuilder()
                        .setSegmentName("complete-completed-segment")
                        .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                        .build();
        segmentMetadata.complete(SSN.of(1L, 1L, 0L), 1);
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testCompleteInprogressSegmentWithInvalidSSN() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(1L)
                        .setLedgerId(1L)
                        .setState(State.INPROGRESS)
                        .setCTime(System.currentTimeMillis())
                        .setMTime(System.currentTimeMillis());
        StreamSegmentMetadata segmentMetadata =
                StreamSegmentMetadata.newBuilder()
                        .setSegmentName("complete-inprogress-segment-with-invalid-ssn")
                        .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                        .build();
        segmentMetadata.complete(SSN.of(0L, 1L, 0L), 1);
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testCompleteInprogressSegmentWithInvalidRecordCount() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(1L)
                        .setLedgerId(1L)
                        .setState(State.INPROGRESS)
                        .setCTime(System.currentTimeMillis())
                        .setMTime(System.currentTimeMillis());
        StreamSegmentMetadata segmentMetadata =
                StreamSegmentMetadata.newBuilder()
                        .setSegmentName("complete-inprogress-segment-with-invalid-record-count")
                        .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                        .build();
        segmentMetadata.complete(SSN.of(1L, 1L, 0L), -1);
    }

    @Test(timeout = 60000)
    public void testCompleteInprogressSegment() throws Exception {
        StreamSegmentMetadataFormat.Builder segmentBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(1L)
                        .setLedgerId(1L)
                        .setState(State.INPROGRESS)
                        .setCTime(System.currentTimeMillis())
                        .setMTime(System.currentTimeMillis());
        StreamSegmentMetadata segmentMetadata =
                StreamSegmentMetadata.newBuilder()
                        .setSegmentName("complete-inprogress-segment-with-invalid-record-count")
                        .setStreamSegmentMetadataFormatBuilder(segmentBuilder)
                        .build();
        StreamSegmentMetadata completedSegmentMetadata =
                segmentMetadata.complete(SSN.of(1L, 1L, 0L), 1);
        assertEquals(segmentMetadata.getSegmentFormat().getVersion(),
                completedSegmentMetadata.getSegmentFormat().getVersion());
        assertEquals(segmentMetadata.getSegmentFormat().getSegmentId(),
                completedSegmentMetadata.getSegmentFormat().getSegmentId());
        assertEquals(segmentMetadata.getSegmentFormat().getLedgerId(),
                completedSegmentMetadata.getSegmentFormat().getLedgerId());
        assertEquals(State.COMPLETED, completedSegmentMetadata.getSegmentFormat().getState());
        assertTrue(completedSegmentMetadata.isCompleted());
        assertEquals(segmentMetadata.getSegmentFormat().getCTime(),
                completedSegmentMetadata.getSegmentFormat().getCTime());
        assertTrue(segmentMetadata.getSegmentFormat().getMTime() <=
                completedSegmentMetadata.getSegmentFormat().getMTime());
        assertTrue(completedSegmentMetadata.getSegmentName().startsWith(StreamSegmentMetadata.PREFIX_COMPLETED));
        assertEquals(SSN.of(1L, 1L, 0L).toProtoSSN(), completedSegmentMetadata.getSegmentFormat().getLastSSN());
        assertEquals(1, completedSegmentMetadata.getSegmentFormat().getRecordCount());
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testResetTruncatedInprogressSegmentWithSSN() throws Exception {
        StreamSegmentMetadata ssm = buildInprogressStreamSegment(1L);
        ssm.setTruncationState(TruncationState.NONE, Optional.of(SSN.of(1L, 1L, 0L)));
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testResetTruncatedCompletedSegmentWithSSN() throws Exception {
        StreamSegmentMetadata ssm =
                buildCompletedStreamSegment(1L, SSN.of(1L, 100L, 0L), 100);
        ssm.setTruncationState(TruncationState.NONE, Optional.of(SSN.of(1L, 50L, 0L)));
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testFullyTruncateInprogressSegment() throws Exception {
        StreamSegmentMetadata ssm = buildInprogressStreamSegment(1L);
        Optional<SSN> ssn = Optional.absent();
        ssm.setTruncationState(TruncationState.FULL, ssn);
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testFullyTruncateCompletedSegmentWithSSN() throws Exception {
        StreamSegmentMetadata ssm =
                buildCompletedStreamSegment(1L, SSN.of(1L, 100L, 0L), 100);
        Optional<SSN> ssn = Optional.of(SSN.of(1L, 50L, 0L));
        ssm.setTruncationState(TruncationState.FULL, ssn);
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testPartialTruncateInprogressSegmentWithoutSSN() throws Exception {
        StreamSegmentMetadata ssm = buildInprogressStreamSegment(1L);
        Optional<SSN> ssn = Optional.absent();
        ssm.setTruncationState(TruncationState.PARTIAL, ssn);
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testPartialTruncateCompletedSegmentWithoutSSN() throws Exception {
        StreamSegmentMetadata ssm =
                buildCompletedStreamSegment(1L, SSN.of(1L, 100L, 0L), 100);
        Optional<SSN> ssn = Optional.absent();
        ssm.setTruncationState(TruncationState.PARTIAL, ssn);
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testTruncateCompletedSegmentWithInvalidSSN() throws Exception {
        StreamSegmentMetadata ssm =
                buildCompletedStreamSegment(1L, SSN.of(1L, 100L, 0L), 100);
        ssm.setTruncationState(TruncationState.PARTIAL, Optional.of(SSN.of(2L, 50L, 0L)));
    }

    @Test(timeout = 60000, expected = IllegalArgumentException.class)
    public void testTruncateCompletedSegmentWithLargerSSN() throws Exception {
        StreamSegmentMetadata ssm =
                buildCompletedStreamSegment(1L, SSN.of(1L, 100L, 0L), 100);
        ssm.setTruncationState(TruncationState.PARTIAL, Optional.of(SSN.of(1L, 200L, 0L)));
    }

    @Test(timeout = 60000)
    public void testTruncateCompletedSegment() throws Exception {
        StreamSegmentMetadata ssm =
                buildCompletedStreamSegment(1L, SSN.of(1L, 100L, 0L), 100);
        // partial truncation
        StreamSegmentMetadata ssm1 =
                ssm.setTruncationState(TruncationState.PARTIAL, Optional.of(SSN.of(1L, 50L, 0L)));
        assertEquals(TruncationState.PARTIAL, ssm1.getSegmentFormat().getTruncationState());
        assertEquals(SSN.of(1L, 50L, 0L).toProtoSSN(), ssm1.getSegmentFormat().getTruncatedSSN());
        assertTrue(ssm.getSegmentFormat().getMTime() <= ssm1.getSegmentFormat().getMTime());
        // fully truncation
        Optional<SSN> ssn = Optional.absent();
        StreamSegmentMetadata ssm2 =
                ssm1.setTruncationState(TruncationState.FULL, ssn);
        assertEquals(TruncationState.FULL, ssm2.getSegmentFormat().getTruncationState());
        assertFalse(ssm2.getSegmentFormat().hasTruncatedSSN());
        // reset truncation state
        StreamSegmentMetadata ssm3 =
                ssm2.setTruncationState(TruncationState.NONE, ssn);
        assertEquals(TruncationState.NONE, ssm3.getSegmentFormat().getTruncationState());
        assertFalse(ssm3.getSegmentFormat().hasTruncatedSSN());
    }

    @Test(timeout = 60000, expected = MetadataException.class)
    public void testEmptyStreamSegmentMetadata() throws Exception {
        StreamSegmentMetadata.deserialize("empty_stream_segment", new byte[0]);
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testDesrializeStreamSegmentMetadataWithNullName() throws Exception {
        StreamSegmentMetadata ssm = buildCompletedStreamSegment(1L, SSN.of(1L, 100L, 0L), 100);
        byte[] data = ssm.serialize();
        StreamSegmentMetadata.deserialize(null, data);
    }

    @Test(timeout = 60000)
    public void testDesrializeCorruptedMetadata() throws Exception {
        StreamSegmentMetadata ssm = buildCompletedStreamSegment(1L, SSN.of(1L, 100L, 0L), 100);
        byte[] data = ssm.serialize();
        byte[] corruptedData = new byte[data.length - 4];
        System.arraycopy(data, 0, corruptedData, 0, corruptedData.length);
        StreamSegmentMetadata.deserialize("corrupted-data", corruptedData);
    }

    @Test(timeout = 60000)
    public void testSerializeDeserialize() throws Exception {
        StreamSegmentMetadata ssm = buildCompletedStreamSegment(1L, SSN.of(1L, 100L, 0L), 100);
        byte[] data = ssm.serialize();
        StreamSegmentMetadata dSSM =
                StreamSegmentMetadata.deserialize(StreamSegmentMetadata.segmentName(1L, false), data);
        assertEquals("Deserialized stream segment metadata should not be changed", ssm, dSSM);
    }

    @Test(timeout = 60000)
    public void testComparator() throws Exception {
        StreamSegmentMetadata segment1 = buildCompletedStreamSegment(1L, SSN.of(1L, 100L, 0L), 100);
        StreamSegmentMetadata segment2 = buildCompletedStreamSegment(2L, SSN.of(2L, 100L, 0L), 100);
        StreamSegmentMetadata segment3 = buildInprogressStreamSegment(3L);
        StreamSegmentMetadata segment30 = buildCompletedStreamSegment(3L, SSN.of(3L, 100L, 0L), 100);

        assertTrue(StreamSegmentMetadata.COMPARATOR.compare(segment1, segment2) < 0);
        assertTrue(StreamSegmentMetadata.COMPARATOR.compare(segment1, segment3) < 0);
        assertTrue(StreamSegmentMetadata.COMPARATOR.compare(segment2, segment3) < 0);
        assertTrue(StreamSegmentMetadata.COMPARATOR.compare(segment3, segment30) == 0);
    }

    @Test(timeout = 60000)
    public void testDescComparator() throws Exception {
        StreamSegmentMetadata segment1 = buildCompletedStreamSegment(1L, SSN.of(1L, 100L, 0L), 100);
        StreamSegmentMetadata segment2 = buildCompletedStreamSegment(2L, SSN.of(2L, 100L, 0L), 100);
        StreamSegmentMetadata segment3 = buildInprogressStreamSegment(3L);
        StreamSegmentMetadata segment30 = buildCompletedStreamSegment(3L, SSN.of(3L, 100L, 0L), 100);

        assertTrue(StreamSegmentMetadata.DESC_COMPARATOR.compare(segment1, segment2) > 0);
        assertTrue(StreamSegmentMetadata.DESC_COMPARATOR.compare(segment1, segment3) > 0);
        assertTrue(StreamSegmentMetadata.DESC_COMPARATOR.compare(segment2, segment3) > 0);
        assertTrue(StreamSegmentMetadata.DESC_COMPARATOR.compare(segment3, segment30) == 0);
    }
}
