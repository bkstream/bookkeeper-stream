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

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stream.SSN;
import org.apache.bookkeeper.stream.common.Scheduler;
import org.apache.bookkeeper.stream.common.Scheduler.OrderingListenableFuture;
import org.apache.bookkeeper.stream.exceptions.BKException;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamSegmentMetadataFormat;
import org.apache.bookkeeper.stream.proto.DataFormats.StreamSegmentMetadataFormat.State;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.ExecutionException;

import static org.apache.bookkeeper.stream.Constants.BK_DIGEST_TYPE;
import static org.apache.bookkeeper.stream.Constants.BK_PASSWD;
import static org.junit.Assert.*;

/**
 * Basic Test Case for BK Segments
 */
public class BKSegmentTestCase extends BookKeeperClusterTestCase {

    protected static class TestSegment implements Segment {

        private final String streamName;
        private final StreamSegmentMetadata segmentMetadata;

        TestSegment(String streamName, StreamSegmentMetadata segmentMetadata) {
            this.streamName = streamName;
            this.segmentMetadata = segmentMetadata;
        }

        @Override
        public String getStreamName() {
            return streamName;
        }

        @Override
        public StreamSegmentMetadata getSegmentMetadata() {
            return segmentMetadata;
        }

        @Override
        public void registerSegmentListener(Listener listener) {
            // no-op
        }
    }

    protected final Scheduler scheduler;

    public BKSegmentTestCase(int numBookies) {
        super(numBookies);
        scheduler = Scheduler.newBuilder()
                .name("bk-segment-testcase")
                .numExecutors(4)
                .build();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        scheduler.shutdown();
    }

    protected Pair<LedgerHandle, Segment> createInprogressSegment(String streamName, long segmentId)
            throws Exception {
        long curTime = System.currentTimeMillis();
        LedgerHandle lh = this.bkc.createLedger(2, 2, 2, BK_DIGEST_TYPE, BK_PASSWD);
        StreamSegmentMetadataFormat.Builder metadataBuilder =
                StreamSegmentMetadataFormat.newBuilder()
                        .setSegmentId(segmentId)
                        .setLedgerId(lh.getId())
                        .setState(State.INPROGRESS)
                        .setCTime(curTime)
                        .setMTime(curTime);
        StreamSegmentMetadata segmentMetadata = StreamSegmentMetadata.newBuilder()
                .setSegmentName(StreamSegmentMetadata.segmentName(segmentId, true))
                .setStreamSegmentMetadataFormatBuilder(metadataBuilder)
                .build();
        Segment segment = new TestSegment(streamName, segmentMetadata);
        return Pair.of(lh, segment);
    }

    protected Segment completeInprogressSegment(Segment segment, SSN lastSSN, int numRecords) {
        long completionTime = System.currentTimeMillis();
        StreamSegmentMetadataFormat inprogressSegmentMetadata =
                segment.getSegmentMetadata().getSegmentFormat();
        StreamSegmentMetadataFormat.Builder metadataBuilder =
                StreamSegmentMetadataFormat.newBuilder(inprogressSegmentMetadata)
                        .setState(State.COMPLETED)
                        .setLastSSN(lastSSN.toProtoSSN())
                        .setRecordCount(numRecords)
                        .setMTime(completionTime)
                        .setCompletionTime(completionTime);
        StreamSegmentMetadata completedSegmentMetadata = StreamSegmentMetadata.newBuilder()
                .setSegmentName(StreamSegmentMetadata.segmentName(inprogressSegmentMetadata.getSegmentId(), false))
                .setStreamSegmentMetadataFormatBuilder(metadataBuilder)
                .build();
        return new TestSegment(segment.getStreamName(), completedSegmentMetadata);
    }

    static void assertFuture(OrderingListenableFuture<SSN> future, int expectedRc) throws InterruptedException {
        try {
            future.get();
            fail("Should fail the operation since ledger handle is fenced");
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            assertEquals(BKException.class, cause.getClass());
            BKException bke = (BKException) cause;
            assertEquals(expectedRc, bke.getBkCode());
        }
    }

    static void assertFuture(OrderingListenableFuture<SSN> future, Class<? extends Exception> expectedClass)
        throws InterruptedException {
        try {
            future.get();
            fail("Should cancel the operation since the writer is already in error state");
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            assertEquals(expectedClass, cause.getClass());
        }
    }
}
