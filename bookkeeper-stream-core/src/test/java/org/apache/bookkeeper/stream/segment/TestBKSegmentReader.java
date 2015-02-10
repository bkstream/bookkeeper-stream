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

import com.google.common.util.concurrent.Futures;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stream.SSN;
import org.apache.bookkeeper.stream.cache.RecordCache;
import org.apache.bookkeeper.stream.cache.RecordCacheImpl;
import org.apache.bookkeeper.stream.cache.RecordCacheItemsPolicy;
import org.apache.bookkeeper.stream.common.Scheduler.OrderingListenableFuture;
import org.apache.bookkeeper.stream.conf.StreamConfiguration;
import org.apache.bookkeeper.stream.io.Record;
import org.apache.bookkeeper.stream.segment.SegmentReader.Listener;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

/**
 * Test Case for {@link org.apache.bookkeeper.stream.segment.BKSegmentReader}
 */
public class TestBKSegmentReader extends BKSegmentTestCase {

    private static final int NUM_BOOKIES = 3;

    public TestBKSegmentReader() {
        super(NUM_BOOKIES);
    }

    @Test(timeout = 60000)
    public void testBasicReadRecords() throws Exception {
        StreamConfiguration conf = new StreamConfiguration();
        conf.setSegmentWriterEntryBufferSize(4096);
        conf.setSegmentWriterFlushIntervalMs(999999000);
        conf.setSegmentWriterCommitDelayMs(999999000);

        String streamName = "test-basic-read-records";
        long segmentId = 1L;
        Pair<LedgerHandle, Segment> segmentPair = createInprogressSegment(streamName, segmentId);

        BKSegmentWriter writer = BKSegmentWriter.newBuilder()
                .conf(conf)
                .segment(segmentPair.getRight())
                .ledgerHandle(segmentPair.getLeft())
                .scheduler(scheduler)
                .statsLogger(NullStatsLogger.INSTANCE)
                .build();

        // Write Records

        int numRecords = 10;
        int numLoops = 3;
        for (int i = 0; i < numLoops; i++) {
            List<OrderingListenableFuture<SSN>> writeFutures = new ArrayList<>(numRecords);
            for (int j = 0; j < numRecords; j++) {
                int recordId = i * numRecords + j;
                Record record = Record.newBuilder()
                        .setRecordId(recordId)
                        .setData(("record-" + recordId).getBytes(UTF_8))
                        .build();
                writeFutures.add(writer.write(record));
            }
            writer.flush().get();
            List<SSN> results = Futures.allAsList(writeFutures).get();
            assertEquals(numRecords, results.size());
            writer.commit().get();
        }
        // close writer to complete segment
        SSN lastSSN = writer.close().get();
        // completed segment
        Segment completedSegment =
                completeInprogressSegment(segmentPair.getRight(), lastSSN, numRecords * numLoops);

        StreamConfiguration readConf = new StreamConfiguration();
        readConf.setReaderCacheMaxNumRecords(99999999);
        readConf.setReaderCacheMaxNumBytes(99999999);

        // Read Records
        BKSegmentReader reader = BKSegmentReader.newBuilder()
                .conf(readConf)
                .segment(completedSegment)
                .startEntryId(0L)
                .bookkeeper(bkc)
                .scheduler(scheduler)
                .statsLogger(NullStatsLogger.INSTANCE)
                .build();

        // Record Cache
        RecordCache recordCache = RecordCacheImpl.newBuilder()
                .streamName(streamName)
                .streamConf(readConf)
                .cachePolicy(new RecordCacheItemsPolicy(readConf))
                .statsLogger(NullStatsLogger.INSTANCE)
                .build();

        final CountDownLatch eosLatch = new CountDownLatch(1);
        Listener readerListener = new Listener() {
            @Override
            public void onEndOfSegment() {
                eosLatch.countDown();
            }

            @Override
            public void onError() {
                // no-op
            }
        };

        // start the reader
        reader.start(recordCache, readerListener);

        // wait until reaching end of segment
        eosLatch.await();

        int numReads = 0;
        Record record = recordCache.pollNextRecord();
        while (null != record) {
            assertEquals(numReads, record.getRecordId());
            assertEquals("record-" + numReads, new String(record.getData(), UTF_8));
            ++numReads;

            record = recordCache.pollNextRecord();
        }
        assertEquals(numRecords * numLoops, numReads);

        reader.close().get();
    }
}
