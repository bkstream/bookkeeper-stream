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
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stream.SSN;
import org.apache.bookkeeper.stream.common.Scheduler.OrderingListenableFuture;
import org.apache.bookkeeper.stream.conf.StreamConfiguration;
import org.apache.bookkeeper.stream.exceptions.WriteCancelledException;
import org.apache.bookkeeper.stream.io.Entry;
import org.apache.bookkeeper.stream.io.Record;
import org.apache.bookkeeper.stream.io.RecordReader;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.bookkeeper.stream.Constants.BK_DIGEST_TYPE;
import static org.apache.bookkeeper.stream.Constants.BK_PASSWD;
import static org.junit.Assert.*;
import static com.google.common.base.Charsets.*;

/**
 * Test Case for {@link org.apache.bookkeeper.stream.segment.BKSegmentWriter}
 */
public class TestBKSegmentWriter extends BKSegmentTestCase {

    private static final int NUM_BOOKIES = 3;

    public TestBKSegmentWriter() {
        super(NUM_BOOKIES);
    }

    @Test(timeout = 60000)
    public void testWriteRecords() throws Exception {
        StreamConfiguration conf = new StreamConfiguration();
        conf.setSegmentWriterEntryBufferSize(1024);
        conf.setSegmentWriterFlushIntervalMs(999999000);
        conf.setSegmentWriterCommitDelayMs(999999000);

        String streamName = "test-write-records";
        long segmentId = 1L;
        Pair<LedgerHandle, Segment> segmentPair = createInprogressSegment(streamName, segmentId);

        BKSegmentWriter writer = BKSegmentWriter.newBuilder()
                .conf(conf)
                .segment(segmentPair.getRight())
                .ledgerHandle(segmentPair.getLeft())
                .scheduler(scheduler)
                .statsLogger(NullStatsLogger.INSTANCE)
                .build();

        int numRecords = 3;
        List<OrderingListenableFuture<SSN>> writeFutures = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            Record record = Record.newBuilder()
                    .setRecordId(i)
                    .setData(("record-" + i).getBytes(UTF_8))
                    .build();
            writeFutures.add(writer.write(record));
        }
        OrderingListenableFuture<SSN> flushFuture = writer.flush();
        SSN lastFlushedSSN = flushFuture.get();
        assertEquals(SSN.of(segmentId, 0L, numRecords - 1), lastFlushedSSN);
        List<SSN> results = Futures.allAsList(writeFutures).get();
        assertEquals(numRecords, results.size());
        for (int i = 0; i < numRecords; i++) {
            assertEquals(SSN.of(segmentId, 0L, i), results.get(i));
        }

        // entry is flushed but not committed
        assertEquals(0L, segmentPair.getLeft().getLastAddConfirmed());

        // commit records
        OrderingListenableFuture<SSN> commitFuture = writer.commit();
        SSN lastCommittedSSN = commitFuture.get();
        assertEquals(SSN.of(segmentId, 0L, numRecords - 1), lastCommittedSSN);

        // entry is committed
        assertEquals(1L, segmentPair.getLeft().getLastAddConfirmed());

        LedgerHandle openLh = this.bkc.openLedgerNoRecovery(segmentPair.getLeft().getId(), BK_DIGEST_TYPE, BK_PASSWD);
        long lac = openLh.readLastConfirmed();
        assertEquals(0L, lac);

        // read entries
        Enumeration<LedgerEntry> entries = openLh.readEntries(0L, 0L);
        assertTrue(entries.hasMoreElements());
        LedgerEntry entry = entries.nextElement();
        byte[] entryData = entry.getEntry();
        RecordReader rr = Entry.of(segmentId, entry.getEntryId(), entryData, 0, entryData.length).asRecordReader();
        int numReads = 0;
        Record record = rr.readRecord();
        while (null != record) {
            assertEquals(numReads, record.getRecordId());
            assertEquals(SSN.of(segmentId, entry.getEntryId(), numReads), record.getSSN());
            assertEquals("record-" + numReads, new String(record.getData(), UTF_8));

            ++numReads;
            record = rr.readRecord();
        }
        assertEquals(numReads, numReads);

        SSN lastSSN = writer.close().get();
        assertEquals(SSN.of(segmentId, 0L, numRecords - 1), lastSSN);

        // double close
        lastSSN = writer.close().get();
        assertEquals(SSN.of(segmentId, 0L, numRecords - 1), lastSSN);
    }

    @Test(timeout = 60000)
    public void testWriteRecordsAfterClose() throws Exception {
        StreamConfiguration conf = new StreamConfiguration();
        conf.setSegmentWriterCommitDelayMs(0);
        conf.setSegmentWriterEntryBufferSize(0);

        String streamName = "test-write-records-after-close";
        long segmentId = 1L;
        Pair<LedgerHandle, Segment> segmentPair = createInprogressSegment(streamName, segmentId);

        BKSegmentWriter writer = BKSegmentWriter.newBuilder()
                .conf(conf)
                .segment(segmentPair.getRight())
                .ledgerHandle(segmentPair.getLeft())
                .scheduler(scheduler)
                .statsLogger(NullStatsLogger.INSTANCE)
                .build();

        // close ledger handle
        writer.close();

        int numRecords = 3;
        List<OrderingListenableFuture<SSN>> writeFutures = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            Record record = Record.newBuilder()
                    .setRecordId(i)
                    .setData(("record-" + i).getBytes(UTF_8))
                    .build();
            writeFutures.add(writer.write(record));
        }

        for (OrderingListenableFuture<SSN> future : writeFutures) {
            try {
                future.get();
                fail("Should fail writing record after writer is closed");
            } catch (ExecutionException wce) {
                Throwable cause = wce.getCause();
                assertEquals(WriteCancelledException.class, cause.getClass());
            }
        }

        assertEquals(SSN.of(segmentId, -1L, -1L), writer.flush().get());
        assertEquals(SSN.of(segmentId, -1L, -1L), writer.commit().get());
    }

    @Test(timeout = 60000)
    public void testOperationsOnErrorWriter() throws Exception {
        StreamConfiguration conf = new StreamConfiguration();
        conf.setSegmentWriterCommitDelayMs(999999000);
        conf.setSegmentWriterFlushIntervalMs(999999000);
        conf.setSegmentWriterEntryBufferSize(4096);

        String streamName = "test-operations-on-error-writer";
        long segmentId = 1L;
        Pair<LedgerHandle, Segment> segmentPair = createInprogressSegment(streamName, segmentId);

        BKSegmentWriter writer = BKSegmentWriter.newBuilder()
                .conf(conf)
                .segment(segmentPair.getRight())
                .ledgerHandle(segmentPair.getLeft())
                .scheduler(scheduler)
                .statsLogger(NullStatsLogger.INSTANCE)
                .build();

        int numRecords = 5;
        List<OrderingListenableFuture<SSN>> writeFutures = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            Record record = Record.newBuilder()
                    .setRecordId(i)
                    .setData(("record-" + i).getBytes(UTF_8))
                    .build();
            writeFutures.add(writer.write(record));
        }

        // close the ledger to fence writes
        LedgerHandle openLh = bkc.openLedger(segmentPair.getLeft().getId(), BK_DIGEST_TYPE, BK_PASSWD);
        openLh.close();

        OrderingListenableFuture<SSN> flushFuture = writer.flush();
        assertFuture(flushFuture, Code.LedgerFencedException);
        // checking write results
        for (OrderingListenableFuture<SSN> future : writeFutures) {
            assertFuture(future, Code.LedgerFencedException);
        }

        // write record to an error writer will be cancelled
        Record record = Record.newBuilder()
                .setRecordId(numRecords)
                .setData(("record-" + numRecords).getBytes(UTF_8))
                .build();
        OrderingListenableFuture<SSN> writeFuture = writer.write(record);
        assertFuture(writeFuture, WriteCancelledException.class);

        // flush and commit will return the last flushed ssn
        assertEquals(SSN.of(segmentId, -1L, -1L), writer.flush().get());
        assertEquals(SSN.of(segmentId, -1L, -1L), writer.commit().get());
        assertEquals(SSN.of(segmentId, -1L, -1L), writer.close().get());
    }

}
