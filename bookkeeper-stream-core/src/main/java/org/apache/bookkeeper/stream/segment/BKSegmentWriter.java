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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.SSN;
import org.apache.bookkeeper.stream.common.Scheduler;
import org.apache.bookkeeper.stream.common.Scheduler.OrderingListenableFuture;
import org.apache.bookkeeper.stream.conf.StreamConfiguration;
import org.apache.bookkeeper.stream.exceptions.BKException;
import org.apache.bookkeeper.stream.exceptions.WriteCancelledException;
import org.apache.bookkeeper.stream.io.Entry;
import org.apache.bookkeeper.stream.io.Entry.EntryBuilder;
import org.apache.bookkeeper.stream.io.Entry.EntryData;
import org.apache.bookkeeper.stream.io.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * BookKeeper Based Segment Writer
 */
public class BKSegmentWriter implements SegmentWriter, AddCallback {

    private static final Logger logger = LoggerFactory.getLogger(BKSegmentWriter.class);

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private StreamConfiguration _conf;
        private Segment _segment;
        private LedgerHandle _lh;
        private Scheduler _scheduler;
        private StatsLogger _statsLogger = NullStatsLogger.INSTANCE;

        private Builder() {}

        /**
         * Set stream configuration.
         *
         * @param conf stream configuration
         * @return builder
         */
        public Builder conf(StreamConfiguration conf) {
            this._conf = conf;
            return this;
        }

        /**
         * Set segment
         *
         * @param segment segment
         * @return builder
         */
        public Builder segment(Segment segment) {
            this._segment = segment;
            return this;
        }

        /**
         * Set ledger handle to build segment writer.
         *
         * @param lh ledger handle
         * @return builder
         */
        public Builder ledgerHandle(LedgerHandle lh) {
            this._lh = lh;
            return this;
        }

        /**
         * Set scheduler to build segment writer.
         *
         * @param scheduler scheduler used by segment writer.
         * @return builder
         */
        public Builder scheduler(Scheduler scheduler) {
            this._scheduler = scheduler;
            return this;
        }

        /**
         * Set stats logger used by segment writer.
         *
         * @param statsLogger stats logger
         * @return builder
         */
        public Builder statsLogger(StatsLogger statsLogger) {
            this._statsLogger = statsLogger;
            return this;
        }

        /**
         * Build the bookkeeper segment writer.
         *
         * @return bookkeeper segment writer.
         * @throws IOException on failing to build segment writer.
         */
        public BKSegmentWriter build() throws IOException {
            return new BKSegmentWriter(
                    _conf,
                    _segment,
                    _lh,
                    _scheduler,
                    _statsLogger);
        }

    }

    private static class AddEntryContext {
        private final Entry entry;
        private final SettableFuture<SSN> future;

        private AddEntryContext(Entry entry, SettableFuture<SSN> future) {
            this.entry = entry;
            this.future = future;
        }
    }

    // stream configuration
    private final StreamConfiguration conf;
    private final int entryBufferSize;
    private final int commitDelayMs;
    // scheduler
    private final Scheduler scheduler;
    // stats logger
    private final StatsLogger statsLogger;

    // Segment Variables
    private final String streamName;
    private final String segmentName;
    private final long segmentId;
    private final LedgerHandle lh;

    // pending entries
    private long lastNumRecords = 0L;
    private long lastNumBytes = 0L;
    private long numEntries = 0;
    private EntryBuilder curEntryBuilder;
    private final Queue<Entry> pendingEntries = new ConcurrentLinkedQueue<>();
    private final Queue<SettableFuture<SSN>> errorQueue = new ConcurrentLinkedQueue<>();

    // flush state
    private int lastBkResult = Code.OK;
    private boolean hasDataUncommitted = false;
    private boolean isCommitScheduled = false;
    private SSN lastFlushedSSN;

    // stream state
    private boolean inErrorState = false;
    private boolean closed = false;

    BKSegmentWriter(StreamConfiguration conf,
                    Segment segment,
                    LedgerHandle lh,
                    Scheduler scheduler,
                    StatsLogger statsLogger) throws IOException {
        this.conf = conf;
        this.streamName = segment.getStreamName();
        this.segmentName = segment.getSegmentMetadata().getSegmentName();
        this.segmentId = segment.getSegmentMetadata().getSegmentFormat().getSegmentId();
        this.lh = lh;
        this.scheduler = scheduler;
        this.statsLogger = statsLogger;
        // settings
        this.entryBufferSize = conf.getSegmentWriterEntryBufferSize();
        this.commitDelayMs = conf.getSegmentWriterCommitDelayMs();

        // entry
        this.curEntryBuilder = nextEntryBuilder();
        // flush state
        lastFlushedSSN = SSN.of(segmentId, -1L, -1L);
    }

    private EntryBuilder nextEntryBuilder() throws IOException {
        // estimate average entry size
        int avgEntrySize = 0;
        if (numEntries > 0) {
            avgEntrySize = (int) (lastNumBytes / numEntries);
        }
        return Entry.newBuilder(segmentId, -1L,
                lastNumRecords, lastNumBytes, Math.max(entryBufferSize, avgEntrySize));
    }

    /**
     * Write record to the segment.
     *
     * @param record record to write
     * @return future representing the written result.
     */
    @Override
    public OrderingListenableFuture<SSN> write(final Record record) {
        final SettableFuture<SSN> future = SettableFuture.create();
        scheduler.submit(streamName, new Runnable() {
            @Override
            public void run() {
                write0(record, future);
            }
        });
        return scheduler.createOrderingFuture(streamName, future);
    }

    private void write0(Record record, SettableFuture<SSN> future) {
        if (inErrorState) {
            if (pendingEntries.isEmpty()) {
                future.setException(new WriteCancelledException(
                        "Writing record cancelled because segment " + segmentName + "@"
                                + streamName + " is already in error state : bk result = " + lastBkResult));
            } else {
                errorQueue.add(future);
            }
            return;
        }

        if (closed) {
            if (pendingEntries.isEmpty()) {
                future.setException(new WriteCancelledException(
                        "Writing record cancelled because segment " + segmentName + "@"
                                + streamName + " is closed : bk result = " + lastBkResult));
            } else {
                errorQueue.add(future);
            }
            return;
        }

        try {
            curEntryBuilder.addRecord(record, future);
        } catch (IOException ioe) {
            logger.error("Encountered unexpected exception on adding record {} to {}@{} : ",
                    new Object[] { record, segmentName, streamName, ioe });
            inErrorState = true;
            if (pendingEntries.isEmpty()) {
                WriteCancelledException wce = new WriteCancelledException(
                        "Writing record cancelled because segment " + segmentName + "@"
                                + streamName + " encountered exception on current entry : ", ioe);
                cancelCurrentEntry(wce);
                future.setException(wce);
            } else {
                Entry entry = curEntryBuilder.asDataEntry().build();
                List<SettableFuture<SSN>> futureList = entry.getRecordFutureList().get();
                errorQueue.addAll(futureList);
                errorQueue.add(future);
                curEntryBuilder = null;
            }
            return;
        }
        flushIfNeeded();
    }

    /**
     * Flush current entry buffer if needed
     */
    private void flushIfNeeded() {
        if (null != curEntryBuilder &&
                curEntryBuilder.getBufferSize() > entryBufferSize) {
            flush0(false, null);
        }
    }

    @Override
    public OrderingListenableFuture<SSN> flush() {
        final SettableFuture<SSN> future = SettableFuture.create();
        scheduler.submit(streamName, new Runnable() {
            @Override
            public void run() {
                flush0(false, future);
            }
        });
        return scheduler.createOrderingFuture(streamName, future);
    }

    /**
     * Flush current entry buffer as an entry to bookkeeper.
     */
    private void flush0(boolean isCommitEntry,
                        SettableFuture<SSN> future) {
        if (null == curEntryBuilder) {
            if (null != future) {
                future.set(lastFlushedSSN);
            }
            return;
        }

        // 1. build current entry
        EntryBuilder entryBuilder = curEntryBuilder;
        if (isCommitEntry) {
            entryBuilder.asCommitEntry();
        } else {
            entryBuilder.asDataEntry();
        }
        Entry entry = entryBuilder.build();

        // 2. add current entry to pending queue
        pendingEntries.add(entry);

        // 3. flush current entry to bookkeeper
        AddEntryContext addCtx = new AddEntryContext(entry, future);
        EntryData entryData = entry.getEntryData();
        lh.asyncAddEntry(entryData.data, entryData.offset, entryData.len, this, addCtx);

        // 4. flushing an entry will commit any uncommitted data.
        hasDataUncommitted = false;

        // 5. create next entry builder
        lastNumBytes = entry.getLastNumBytes() + entry.getNumBytes();
        lastNumRecords = entry.getLastNumRecords() + entry.getNumRecords();
        ++numEntries;

        try {
            curEntryBuilder = nextEntryBuilder();
        } catch (IOException e) {
            logger.error("Error on creating next entry on segment {}@{} : ",
                    new Object[] { segmentName, streamName, e });
            inErrorState = true;
            curEntryBuilder = null;
        }
    }

    /**
     * Commit already flushed data. After commit completed, those flushed data is readable
     * by readers.
     *
     * @return future representing commit result
     */
    @Override
    public OrderingListenableFuture<SSN> commit() {
        final SettableFuture<SSN> future = SettableFuture.create();
        scheduler.submit(streamName, new Runnable() {
            @Override
            public void run() {
                commit(future);
            }
        });
        return scheduler.createOrderingFuture(streamName, future);
    }

    /**
     * Commit any flushed data. After commit completed, those flushed data is readable
     * on reader side.
     */
    private void commit(SettableFuture<SSN> future) {
        isCommitScheduled = false;
        boolean hasPendingRecords = null != curEntryBuilder &&
                curEntryBuilder.getNumPendingRecords() > 0;
        if (hasDataUncommitted || hasPendingRecords) {
            flush0(!hasPendingRecords, future);
        } else {
            if (null != future) {
                future.set(lastFlushedSSN);
            }
        }
    }

    /**
     * Schedule a commit to commit previous flushed entries.
     */
    private void scheduleCommit() {
        if (isCommitScheduled) {
            return;
        }
        scheduler.schedule(streamName, new Runnable() {
            @Override
            public void run() {
                commit(null);
            }
        }, commitDelayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void addComplete(final int rc,
                            final LedgerHandle lh,
                            final long entryId,
                            final Object ctx) {
        final AddEntryContext addCtx = (AddEntryContext) ctx;
        scheduler.submit(streamName, new Runnable() {
            @Override
            public void run() {
                addComplete0(rc, entryId, addCtx);
            }
        });
    }

    private void addComplete0(int rc, long entryId, AddEntryContext addCtx) {
        if (Code.OK != lastBkResult) {
            // all pending entries are already error out.
            return;
        }

        if (Code.OK == rc) {
            // entry is flushed to bookkeeper, but not committed yet.
            if (addCtx.entry.isDataEntry()) {
                hasDataUncommitted = true;
                scheduleCommit();
            }
            completeEntry(entryId, addCtx.entry);
            if (addCtx.future != null) {
                addCtx.future.set(lastFlushedSSN);
            }
        } else {
            lastBkResult = rc;
            BKException bkException = new BKException(rc,
                    org.apache.bookkeeper.client.BKException.getMessage(rc));
            // error out all pending entries
            errorOutPendingEntries(bkException);
            // cancel current entry
            cancelCurrentEntry(bkException);
            if (addCtx.future != null) {
                addCtx.future.setException(new BKException(rc,
                        org.apache.bookkeeper.client.BKException.getMessage(rc)));
            }
        }

        if ((inErrorState || closed) && pendingEntries.isEmpty()) {
            WriteCancelledException wce = new WriteCancelledException(
                    "Writing record cancelled because segment " + segmentName + "@"
                    + streamName + " is : closed = " + closed + ", in error state = "
                    + inErrorState);
            SettableFuture<SSN> future;
            while ((future = errorQueue.poll()) != null) {
                future.setException(wce);
            }
        }
    }

    /**
     * Complete <i>entry</i> with <i>entryId</i>.
     *
     * @param entryId entry id
     * @param entry entry to complete
     */
    private void completeEntry(long entryId, Entry entry) {
        pendingEntries.remove(entry);
        SSN lastSSNInEntry = entry.completeRecordFutures(entryId);
        if (lastSSNInEntry.compareTo(lastFlushedSSN) > 0) {
            lastFlushedSSN = lastSSNInEntry;
        }
    }

    /**
     * Error out all pending entries when encountered error from bookkeeper.
     *
     * @param t exception to error out pending entries.
     */
    private void errorOutPendingEntries(Throwable t) {
        Entry entry;
        while (null != (entry = pendingEntries.poll())) {
            entry.cancelRecordFutures(t);
        }
    }

    /**
     * Cancel current entry by error out its records.
     *
     * @param t exception to cancel current entry
     */
    private void cancelCurrentEntry(Throwable t) {
        if (null == curEntryBuilder) {
            return;
        }

        WriteCancelledException wce =
                new WriteCancelledException("Record written to segment " + segmentName
                        + "@" + streamName + " is cancelled because encountering bookkeeper exception : ", t);
        Entry entry = curEntryBuilder.asDataEntry().build();
        entry.cancelRecordFutures(wce);
    }

    @Override
    public OrderingListenableFuture<SSN> close() {
        final SettableFuture<SSN> future = SettableFuture.create();
        scheduler.submit(streamName, new Runnable() {
            @Override
            public void run() {
                close0(future);
            }
        });
        return scheduler.createOrderingFuture(streamName, future);
    }

    private void close0(SettableFuture<SSN> closeFuture) {
        if (closed) {
            closeFuture.set(null);
            return;
        }
        closed = true;
        if (Code.OK == lastBkResult) {
            flushAndCloseLedger(closeFuture);
            return;
        }
        // segment is already in a bad state.
        errorQueue.add(closeFuture);
    }

    /**
     * Flush buffered records adn close ledger.
     *
     * @param closeFuture future representing the close result.
     */
    private void flushAndCloseLedger(final SettableFuture<SSN> closeFuture) {
        final SettableFuture<SSN> flushFuture = SettableFuture.create();
        flush0(false, flushFuture);
        Futures.addCallback(flushFuture, new FutureCallback<SSN>() {
            @Override
            public void onSuccess(SSN ssn) {
                closeLedger(ssn, closeFuture);
            }

            @Override
            public void onFailure(Throwable t) {
                closeFuture.setException(t);
            }
        });
    }

    /**
     * Close the ledger.
     *
     * @param closeFuture future representing the close result.
     */
    private void closeLedger(final SSN ssn, final SettableFuture<SSN> closeFuture) {
        lh.asyncClose(new CloseCallback() {
            @Override
            public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
                if (Code.OK != rc) {
                    closeFuture.setException(new BKException(rc, "Failed to close ledger " + lh.getId() + " : "
                            + org.apache.bookkeeper.client.BKException.getMessage(rc)));
                } else {
                    closeFuture.set(ssn);
                }
            }
        }, null);
    }

}

