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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.cache.RecordCache;
import org.apache.bookkeeper.stream.common.Resumeable;
import org.apache.bookkeeper.stream.common.Scheduler;
import org.apache.bookkeeper.stream.common.Scheduler.OrderingListenableFuture;
import org.apache.bookkeeper.stream.conf.StreamConfiguration;
import org.apache.bookkeeper.stream.io.Entry;
import org.apache.bookkeeper.stream.segment.Segment.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.bookkeeper.stream.Constants.BK_DIGEST_TYPE;
import static org.apache.bookkeeper.stream.Constants.BK_PASSWD;

/**
 * BookKeeper Based Segment Reader
 */
public class BKSegmentReader implements SegmentReader, Listener, Runnable, Resumeable,
        OpenCallback, ReadLastConfirmedCallback, CloseCallback, ReadCallback {

    private static final Logger logger = LoggerFactory.getLogger(BKSegmentReader.class);

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private StreamConfiguration _conf;
        private Segment _segment;
        private long _startEntryId;
        private BookKeeper _bk;
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
         * Set start entry id to read from.
         *
         * @param entryId start entry id to read from.
         * @return builder
         */
        public Builder startEntryId(long entryId) {
            this._startEntryId = entryId;
            return this;
        }

        /**
         * Set bookkeeper client.
         *
         * @param bk bookkeeper client
         * @return builder
         */
        public Builder bookkeeper(BookKeeper bk) {
            this._bk = bk;
            return this;
        }

        /**
         * Set scheduler used by segment reader.
         *
         * @param scheduler scheduler used by segment reader.
         * @return builder
         */
        public Builder scheduler(Scheduler scheduler) {
            this._scheduler = scheduler;
            return this;
        }

        /**
         * Set stats logger used by segment reader.
         *
         * @param statsLogger stats logger
         * @return builder
         */
        public Builder statsLogger(StatsLogger statsLogger) {
            this._statsLogger = statsLogger;
            return this;
        }

        /**
         * Build the bookkeeper segment reader.
         *
         * @return bookkeeper segment reader.
         */
        public BKSegmentReader build() {
            return new BKSegmentReader(
                    _conf,
                    _segment,
                    _startEntryId,
                    _bk,
                    _scheduler,
                    _statsLogger);
        }

    }

    private static enum State {
        UNINITIALIZED,
        INITIALIZED,
        CLOSING,
        CLOSED
    }

    // stream configuration
    private final StreamConfiguration conf;
    // bookkeeper client
    private final BookKeeper bk;
    // scheduler
    private final Scheduler scheduler;
    // stats logger
    private final StatsLogger statsLogger;

    // Segment Variables
    private final String streamName;
    private final long segmentId;
    private final long ledgerId;

    // ReadAhead Parameters
    private final int readerWaitMs;
    private final int maxOutstandingReads;

    // Read State
    private StreamSegmentMetadata metadata;
    private LedgerHandle lh;
    private boolean started;
    private long nextEntryId;
    private boolean inprogressChanged = false;
    private ListenableFuture<?> waitFuture;

    // reader receiver and listener
    private RecordCache recordCache;
    private Listener readerListener;

    // reader state
    private State state = State.UNINITIALIZED;
    private final Queue<SettableFuture<Void>> closeFutures =
            new LinkedBlockingQueue<>();

    BKSegmentReader(StreamConfiguration conf,
                    Segment segment,
                    long startEntryId,
                    BookKeeper bk,
                    Scheduler scheduler,
                    StatsLogger statsLogger) {
        this.conf = conf;
        this.streamName = segment.getStreamName();
        this.metadata = segment.getSegmentMetadata();
        this.segmentId = metadata.getSegmentFormat().getSegmentId();
        this.ledgerId = metadata.getSegmentFormat().getLedgerId();

        this.bk = bk;
        this.scheduler = scheduler;
        this.statsLogger = statsLogger;

        // reader wait parameters
        this.readerWaitMs = conf.getSegmentReaderCommitWaitMs();
        this.maxOutstandingReads = conf.getSegmentReaderMaxOutstandingReadEntries();

        // read state
        this.nextEntryId = startEntryId;

        // reader state
        this.state = State.INITIALIZED;
    }

    @Override
    public void start(RecordCache cache, Listener listener) {
        synchronized (this) {
            Preconditions.checkState(!started, "SegmentReader for segment " + segmentId + "@" + streamName
                    + " is already started.");
            Preconditions.checkNotNull(cache, "No record cache provided to receive records");
            Preconditions.checkNotNull(listener, "No reader listener provided to receive reader state.");
            started = true;
        }
        this.recordCache = cache;
        this.readerListener = listener;
        // start the reader
        onResume();
    }

    private synchronized void backoff() {
        waitFuture = scheduler.schedule(streamName, this, readerWaitMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        // set wait future to null after backoff is executed.
        waitFuture = null;

        if (State.CLOSED == state) {
            return;
        }

        if (State.CLOSING == state) {
            completeCloseFutures();
            return;
        }

        checkOrOpenLedger();
    }

    /**
     * Open the ledger of this stream segment.
     */
    private void checkOrOpenLedger() {
        if (metadata.isInprogress()) {
            if (null == lh) {
                openLedgerNoRecovery();
            } else {
                readEntriesOrLacFromInprogressSegment();
            }
        } else {
            if (null == lh) {
                openLedger();
            } else {
                if (inprogressChanged) {
                    reopenLedger();
                } else {
                    readEntriesFromCompletedSegment();
                }
            }
        }
    }

    private void reopenLedger() {
        lh.asyncClose(this, null);
    }

    @Override
    public void closeComplete(final int rc, final LedgerHandle lh, final Object ctx) {
        scheduler.submit(streamName, new Runnable() {
            @Override
            public void run() {
                closeCompleted(rc, lh, ctx);
            }
        });
    }

    private void closeCompleted(int rc, LedgerHandle lh, Object ctx) {
        if (BKException.Code.OK != rc) {
            handleException(rc);
            return;
        }
        this.lh = null;
        this.inprogressChanged = false;
        checkOrOpenLedger();
    }

    /**
     * Open the ledger used by the segment (for inprogress stream segment)
     */
    private void openLedgerNoRecovery() {
        this.bk.asyncOpenLedgerNoRecovery(ledgerId, BK_DIGEST_TYPE, BK_PASSWD, this, null);
    }

    /**
     * Open the ledger used by the segment (for complete stream segment)
     */
    private void openLedger() {
        this.bk.asyncOpenLedger(ledgerId, BK_DIGEST_TYPE, BK_PASSWD, this, null);
    }

    @Override
    public void openComplete(final int rc, final LedgerHandle lh, final Object ctx) {
        scheduler.submit(streamName, new Runnable() {
            @Override
            public void run() {
                openCompleted(rc, lh);
            }
        });
    }

    /**
     * Process the result of opening a ledger handle.
     *
     * @param rc result of opening a ledger
     * @param lh ledger handle
     */
    private void openCompleted(int rc, LedgerHandle lh) {
        if (BKException.Code.OK != rc) {
            logger.debug("Encountered bookkeeper exception while opening ledger {} : rc = {}", ledgerId, rc);
            handleException(rc);
            return;
        }
        this.lh = lh;
        logger.info("Opened ledger of segment {} for {}.", metadata, streamName);
        readEntries();
    }

    /**
     * Read entries from completed segment.
     */
    private void readEntriesFromCompletedSegment() {
        long lac = lh.getLastAddConfirmed();
        if (lac < nextEntryId) {
            this.readerListener.onEndOfSegment();
            closeInternal("reaching end of segment");
        } else {
            readEntries();
        }
    }

    /**
     * Read entries or read lac from inprogress segment.
     */
    private void readEntriesOrLacFromInprogressSegment() {
        long lac = lh.getLastAddConfirmed();
        if (lac < nextEntryId) {
            readLac();
        } else {
            readEntries();
        }
    }

    /**
     * Read last add confirmed.
     */
    private void readLac() {
        this.lh.asyncTryReadLastConfirmed(this, null);
    }

    /**
     * Callback of reading last add confirmed.
     *
     * @param rc result of reading last add confirmed.
     * @param lac last add confirmed.
     * @param ctx read last add confirmed context.
     */
    @Override
    public void readLastConfirmedComplete(final int rc, final long lac, final Object ctx) {
        scheduler.submit(streamName, new Runnable() {
            @Override
            public void run() {
                readLastConfirmedCompleted(rc, lac, ctx);
            }
        });
    }

    private void readLastConfirmedCompleted(int rc, long lac, Object ctx) {
        if (BKException.Code.OK != rc) {
            handleReadLastConfirmedError(rc);
            return;
        }
        readEntries();
    }

    /**
     * Handle error on reading last add confirmed.
     *
     * @param rc result of reading last add confirmed.
     */
    private void handleReadLastConfirmedError(int rc) {
        if (BKException.Code.NoSuchLedgerExistsException == rc) {
            // backoff to wait until first entry published
            backoff();
        } else if (BKException.Code.OK != rc) {
            handleException(rc);
        }
    }

    private void readEntries() {
        long lac = lh.getLastAddConfirmed();
        if (lac < nextEntryId) {
            logger.debug("Nothing to read from segment {} of {} : lac = {}, next entry = {}",
                    new Object[] { segmentId, streamName, lac, nextEntryId });
            readEntriesComplete();
            return;
        }
        long startEntryId = nextEntryId;
        long endEntryId = Math.min(lac, nextEntryId + maxOutstandingReads - 1);
        readEntries(startEntryId, endEntryId);
    }

    private void readEntries(long startEntryId, long endEntryId) {
        lh.asyncReadEntries(startEntryId, endEntryId, this, null);
    }

    @Override
    public void readComplete(final int rc, final LedgerHandle lh,
                             final Enumeration<LedgerEntry> entries,
                             final Object ctx) {
        scheduler.submit(streamName, new Runnable() {
            @Override
            public void run() {
                readCompleted(rc, lh, entries, ctx);
            }
        });
    }

    private void readCompleted(int rc, LedgerHandle lh, Enumeration<LedgerEntry> entries, Object ctx) {
        if (Code.OK != rc) {
            handleException(rc);
            return;
        }
        while (entries.hasMoreElements()) {
            LedgerEntry ledgerEntry = entries.nextElement();
            byte[] entryData = ledgerEntry.getEntry();
            Entry entry = Entry.of(segmentId, ledgerEntry.getEntryId(), entryData, 0, entryData.length);
            recordCache.addEntry(entry);
            // advance entry id
            ++nextEntryId;
        }
        if (recordCache.isCacheFull()) {
            readEntriesComplete();
        } else {
            // continue to read entries
            readEntries();
        }
    }

    private void readEntriesComplete() {
        if (recordCache.isCacheFull()) {
            logger.debug("Record cache for {} is full. ");
            recordCache.setReadCallback(this);
        } else if (this.metadata.isInprogress()) {
            logger.debug("Reach end of inprogress segment {} for {}. Backoff reading for {} ms.",
                    new Object[] { segmentId, streamName, readerWaitMs });
            backoff();
        } else {
            checkOrOpenLedger();
        }
    }

    private void handleException(int rc) {
        if (Code.OK != rc) {
            logger.error("Encountered exception on reading segment {} of stream {} : bk rc = {}",
                    new Object[]{segmentId, streamName, rc});
            this.readerListener.onError();
            closeInternal("encountered exceptions");
        } else {
            onResume();
        }
    }

    @Override
    public void onResume() {
        if (logger.isTraceEnabled()) {
            logger.trace("Resume reading records from segment {} of stream {} : next entry id = {}, metadata = {}",
                    new Object[] { segmentId, streamName, nextEntryId, metadata });
        }
        scheduler.submit(streamName, this);
    }

    @Override
    public void onSegmentChanged(final StreamSegmentMetadata metadata) {
        if (segmentId == metadata.getSegmentFormat().getSegmentId()) {
            scheduler.submit(streamName, new Runnable() {
                @Override
                public void run() {
                    updateSegment(metadata);
                }
            });
        }
    }

    private void updateSegment(StreamSegmentMetadata newMetadata) {
        StreamSegmentMetadata oldMetadata = this.metadata;
        this.metadata = newMetadata;
        logger.info("Segment {} @ {} is updated from : {} -> {}.",
                new Object[] { segmentId, streamName, oldMetadata, newMetadata });

        inprogressChanged = oldMetadata.isInprogress() && newMetadata.isCompleted();
        interrupt();
    }

    /**
     * Interrupt the wait future.
     */
    private void interrupt() {
        if (null != waitFuture) {
            logger.debug("Interrupting wait future on reader for segment {} @ {}.", segmentId, streamName);
            // as the same thread guarantee, the wait future won't be run
            if (!waitFuture.cancel(true)) {
                logger.warn("Failed to cancel reader waiting when segment {} @ {} is update.",
                        segmentId, streamName);
            } else {
                onResume();
            }
            waitFuture = null;
        }
    }

    @Override
    public OrderingListenableFuture<Void> close() {
        final SettableFuture<Void> future = SettableFuture.create();
        scheduler.submit(streamName, new Runnable() {
            @Override
            public void run() {
                close0(future);
            }
        });
        return scheduler.createOrderingFuture(streamName, future);
    }

    private void closeInternal(final String reason) {
        SettableFuture<Void> future = SettableFuture.create();
        Futures.addCallback(future, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                logger.info("Closed reader on segment {} of {} due to {}",
                        new Object[] { segmentId, streamName, reason });
            }

            @Override
            public void onFailure(Throwable t) {
                logger.info("Failed to close reader on segment {} of {} due to {} : ",
                        new Object[] { segmentId, streamName, reason, t });
            }
        });
        close0(future);
        completeCloseFutures();
    }

    private void close0(SettableFuture<Void> future) {
        if (State.CLOSED == state) {
            future.set(null);
            return;
        }
        closeFutures.add(future);
        if (State.CLOSING == state) {
            return;
        }
        this.state = State.CLOSING;
        interrupt();
    }

    private void completeCloseFutures() {
        this.state = State.CLOSED;

        if (null != lh) {
            lh.asyncClose(new CloseCallback() {
                @Override
                public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
                    scheduler.submit(streamName, new Runnable() {
                        @Override
                        public void run() {
                            completeCloseFutures0();
                        }
                    });
                }
            }, null);
            lh = null;
        } else {
            completeCloseFutures0();
        }
    }

    private void completeCloseFutures0() {
        SettableFuture<Void> closeFuture;
        while (null != (closeFuture = closeFutures.poll())) {
            closeFuture.set(null);
        }
    }
}
