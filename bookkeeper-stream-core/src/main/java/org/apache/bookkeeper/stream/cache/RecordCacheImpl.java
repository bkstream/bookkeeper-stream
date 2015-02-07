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
package org.apache.bookkeeper.stream.cache;

import com.google.common.base.Preconditions;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.SSN;
import org.apache.bookkeeper.stream.common.Resumeable;
import org.apache.bookkeeper.stream.conf.StreamConfiguration;
import org.apache.bookkeeper.stream.exceptions.InvalidRecordException;
import org.apache.bookkeeper.stream.exceptions.OutOfOrderReadException;
import org.apache.bookkeeper.stream.exceptions.StreamException;
import org.apache.bookkeeper.stream.io.Entry;
import org.apache.bookkeeper.stream.io.Record;
import org.apache.bookkeeper.stream.io.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cache to cache records.
 */
public class RecordCacheImpl implements RecordCache {

    private static final Logger logger = LoggerFactory.getLogger(RecordCacheImpl.class);

    /**
     * Create a builder to build record cache.
     *
     * @return record cache builder.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder to build entry cache.
     */
    public static class Builder {

        private String _streamName;
        private StreamConfiguration _streamConf;
        private RecordCachePolicy _cachePolicy;
        private StatsLogger _statsLogger = NullStatsLogger.INSTANCE;

        private Builder() {}

        public Builder streamName(String name) {
            this._streamName = name;
            return this;
        }

        public Builder streamConf(StreamConfiguration conf) {
            this._streamConf = conf;
            return this;
        }

        public Builder cachePolicy(RecordCachePolicy cachePolicy) {
            this._cachePolicy = cachePolicy;
            return this;
        }

        public Builder statsLogger(StatsLogger statsLogger) {
            this._statsLogger = statsLogger;
            return this;
        }

        public RecordCacheImpl build() {
            Preconditions.checkNotNull(_streamName, "No stream name provided");
            Preconditions.checkNotNull(_streamConf, "No stream configuration provided");
            Preconditions.checkNotNull(_cachePolicy, "No cache policy provided.");
            Preconditions.checkNotNull(_statsLogger, "No stats logger provided");

            return new RecordCacheImpl(_streamName, _streamConf, _cachePolicy, _statsLogger);
        }

    }

    private final String streamName;
    private final StreamConfiguration streamConf;
    private final RecordCachePolicy cachePolicy;
    private final StatsLogger statsLogger;
    // cache records
    private final LinkedBlockingQueue<Record> records;
    // cache state
    private SSN lastSSN = SSN.INVALID_SSN;
    private final AtomicReference<StreamException> lastException;
    // cache callback
    private Resumeable cacheCallback = null;
    private final CopyOnWriteArraySet<RecordCacheListener> listeners;

    private RecordCacheImpl(String streamName,
                            StreamConfiguration streamConf,
                            RecordCachePolicy cachePolicy,
                            StatsLogger statsLogger) {
        this.streamName = streamName;
        this.streamConf = streamConf;
        this.cachePolicy = cachePolicy;
        this.statsLogger = statsLogger;
        this.records = new LinkedBlockingQueue<>();

        // cache state
        this.lastException = new AtomicReference<>(null);
        // cache listeners
        this.listeners = new CopyOnWriteArraySet<>();
    }

    @Override
    public void addListener(RecordCacheListener listener) {
        this.listeners.add(listener);
    }

    @Override
    public void removeListener(RecordCacheListener listener) {
        this.listeners.remove(listener);
    }

    private void notifyRecordAvailable() {
        for (RecordCacheListener listener : listeners) {
            listener.onRecordAvailable();
        }
    }

    @Override
    public void addEntry(Entry entry) {
        if (entry.isCommitEntry()) {
            // skip commit entry
            return;
        }
        RecordReader rr = entry.asRecordReader();
        Record record;
        try {
            record = rr.readRecord();
            while (null != record) {
                setLastSSN(record.getSSN());
                addRecord(record);

                // read next record
                record = rr.readRecord();
            }
        } catch (IOException ioe) {
            setLastException(new InvalidRecordException("Found invalid record in entry " + entry.getEntryId()
                    + " of segment " + entry.getSegmentId() + " : ", ioe));
        } catch (StreamException se) {
            setLastException(se);
        }
        // notify listeners that records are ready for consuming
        notifyRecordAvailable();
    }

    private void addRecord(Record record) {
        records.add(record);
        cachePolicy.onRecordAdded(record);
    }

    @Override
    public Record pollNextRecord() throws StreamException {
        StreamException se = lastException.get();
        if (null != se) {
            throw se;
        }

        return removeRecord();
    }

    private Record removeRecord() {
        Record record = records.poll();
        if (null != record) {
            triggerCacheCallback();
            cachePolicy.onRecordRemoved(record);
        }
        return record;
    }

    private synchronized void setLastSSN(SSN ssn) throws StreamException {
        if (lastSSN.compareTo(ssn) >= 0) {
            logger.warn("Received out-of-order record : current = {}, last = {}", ssn, lastSSN);
            throw new OutOfOrderReadException(lastSSN, ssn);
        }
        lastSSN = ssn;
    }

    private void setLastException(StreamException se) {
        lastException.set(se);
    }

    @Override
    public boolean isCacheFull() {
        return cachePolicy.isCacheFull();
    }

    @Override
    public void setReadCallback(Resumeable resumeable) {
        synchronized (this) {
            this.cacheCallback = resumeable;
        }
        if (!isCacheFull()) {
            triggerCacheCallback();
        }
    }

    private void triggerCacheCallback() {
        Resumeable cb;
        synchronized (this) {
            cb = cacheCallback;
            cacheCallback = null;
        }
        if (null != cb) {
            cb.onResume();
        }
    }
}
