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
package org.apache.bookkeeper.stream.conf;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;

import java.net.URL;

import static org.apache.bookkeeper.stream.Constants.KB;
import static org.apache.bookkeeper.stream.Constants.MB;

/**
 * Configuration
 */
public class StreamConfiguration extends CompositeConfiguration {

    // ZooKeeper Settings
    private static final String ZK_SESSION_TIMEOUT_MS = "zk.session.timeout.ms";
    private static final int ZK_SESSION_TIMEOUT_MS_DEFAULT = 30000;

    // Writer Settings
    private static final String SEGMENT_WRITER_ENTRY_BUFFER_SIZE = "segment.writer.entry.buffer.size";
    private static final int SEGMENT_WRITER_ENTRY_BUFFER_SIZE_DEFAULT = 128 * KB;
    private static final String SEGMENT_WRITER_COMMIT_DELAY_MS = "segment.writer.commit.delay.ms";
    private static final int SEGMENT_WRITER_COMMIT_DELAY_MS_DEFAULT = 20;
    private static final String SEGMENT_WRITER_FLUSH_INTERVAL_MS = "segment.writer.flush.interval.ms";
    private static final int SEGMENT_WRITER_FLUSH_INTERVAL_MS_DEFAULT = 20;

    // Reader Settings
    private static final String SEGMENT_READER_COMMIT_WAIT_MS = "segment.reader.commit.wait.ms";
    private static final int SEGMENT_READER_COMMIT_WAIT_MS_DEFAULT = 100;
    private static final String SEGMENT_READER_MAX_OUTSTANDING_READ_ENTRIES =
            "segment.reader.max.outstanding.read.entries";
    private static final int SEGMENT_READER_MAX_OUTSTANDING_READ_ENTRIES_DEFAULT = 20;

    // Cache Settings
    private static final String READER_CACHE_MAX_NUM_RECORDS = "reader.cache.max.num.records";
    private static final int READER_CACHE_MAX_NUM_RECORDS_DEFAULT = 1000000;
    private static final String READER_CACHE_MAX_NUM_BYTES = "reader.cache.max.num.bytes";
    private static final int READER_CACHE_MAX_NUM_BYTES_DEFAULT = 64 * 1024 * 1024; // 64M

    public StreamConfiguration() {
        super();
        addConfiguration(new SystemConfiguration());
    }

    /**
     * Load configuration from a given <i>url</i>.
     *
     * @param url url to load configuration from.
     * @throws ConfigurationException
     * @return stream configuration
     */
    public StreamConfiguration loadConf(URL url) throws ConfigurationException {
        Configuration loadedConf = new PropertiesConfiguration(url);
        addConfiguration(loadedConf);
        return this;
    }

    /**
     * Load configuration other configuration object <i>conf</i>.
     *
     * @param conf other configuration object.
     * @return stream configuration.
     */
    public StreamConfiguration loadConf(Configuration conf) {
        addConfiguration(conf);
        return this;
    }

    /**
     * Validate if current configuration is valid.
     *
     * @throws ConfigurationException
     */
    public void validate() throws ConfigurationException {
        if (getSegmentWriterEntryBufferSize() > MB) {
            throw new ConfigurationException("Too large write entry buffer size " + getSegmentWriterEntryBufferSize());
        }
    }

    /**
     * Get ZooKeeper Session Timeout In Millis.
     *
     * @return zookeeper session timeout in millis.
     */
    public int getZkSessionTimeoutMs() {
        return getInt(ZK_SESSION_TIMEOUT_MS, ZK_SESSION_TIMEOUT_MS_DEFAULT);
    }

    /**
     * Set ZooKeeper Session Timeout In Millis.
     *
     * @param zkSessionTimeoutMs zookeeper session timeout in millis
     * @return stream configuration
     */
    public StreamConfiguration setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
        setProperty(ZK_SESSION_TIMEOUT_MS, zkSessionTimeoutMs);
        return this;
    }

    /**
     * Get writer entry buffer size in bytes.
     *
     * @return writer entry buffer size.
     */
    public int getSegmentWriterEntryBufferSize() {
        return getInt(SEGMENT_WRITER_ENTRY_BUFFER_SIZE, SEGMENT_WRITER_ENTRY_BUFFER_SIZE_DEFAULT);
    }

    /**
     * Set writer entry buffer size in bytes.
     *
     * @see #getSegmentWriterEntryBufferSize()
     * @param writerEntryBufferSize
     *          writer entry buffer size in bytes.
     * @return stream configuration.
     */
    public StreamConfiguration setSegmentWriterEntryBufferSize(int writerEntryBufferSize) {
        setProperty(SEGMENT_WRITER_ENTRY_BUFFER_SIZE, writerEntryBufferSize);
        return this;
    }

    /**
     * Get writer commit delay in millis. If delay is zero, a commit entry is flushed
     * immediately after previous entry flush is complete.
     *
     * @return writer commit delay in millis.
     */
    public int getSegmentWriterCommitDelayMs() {
        return getInt(SEGMENT_WRITER_COMMIT_DELAY_MS, SEGMENT_WRITER_COMMIT_DELAY_MS_DEFAULT);
    }

    /**
     * Set writer commit delay in millis.
     *
     * @see #getSegmentWriterCommitDelayMs()
     * @param delayMs writer commit delay in millis
     * @return stream configuration.
     */
    public StreamConfiguration setSegmentWriterCommitDelayMs(int delayMs) {
        setProperty(SEGMENT_WRITER_COMMIT_DELAY_MS, delayMs);
        return this;
    }

    /**
     * Get writer flush interval in millis. If interval is zero, records are only flushed
     * when records buffer reaches entry buffer size. If interval is not zero, records will
     * be flushed when the time that records kept in buffer are more than writer flush interval.
     *
     * @return writer flush interval ms.
     */
    public int getSegmentWriterFlushIntervalMs() {
        return getInt(SEGMENT_WRITER_FLUSH_INTERVAL_MS, SEGMENT_WRITER_FLUSH_INTERVAL_MS_DEFAULT);
    }

    /**
     * Set writer flush interval in millis.
     *
     * @see #getSegmentWriterFlushIntervalMs()
     * @param flushIntervalMs writer flush interval in millis.
     * @return stream configuration.
     */
    public StreamConfiguration setSegmentWriterFlushIntervalMs(int flushIntervalMs) {
        setProperty(SEGMENT_WRITER_FLUSH_INTERVAL_MS, flushIntervalMs);
        return this;
    }

    /**
     * Get the time period that a reader need to wait for commits if it already reaches end
     * of an inprogress segment. The time unit is millis.
     *
     * @return time period in millis a reader need to wait for commits.
     */
    public int getSegmentReaderCommitWaitMs() {
        return getInt(SEGMENT_READER_COMMIT_WAIT_MS, SEGMENT_READER_COMMIT_WAIT_MS_DEFAULT);
    }

    /**
     * Set the time period that a reader need to wait for commits if it already reaches end
     * of an inprogress segment. The time unit is millis.
     *
     * @see #getSegmentReaderCommitWaitMs()
     * @param waitMs wait time period in millis, if a reader reaches end of an inprogress segment.
     * @return stream configuration.
     */
    public StreamConfiguration setSegmentReaderCommitWaitMs(int waitMs) {
        setProperty(SEGMENT_READER_COMMIT_WAIT_MS, waitMs);
        return this;
    }

    /**
     * Get the max outstanding read entries that a segment reader could issue.
     *
     * @return max outstanding read entries that a segment reader could issue.
     */
    public int getSegmentReaderMaxOutstandingReadEntries() {
        return getInt(SEGMENT_READER_MAX_OUTSTANDING_READ_ENTRIES,
                SEGMENT_READER_MAX_OUTSTANDING_READ_ENTRIES_DEFAULT);
    }

    /**
     * Set the max outstanding read entries that a segment reader could issue.
     *
     * @see #getSegmentReaderMaxOutstandingReadEntries()
     * @param maxEntries
     *          max outstanding read entries that a segment reader could issue.
     * @return stream configuration.
     */
    public StreamConfiguration setSegmentReaderMaxOutstandingReadEntries(int maxEntries) {
        setProperty(SEGMENT_READER_MAX_OUTSTANDING_READ_ENTRIES, maxEntries);
        return this;
    }

    /**
     * Get max number of records in reader cache. The reader cache setting is per stream.
     *
     * @return max number of records in reader cache.
     */
    public int getReaderCacheMaxNumRecords() {
        return getInt(READER_CACHE_MAX_NUM_RECORDS, READER_CACHE_MAX_NUM_RECORDS_DEFAULT);
    }

    /**
     * Set max number of records in reader cache.
     *
     * @see #getReaderCacheMaxNumRecords()
     * @param numRecords num of records in reader cache.
     * @return stream configuration.
     */
    public StreamConfiguration setReaderCacheMaxNumRecords(int numRecords) {
        setProperty(READER_CACHE_MAX_NUM_RECORDS, numRecords);
        return this;
    }

    /**
     * Get max number of bytes in reader cache. The reader cache setting is per stream.
     *
     * @return max number of bytes in reader cache.
     */
    public int getReaderCacheMaxNumBytes() {
        return getInt(READER_CACHE_MAX_NUM_BYTES, READER_CACHE_MAX_NUM_BYTES_DEFAULT);
    }

    /**
     * Set max number of bytes in reader cache.
     *
     * @see #getReaderCacheMaxNumBytes()
     * @param numBytes num of bytes in reader cache.
     * @return stream configuration.
     */
    public StreamConfiguration setReaderCacheMaxNumBytes(int numBytes) {
        setProperty(READER_CACHE_MAX_NUM_BYTES, numBytes);
        return this;
    }

}
