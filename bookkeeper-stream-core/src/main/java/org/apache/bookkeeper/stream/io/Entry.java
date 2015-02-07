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
package org.apache.bookkeeper.stream.io;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.bookkeeper.stream.SSN;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * Entry is the envelope carrying {@link org.apache.bookkeeper.stream.io.Record}s
 * written to storage backend.
 *
 * ----------------------------------------------------------------
 * byte 0 - 7   : flags
 * byte 8 - 15  : num user records added before this entry
 * byte 16 - 23 : num user bytes added before this entry
 * byte 24 - 27 : num records in this entry
 * byte 28 - 31 : original payload length (before compression)
 * byte 32 - 35 : payload length
 * bytes        : data
 * ----------------------------------------------------------------
 *
 */
public class Entry {

    // entry type
    private static final long FLAG_TYPE_MASK        = 0xfffffffffffffffcL;
    private static final long FLAG_TYPE_BITS        = 0x3L;
    private static final long FLAG_DATA_ENTRY       = 0x0L;
    private static final long FLAG_COMMIT_ENTRY     = 0x1L;

    // entry header size
    private static final int ENTRY_HEADER_SIZE =
            ((Long.SIZE * 3) + (Integer.SIZE * 3)) / Byte.SIZE;

    public static class EntryData {
        public final byte[] data;
        public final int offset;
        public final int len;

        private EntryData(byte[] data, int offset, int len) {
            this.data   = data;
            this.offset = offset;
            this.len    = len;
        }
    }

    public static class EntryBuilder {

        private final long segmentId;
        private final long entryId;
        private final long lastNumRecords;
        private final long lastNumBytes;
        private long flags = 0L;
        private final List<SettableFuture<SSN>> resultList;
        private final DataOutputBuffer recordBuffer;
        private final DataOutputStream recordStream;

        private EntryBuilder(long segmentId,
                             long entryId,
                             long lastNumRecords,
                             long lastNumBytes,
                             int initialRecordBufferSize)
                throws IOException {
            this.segmentId      = segmentId;
            this.entryId        = entryId;
            this.lastNumRecords = lastNumRecords;
            this.lastNumBytes   = lastNumBytes;
            this.resultList     = new LinkedList<>();
            this.recordBuffer   = new DataOutputBuffer(initialRecordBufferSize);
            this.recordStream   = new DataOutputStream(this.recordBuffer);
            // write header
            this.recordStream.writeLong(flags);
            this.recordStream.writeLong(lastNumRecords);
            this.recordStream.writeLong(lastNumBytes);
            this.recordStream.writeInt(0);
            this.recordStream.writeInt(0);
            this.recordStream.writeInt(0);
        }

        /**
         * Add <i>record</i> to this entry and register a <i>future</i> for
         * callback when the record is persisted.
         *
         * @param record record to add
         * @param future future for callback on record persisted
         * @throws IOException
         */
        public synchronized EntryBuilder addRecord(Record record, SettableFuture<SSN> future)
                throws IOException {
            resultList.add(future);
            record.write(this.recordStream);
            return this;
        }

        /**
         * Set this entry as data entry.
         *
         * @return entry builder.
         */
        public synchronized EntryBuilder asDataEntry() {
            return setEntryType(FLAG_DATA_ENTRY);
        }

        /**
         * Set this entry as a commit entry.
         *
         * @return entry builder.
         */
        public synchronized EntryBuilder asCommitEntry() {
            return setEntryType(FLAG_COMMIT_ENTRY);
        }

        /**
         * Set entry type.
         *
         * @param entryType
         *          entry type
         * @return entry builder
         */
        private synchronized EntryBuilder setEntryType(long entryType) {
            this.flags = (this.flags & FLAG_TYPE_MASK) | entryType;
            return this;
        }

        /**
         * Get the buffer size of the entry.
         *
         * @return buffer size of the entry.
         */
        public synchronized int getBufferSize() {
            return this.recordBuffer.size();
        }

        /**
         * Get number pending records in current entry builder.
         *
         * @return number pending records in current entry builder.
         */
        public synchronized int getNumPendingRecords() {
            return resultList.size();
        }

        /**
         * Build entry.
         *
         * @return immutable entry representation
         */
        public synchronized Entry build() {
            int size = this.recordBuffer.size();
            byte[] data = this.recordBuffer.getData();
            int numBytes = size - ENTRY_HEADER_SIZE;
            ByteBuffer headerBuf = ByteBuffer.wrap(data, 0, ENTRY_HEADER_SIZE);
            headerBuf.putLong(flags);
            headerBuf.putLong(lastNumRecords);
            headerBuf.putLong(lastNumBytes);
            headerBuf.putInt(resultList.size());
            headerBuf.putInt(numBytes);
            headerBuf.putInt(numBytes);

            EntryData entryData = new EntryData(data, 0, size);
            return new Entry(segmentId, entryId, flags,
                    resultList.size(), numBytes, lastNumRecords, lastNumBytes,
                    entryData, Optional.of(resultList));
        }

    }

    /**
     * Builder for an entry.
     *
     * @param segmentId
     *          segment id
     * @param entryId
     *          entry id
     * @param lastNumRecords
     *          num records added so far before this entry.
     * @param lastNumBytes
     *          num bytes added so far before this entry.
     * @param initialBufferSize
     *          initial buffer size for records in this entry.
     * @return entry builder
     * @throws IOException
     */
    public static EntryBuilder newBuilder(long segmentId,
                                          long entryId,
                                          long lastNumRecords,
                                          long lastNumBytes,
                                          int initialBufferSize)
        throws IOException {
        return new EntryBuilder(segmentId, entryId,
                lastNumRecords, lastNumBytes, initialBufferSize);
    }

    /**
     * Build next entry from current <i>entry</i>.
     *
     * @param entry current entry
     * @return entry builder for next entry
     * @throws IOException
     */
    public static EntryBuilder nextEntry(Entry entry) throws IOException {
        return new EntryBuilder(
                entry.getSegmentId(), entry.getEntryId() + 1,
                entry.getLastNumRecords() + entry.getNumRecords(),
                entry.getLastNumBytes() + entry.getNumBytes(),
                entry.getEntryData().len);
    }

    /**
     * Build an entry of <i>data</i> array.
     *
     * @param segmentId
     *          segment id
     * @param entryId
     *          entry id
     * @param data
     *          data bytes
     * @param offset
     *          offset of data bytes array
     * @param len
     *          len of data bytes array
     * @return entry
     */
    public static Entry of(long segmentId,
                           long entryId,
                           byte[] data,
                           int offset,
                           int len) {
        Preconditions.checkArgument(data.length >= ENTRY_HEADER_SIZE,
                "Too small entry");
        ByteBuffer headerBuf = ByteBuffer.wrap(data, 0, ENTRY_HEADER_SIZE);
        long flags          = headerBuf.getLong();
        long lastNumRecords = headerBuf.getLong();
        long lastNumBytes   = headerBuf.getLong();
        int numRecords      = headerBuf.getInt();
        int numBytes        = headerBuf.getInt();
        // no compression support yet.
        headerBuf.getInt();

        Optional<List<SettableFuture<SSN>>> recordFutureList = Optional.absent();
        return new Entry(segmentId, entryId, flags, numRecords, numBytes,
                lastNumRecords, lastNumBytes, new EntryData(data, offset, len), recordFutureList);
    }

    private final long segmentId;
    private final long entryId;
    private final long flags;
    private final int numRecords;
    private final int numBytes;
    private final long lastNumRecords;
    private final long lastNumBytes;
    private final EntryData entryData;
    private final Optional<List<SettableFuture<SSN>>> recordFutureList;

    private Entry(long segmentId,
                  long entryId,
                  long flags,
                  int numRecords,
                  int numBytes,
                  long lastNumRecords,
                  long lastNumBytes,
                  EntryData entryData,
                  Optional<List<SettableFuture<SSN>>> recordFutureList) {
        this.segmentId      = segmentId;
        this.entryId        = entryId;
        this.flags          = flags;
        this.numRecords     = numRecords;
        this.numBytes       = numBytes;
        this.lastNumRecords = lastNumRecords;
        this.lastNumBytes   = lastNumBytes;
        this.entryData      = entryData;
        this.recordFutureList = recordFutureList;
    }

    /**
     * @return segment id
     */
    public long getSegmentId() {
        return segmentId;
    }

    /**
     * @return entry id
     */
    public long getEntryId() {
        return entryId;
    }

    /**
     * @return true if this entry is a data entry. otherwise false.
     */
    public boolean isDataEntry() {
        return (flags & FLAG_TYPE_BITS) == FLAG_DATA_ENTRY;
    }

    /**
     * @return true if this entry is a commit entry. otherwise false.
     */
    public boolean isCommitEntry() {
        return (flags & FLAG_TYPE_BITS) == FLAG_COMMIT_ENTRY;
    }

    /**
     * @return number of records added in this entry
     */
    public int getNumRecords() {
        return numRecords;
    }

    /**
     * @return number of bytes added in this entry
     */
    public int getNumBytes() {
        return numBytes;
    }

    /**
     * @return last number of records added so far before this entry
     */
    public long getLastNumRecords() {
        return lastNumRecords;
    }

    /**
     * @return last number of bytes added so far before this entry
     */
    public long getLastNumBytes() {
        return lastNumBytes;
    }

    /**
     * @return entry data
     */
    public EntryData getEntryData() {
        return entryData;
    }

    /**
     * @return record future list.
     */
    public Optional<List<SettableFuture<SSN>>> getRecordFutureList() {
        return recordFutureList;
    }

    /**
     * Complete record futures.
     */
    public SSN completeRecordFutures(long entryId) {
        if (!recordFutureList.isPresent()) {
            return SSN.INVALID_SSN;
        }
        long slotId = 0L;
        SSN ssn = SSN.INVALID_SSN;
        for (SettableFuture<SSN> future : recordFutureList.get()) {
            ssn = SSN.of(segmentId, entryId, slotId);
            future.set(ssn);
            ++slotId;
        }
        return ssn;
    }

    /**
     * Cancel record futures when encountering exception <i>throwable</i>.
     *
     * @param throwable exception to cancel record futures
     */
    public void cancelRecordFutures(Throwable throwable) {
        if (!recordFutureList.isPresent()) {
            return;
        }
        for (SettableFuture<SSN> future : recordFutureList.get()) {
            future.setException(throwable);
        }
    }

    /**
     * Create record reader for this entry.
     *
     * @return record reader
     */
    public RecordReader asRecordReader() {
        return new RecordReader(new SSNStream() {

            long slotId = 0L;

            @Override
            public SSN getCurrentSSN() {
                return SSN.of(segmentId, entryId, slotId);
            }

            @Override
            public void advance() {
                ++slotId;
            }
        }, new DataInputStream(new ByteArrayInputStream(entryData.data,
                entryData.offset + ENTRY_HEADER_SIZE, numBytes)));
    }

}
