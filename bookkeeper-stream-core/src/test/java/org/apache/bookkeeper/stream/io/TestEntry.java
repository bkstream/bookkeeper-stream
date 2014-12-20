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

import com.google.common.util.concurrent.SettableFuture;
import org.apache.bookkeeper.stream.SSN;
import org.apache.bookkeeper.stream.io.Entry.EntryBuilder;
import org.apache.bookkeeper.stream.io.Entry.EntryData;

import org.junit.Test;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

/**
 * Test Cases for {@link org.apache.bookkeeper.stream.io.Entry}
 */
public class TestEntry {

    @Test(timeout = 60000)
    public void testWriteReadEntry() throws Exception {
        long segmentId      = 2L;
        long entryId        = 0L;
        long lastNumRecords = 1234L;
        long lastNumBytes   = 12345678L;

        int numRecords = 10;
        int numBytes = 0;

        EntryBuilder entryBuilder =
                Entry.newBuilder(segmentId, entryId, lastNumRecords, lastNumBytes, 32);
        for (int i = 0; i < numRecords; i++) {
            SettableFuture<SSN> recordFuture = SettableFuture.create();
            Record record = Record.newBuilder()
                    .setRecordId(i)
                    .setData(("record-" + i).getBytes(UTF_8))
                    .build();
            numBytes += record.getPersistenceSize();
            entryBuilder.addRecord(record, recordFuture);
        }
        // build as a commit entry
        Entry entry = entryBuilder.asCommitEntry().build();
        assertEquals(segmentId, entry.getSegmentId());
        assertEquals(entryId, entry.getEntryId());
        assertTrue(entry.isCommitEntry());
        assertFalse(entry.isDataEntry());
        assertEquals(numRecords, entry.getNumRecords());
        assertEquals(numBytes, entry.getNumBytes());
        assertEquals(lastNumRecords, entry.getLastNumRecords());
        assertEquals(lastNumBytes, entry.getLastNumBytes());
        assertTrue(entry.getRecordFutureList().isPresent());
        assertEquals(numRecords, entry.getRecordFutureList().get().size());

        // read from the serialized data
        EntryData entryData = entry.getEntryData();
        Entry readEntry = Entry.of(segmentId, entryId,
                entryData.data, entryData.offset, entryData.len);
        assertEquals(segmentId, readEntry.getSegmentId());
        assertEquals(entryId, readEntry.getEntryId());
        assertTrue(readEntry.isCommitEntry());
        assertFalse(readEntry.isDataEntry());
        assertEquals(numRecords, readEntry.getNumRecords());
        assertEquals(numBytes, readEntry.getNumBytes());
        assertEquals(lastNumRecords, readEntry.getLastNumRecords());
        assertEquals(lastNumBytes, readEntry.getLastNumBytes());
        assertFalse(readEntry.getRecordFutureList().isPresent());

        // read records from serialized entry
        RecordReader rr = readEntry.asRecordReader();
        Record record;
        int expectedRecordId = 0;
        int numReadRecords = 0;
        while ((record = rr.readRecord()) != null) {
            assertEquals(expectedRecordId, record.getRecordId());
            assertArrayEquals(("record-" + expectedRecordId).getBytes(UTF_8), record.getData());
            assertEquals(SSN.of(segmentId, entryId, expectedRecordId), record.getSSN());
            ++expectedRecordId;
            ++numReadRecords;
        }
        assertEquals(numRecords, numReadRecords);

        // build next entry
        Entry nextEntry = Entry.nextEntry(entry)
                .asDataEntry().build();
        assertEquals(segmentId, nextEntry.getSegmentId());
        assertEquals(entryId + 1, nextEntry.getEntryId());
        assertTrue(nextEntry.isDataEntry());
        assertFalse(nextEntry.isCommitEntry());
        assertEquals(0, nextEntry.getNumRecords());
        assertEquals(0, nextEntry.getNumBytes());
        assertEquals(lastNumRecords + numRecords, nextEntry.getLastNumRecords());
        assertEquals(lastNumBytes + numBytes, nextEntry.getLastNumBytes());
        assertTrue(nextEntry.getRecordFutureList().isPresent());
        assertEquals(0, nextEntry.getRecordFutureList().get().size());
    }

}
