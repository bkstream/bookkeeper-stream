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

import org.apache.bookkeeper.stream.SSN;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

/**
 * Test cases for record writer & reader.
 */
public class TestRecordWriterReader {

    @Test(timeout = 60000)
    public void testRecordWriterReader() throws Exception {
        DataOutputBuffer outBuf = new DataOutputBuffer(32);
        DataOutputStream out = new DataOutputStream(outBuf);

        int numRecords = 10;
        // write 10 records
        for(int i = 0; i < numRecords; i++) {
            byte[] data = ("record-" + i).getBytes(UTF_8);
            Record record = Record.newBuilder()
                    .setRecordId(i)
                    .setData(data)
                    .build();
            record.write(out);
        }

        DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(outBuf.getData(), 0, outBuf.size()));

        // read those records
        RecordReader reader = RecordReader.of(in);
        int numRecordsRead = 0;
        Record record;
        while ((record = reader.readRecord()) != null) {
            assertEquals(numRecordsRead, record.getRecordId());
            assertTrue(Arrays.equals(("record-" + numRecordsRead).getBytes(UTF_8), record.getData()));
            ++numRecordsRead;
        }
        assertEquals(numRecords, numRecordsRead);

        // skip to smaller ssn
        skipReaderToSSN(2L, 0L, outBuf.getData(), 0, outBuf.size(),
                SSN.of(1L, 0L, 0L), true, 10, 0);
        // skip to exist ssn
        skipReaderToSSN(2L, 0L, outBuf.getData(), 0, outBuf.size(),
                SSN.of(2L, 0L, 5L), true, 5, 5);
        // skip to larger ssn
        skipReaderToSSN(2L, 0L, outBuf.getData(), 0, outBuf.size(),
                SSN.of(3L, 0L, 0L), false, 0, 0);
    }

    private void skipReaderToSSN(final long segmentId, final long entryId,
                                 byte[] data, int offset, int len, SSN skipTo,
                                 boolean skipToSuccess, int expectedNumReads,
                                 int expectedRecordId)
            throws Exception {
        DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(data, offset, len));
        RecordReader reader = RecordReader.of(new SSNStream() {

            long slotId = 0L;

            @Override
            public SSN getCurrentSSN() {
                return SSN.of(segmentId, entryId, slotId);
            }

            @Override
            public void advance() {
                ++slotId;
            }
        }, in);

        boolean success = reader.skipTo(skipTo);
        assertEquals(skipToSuccess, success);

        int numRecordsRead = 0;
        Record record;
        while ((record = reader.readRecord()) != null) {
            assertEquals(expectedRecordId, record.getRecordId());
            assertTrue(Arrays.equals(("record-" + expectedRecordId).getBytes(UTF_8), record.getData()));
            ++numRecordsRead;
            ++expectedRecordId;
        }
        assertEquals(expectedNumReads, numRecordsRead);
    }

    @Test(timeout = 60000)
    public void testReadTruncatedPayload() throws Exception {
        DataOutputBuffer outBuf = new DataOutputBuffer(32);
        DataOutputStream out = new DataOutputStream(outBuf);

        int numRecords = 10;
        // write 10 records
        for (int i = 0; i < numRecords; i++) {
            byte[] data = ("record-" + i).getBytes(UTF_8);
            Record record = Record.newBuilder()
                    .setRecordId(i)
                    .setData(data)
                    .build();
            record.write(out);
        }

        DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(outBuf.getData(), 0, outBuf.size() - 3));

        // read those records
        RecordReader reader = RecordReader.of(in);
        int numRecordsRead = 0;
        Record record;
        while ((record = reader.readRecord()) != null) {
            assertEquals(numRecordsRead, record.getRecordId());
            assertTrue(Arrays.equals(("record-" + numRecordsRead).getBytes(UTF_8), record.getData()));
            ++numRecordsRead;
        }
        assertEquals(numRecords - 1, numRecordsRead);
    }

}
