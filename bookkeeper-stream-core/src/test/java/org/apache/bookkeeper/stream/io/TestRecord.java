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
 * Test Cases for {@link org.apache.bookkeeper.stream.io.Record}
 */
public class TestRecord {

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testBuildRecordWithNullData() {
        Record.newBuilder().build();
    }

    @Test(timeout = 60000, expected = NullPointerException.class)
    public void testBuildRecordWithNullSSN() {
        Record.newBuilder().setData("builder-with-null-ssn".getBytes(UTF_8)).setSSN(null).build();
    }

    @Test(timeout = 60000)
    public void testBuildRecord() {
        byte[] data = "builder-data".getBytes(UTF_8);
        Record record = Record.newBuilder()
                .setRecordId(1234L)
                .setData(data)
                .setSSN(SSN.of(1L, 0L, 0L))
                .build();

        assertEquals(1234L, record.getRecordId());
        assertTrue(data == record.getData());
        assertTrue(Arrays.equals(data, record.getData()));
    }

    @Test(timeout = 60000)
    public void testWriteReadRecord() throws Exception {
        DataOutputBuffer dataBuf = new DataOutputBuffer(32);
        byte[] data = "write-read-record".getBytes(UTF_8);
        Record record = Record.newBuilder()
                .setRecordId(123456L)
                .setData(data)
                .build();

        record.write(new DataOutputStream(dataBuf));
        byte[] recordData = dataBuf.getData();

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(recordData, 0, dataBuf.size()));
        Record.Builder recordBuilder = Record.read(in);
        Record readRecord = recordBuilder.build();

        assertTrue("read record should have same content as the written record", record.isSameContent(readRecord));
        assertEquals("read record should be same as the record written", record, readRecord);
    }

}
