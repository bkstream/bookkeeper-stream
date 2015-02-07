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

import org.apache.bookkeeper.stream.SSN;
import org.apache.bookkeeper.stream.conf.StreamConfiguration;
import org.apache.bookkeeper.stream.io.Record;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test Case for {@link org.apache.bookkeeper.stream.cache.RecordCachePolicy}
 */
public class TestRecordCachePolicy {

    @Test(timeout = 60000)
    public void testCachePolicyByBytes() throws Exception {
        StreamConfiguration conf = new StreamConfiguration();
        conf.setReaderCacheMaxNumBytes(20);
        Record record = Record.newBuilder()
                .setRecordId(1L)
                .setData(new byte[10])
                .setSSN(SSN.of(1L, 0L, 0L))
                .build();
        RecordCachePolicy cachePolicy = new RecordCacheBytesPolicy(conf);
        // add records
        cachePolicy.onRecordAdded(record);
        assertFalse(cachePolicy.isCacheFull());
        cachePolicy.onRecordAdded(record);
        assertTrue(cachePolicy.isCacheFull());
        cachePolicy.onRecordAdded(record);
        assertTrue(cachePolicy.isCacheFull());
        // remove records
        cachePolicy.onRecordRemoved(record);
        assertTrue(cachePolicy.isCacheFull());
        cachePolicy.onRecordRemoved(record);
        assertFalse(cachePolicy.isCacheFull());
    }

    @Test(timeout = 60000)
    public void testCachePolicyByItems() throws Exception {
        StreamConfiguration conf = new StreamConfiguration();
        conf.setReaderCacheMaxNumRecords(2);
        Record record = Record.newBuilder()
                .setRecordId(1L)
                .setData(new byte[10])
                .setSSN(SSN.of(1L, 0L, 0L))
                .build();
        RecordCachePolicy cachePolicy = new RecordCacheItemsPolicy(conf);
        // add records
        cachePolicy.onRecordAdded(record);
        assertFalse(cachePolicy.isCacheFull());
        cachePolicy.onRecordAdded(record);
        assertTrue(cachePolicy.isCacheFull());
        cachePolicy.onRecordAdded(record);
        assertTrue(cachePolicy.isCacheFull());
        // remove records
        cachePolicy.onRecordRemoved(record);
        assertTrue(cachePolicy.isCacheFull());
        cachePolicy.onRecordRemoved(record);
        assertFalse(cachePolicy.isCacheFull());
    }
}
