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

import com.google.common.util.concurrent.SettableFuture;
import org.apache.bookkeeper.stream.SSN;
import org.apache.bookkeeper.stream.common.Resumeable;
import org.apache.bookkeeper.stream.conf.StreamConfiguration;
import org.apache.bookkeeper.stream.exceptions.OutOfOrderReadException;
import org.apache.bookkeeper.stream.io.Entry;
import org.apache.bookkeeper.stream.io.Entry.EntryBuilder;
import org.apache.bookkeeper.stream.io.Record;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

/**
 * Test Case for {@link org.apache.bookkeeper.stream.cache.RecordCache}
 */
public class TestRecordCache {

    @Test(timeout = 60000)
    public void testAddEntry() throws Exception {
        StreamConfiguration conf = new StreamConfiguration();
        conf.setReaderCacheMaxNumRecords(5);
        RecordCachePolicy cachePolicy = new RecordCacheItemsPolicy(conf);

        RecordCache recordCache = RecordCacheImpl.newBuilder()
                .streamName("test-add-entry")
                .streamConf(conf)
                .cachePolicy(cachePolicy)
                .build();
        EntryBuilder firstEntryBuilder = Entry.newBuilder(1L, 0L, 0, 0, 1024);
        for (int i = 0; i < 4; i++) {
            Record record = Record.newBuilder()
                    .setRecordId(i)
                    .setData(("record-" + i).getBytes(UTF_8))
                    .build();
            firstEntryBuilder.addRecord(record, SettableFuture.<SSN>create());
        }
        Entry firstEntry = firstEntryBuilder.build();
        recordCache.addEntry(firstEntry);
        assertFalse(recordCache.isCacheFull());
        EntryBuilder secondEntryBuilder = Entry.nextEntry(firstEntry);
        for (int i = 4; i < 8; i++) {
            Record record = Record.newBuilder()
                    .setRecordId(i)
                    .setData(("record-" + i).getBytes(UTF_8))
                    .build();
            secondEntryBuilder.addRecord(record, SettableFuture.<SSN>create());
        }
        Entry secondEntry = secondEntryBuilder.build();
        recordCache.addEntry(secondEntry);
        assertTrue(recordCache.isCacheFull());
    }

    @Test(timeout = 60000)
    public void testRemoveEntry() throws Exception {
        StreamConfiguration conf = new StreamConfiguration();
        conf.setReaderCacheMaxNumRecords(5);
        RecordCacheItemsPolicy cachePolicy = new RecordCacheItemsPolicy(conf);

        RecordCache recordCache = RecordCacheImpl.newBuilder()
                .streamName("test-remove-entry")
                .streamConf(conf)
                .cachePolicy(cachePolicy)
                .build();
        EntryBuilder entryBuilder = Entry.newBuilder(1L, 0L, 0, 0, 1024);
        for (int i = 0; i < 8; i++) {
            Record record = Record.newBuilder()
                    .setRecordId(i)
                    .setData(("record-" + i).getBytes(UTF_8))
                    .build();
            entryBuilder.addRecord(record, SettableFuture.<SSN>create());
        }
        Entry entry = entryBuilder.build();
        recordCache.addEntry(entry);
        assertTrue(recordCache.isCacheFull());

        // poll entries from the cache
        int numRecords = 0;
        Record record = recordCache.pollNextRecord();
        while (null != record) {
            assertEquals(numRecords, record.getRecordId());
            assertEquals("record-" + numRecords, new String(record.getData(), UTF_8));
            ++numRecords;

            record = recordCache.pollNextRecord();
        }
        assertEquals(8, numRecords);
    }

    @Test(timeout = 60000)
    public void testRecordCacheListener() throws Exception {
        StreamConfiguration conf = new StreamConfiguration();
        conf.setReaderCacheMaxNumRecords(5);
        RecordCachePolicy cachePolicy = new RecordCacheItemsPolicy(conf);

        RecordCache recordCache = RecordCacheImpl.newBuilder()
                .streamName("test-record-cache-listener")
                .streamConf(conf)
                .cachePolicy(cachePolicy)
                .build();

        int numEntries = 8;
        final CountDownLatch notificationLatch = new CountDownLatch(numEntries);
        final RecordCacheListener listener = new RecordCacheListener() {
            @Override
            public void onRecordAvailable() {
                notificationLatch.countDown();
            }
        };
        recordCache.addListener(listener);
        for (int i = 0; i < numEntries; i++) {
            EntryBuilder entryBuilder = Entry.newBuilder(1L, i, 0, 0, 1024);
            Record record = Record.newBuilder()
                    .setRecordId(i)
                    .setData(("record-" + i).getBytes(UTF_8))
                    .build();
            entryBuilder.addRecord(record, SettableFuture.<SSN>create());
            recordCache.addEntry(entryBuilder.build());
        }
        assertTrue(recordCache.isCacheFull());
        assertTrue(notificationLatch.await(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000, expected = OutOfOrderReadException.class)
    public void testOutOfOrderRead() throws Exception {
        StreamConfiguration conf = new StreamConfiguration();
        RecordCachePolicy cachePolicy = new RecordCacheItemsPolicy(conf);

        RecordCache recordCache = RecordCacheImpl.newBuilder()
                .streamName("test-out-of-order-read")
                .streamConf(conf)
                .cachePolicy(cachePolicy)
                .build();

        Record firstRecord = Record.newBuilder()
                .setRecordId(1)
                .setData("record-1".getBytes(UTF_8))
                .build();
        Record secondRecord = Record.newBuilder()
                .setRecordId(2)
                .setData("record-2".getBytes(UTF_8))
                .build();
        Entry firstEntry = Entry.newBuilder(1L, 0L, 0, 0, 1024)
                .addRecord(firstRecord, SettableFuture.<SSN>create())
                .build();
        Entry secondEntry = Entry.newBuilder(1L, 1L, 0, 0, 1024)
                .addRecord(secondRecord, SettableFuture.<SSN>create())
                .build();
        recordCache.addEntry(secondEntry);
        recordCache.addEntry(firstEntry);
        recordCache.pollNextRecord();
    }

    @Test(timeout = 60000)
    public void testReadCallback() throws Exception {
        StreamConfiguration conf = new StreamConfiguration();
        conf.setReaderCacheMaxNumRecords(5);
        RecordCachePolicy cachePolicy = new RecordCacheItemsPolicy(conf);

        RecordCache recordCache = RecordCacheImpl.newBuilder()
                .streamName("test-read-callback")
                .streamConf(conf)
                .cachePolicy(cachePolicy)
                .build();

        // read callback on cache isn't full
        final CountDownLatch latch1 = new CountDownLatch(1);
        final Resumeable callback1 = new Resumeable() {
            @Override
            public void onResume() {
                latch1.countDown();
            }
        };
        recordCache.setReadCallback(callback1);
        assertTrue(latch1.await(10, TimeUnit.SECONDS));
        // read callback on cache full
        EntryBuilder entryBuilder = Entry.newBuilder(1L, 0L, 0, 0, 1024);
        for (int i = 0; i < 8; i++) {
            Record record = Record.newBuilder()
                    .setRecordId(i)
                    .setData(("record-" + i).getBytes(UTF_8))
                    .build();
            entryBuilder.addRecord(record, SettableFuture.<SSN>create());
        }
        Entry entry = entryBuilder.build();
        recordCache.addEntry(entry);
        assertTrue(recordCache.isCacheFull());

        // read callback on cache is full
        final AtomicInteger numNotifications = new AtomicInteger(0);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final Resumeable callback2 = new Resumeable() {
            @Override
            public void onResume() {
                numNotifications.incrementAndGet();
                latch2.countDown();
            }
        };
        recordCache.setReadCallback(callback2);
        Record record = recordCache.pollNextRecord();
        while (null != record) {
            record = recordCache.pollNextRecord();
        }
        assertTrue(latch2.await(10, TimeUnit.SECONDS));
        assertEquals(1, numNotifications.get());
    }
}
