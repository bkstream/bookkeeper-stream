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

import org.apache.bookkeeper.stream.conf.StreamConfiguration;
import org.apache.bookkeeper.stream.io.Record;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cache Policy by bytes
 */
public class RecordCacheBytesPolicy implements RecordCachePolicy {

    private final int maxCacheBytes;
    private final AtomicInteger cacheBytes;

    public RecordCacheBytesPolicy(StreamConfiguration conf) {
        this.maxCacheBytes = conf.getReaderCacheMaxNumBytes();
        this.cacheBytes = new AtomicInteger(0);
    }

    @Override
    public void onRecordAdded(Record record) {
        this.cacheBytes.addAndGet(record.getData().length);
    }

    @Override
    public void onRecordRemoved(Record record) {
        this.cacheBytes.addAndGet(-record.getData().length);
    }

    @Override
    public boolean isCacheFull() {
        return this.cacheBytes.get() >= maxCacheBytes;
    }
}
