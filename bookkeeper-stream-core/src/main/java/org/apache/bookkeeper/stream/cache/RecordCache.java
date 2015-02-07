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

import org.apache.bookkeeper.stream.common.Resumeable;
import org.apache.bookkeeper.stream.exceptions.StreamException;
import org.apache.bookkeeper.stream.io.Entry;
import org.apache.bookkeeper.stream.io.Record;

/**
 * Cache to cache entries
 */
public interface RecordCache {

    /**
     * Register <i>listener</i> to listen on record available event.
     *
     * @param listener listener to listen on record available event.
     */
    void addListener(RecordCacheListener listener);

    /**
     * Unregister <i>listener</i>.
     *
     * @param listener listener to listen on record available event.
     */
    void removeListener(RecordCacheListener listener);

    /**
     * Add <i>entry</i> to the entry cache.
     *
     * @param entry received entry.
     */
    void addEntry(Entry entry);

    /**
     * whether the cache is full or not.
     *
     * @return true if cache is full, otherwise false.
     */
    boolean isCacheFull();

    /**
     * Set callback <i>resumeable</i>. It would trigger the read callback
     * when cache entries decrease to below cache threshold.
     *
     * @param resumeable resumable callback
     */
    void setReadCallback(Resumeable resumeable);

    /**
     * Retrieves and removes next record from the records cache, or returns
     * <i>null</i> if the record cache is empty.
     *
     * @return next record from the records cache, or <i>null</i> if the cache is empty.
     * @throws org.apache.bookkeeper.stream.exceptions.StreamException when encountered exception in cache
     */
    Record pollNextRecord() throws StreamException;

}
