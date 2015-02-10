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
package org.apache.bookkeeper.stream.segment;

import com.google.common.annotations.Beta;
import org.apache.bookkeeper.stream.cache.RecordCache;
import org.apache.bookkeeper.stream.common.OrderingFutureCloseable;

/**
 * Reader to read entries from a single segment.
 */
@Beta
public interface SegmentReader extends OrderingFutureCloseable<Void> {

    public static interface Listener {
        /**
         * Trigger when the reader reaches end of segment.
         */
        void onEndOfSegment();

        /**
         * Trigger when the reader encountered error.
         */
        void onError();
    }

    /**
     * Start reading entries from the segment.
     *
     * @param recordCache record cache to receive read entries.
     * @param listener listener on reader events
     * @throws java.lang.IllegalStateException if the reader is already started.
     */
    void start(RecordCache recordCache, Listener listener);

}
