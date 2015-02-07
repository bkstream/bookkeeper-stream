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

import org.apache.bookkeeper.stream.SSN;
import org.apache.bookkeeper.stream.common.Scheduler.OrderingListenableFuture;
import org.apache.bookkeeper.stream.io.Record;

/**
 * Writer to write records to a single segment.
 */
public interface SegmentWriter {

    /**
     * Write record to the segment
     *
     * @param record record to write
     * @return future representing the written result.
     */
    OrderingListenableFuture<SSN> write(Record record);

    /**
     * Flush records to the segment. After the flush completed, all
     * records added before the call are all persisted to backend.
     *
     * @return future representing the flush result.
     */
    OrderingListenableFuture<SSN> flush();

    /**
     * Commit all the records flushed to the segment. After the commit
     * completed, all records flushed before the call are readable by
     * the readers.
     *
     * @return future representing the commit result.
     */
    OrderingListenableFuture<SSN> commit();

    /**
     * Close the writer.
     *
     * @return future representing the close result.
     */
    OrderingListenableFuture<SSN> close();

}
