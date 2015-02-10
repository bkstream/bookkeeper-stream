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
package org.apache.bookkeeper.stream;

import com.google.common.annotations.Beta;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.bookkeeper.stream.common.FutureCloseable;
import org.apache.bookkeeper.stream.io.Record;

import java.util.List;

/**
 * Writer to write records into a stream.
 */
@Beta
public interface StreamWriter extends FutureCloseable<Void> {

    /**
     * Get the stream name that the reader is reading from.
     *
     * @return stream name.
     */
    String getStreamName();

    /**
     * Write a record to the stream.
     *
     * @param record single record
     * @return A future representing the written result. if the record is written successfully,
     * the future will return its {@link SSN}. Otherwise, the future
     * will throw the exception for the failure.
     */
    ListenableFuture<SSN> write(Record record);

    /**
     * Write list of records to the stream in bulk.
     *
     * @param records list of records
     * @return A future representing the bulk written result, which contains a list of futures
     * representing each written result.
     */
    ListenableFuture<List<ListenableFuture<SSN>>> writeBulk(List<Record> records);

    /**
     * Truncate the stream until given <i>ssn</i>.
     *
     * @param ssn ssn to truncate until
     * @return A future representing whether the truncation succeeds or not. The latest truncation
     * point is returned.
     */
    ListenableFuture<SSN> truncate(SSN ssn);

    /**
     * Abort the stream writer.
     */
    void abort();

}
