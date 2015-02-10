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

/**
 * A stream is responsible for managing records.
 */
@Beta
public interface Stream extends FutureCloseable<Void> {

    /**
     * Get the stream name that the reader is reading from.
     *
     * @return stream name.
     */
    String getStreamName();

    /**
     * Get the {@link StreamReader} to read
     * records starting from <i>fromSSN</i>.
     *
     * @param fromSSN ssn to start reading records from stream
     * @return A future representing an available stream reader to read records.
     */
    ListenableFuture<StreamReader> getReader(SSN fromSSN);

    /**
     * Get the {@link StreamWriter} to write
     * records to the current stream. It would recover previous in-complete
     * stream segments and start writing to new stream segment.
     *
     * @return A future representing an available stream writer to write records.
     */
    ListenableFuture<StreamWriter> getWriter();

    /**
     * Get the {@link StreamReaderWriter} to read/write records.
     *
     * @param fromSSN ssn to start reading/writing records from/to stream
     * @return A future representing an available stream reader/writer to read/write records.
     */
    ListenableFuture<StreamReaderWriter> getReaderWriter(SSN fromSSN);

}
