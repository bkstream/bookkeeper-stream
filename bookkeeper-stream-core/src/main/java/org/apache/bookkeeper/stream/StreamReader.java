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

/**
 * Reader to read {@link Record}s from a given stream.
 */
@Beta
public interface StreamReader extends FutureCloseable {

    /**
     * Get the stream name that the reader is reading from.
     *
     * @return stream name.
     */
    String getStreamName();

    /**
     * Read next record from the given stream. The future is satisfied when a record is available and readable.
     *
     * @return a future listening on the arrival of next record in the given stream.
     */
    ListenableFuture<Record> readNext();

}
