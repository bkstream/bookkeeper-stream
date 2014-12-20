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

/**
 * Record represents an entity inside a stream.
 */
public class Record {

    private long flags;
    private long rid;
    private byte[] data;
    private SSN ssn;

    private Record() {
        this(null, 0L, null);
    }

    public Record(long rid, byte[] data) {
        this(null, rid, data);
    }

    protected Record(SSN ssn, long rid, byte[] data) {
        this.ssn = ssn;
        this.flags = 0L;
        this.rid = rid;
        this.data = data;
    }

    /**
     * Get application-specific record id of current record.
     *
     * @return application-specific record id
     */
    public long getRecordId() {
        return this.rid;
    }

    /**
     * Get system-generated sequence number of current record in the stream.
     *
     * @return system generated sequence number
     */
    public SSN getSSN() {
        return this.ssn;
    }

    /**
     * Get application-specific data of current record.
     *
     * @return application-specific record data
     */
    public byte[] getData() {
        return this.data;
    }

}
