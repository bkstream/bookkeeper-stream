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
package org.apache.bookkeeper.stream.io;

import org.apache.bookkeeper.stream.SSN;

/**
 * Record represents an entity inside a stream.
 */
public class Record {

    // record type
    private static final int FLAG_TYPE_MASK         = 0xfffffffc;
    private static final int FLAG_USER_RECORD       = 0x0;
    private static final int FLAG_COMMIT_RECORD     = 0x1;

    public static class Builder {

        private long    _rid;
        private int     _pos;
        private int     _flags;
        private byte[]  _data;

        private Builder() {
            _rid    = -1L;
            _pos    = -1;
            _flags  = 0;
            _data   = null;
        }

        private Builder(Record record) {
            _rid    = record.rid;
            _pos    = record.pos;
            _flags  = record.flags;
            _data   = record.data;
        }

        public Builder setRecordId(long recordId) {
            this._rid = recordId;
            return this;
        }

        public Builder setPos(int pos) {
            this._pos = pos;
            return this;
        }

        public Builder setData(byte[] data) {
            this._data = data;
            return this;
        }

        public Builder asUserRecord() {
            return asRecord(FLAG_USER_RECORD);
        }

        private Builder asRecord(int type) {
            this._flags = (this._flags & FLAG_TYPE_MASK) | type;
            return this;
        }

    }

    private final long      rid;
    private final int       pos;
    private final int       flags;
    private final byte[]    data;
    private final SSN ssn;

    protected Record(SSN ssn, long rid, int pos, int flags, byte[] data) {
        this.ssn = ssn;
        this.rid = rid;
        this.pos = pos;
        this.flags = flags;
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
