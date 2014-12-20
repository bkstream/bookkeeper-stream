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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.bookkeeper.stream.SSN;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Record represents an entity inside a stream.
 */
public class Record {

    private static final int RECORD_HEADER_SIZE =
            (Long.SIZE + Integer.SIZE) / Byte.SIZE;

    /**
     * Create a builder to build record.
     *
     * @return record builder.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder to build record.
     */
    public static class Builder {

        private long    _rid;
        private byte[]  _data;
        private SSN     _ssn;

        private Builder() {
            _rid    = -1L;
            _data   = null;
            _ssn    = SSN.INVALID_SSN;
        }

        /**
         * Set application-specific record id for the record.
         *
         * @param recordId
         *          application specific record id
         * @return record builder
         */
        public Builder setRecordId(long recordId) {
            this._rid = recordId;
            return this;
        }

        /**
         * Set data as the record payload.
         *
         * @param data
         *          record payload.
         * @return record builder.
         */
        public Builder setData(byte[] data) {
            this._data = data;
            return this;
        }

        /**
         * Set system-generated ssn.
         *
         * @param ssn
         *          system generated ssn.
         * @return record builder.
         */
        public Builder setSSN(SSN ssn) {
            this._ssn = ssn;
            return this;
        }

        /**
         * Build the record.
         *
         * @return build the record.
         */
        public Record build() {
            Preconditions.checkNotNull(_data, "No data provided for the record");
            Preconditions.checkNotNull(_ssn, "Null SSN provided for the record");
            return new Record(_ssn, _rid, _data);
        }

    }

    private final long      rid;
    private final byte[]    data;
    private final SSN ssn;

    protected Record(SSN ssn, long rid, byte[] data) {
        this.ssn = ssn;
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

    /**
     * Return the persistence size of the record.
     *
     * @return persistence size of the record.
     */
    int getPersistenceSize() {
        return RECORD_HEADER_SIZE + data.length;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("record(ssn = ").append(ssn)
                .append(", rid = ").append(rid)
                .append(", len = ").append(data.length)
                .append(")");
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ssn, rid, data);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Record)) {
            return false;
        }
        Record that = (Record) obj;
        return Objects.equal(this.ssn, that.ssn) &&
                Objects.equal(this.rid, that.rid) &&
                Arrays.equals(this.data, that.data);
    }

    /**
     * Whether the record equals other <i>obj</i>, without comparing ssn.
     *
     * @param that
     *          other record
     * @return true if record is same in content
     */
    public boolean isSameContent(Record that) {
        return Objects.equal(this.ssn, that.ssn) &&
                Objects.equal(this.rid, that.rid);
    }

    /**
     * Write the record to stream <i>out</i>.
     *
     * @param out
     *          output stream.
     * @throws IOException
     */
    public void write(DataOutputStream out) throws IOException {
        // write record id
        out.writeLong(rid);
        // write data length
        out.writeInt(data.length);
        // write data
        out.write(data);
    }

    /**
     * Read the record from stream <i>in</i>.
     *
     * @param in
     *          input stream.
     * @return record builder.
     * @throws IOException
     */
    static Builder read(DataInputStream in) throws IOException {
        Builder recordBuilder = newBuilder();
        recordBuilder.setRecordId(in.readLong());
        int len = in.readInt();
        byte[] data = new byte[len];
        in.readFully(data);
        recordBuilder.setData(data);
        return recordBuilder;
    }

}
