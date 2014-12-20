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
package org.apache.bookkeeper.stream.exceptions;

/**
 * Basic Exception
 */
public abstract class StreamException extends Exception {

    private static final long serialVersionUID = -3957478812803135110L;

    // Follow Http Code Style
    public static interface Code {
        // 2xx: actions were received, accepted and processed successfully
        public static final int SUCCESS = 200;

        // 4xx: invalid actions
        public static final int INVALID_SSN = 400;

        // 6xx:
        // unexpected situation
        public static final int UNEXPECTED = 600;

        // 10xx: metadata related exception
        public static final int METADATA_EXCEPTION = 1000;
        public static final int ZK_EXCEPTION = 1001;
    }

    private final int code;

    protected StreamException(int code) {
        super();
        this.code = code;
    }

    protected StreamException(int code, String msg) {
        super(msg);
        this.code = code;
    }

    protected StreamException(int code, Throwable t) {
        super(t);
        this.code = code;
    }

    protected StreamException(int code, String msg, Throwable t) {
        super(msg, t);
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }
}
