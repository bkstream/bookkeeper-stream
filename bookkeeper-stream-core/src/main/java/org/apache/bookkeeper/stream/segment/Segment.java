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

/**
 * Segment
 */
public interface Segment {

    public static interface Listener {
        /**
         * Event on segment changed.
         *
         * @param metadata new segment metadata.
         */
        void onSegmentChanged(StreamSegmentMetadata metadata);
    }

    /**
     * @return stream name that the segment belongs to.
     */
    String getStreamName();

    /**
     * @return segment metadata of current segment.
     */
    StreamSegmentMetadata getSegmentMetadata();

    /**
     * Register segment <i>listener</i> to listen on segment state changes.
     *
     * @param listener segment listener to listen on segment state changes.
     */
    void registerSegmentListener(Listener listener);
}
