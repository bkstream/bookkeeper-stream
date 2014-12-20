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

import org.apache.bookkeeper.stream.exceptions.InvalidSSNException;
import org.apache.bookkeeper.stream.proto.DataFormats;
import org.junit.Test;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.*;

/**
 * Test Case for {@link org.apache.bookkeeper.stream.SSN}
 */
public class TestSSN {

    @Test(timeout = 60000)
    public void testComparison() throws Exception {
        SSN ssn111 = SSN.of(1L, 1L, 1L);
        SSN ssn112 = SSN.of(1L, 1L, 2L);
        SSN ssn121 = SSN.of(1L, 2L, 1L);
        SSN ssn211 = SSN.of(2L, 1L, 1L);

        // same ssn
        assertTrue(SSN.of(1L, 1L, 1L).compareTo(ssn111) == 0);
        assertEquals(SSN.of(1L, 1L, 1L), ssn111);

        // different slot id
        assertTrue(ssn111.compareTo(ssn112) < 0);
        assertTrue(ssn121.compareTo(ssn111) > 0);

        // different entry id
        assertTrue(ssn111.compareTo(ssn121) < 0);
        assertTrue(ssn121.compareTo(ssn111) > 0);

        // different segment id
        assertTrue(ssn111.compareTo(ssn211) < 0);
        assertTrue(ssn211.compareTo(ssn111) > 0);

        // not greater than
        assertTrue(ssn111.notGreaterThan(ssn211));
        assertTrue(ssn111.notGreaterThan(SSN.of(1L, 1L, 1L)));
    }

    @Test(timeout = 60000)
    public void testProtoComparison() throws Exception {
        DataFormats.SSN ssn111 = SSN.of(1L, 1L, 1L).toProtoSSN();
        DataFormats.SSN ssn112 = SSN.of(1L, 1L, 2L).toProtoSSN();
        DataFormats.SSN ssn121 = SSN.of(1L, 2L, 1L).toProtoSSN();
        DataFormats.SSN ssn211 = SSN.of(1L, 1L, 1L).toProtoSSN();

        // different slot id
        assertTrue(SSN.notGreaterThan(ssn111, ssn112));

        // different entry id
        assertTrue(SSN.notGreaterThan(ssn111, ssn121));

        // different segment id
        assertTrue(SSN.notGreaterThan(ssn111, ssn211));

        // same ssn
        assertTrue(SSN.notGreaterThan(ssn111, SSN.of(1L, 1L, 1L).toProtoSSN()));
    }

    @Test(timeout = 60000)
    public void testSerializeDeserialize() throws Exception {
        SSN ssn = SSN.of(1L, 2L, 3L);
        String data = ssn.serialize();
        SSN dSSN = SSN.deserialize(data);
        assertEquals(ssn, dSSN);
    }

    @Test(timeout = 60000, expected = InvalidSSNException.class)
    public void testDeserializeTailTruncatedSSN() throws Exception {
        SSN ssn = SSN.of(99L, 2L, 3L);
        String str = ssn.serialize();
        byte[] data = str.getBytes(UTF_8);
        byte[] corruptedData = new byte[data.length - 4];
        System.arraycopy(data, 0, corruptedData, 0, corruptedData.length);
        String corruptedStr = new String(corruptedData, UTF_8);
        SSN.deserialize(corruptedStr);
    }

    @Test(timeout = 60000, expected = InvalidSSNException.class)
    public void testDeserializeHeadTruncatedSSN() throws Exception {
        SSN ssn = SSN.of(99L, 2L, 3L);
        String str = ssn.serialize();
        byte[] data = str.getBytes(UTF_8);
        byte[] corruptedData = new byte[data.length - 4];
        System.arraycopy(data, 3, corruptedData, 0, corruptedData.length);
        String corruptedStr = new String(corruptedData, UTF_8);
        SSN.deserialize(corruptedStr);
    }
}
