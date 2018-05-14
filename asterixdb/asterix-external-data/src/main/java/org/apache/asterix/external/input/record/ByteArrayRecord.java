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
package org.apache.asterix.external.input.record;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.util.ExternalDataConstants;

public class ByteArrayRecord implements IRawRecord<byte[]> {

    private byte[] value;
    private int size;

    public ByteArrayRecord() {
        value = new byte[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
        size = 0;
    }

    @Override
    public byte[] getBytes() {
        return new String(value).getBytes();
    }

    @Override
    public byte[] get() {
        return value;
    }

    @Override
    public int size() {
        return size;
    }

    public void setValue(byte[] recordBuffer, int offset, int length) {
        if (value.length < length) {
            value = new byte[length];
        }
        System.arraycopy(recordBuffer, offset, value, 0, length);
        size = length;
    }

    @Override
    public void reset() {
        size = 0;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Override
    public void set(byte[] value) {
        this.value = value;
        this.size = value.length;
    }
}
