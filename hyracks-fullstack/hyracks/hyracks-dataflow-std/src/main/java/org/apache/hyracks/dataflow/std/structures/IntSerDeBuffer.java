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
package org.apache.hyracks.dataflow.std.structures;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class IntSerDeBuffer {

    protected static final byte INVALID_BYTE_VALUE = (byte) 0xFF;
    protected static final int INT_SIZE = 4;

    ByteBuffer byteBuffer;
    byte[] bytes;

    public IntSerDeBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
        this.bytes = byteBuffer.array();
        resetFrame();
    }

    public int getInt(int pos) {
        int offset = pos * 4;
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + (bytes[offset + 3] & 0xff);
    }

    public void writeInt(int pos, int value) {
        int offset = pos * 4;
        bytes[offset++] = (byte) (value >> 24);
        bytes[offset++] = (byte) (value >> 16);
        bytes[offset++] = (byte) (value >> 8);
        bytes[offset] = (byte) (value);
    }

    public void writeInvalidVal(int intPos, int intRange) {
        int offset = intPos * 4;
        Arrays.fill(bytes, offset, offset + INT_SIZE * intRange, INVALID_BYTE_VALUE);
    }

    public int capacity() {
        return bytes.length / 4;
    }

    public int getByteCapacity() {
        return bytes.length;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public void resetFrame() {
        Arrays.fill(bytes, INVALID_BYTE_VALUE);
    }

}