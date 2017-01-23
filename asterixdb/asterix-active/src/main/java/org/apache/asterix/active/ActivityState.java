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
package org.apache.asterix.active;

public class ActivityState {
    /**
     * The starting state and a possible terminal state. Next state can only be {@code ActivityState.STARTING}
     */
    public static final byte STOPPED = 0x00;
    /**
     * A terminal state
     */
    public static final byte FAILED = 0x01;
    /**
     * An intermediate state. Next state can only be {@code ActivityState.STARTED} or {@code ActivityState.FAILED}
     */
    public static final byte STARTING = 0x02;
    /**
     * An intermediate state. Next state can only be {@code ActivityState.STOPPING} or {@code ActivityState.FAILED}
     */
    public static final byte STARTED = 0x03;
    /**
     * An intermediate state. Next state can only be {@code ActivityState.STOPPED} or {@code ActivityState.FAILED}
     */
    public static final byte STOPPING = 0x04;

    private ActivityState() {
    }

}