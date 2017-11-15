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
package org.apache.asterix.external.feed.dataflow;

import java.nio.ByteBuffer;

import org.apache.asterix.active.EntityId;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FeedFrameCollector implements IFrameWriter {

    private final EntityId collectorId;
    private final IFrameWriter writer;
    private State state;

    public enum State {
        CREATED,
        ACTIVE,
        FINISHED,
    }

    public FeedFrameCollector(IFrameWriter writer, EntityId collectorId) {
        this.writer = writer;
        this.state = State.CREATED;
        this.collectorId = collectorId;
    }

    @Override
    public synchronized void close() throws HyracksDataException {
        writer.close();
        state = State.FINISHED;
        notify();
    }

    public synchronized void waitForFinish() throws HyracksDataException {
        while(state != State.FINISHED) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public synchronized void nextFrame(ByteBuffer frame) throws HyracksDataException {
        writer.nextFrame(frame);
    }

    public synchronized State getState() {
        return state;
    }

    public synchronized void setState(State state) {
        this.state = state;
        switch (state) {
            case FINISHED:
                notifyAll();
                break;
            default:
                break;
        }
    }

    @Override
    public String toString() {
        return "FrameCollector " + collectorId + "," + state + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof FeedFrameCollector) {
            return collectorId.equals(((FeedFrameCollector) o).collectorId);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return collectorId.toString().hashCode();
    }

    @Override
    public synchronized void flush() throws HyracksDataException {
        writer.flush();
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    public EntityId getCollectorId() {
        return collectorId;
    }
}
