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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import java.nio.ByteBuffer;

/*
* Add wait-for-close logic for writers in the sub-workflow
* */
public class FeedSubWorkflowFrameCollector implements IFrameWriter {

    public enum State {
        CREATED,
        STARTED,
        FINISHED
    }

    private final IFrameWriter writer;
    private State state;

    public FeedSubWorkflowFrameCollector(IFrameWriter writer) {
        this.writer = writer;
        this.state = State.CREATED;
    }

    @Override
    public void open() throws HyracksDataException {
        this.writer.open();
        this.state = State.STARTED;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        this.writer.nextFrame(buffer);
    }

    public synchronized void waitForClose() throws InterruptedException {
        while (this.state != State.FINISHED) {
            wait();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        this.writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        this.writer.close();
        this.state = State.FINISHED;
        notifyAll();
    }
}
