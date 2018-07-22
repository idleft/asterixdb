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
package org.apache.hyracks.dataflow.std.misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;

public class MemoryBackedState extends FeedWritableState {

    List<ByteBuffer> inMemBufferList;

    public MemoryBackedState(JobId jobId, Object stateId) {
        super(jobId, stateId);
        this.inMemBufferList = new ArrayList<>();
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {
        // do nothing
    }

    @Override
    public void fromBytes(DataInput in) throws IOException {
        // do nothing
    }

    @Override
    public void open(IHyracksTaskContext ctx) {

    }

    @Override
    public void appendFrame(ByteBuffer buffer) throws HyracksDataException {
        inMemBufferList.add(buffer);
    }

    @Override
    public void writeOut(IFrameWriter writer, IFrame frame) throws HyracksDataException {
        writer.open();
        try {
            for (ByteBuffer inMemFrame : inMemBufferList) {
                FrameUtils.copyAndFlip(inMemFrame, frame.getBuffer());
                writer.nextFrame(inMemFrame);
            }
        } catch (Exception e) {
            writer.fail();
            throw e;
        } finally {
            writer.close();
        }
    }

    @Override
    public void close() throws HyracksDataException {
        // do nothing
    }
}