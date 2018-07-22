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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;

public class FileBackedState extends FeedWritableState {

    private RunFileWriter out;

    public FileBackedState(JobId jobId, Object stateId) {
        super(jobId, stateId);
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
    public void open(IHyracksTaskContext ctx) throws HyracksDataException {
        FileReference file =
                ctx.getJobletContext().createManagedWorkspaceFile(FileBackedState.class.getSimpleName() + id);
        out = new RunFileWriter(file, ctx.getIoManager());
        out.open();
    }

    @Override
    public void appendFrame(ByteBuffer buffer) throws HyracksDataException {
        out.nextFrame(buffer);
    }

    @Override
    public void close() throws HyracksDataException {
        if (out != null) {
            out.close();
        }
    }

    @Override
    public void writeOut(IFrameWriter writer, IFrame frame) throws HyracksDataException {
        RunFileReader in = null;
        if (out != null) {
            in = out.createReader();
        }
        writer.open();
        try {
            if (in != null) {
                in.open();
                try {
                    while (in.nextFrame(frame)) {
                        writer.nextFrame(frame.getBuffer());
                    }
                } finally {
                    in.close();
                }
            }
        } catch (Exception e) {
            writer.fail();
            throw e;
        } finally {
            writer.close();
        }
    }
}