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
package org.apache.hyracks.dataflow.std.connectors;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.util.trace.ITracer;

public class DividePartitionDataWriter extends PartitionDataWriter {

    private final int offset;

    public DividePartitionDataWriter(IHyracksTaskContext ctx, int consumerPartitionCount,
            IPartitionWriterFactory pwFactory, RecordDescriptor recordDescriptor, ITuplePartitionComputer tpc,
            int offset) throws HyracksDataException {
        super(ctx, consumerPartitionCount, recordDescriptor, tpc);
        this.offset = offset;
        initializeAppenders(pwFactory);
        System.out.println("DPDW created on " + Thread.currentThread().getName() + " with offset " + offset);
    }

    @Override
    protected void initializeAppenders(IPartitionWriterFactory pwFactory) throws HyracksDataException {
        for (int i = 0; i < consumerPartitionCount; ++i) {
            try {
                System.out.println("DPDW " + (offset + i) + " initialized on " + Thread.currentThread().getName()
                        + " with offset " + offset);
                pWriters[i] = pwFactory.createFrameWriter(offset + i);
                appenders[i] = createTupleAppender(ctx);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }
}
