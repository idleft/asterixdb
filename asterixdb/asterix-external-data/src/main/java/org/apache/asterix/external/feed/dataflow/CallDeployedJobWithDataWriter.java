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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;

import io.netty.buffer.ByteBuf;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.message.InvokeDeployedMessage;
import org.apache.asterix.transaction.management.service.logging.LogManager;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.application.NCServiceContext;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.logging.log4j.Logger;

public class CallDeployedJobWithDataWriter extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger();

    private NodeControllerService ncs;
    private IHyracksTaskContext ctx;
    private DeployedJobSpecId deployedJobSpecId;
    private final int connDs;
    private long frameCounter;
    private LinkedBlockingDeque<Pair<Long, ByteBuffer>> frameBuffer;
    private final ActiveRuntimeId runtimeId;
    private final String ncId;

    public CallDeployedJobWithDataWriter(IHyracksTaskContext ctx, IFrameWriter writer, DeployedJobSpecId jobSpecId,
            int connDs, ActiveRuntimeId runtimeId, int bufferSize) {
        this.writer = writer;
        this.ctx = ctx;
        this.deployedJobSpecId = jobSpecId;
        this.connDs = connDs;
        this.frameCounter = 0;
        this.frameBuffer = new LinkedBlockingDeque<>(bufferSize == 0 ? Integer.MAX_VALUE : bufferSize);
        this.ncs = (NodeControllerService) ctx.getJobletContext().getServiceContext().getControllerService();
        this.ncId = ncs.getId();
        this.runtimeId = runtimeId;
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public void nextFrame(ByteBuffer frame) throws HyracksDataException {
        try {
            frameBuffer.put(Pair.of(frameCounter, frame));
            InvokeDeployedMessage msg =
                    new InvokeDeployedMessage(deployedJobSpecId, frame.array(), connDs, frameCounter, ncId, runtimeId);
            ncs.sendApplicationMessageToCC(ctx.getJobletContext().getJobId().getCcId(),
                    JavaSerializationUtils.serialize(msg), null);
            frameCounter++;
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            while (frameBuffer.size() != 0) {
                synchronized (frameBuffer) {
                    frameBuffer.wait();
                }
            }
        } catch (InterruptedException e) {
            LOGGER.warn("CallDeployedJobWriter is stopped some data left");
            throw new HyracksDataException(e);
        }
        writer.close();
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }

    public void ackFrame(long frameId) {
        // remove frame from queue
        frameBuffer.removeIf(p -> p.getLeft() == frameId);
        if (frameBuffer.isEmpty()) {
            synchronized (frameBuffer) {
                frameBuffer.notify();
            }
        }
    }
}
