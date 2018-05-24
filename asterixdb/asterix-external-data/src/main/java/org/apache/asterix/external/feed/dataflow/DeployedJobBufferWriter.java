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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.message.StartFeedConnWorkersMsg;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeployedJobBufferWriter extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final Level logLevel = Level.DEBUG;

    private final NodeControllerService ncs;
    private final IHyracksTaskContext ctx;
    private final DeployedJobSpecId deployedJobSpecId;
    private ByteBuffer poisonFrame = ByteBuffer.allocate(0);
    long frameIdx = 0;
    private ArrayBlockingQueue<Long> frameIdQueue;
    private Map<Long, ByteBuffer> frameMap;
    private int batchSize;
    private int workerNum;
    private AtomicInteger ackCounter;
    private AtomicBoolean preClose;
    private EntityId entityId;
    private int liveCollPartitionN;
    private Set<String> stgNodes;
    int pid;

    public DeployedJobBufferWriter(IHyracksTaskContext ctx, IFrameWriter writer, DeployedJobSpecId jobSpecId,
            EntityId entityId, int workerNum, int liveCollPartitionN, Set<String> stgNodes, int pid) {
        this.writer = writer;
        this.ctx = ctx;
        this.deployedJobSpecId = jobSpecId;
        this.workerNum = workerNum;
        this.ncs = (NodeControllerService) ctx.getJobletContext().getServiceContext().getControllerService();
        this.entityId = entityId;
        this.liveCollPartitionN = liveCollPartitionN;
        this.stgNodes = stgNodes;
        this.pid = pid;
    }

    private void sendMsgToCC(ICcAddressedMessage msg) throws Exception {
        ncs.sendApplicationMessageToCC(ctx.getJobletContext().getJobId().getCcId(),
                JavaSerializationUtils.serialize(msg), null);
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
        try {
            if (pid == 0) {
                StartFeedConnWorkersMsg msg = new StartFeedConnWorkersMsg(deployedJobSpecId, workerNum, entityId,
                        liveCollPartitionN, stgNodes);
                sendMsgToCC(msg);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e.getMessage());
        }
    }

    @Override
    public void nextFrame(ByteBuffer frame) throws HyracksDataException {
        try {
            writer.nextFrame(frame);
        } catch (Exception e) {
            throw new HyracksDataException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.flush();
        writer.close();
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }
}
