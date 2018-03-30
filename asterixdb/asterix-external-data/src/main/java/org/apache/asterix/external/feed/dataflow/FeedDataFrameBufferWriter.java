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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.handler.logging.LogLevel;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.message.KeepDeployedJobMessage;
import org.apache.asterix.active.message.StartFeedConnWorkersMsg;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FeedDataFrameBufferWriter extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

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
    private ActiveRuntimeId runtimeId;

    public FeedDataFrameBufferWriter(IHyracksTaskContext ctx, IFrameWriter writer, DeployedJobSpecId jobSpecId,
            ActiveRuntimeId runtimeId, int workerNum, int batchSize, int bufferSize) {
        this.writer = writer;
        this.ctx = ctx;
        this.deployedJobSpecId = jobSpecId;
        this.workerNum = workerNum;
        this.batchSize = batchSize;
        this.ncs = (NodeControllerService) ctx.getJobletContext().getServiceContext().getControllerService();
        this.frameIdQueue = new ArrayBlockingQueue<>(workerNum * bufferSize);
        this.frameMap = new ConcurrentHashMap<>();
        this.ackCounter = new AtomicInteger(0);
        this.preClose = new AtomicBoolean(false);
        this.runtimeId = runtimeId;
    }

    private void sendMsgToCC(ICcAddressedMessage msg) throws Exception {
        ncs.sendApplicationMessageToCC(ctx.getJobletContext().getJobId().getCcId(),
                JavaSerializationUtils.serialize(msg), null);
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
        try {
            StartFeedConnWorkersMsg msg = new StartFeedConnWorkersMsg(deployedJobSpecId, workerNum);
            sendMsgToCC(msg);
            KeepDeployedJobMessage keepMsg = new KeepDeployedJobMessage(runtimeId, deployedJobSpecId);
            sendMsgToCC(keepMsg);
        } catch (Exception e) {
            throw new HyracksDataException(e.getMessage());
        }
    }

    @Override
    public void nextFrame(ByteBuffer frame) throws HyracksDataException {
        try {
            ByteBuffer cloneFrame = ByteBuffer.allocate(frame.capacity());
            frame.rewind();//copy from the beginning
            cloneFrame.put(frame);
            cloneFrame.flip();
            frameMap.put(frameIdx, cloneFrame);
            frameIdQueue.put(frameIdx);
            frameIdx++;
        } catch (Exception e) {
            throw new HyracksDataException(e.getMessage());
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            preClose.set(true);
            for (int iter1 = 0; iter1 < workerNum; iter1++) {
                frameMap.put(frameIdx, poisonFrame);
                frameIdQueue.put(frameIdx);
                LOGGER.log(logLevel, "POISON starts " + frameIdx);
                frameIdx++;
            }
            LOGGER.log(logLevel, "wait for empty " + frameIdQueue.size());
            waitForMapEmpty();
        } catch (Exception e) {
            LOGGER.warn("CallDeployedJobWriter is stopped some data left");
            throw new HyracksDataException(e.getMessage());
        }
        writer.close();
    }

    private void waitForMapEmpty() throws InterruptedException {
        synchronized (frameMap) {
            while (frameIdQueue.size() > 0 || frameMap.size() > 0) {
                LOGGER.log(logLevel, "Pre wait " + frameMap.size() + " | " + StringUtils.join(frameMap.keySet(), " ")
                        + " | " + StringUtils.join(frameIdQueue, " "));
                frameMap.wait();
                LOGGER.log(logLevel, " Wait notified " + frameMap.size() + " | "
                        + StringUtils.join(frameMap.keySet(), " ") + " | " + StringUtils.join(frameIdQueue, " "));
            }
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }

    public Pair<List<Long>, List<ByteBuffer>> obtainFrames(JobId jobId) throws Exception {
        ByteBuffer frame;
        List<ByteBuffer> frames = new ArrayList<>();
        List<Long> frameIds = new ArrayList<>();
        for (int iter1 = 0; iter1 < batchSize; iter1++) {
            LOGGER.log(logLevel,
                    jobId + " Prepare Dequeue " + frameIdQueue.size() + " | " + StringUtils.join(frameIdQueue, " "));
            Long frameId;
            frameId = frameIdQueue.take();
            frame = frameMap.get(frameId);
            if (frame == null) {
                System.err.println("OHHHHHHHHHHHHHHHH  " + frameId);
            }
            LOGGER.log(logLevel, jobId + " FrameId Dequeue " + frameId + " with  size " + frame.capacity()
                    + " remaining " + frameIdQueue.size() + " | " + StringUtils.join(frameIdQueue, " "));
            frames.add(frame);
            frameIds.add(frameId);
            if (frame.capacity() == 0 || preClose.get()) {
                // early stop due to no more new frames
                break;
            }
            //            synchronized (frameIdQueue) {
            //                frameIdQueue.notifyAll();
            //            }
        }
        return Pair.of(frameIds, frames);
    }

    public synchronized void ackFrames(List<Long> frameIds) throws Exception {
        ByteBuffer frame;
        boolean newWorker = true;
        for (Long framId : frameIds) {
            frame = frameMap.get(framId);
            synchronized (frameMap) {
                frameMap.remove(framId);
                frameMap.notifyAll();
            }
            if (frame.capacity() > 0) {
                frame.clear();
            } else {
                newWorker = false;
            }
        }
        LOGGER.log(logLevel,
                " ACK Received for " + frameIds + " new " + newWorker + " remaining " + frameIdQueue.size() + " | ");
        //        if (newWorker) {
        //            StartFeedConnWorkersMsg msg = new StartFeedConnWorkersMsg(deployedJobSpecId, 1);
        //            ncs.sendApplicationMessageToCC(ctx.getJobletContext().getJobId().getCcId(),
        //                    JavaSerializationUtils.serialize(msg), null);
        //            LOGGER.log(logLevel, ackCounter.incrementAndGet() + " ack received");
        //        }
    }
}
