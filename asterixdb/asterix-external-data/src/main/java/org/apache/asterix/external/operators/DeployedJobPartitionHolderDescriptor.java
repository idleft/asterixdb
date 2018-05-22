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
package org.apache.asterix.external.operators;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.message.DropDeployedJobMessage;
import org.apache.asterix.active.partition.PartitionHolderId;
import org.apache.asterix.active.partition.PullablePartitionHolderPushable;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeployedJobPartitionHolderDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final Logger LOGGER = LogManager.getLogger();

    private static ByteBuffer POISON_PILL = ByteBuffer.allocate(0);

    private final EntityId enid;
    private final int poolSize;
    private final String runtimeName;
    private final int workerN;
    private AtomicBoolean readyToStop;

    public DeployedJobPartitionHolderDescriptor(IOperatorDescriptorRegistry spec, int poolSize, EntityId entityId,
            String runtimeName, int workerN) {
        super(spec, 1, 0);
        this.poolSize = poolSize;
        this.enid = entityId;
        this.runtimeName = runtimeName;
        this.workerN = workerN;
        this.readyToStop = new AtomicBoolean(false);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        PartitionHolderId phid = new PartitionHolderId(enid, runtimeName, partition);

        return new PullablePartitionHolderPushable(ctx, phid) {

            private ArrayBlockingQueue<ByteBuffer> bufferPool;
            private ReentrantLock arrayLock;
            private int recordCtr = 0;
            private NodeControllerService ncs;
            private FrameTupleAccessor fta;

            @Override
            public void open() throws HyracksDataException {
                this.bufferPool = new ArrayBlockingQueue<>(poolSize);
                this.arrayLock = new ReentrantLock();
                this.ncs = (NodeControllerService) ctx.getJobletContext().getServiceContext().getControllerService();
                fta = new FrameTupleAccessor(null);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    ByteBuffer cloneFrame = ByteBuffer.allocate(buffer.capacity());
                    buffer.rewind();//copy from the beginning
                    cloneFrame.put(buffer);
                    cloneFrame.flip();
                    bufferPool.put(cloneFrame);
                    fta.reset(cloneFrame);
                    recordCtr += fta.getTupleCount();
                    //                                        if (LOGGER.isDebugEnabled()) {
                    //                                            LOGGER.log(Level.DEBUG, phid + " gets a frame " + fta.getTupleCount());
                    //                                        }
                } catch (InterruptedException e) {
                    LOGGER.log(Level.FATAL, "Partition holder is interrupted.");
                    throw new HyracksDataException(e.getMessage());
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                if (bufferPool.size() != 0) {
                    String msg = phid + "failed when pool is not empty " + bufferPool.size();
                    LOGGER.log(Level.FATAL, msg);
                    throw new HyracksDataException(msg);
                }
            }

            private void untrackDeployedJob() throws Exception {
                // The untrack deploy can be done either on the collector side or intake buffer side.
                // Either way the collector has to check whether holder is null or not.
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(this + " poisoned to untrack deployed job.");
                }
                DropDeployedJobMessage msg = new DropDeployedJobMessage(enid);
                ncs.sendApplicationMessageToCC(ctx.getJobletContext().getJobId().getCcId(),
                        JavaSerializationUtils.serialize(msg), null);
            }

            @Override
            public void close() throws HyracksDataException {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, phid + " is closing.");
                }
                try {
                    //                    if (LOGGER.isDebugEnabled()) {
                    //                        LOGGER.log(Level.DEBUG, phid + " " + recordCtr + " processed. Wait for empty.");
                    //                    }
                    // Send poison
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.log(Level.DEBUG, phid + " sends poisons ");
                    }
                    bufferPool.put(POISON_PILL);
                    synchronized (bufferPool) {
                        while (bufferPool.size() > 1) {
                            bufferPool.wait();
                        }
                    }
                    untrackDeployedJob();
                    // Makes sure it's ok to send poison now so that no worker takes poison and be restarted.
                    //                    synchronized (readyToStop) {
                    //                        while (readyToStop.get() != true) {
                    //                            readyToStop.wait();
                    //                        }
                    //                    }
                    // shutdown all local stg partition holders
                } catch (Exception e) {
                    LOGGER.debug(phid + " is closing interrupted " + bufferPool.size());
                    throw new HyracksDataException(e.getMessage());
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, phid + " closed. " + bufferPool.size());
                }
            }

            //            @Override
            //            public void shutdown() {
            //                synchronized (readyToStop) {
            //                    if (LOGGER.isDebugEnabled()) {
            //                        LOGGER.log(Level.DEBUG, phid + " ready to stop ");
            //                    }
            //                    readyToStop = true;
            //                    readyToStop.notifyAll();
            //                }
            //            }

            @Override
            public ByteBuffer getHoldFrame() throws InterruptedException {
                ByteBuffer frame;
                synchronized (bufferPool) {
                    frame = bufferPool.take();
                    bufferPool.notify();
                }
                // if get a poison, put it back so others can see it too
                if (frame.capacity() == 0) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.log(Level.DEBUG, phid + " poisoned, put the frame back.");
                    }
                    bufferPool.put(frame);
                }
                //                    bufferPool.notifyAll();
                //                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, phid + " delivered one frame.");
                }
                return frame;
            }

            @Override
            public void shutdown() {
                synchronized (readyToStop) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.log(Level.DEBUG, phid + " ready to stop ");
                    }
                    readyToStop.set(true);
                    readyToStop.notify();
                }
            }
        };
    }
}
