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
import org.apache.asterix.active.partition.PartitionHolderId;
import org.apache.asterix.active.partition.PartitionHolderPushable;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeployedJobPartitionHolderDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final Logger LOGGER = LogManager.getLogger();

    private static ByteBuffer poisonFrame = ByteBuffer.allocate(0);

    private final EntityId enid;
    private final int poolSize;
    private final String runtimeName;
    private final int deployedJobs;

    public DeployedJobPartitionHolderDescriptor(IOperatorDescriptorRegistry spec, int poolSize, EntityId entityId,
            String runtimeName, int deployedJobs) {
        super(spec, 1, 0);
        this.poolSize = poolSize;
        this.enid = entityId;
        this.runtimeName = runtimeName;
        this.deployedJobs = deployedJobs;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        PartitionHolderId phid = new PartitionHolderId(enid, runtimeName, partition);

        return new PartitionHolderPushable(ctx, phid) {

            private ArrayBlockingQueue<ByteBuffer> bufferPool;
            private int recordCtr = 0;
            //            private FrameTupleAccessor fta;

            @Override
            public void open() throws HyracksDataException {
                bufferPool = new ArrayBlockingQueue<>(poolSize);
                //                fta = new FrameTupleAccessor(null);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    ByteBuffer cloneFrame = ByteBuffer.allocate(buffer.capacity());
                    buffer.rewind();//copy from the beginning
                    cloneFrame.put(buffer);
                    cloneFrame.flip();
                    bufferPool.put(cloneFrame);
                    //                    fta.reset(cloneFrame);
                    //                    recordCtr += fta.getTupleCount();
                    //                    if (LOGGER.isDebugEnabled()) {
                    //                        LOGGER.log(Level.DEBUG, phid + " gets a frame " + fta.getTupleCount());
                    //                    }
                } catch (InterruptedException e) {
                    LOGGER.log(Level.FATAL, "Partition holder is interrupted.");
                    throw new HyracksDataException(e.getMessage());
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                if (bufferPool.size() != 0) {
                    String msg = phid + "failed when pool is not empty";
                    LOGGER.log(Level.FATAL, msg);
                    throw new HyracksDataException(msg);
                }
            }

            @Override
            public void close() throws HyracksDataException {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, phid + " is closing.");
                }
                try {
                    for (int iter1 = 0; iter1 < deployedJobs; iter1++) {
                        bufferPool.put(poisonFrame);
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.log(Level.DEBUG, phid + " is poisoned.");
                        LOGGER.log(Level.DEBUG, phid + " " + recordCtr + " processed.");
                    }
                    while (!bufferPool.isEmpty()) {
                        synchronized (this) {
                            this.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    LOGGER.debug("Closing interrupted");
                    throw new HyracksDataException(e.getMessage());
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, phid + " closed.");
                }
            }

            @Override
            public synchronized ByteBuffer getHoldFrame() throws InterruptedException {
                ByteBuffer frame;
                synchronized (this) {
                    frame = bufferPool.take();
                    this.notifyAll();
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, phid + " delivered one frame.");
                }
                return frame;
            }
        };
    }
}
