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
import org.apache.asterix.active.message.UntrackIntakePartitionHolderMessage;
import org.apache.asterix.active.partition.PartitionHolderId;
import org.apache.asterix.active.partition.PullablePartitionHolderByRecordRuntime;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedConstants;
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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.util.trace.ITracer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

public class DeployedJobPartitionHolderDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final Logger LOGGER = LogManager.getLogger();

    private static ByteBuffer POISON_PILL = ByteBuffer.allocate(0);

    private final EntityId enid;
    private final int poolSize;
    private final String runtimeName;

    public DeployedJobPartitionHolderDescriptor(IOperatorDescriptorRegistry spec, int poolSize, EntityId entityId,
            String runtimeName) {
        super(spec, 1, 0);
        this.poolSize = poolSize;
        this.enid = entityId;
        this.runtimeName = runtimeName;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        PartitionHolderId phid = new PartitionHolderId(enid, runtimeName, partition);

        return new PullablePartitionHolderByRecordRuntime(ctx, phid) {

            private ArrayBlockingQueue<ByteBuffer> bufferPool;
            private NodeControllerService ncs;
            private FrameTupleAccessor curAcc;
            private final ITracer tracer = ctx.getJobletContext().getServiceContext().getTracer();
            private final long registry = tracer.getRegistry().get(FeedConstants.FEED_TRACER_CATEGORY);
            private AtomicInteger rloc;
            private long ltid;
            private AtomicBoolean closed;

            @Override
            public void open() {
                this.bufferPool = new ArrayBlockingQueue<>(poolSize);
                this.ncs = (NodeControllerService) ctx.getJobletContext().getServiceContext().getControllerService();
                curAcc = new FrameTupleAccessor(null);
                rloc = new AtomicInteger(0);
                ltid = tracer.durationB("Deployed Job Partition Holder", registry, null);
                closed = new AtomicBoolean(false);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    long ntid = tracer.durationB("Deployed Job Next Frame", registry, null);
                    ByteBuffer cloneFrame = ByteBuffer.allocate(buffer.capacity());
                    buffer.rewind();//copy from the beginning
                    cloneFrame.put(buffer);
                    cloneFrame.flip();
                    tracer.instant("Deployed Job copied frame", registry, ITracer.Scope.t, null);
                    bufferPool.put(cloneFrame);
                    tracer.durationE(ntid, registry, null);
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
                UntrackIntakePartitionHolderMessage msg = new UntrackIntakePartitionHolderMessage(enid);
                ncs.sendApplicationMessageToCC(ctx.getJobletContext().getJobId().getCcId(),
                        JavaSerializationUtils.serialize(msg), null);
            }

            @Override
            public void close() throws HyracksDataException {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, phid + " is closing.");
                }
                try {
                    // Send poison
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.log(Level.DEBUG, phid + " sends poisons ");
                    }
                    bufferPool.put(POISON_PILL);
                    synchronized (closed) {
                        while (!closed.get()) {
                            closed.wait();
                        }
                    }
                    untrackDeployedJob();
                    // shutdown all local stg partition holders
                } catch (Exception e) {
                    LOGGER.debug(phid + " is closing interrupted " + bufferPool.size());
                    throw new HyracksDataException(e.getMessage());
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, phid + " closed. " + bufferPool.size());
                }
                tracer.durationE(ltid, registry, null);
            }

            @Override
            public synchronized String nextRecord() throws InterruptedException {
                if (curAcc.getBuffer() == null
                        || (curAcc.getBuffer().capacity() > 0 && rloc.get() >= curAcc.getTupleCount())) {
                    ByteBuffer frame = bufferPool.take();
                    curAcc.reset(frame);
                    rloc.set(0);
                    if (frame.capacity() == 0) {
                        synchronized (closed) {
                            closed.set(true);
                            closed.notify();
                        }
                    }
                }
                if (closed.get()) {
                    return Strings.EMPTY;
                }
                int returnRecordIdx = rloc.getAndIncrement();
                String record = new String(
                        Arrays.copyOfRange(curAcc.getBuffer().array(), curAcc.getTupleStartOffset(returnRecordIdx),
                                curAcc.getTupleStartOffset(returnRecordIdx) + curAcc.getTupleLength(returnRecordIdx)),
                        StandardCharsets.UTF_8).trim() + ExternalDataConstants.LF;
                return record;
            }
        };
    }
}
