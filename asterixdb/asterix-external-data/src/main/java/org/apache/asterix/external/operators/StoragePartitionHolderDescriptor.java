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

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.partition.PushablePartitionHolderPushable;
import org.apache.asterix.active.partition.PartitionHolderId;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StoragePartitionHolderDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final ByteBuffer POISON = ByteBuffer.allocate(0);

    private final EntityId enid;
    private final int poolSize;
    private final String runtimeName;

    public StoragePartitionHolderDescriptor(IOperatorDescriptorRegistry spec, int poolSize, EntityId entityId,
            String runtimeName, RecordDescriptor recordDescriptor) {
        super(spec, 0, 1);
        this.poolSize = poolSize;
        this.enid = entityId;
        this.runtimeName = runtimeName;
        this.outRecDescs[0] = recordDescriptor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        PartitionHolderId phid = new PartitionHolderId(enid, runtimeName, partition);

        return new PushablePartitionHolderPushable(ctx, phid) {

            private final ITracer tracer = ctx.getJobletContext().getServiceContext().getTracer();
            private final long registry = tracer.getRegistry().get(FeedConstants.FEED_TRACER_CATEGORY);

            private ArrayBlockingQueue<ByteBuffer> bufferPool = new ArrayBlockingQueue<>(poolSize);
            private volatile boolean closed;

            @Override
            public void start() throws HyracksDataException {
                long storage_partition_holder_running =
                        tracer.durationB("Storage Partition Holder Running", registry, null);
                closed = false;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, phid + " started");
                }
                writer.open();
                while (!closed) {
                    try {
                        long get_frame_tid = tracer.durationB("Storage Partition Holdler gets frames", registry, null);
                        ByteBuffer frame = bufferPool.take();
                        tracer.durationE(get_frame_tid, registry, null);
                        long push_frame_tid =
                                tracer.durationB("Storage Partition Holder pushes frames", registry, null);
                        if (frame.capacity() > 0) {
                            writer.nextFrame(frame);
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.log(Level.DEBUG, phid + " frame pushed " + String.valueOf(frame.array()) + " "
                                        + frame.capacity());
                            }
                        } else {
                            closed = true;
                        }
                        tracer.durationE(push_frame_tid, registry, null);
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new HyracksDataException(e.getMessage());
                    }
                }
                writer.flush();
                writer.close();
                tracer.durationE(storage_partition_holder_running, registry, null);
            }

            @Override
            public boolean deposit(ByteBuffer buffer) {
                try {
                    ByteBuffer cloneFrame = ByteBuffer.allocate(buffer.capacity());
                    buffer.rewind();//copy from the beginning
                    cloneFrame.put(buffer);
                    cloneFrame.flip();
                    bufferPool.put(cloneFrame);
                    if (LOGGER.isDebugEnabled()) {
                        FrameTupleAccessor fta = new FrameTupleAccessor(null);
                        fta.reset(cloneFrame);
                        LOGGER.log(Level.DEBUG, phid + " frame received " + String.valueOf(buffer.array()) + " add "
                                + fta.getTupleCount());
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return false;
                }
                return true;
            }

            @Override
            public void shutdown() throws HyracksDataException {
                try {
                    bufferPool.put(POISON);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.log(Level.DEBUG, phid + " poisoned");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new HyracksDataException(e.getMessage());
                }
            }
        };
    }
}
