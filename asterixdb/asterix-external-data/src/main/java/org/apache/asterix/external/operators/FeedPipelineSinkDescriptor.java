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
import org.apache.asterix.active.partition.IPushablePartitionHolderRuntime;
import org.apache.asterix.active.partition.PartitionHolderId;
import org.apache.asterix.active.partition.PartitionHolderManager;
import org.apache.asterix.active.partition.PushablePartitionHolderPushable;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FeedPipelineSinkDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final Logger LOGGER = LogManager.getLogger();

    private final EntityId enid;
    private final String runtimeName;

    public FeedPipelineSinkDescriptor(IOperatorDescriptorRegistry spec, EntityId entityId, String runtimeName) {
        super(spec, 1, 0);
        this.enid = entityId;
        this.runtimeName = runtimeName;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {

            PartitionHolderId stgPhId = new PartitionHolderId(enid, runtimeName, partition);
            PartitionHolderManager phm = (PartitionHolderManager) ((INcApplicationContext) ctx.getJobletContext()
                    .getServiceContext().getApplicationContext()).getPartitionHolderMananger();
            IPushablePartitionHolderRuntime stgPartitionHolder;

            @Override
            public void open() throws HyracksDataException {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, this + " looks for " + stgPhId);
                }
                stgPartitionHolder = (IPushablePartitionHolderRuntime) phm.getPartitionHolderRuntime(stgPhId);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, this + " deposits frame " + String.valueOf(buffer.array()) + " "
                            + buffer.capacity() + " to " + stgPhId);
                }
                stgPartitionHolder.deposit(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                LOGGER.log(Level.ERROR, this + " failed.");
            }

            @Override
            public void close() throws HyracksDataException {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.log(Level.DEBUG, this + " closed.");
                }
            }

            @Override
            public String toString() {
                return enid + "_SinkOP@" + partition;
            }
        };
    }
}
