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
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.message.RegisterFrameMessage;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FeedConnWorkerOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final Logger LOGGER = LogManager.getLogger();

    public static final int DEFAULT_FRAME_SIZE = GlobalConfig.DEFAULT_FRAME_SIZE;
    private static final long serialVersionUID = 1L;
    private final ActiveRuntimeId intakeRuntimeId;
    private final String sourceNC;
    private Level logLevel = Level.DEBUG;

    public FeedConnWorkerOperatorDescriptor(JobSpecification spec, String sourceFeedNC,
            ActiveRuntimeId activeRuntimeId) {
        super(spec, 0, 1);
        EntityId feedIntakerEntityId = new EntityId(activeRuntimeId.getEntityId().getExtensionName(),
                activeRuntimeId.getEntityId().getDataverse(),
                activeRuntimeId.getEntityId().getEntityName().split(":")[0]);
        this.intakeRuntimeId = new ActiveRuntimeId(feedIntakerEntityId,
                FeedIntakeOperatorNodePushable.class.getSimpleName(), activeRuntimeId.getPartition());
        this.sourceNC = sourceFeedNC;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            private final ActiveManager activeManager = (ActiveManager) ((INcApplicationContext) ctx.getJobletContext()
                    .getServiceContext().getApplicationContext()).getActiveManager();
            private FeedIntakeOperatorNodePushable feedIntakeRuntime =
                    (FeedIntakeOperatorNodePushable) activeManager.getRuntime(intakeRuntimeId);
            private NodeControllerService ncs =
                    (NodeControllerService) ctx.getJobletContext().getServiceContext().getControllerService();

            private void registerFrameWithCC(List<Long> frameIds, boolean newJob) throws Exception {
                RegisterFrameMessage msg = new RegisterFrameMessage(ctx.getJobletContext().getJobId(), intakeRuntimeId,
                        (ArrayList<Long>) frameIds, sourceNC, newJob);
                ncs.sendApplicationMessageToCC(ctx.getJobletContext().getJobId().getCcId(),
                        JavaSerializationUtils.serialize(msg), null);
            }

            private boolean checkNewJob(List<ByteBuffer> frames) {
                for (ByteBuffer frame : frames) {
                    if (frame.capacity() == 0) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void initialize() throws HyracksDataException {
                try {
                    if (feedIntakeRuntime == null) {
                        System.out.println("Oops, something went wrong.");
                    } else {
                        writer.open();
                        Pair<List<Long>, List<ByteBuffer>> workloads =
                                feedIntakeRuntime.getFrames(ctx.getJobletContext().getJobId());
                        LOGGER.log(logLevel, "ConnJob " + ctx.getJobletContext().getJobId() + " obtained "
                                + workloads.getLeft().size());
                        registerFrameWithCC(workloads.getKey(), checkNewJob(workloads.getRight()));
                        for (ByteBuffer frame : workloads.getRight()) {
                            if (frame.capacity() != 0) {
                                writer.nextFrame(frame);
                                writer.flush();
                            }
                        }
                        LOGGER.log(logLevel, "ConnJob " + ctx.getJobletContext().getJobId() + " finished "
                                + workloads.getLeft().size());
                    }
                } catch (Exception e) {
                    throw new HyracksDataException(e.getMessage());
                } finally {
                    writer.close();
                }
            }
        };
    }
}
