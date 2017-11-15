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

import java.util.Map;

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActiveSourceOperatorNodePushable;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveRuntime;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.external.feed.dataflow.FeedFrameCollector;
import org.apache.asterix.external.feed.dataflow.FeedRuntimeInputHandler;
import org.apache.asterix.external.feed.dataflow.SyncFeedRuntimeInputHandler;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

/**
 * The first operator in a collect job in a feed.
 */
public class FeedCollectOperatorNodePushable extends ActiveSourceOperatorNodePushable implements IActiveRuntime {

    private final FeedPolicyAccessor policyAccessor;
    private final ActiveManager activeManager;
    private final ActiveRuntimeId intakeRuntimeId;
    private final EntityId feedCollectorEntityId;
    private FeedFrameCollector frameCollector;
    private FeedIntakeOperatorNodePushable feedIntakeRuntime;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, EntityId feedCollectorEntityId,
            Map<String, String> feedPolicy, int partition) {
        // TODO: get rid of feed connection id if possible
        super(ctx, new ActiveRuntimeId(feedCollectorEntityId, FeedCollectOperatorNodePushable.class.getSimpleName(),
                partition));
        this.policyAccessor = new FeedPolicyAccessor(feedPolicy);
        this.activeManager = (ActiveManager) ((INcApplicationContext) ctx.getJobletContext().getServiceContext()
                .getApplicationContext()).getActiveManager();
        EntityId feedIntakerEntityId = new EntityId(feedCollectorEntityId.getExtensionName(),
                feedCollectorEntityId.getDataverse(), feedCollectorEntityId.getEntityName().split(":")[0]);
        this.intakeRuntimeId = new ActiveRuntimeId(feedIntakerEntityId,
                FeedIntakeOperatorNodePushable.class.getSimpleName(), partition);
        this.feedIntakeRuntime = (FeedIntakeOperatorNodePushable) activeManager.getRuntime(intakeRuntimeId);
        this.feedCollectorEntityId = feedCollectorEntityId;
    }

    @Override
    public void start() throws HyracksDataException {
        try {
            FrameTupleAccessor tAccessor = new FrameTupleAccessor(recordDesc);
            if (policyAccessor.flowControlEnabled()) {
                writer = new FeedRuntimeInputHandler(ctx, feedCollectorEntityId, runtimeId.getPartition(), writer,
                        policyAccessor, tAccessor, activeManager.getFramePool());
            } else {
                writer = new SyncFeedRuntimeInputHandler(ctx, writer, tAccessor);
            }
            frameCollector = new FeedFrameCollector(writer, feedCollectorEntityId);
            feedIntakeRuntime.subscribe(frameCollector);
            frameCollector.waitForFinish();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    protected void abort() throws HyracksDataException, InterruptedException {
        feedIntakeRuntime.unsubscribe(frameCollector);
    }
}
