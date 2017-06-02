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
import org.apache.asterix.external.feed.dataflow.SyncFeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.w3c.dom.Entity;

/**
 * The first operator in a collect job in a feed.
 */
public class FeedCollectOperatorNodePushable extends ActiveSourceOperatorNodePushable implements IActiveRuntime {

    private final int partition;
    private final FeedConnectionId connectionId;
    private final FeedPolicyAccessor policyAccessor;
    private final ActiveManager activeManager;
    private final ActiveRuntimeId intakeRuntimeId;
    private FeedFrameCollector frameCollector;
    private FeedIntakeOperatorNodePushable feedIntakeRuntime;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicy, int partition) {
        // TODO: get rid of feed connection id if possible
        super(ctx, new ActiveRuntimeId(feedConnectionId.getConnEntityId(),
                FeedCollectOperatorNodePushable.class.getSimpleName(), partition));
        this.partition = partition;
        this.connectionId = feedConnectionId;
        this.policyAccessor = new FeedPolicyAccessor(feedPolicy);
        this.activeManager = (ActiveManager) ((INcApplicationContext) ctx.getJobletContext().getServiceContext()
                .getApplicationContext()).getActiveManager();
        this.intakeRuntimeId = new ActiveRuntimeId(feedConnectionId.getFeedId(),
                FeedIntakeOperatorNodePushable.class.getSimpleName(), partition);
        this.feedIntakeRuntime = (FeedIntakeOperatorNodePushable) activeManager.getRuntime(intakeRuntimeId);
    }

    @Override
    public void start() throws HyracksDataException {
        try {
            Thread.currentThread().setName("Collector Thread " + this.runtimeId.getEntityId().getEntityName());
            FrameTupleAccessor tAccessor = new FrameTupleAccessor(recordDesc);
            // TODO: restore policy handler
            //            if (policyAccessor.flowControlEnabled()) {
            //                writer = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, writer, policyAccessor, tAccessor,
            //                        activeManager.getFramePool());
            //            } else {
            writer = new SyncFeedRuntimeInputHandler(ctx, writer, tAccessor);
            frameCollector = new FeedFrameCollector(writer, connectionId);
            feedIntakeRuntime.subscribe(frameCollector);
            frameCollector.waitForFinish();
//            feedIntakeRuntime.unsubscribe(frameCollector);
            System.out.println("Dummy1");
            //            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    protected void abort() throws HyracksDataException, InterruptedException {
        feedIntakeRuntime.unsubscribe(frameCollector);
    }
}
