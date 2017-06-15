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
import org.apache.asterix.active.IActiveRuntime;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.external.feed.dataflow.FeedFrameCollector;
import org.apache.asterix.external.feed.dataflow.SyncFeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.io.RotateRunFileReader;

/**
 * The first operator in a collect job in a feed.
 */
public class FeedCollectOperatorNodePushable extends ActiveSourceOperatorNodePushable implements IActiveRuntime {

    private final FeedConnectionId connectionId;
    private final FeedPolicyAccessor policyAccessor;
    private final ActiveManager activeManager;
    private final ActiveRuntimeId intakeRuntimeId;
    private FeedIntakeOperatorNodePushable feedIntakeRuntime;
    private RotateRunFileReader rotateRunFileReader;
    private VSizeFrame readerBufferFrame;
    private int batchCounter;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicy, int partition) throws HyracksDataException{
        // TODO: get rid of feed connection id if possible
        super(ctx, new ActiveRuntimeId(feedConnectionId.getConnEntityId(),
                FeedCollectOperatorNodePushable.class.getSimpleName(), partition));
        this.connectionId = feedConnectionId;
        this.policyAccessor = new FeedPolicyAccessor(feedPolicy);
        this.activeManager = (ActiveManager) ((INcApplicationContext) ctx.getJobletContext().getServiceContext()
                .getApplicationContext()).getActiveManager();
        this.intakeRuntimeId = new ActiveRuntimeId(feedConnectionId.getFeedId(),
                FeedIntakeOperatorNodePushable.class.getSimpleName(), partition);
        this.feedIntakeRuntime = (FeedIntakeOperatorNodePushable) activeManager.getRuntime(intakeRuntimeId);
        this.readerBufferFrame = new VSizeFrame(ctx);
        this.batchCounter = 20;
    }

    @Override
    public void start() throws HyracksDataException {
        try {
//            Thread.currentThread().setName("Collector Thread " + this.runtimeId.getEntityId().getEntityName());
            System.out.println("Collector started");
            FrameTupleAccessor tAccessor = new FrameTupleAccessor(recordDesc);
            // TODO: restore policy handler
            //            if (policyAccessor.flowControlEnabled()) {
            //                writer = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, writer, policyAccessor, tAccessor,
            //                        activeManager.getFramePool());
            //            } else {
            writer = new SyncFeedRuntimeInputHandler(ctx, writer, tAccessor);
            rotateRunFileReader = feedIntakeRuntime.subscribe(connectionId);
            rotateRunFileReader.open();
            writer.open();
            while (rotateRunFileReader.nextFrame(readerBufferFrame) && (batchCounter-- > 0)) {
                writer.nextFrame(readerBufferFrame.getBuffer());
            }
            System.out.println("Collector finished");
            //            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
//            rotateRunFileReader.close();
            writer.close();
        }
    }

    @Override
    protected void abort() throws HyracksDataException, InterruptedException {
        System.out.println("Collector aborted");
        feedIntakeRuntime.unsubscribe(connectionId);
    }
}
