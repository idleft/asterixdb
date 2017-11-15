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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActiveSourceOperatorNodePushable;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.asterix.external.feed.dataflow.DistributeFeedFrameWriter;
import org.apache.asterix.external.feed.dataflow.FeedFrameCollector;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.io.MessagingFrameTupleAppender;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}.
 * Provides the core functionality to set up the artifacts for ingestion of a feed.
 * The artifacts are lazily activated when a feed receives a subscription request.
 */
public class FeedIntakeOperatorNodePushable extends ActiveSourceOperatorNodePushable {
    private static final Logger LOGGER = Logger.getLogger(FeedIntakeOperatorNodePushable.class.getName());
    // TODO: Make configurable https://issues.apache.org/jira/browse/ASTERIXDB-2065
    public static final int DEFAULT_ABORT_TIMEOUT = 10000;
    private final FeedIntakeOperatorDescriptor opDesc;
    private final FeedAdapter adapter;
    private final int partition;
    private boolean poisoned = false;
    private DistributeFeedFrameWriter distributeFeedFrameWriter;
    private final int initConnNum;

    public FeedIntakeOperatorNodePushable(IHyracksTaskContext ctx, EntityId feedId, IAdapterFactory adapterFactory,
            int partition, IRecordDescriptorProvider recordDescProvider,
            FeedIntakeOperatorDescriptor feedIntakeOperatorDescriptor, int initConnNum) throws HyracksDataException {
        super(ctx, new ActiveRuntimeId(feedId, FeedIntakeOperatorNodePushable.class.getSimpleName(), partition));
        this.opDesc = feedIntakeOperatorDescriptor;
        this.recordDesc = recordDescProvider.getOutputRecordDescriptor(opDesc.getActivityId(), 0);
        this.partition = partition;
        adapter = (FeedAdapter) adapterFactory.createAdapter(ctx, runtimeId.getPartition());
        this.initConnNum = initConnNum;
    }

    @Override
    protected void start() throws HyracksDataException, InterruptedException {
        String before = Thread.currentThread().getName();
        Thread.currentThread().setName("Intake Thread");
        try {
            distributeFeedFrameWriter = new DistributeFeedFrameWriter(this.runtimeId.getEntityId(), writer, partition, adapter, initConnNum);
            synchronized (this) {
                if (poisoned) {
                    return;
                }
            }
            distributeFeedFrameWriter.open();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failure during data ingestion", e);
            throw e;
        } finally {
            writer.close();
            Thread.currentThread().setName(before);
        }
    }

    @Override
    protected void abort() throws HyracksDataException, InterruptedException {
        LOGGER.info(runtimeId + " aborting...");
        synchronized (this) {
            poisoned = true;
            try {
                if (!adapter.stop(DEFAULT_ABORT_TIMEOUT)) {
                    LOGGER.info(runtimeId + " failed to stop adapter. interrupting the thread...");
                    taskThread.interrupt();
                }
            } catch (HyracksDataException hde) {
                if (hde.getComponent() == ErrorCode.HYRACKS && hde.getErrorCode() == ErrorCode.TIMEOUT) {
                    LOGGER.log(Level.WARNING, runtimeId + " stop adapter timed out. interrupting the thread...", hde);
                    taskThread.interrupt();
                } else {
                    LOGGER.log(Level.WARNING, "Failure during attempt to stop " + runtimeId, hde);
                    throw hde;
                }
            }
        }
    }

    @Override
    public String getStats() {
        if (adapter != null) {
            return "{\"adapter-stats\": " + adapter.getStats() + "}";
        } else {
            return "\"Runtime stats is not available.\"";
        }
    }

    public synchronized void subscribe(FeedFrameCollector frameCollector) throws HyracksDataException {
        distributeFeedFrameWriter.subscribe(frameCollector);
    }

    public synchronized void unsubscribe(FeedFrameCollector frameCollector) throws HyracksDataException {
        distributeFeedFrameWriter.unsubscribeFeed(frameCollector.getCollectorId());
    }
}
