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

import java.util.concurrent.TimeUnit;

import jdk.nashorn.internal.codegen.CompilerConstants;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActiveSourceOperatorNodePushable;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.message.ActiveEntityMessage;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.asterix.external.feed.dataflow.CallDeployedJobWithDataWriter;
import org.apache.asterix.external.feed.dataflow.FeedDataFrameBufferWriter;
import org.apache.asterix.external.feed.dataflow.DeployedJobBufferWriter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}.
 * Provides the core functionality to set up the artifacts for ingestion of a feed.
 * The artifacts are lazily activated when a feed receives a subscription request.
 */
public class FeedIntakeOperatorNodePushable extends ActiveSourceOperatorNodePushable {
    private static final Logger LOGGER = LogManager.getLogger();
    private final FeedAdapter adapter;
    private boolean poisoned = false;
    private DeployedJobSpecId connJobId;
    private int workerNum;
    private int batchSize;
    private int bufferSize;
    private EntityId feedId;

    public FeedIntakeOperatorNodePushable(IHyracksTaskContext ctx, EntityId feedId, IAdapterFactory adapterFactory,
            int partition, IRecordDescriptorProvider recordDescProvider,
            FeedIntakeOperatorDescriptor feedIntakeOperatorDescriptor, DeployedJobSpecId jobSpecId, String workerNum,
            String batchSize, String bufferSize) throws HyracksDataException {
        super(ctx, new ActiveRuntimeId(feedId, FeedIntakeOperatorNodePushable.class.getSimpleName(), partition));
        this.recordDesc = recordDescProvider.getOutputRecordDescriptor(feedIntakeOperatorDescriptor.getActivityId(), 0);
        adapter = (FeedAdapter) adapterFactory.createAdapter(ctx, runtimeId.getPartition());
        this.workerNum = Integer.valueOf(workerNum);
        this.batchSize = Integer.valueOf(batchSize);
        this.bufferSize = Integer.valueOf(bufferSize);
        this.connJobId = jobSpecId;
        this.feedId = feedId;
    }

    @Override
    protected void start() throws HyracksDataException {
        Throwable failure = null;
        Thread.currentThread().setName("Intake Thread");
        try {
            writer = new DeployedJobBufferWriter(ctx, writer, connJobId, feedId, workerNum, batchSize, bufferSize, 4);
            writer.open();
            synchronized (this) {
                if (poisoned) {
                    return;
                }
            }
            run();
        } catch (Throwable e) {
            failure = e;
            CleanupUtils.fail(writer, e);
            LOGGER.log(Level.WARN, "Failure during data ingestion", e);
        } finally {
            failure = CleanupUtils.close(adapter, failure);
            failure = CleanupUtils.close(writer, failure);
        }
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    private void run() throws HyracksDataException {
        // Start by getting the partition number from the manager
        LOGGER.info("Starting ingestion for partition:" + ctx.getTaskAttemptId().getTaskId().getPartition());
        try {
            doRun();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Unhandled Exception", e);
            throw HyracksDataException.create(e);
        }
    }

    private void doRun() throws HyracksDataException, InterruptedException {
        while (true) {
            try {
                // Start the adapter
                adapter.start(ctx.getTaskAttemptId().getTaskId().getPartition(), writer);
                // Adapter has completed execution
                return;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                LOGGER.log(Level.WARN, "Exception during feed ingestion ", e);
                throw HyracksDataException.create(e);
            }
        }
    }

    @Override
    protected void abort(long timeout, TimeUnit unit) throws HyracksDataException, InterruptedException {
        LOGGER.info(runtimeId + " aborting...");
        synchronized (this) {
            poisoned = true;
            try {
                if (!adapter.stop(unit.toMillis(timeout))) {
                    LOGGER.info(runtimeId + " failed to stop adapter. interrupting the thread...");
                    taskThread.interrupt();
                }
            } catch (HyracksDataException hde) {
                if (hde.getComponent() == ErrorCode.HYRACKS && hde.getErrorCode() == ErrorCode.TIMEOUT) {
                    LOGGER.log(Level.WARN, runtimeId + " stop adapter timed out. interrupting the thread...", hde);
                    taskThread.interrupt();
                } else {
                    LOGGER.log(Level.WARN, "Failure during attempt to stop " + runtimeId, hde);
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
}
