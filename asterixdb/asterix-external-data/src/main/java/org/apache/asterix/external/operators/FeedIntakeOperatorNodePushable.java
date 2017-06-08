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

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActiveSourceOperatorNodePushable;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.asterix.external.feed.dataflow.DistributeFeedFrameWriter;
import org.apache.asterix.external.feed.dataflow.FeedFrameCollector;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.runtime.AdapterRuntimeManager;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.io.RotateRunFileReader;
import org.apache.hyracks.dataflow.common.io.RotateRunFileWriter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}.
 * Provides the core functionality to set up the artifacts for ingestion of a feed.
 * The artifacts are lazily activated when a feed receives a subscription request.
 */
public class FeedIntakeOperatorNodePushable extends ActiveSourceOperatorNodePushable {

    private static final int ROTATE_BUFFER_SIZE = 16;
    private static final int ROTATE_FRAME_PER_FILE = 10;

    private final int partition;
    private final IAdapterFactory adapterFactory;
    private volatile AdapterRuntimeManager adapterRuntimeManager;
    private RotateRunFileWriter rotateRunFileWriter;
    private final int initConnectionsCount;
    private final int frameSize;
    private Map<FeedConnectionId, RotateRunFileReader> readersList;

    public FeedIntakeOperatorNodePushable(IHyracksTaskContext ctx, EntityId feedId, IAdapterFactory adapterFactory,
            int partition, FeedPolicyAccessor policyAccessor, IRecordDescriptorProvider recordDescProvider,
            FeedIntakeOperatorDescriptor feedIntakeOperatorDescriptor, int initConnectionsCount, int frameSize) {
        super(ctx, new ActiveRuntimeId(feedId, FeedIntakeOperatorNodePushable.class.getSimpleName(), partition));
        this.recordDesc = recordDescProvider.getOutputRecordDescriptor(feedIntakeOperatorDescriptor.getActivityId(), 0);
        this.partition = partition;
        this.adapterFactory = adapterFactory;
        this.initConnectionsCount = initConnectionsCount;
        this.frameSize = frameSize;
        this.readersList = new ConcurrentHashMap<>();
    }

    @Override
    protected void start() throws HyracksDataException {
        try {
            Thread.currentThread().setName("Intake Thread " + runtimeId.getEntityId().getEntityName());
            FeedAdapter adapter = (FeedAdapter) adapterFactory.createAdapter(ctx, partition);
            rotateRunFileWriter = new RotateRunFileWriter(this.getRuntimeId().getRuntimeName(), ctx, ROTATE_BUFFER_SIZE,
                    ROTATE_FRAME_PER_FILE, frameSize);
            // the rotateRunFileWriter has to be opened here, as we need to make sure the writer is ready before
            // collectJob starts.
            rotateRunFileWriter.open();
            adapterRuntimeManager = new AdapterRuntimeManager(ctx, runtimeId.getEntityId(), adapter,
                    rotateRunFileWriter, partition, initConnectionsCount);
            adapterRuntimeManager.start();
            adapterRuntimeManager.waitForFinish();
            if (adapterRuntimeManager.isFailed()) {
                throw new RuntimeDataException(
                        ErrorCode.OPERATORS_FEED_INTAKE_OPERATOR_NODE_PUSHABLE_FAIL_AT_INGESTION);
            }
        } catch (Exception e) {
            /*
             * An Interrupted Exception is thrown if the Intake job cannot progress further due to failure of another
             * node involved in the Hyracks job. As the Intake job involves only the intake operator, the exception is
             * indicative of a failure at the sibling intake operator location. The surviving intake partitions must
             * continue to live and receive data from the external source.
             */
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }

    @Override
    protected void abort() throws HyracksDataException, InterruptedException {
        if (adapterRuntimeManager != null) {
            adapterRuntimeManager.stop();
        }
    }

    public RotateRunFileReader subscribe(FeedConnectionId connectionId) throws HyracksDataException {
        if (readersList.containsKey(connectionId)) {
            return readersList.get(connectionId);
        } else {
            RotateRunFileReader newReader = adapterRuntimeManager.subscribe();
            readersList.put(connectionId, newReader);
            return newReader;
        }
    }

    public void unsubscribe(FeedConnectionId connectionId) throws HyracksDataException {
        if (!readersList.containsKey(connectionId)) {
            throw new HyracksDataException("Connection " + connectionId + "is not registered!");
        } else {
            readersList.get(connectionId).close();
            readersList.remove(connectionId);
        }
    }
}
