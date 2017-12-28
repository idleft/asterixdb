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

package org.apache.asterix.external.feed.test;

import junit.framework.TestCase;
import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.memory.ConcurrentFramePool;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.operators.FeedMetaComputeNodePushable;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.mockito.Mockito;
import org.mockito.internal.matchers.Any;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.when;

public class FeedMetaComputeNodePushableUnitTest extends TestCase {

    private static final int FRAME_SIZE = 32768;
    public static final String NODE_ID = "nc0";
    private static final int PARTITION = 0;
    private static final int PARTITION_N = 4;

    private static final String DATAVERSE = "dataverse";
    private static final String DATASET = "dataset";
    private static final String FEED = "feed";
    ConcurrentFramePool framePool = new ConcurrentFramePool(NODE_ID, FRAME_SIZE * 10, FRAME_SIZE);

    private static IOperatorDescriptor getPipelineOp() throws HyracksDataException {
        AlgebricksMetaOperatorDescriptor coreOpDesc = Mockito.mock(AlgebricksMetaOperatorDescriptor.class);

        when(coreOpDesc.createPushRuntime(anyObject(), anyObject(), anyInt(), anyInt()))
                .thenReturn(new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                    @Override
                    public void open() throws HyracksDataException {
                        writer.open();
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        writer.nextFrame(buffer);
                    }

                    @Override
                    public void fail() throws HyracksDataException {
                        writer.fail();
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        writer.close();
                    }
                });
        return coreOpDesc;
    }

    private INcApplicationContext getNcApplicationContext() {
        INcApplicationContext applicationContext = Mockito.mock(INcApplicationContext.class);
        ActiveManager activeManager = Mockito.mock(ActiveManager.class);
        when(activeManager.getFramePool()).thenReturn(framePool);
        when(applicationContext.getActiveManager()).thenReturn(activeManager);
        return applicationContext;
    }

    AbstractUnaryInputUnaryOutputOperatorNodePushable getFeedComputePushable(IHyracksTaskContext ctx)
            throws HyracksDataException {

        IRecordDescriptorProvider recordDescProvider = Mockito.mock(IRecordDescriptorProvider.class);

        FeedMetaOperatorDescriptor metaOpDesc = Mockito.mock(FeedMetaOperatorDescriptor.class);
        FeedConnectionId feedConnectionId = new FeedConnectionId(DATAVERSE, FEED, DATASET);

        return new FeedMetaComputeNodePushable(ctx, recordDescProvider, PARTITION, PARTITION_N, getPipelineOp(),
                feedConnectionId, Collections.EMPTY_MAP, metaOpDesc);
    }

    @org.junit.Test
    public void testMultipleThreadPipelineWorker() throws HyracksDataException {

        int FRAME_NUM = 100;
        ByteBuffer dataBuffer = ByteBuffer.allocate(FRAME_SIZE);
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE, getNcApplicationContext());
        WriterWithCounter writer = new WriterWithCounter();
        AbstractUnaryInputUnaryOutputOperatorNodePushable feedComputePushable = getFeedComputePushable(ctx);
        feedComputePushable.setOutputFrameWriter(0, writer, null);

        feedComputePushable.open();

        for (int iter1 = 0; iter1 < FRAME_NUM; iter1++) {
            feedComputePushable.nextFrame(dataBuffer);
        }

        feedComputePushable.close();

        Assert.assertEquals(FRAME_NUM, writer.counter);
    }

    private class WriterWithCounter implements IFrameWriter {

        private int counter;

        public WriterWithCounter() {
            counter = 0;
        }

        @Override
        public void open() throws HyracksDataException {
            // no op
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            counter++;
        }

        @Override
        public void fail() throws HyracksDataException {
            counter = -1;
        }

        @Override
        public void close() throws HyracksDataException {
            // no op
        }
    }
}
