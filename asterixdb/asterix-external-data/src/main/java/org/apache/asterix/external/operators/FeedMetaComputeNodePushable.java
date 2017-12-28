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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.memory.ConcurrentFramePool;
import org.apache.asterix.external.feed.dataflow.FeedExceptionHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/*
 * This IFrameWriter doesn't follow the contract
 */
public class FeedMetaComputeNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final int WORKER_N = 4;
    private static final ByteBuffer POISON_PILL = ByteBuffer.allocate(0);

    /**
     * A policy accessor that ensures dynamic decisions for a feed are taken
     * in accordance with the associated ingestion policy
     **/
    private FeedPolicyAccessor policyAccessor;

    /**
     * A unique identifier for the feed instance. A feed instance represents
     * the flow of data from a feed to a dataset.
     **/
    private FeedConnectionId connectionId;

    /**
     * Denotes the i'th operator instance in a setting where K operator
     * instances are scheduled to run in parallel
     **/
    private int partition;

    /** The (singleton) instance of IFeedManager **/
    private ActiveManager feedManager;

    private FrameTupleAccessor fta;

    private final IHyracksTaskContext ctx;

    private final FeedRuntimeType runtimeType = FeedRuntimeType.COMPUTE;

    private final FeedMetaOperatorDescriptor opDesc;

    private final IRecordDescriptorProvider recordDescProvider;

    private boolean opened;

    private final BlockingQueue<ByteBuffer> inbox;

    private final FeedExceptionHandler feedExceptionHandler;

    private final ConcurrentFramePool framepool;
    private final AbstractUnaryInputUnaryOutputOperatorNodePushable[] pipelineList;
    private final PipelineWorker[] workerList;
    private final ExecutorService threadPoolExecutor;
    private final int initialFrameSize;

    /*
     * In this operator:
     * nextWriter is the network partitioner
     * coreOperator is the first operator
     */
    public FeedMetaComputeNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
            int partition, int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicyProperties, FeedMetaOperatorDescriptor feedMetaOperatorDescriptor)
            throws HyracksDataException {
        this.ctx = ctx;
        this.pipelineList = new AbstractUnaryInputUnaryOutputOperatorNodePushable[WORKER_N];
        this.workerList = new PipelineWorker[WORKER_N];
        for (int iter1 = 0; iter1 < WORKER_N; iter1++) {
            pipelineList[iter1] = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                    .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
        }
        this.policyAccessor = new FeedPolicyAccessor(feedPolicyProperties);
        this.partition = partition;
        this.connectionId = feedConnectionId;
        this.feedManager = (ActiveManager) ((INcApplicationContext) ctx.getJobletContext().getServiceContext()
                .getApplicationContext()).getActiveManager();
        this.opDesc = feedMetaOperatorDescriptor;
        this.recordDescProvider = recordDescProvider;
        this.inbox = new LinkedBlockingQueue<>();
        this.fta = new FrameTupleAccessor(recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0));
        this.feedExceptionHandler = new FeedExceptionHandler(ctx, fta);
        this.framepool = feedManager.getFramePool();
        this.threadPoolExecutor = Executors.newFixedThreadPool(WORKER_N);
        this.initialFrameSize = ctx.getInitialFrameSize();
    }

    @Override
    public void open() throws HyracksDataException {
        ActiveRuntimeId runtimeId = new ActiveRuntimeId(connectionId.getFeedId(), runtimeType.toString(), partition);
        try {
            initializeNewFeedRuntime(runtimeId);
            opened = true;
            if (LOGGER.isInfoEnabled()) {
                LOGGER.log(Level.INFO, this.getClass().getSimpleName() + " on " + partition + " is opened.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private void initializeNewFeedRuntime(ActiveRuntimeId runtimeId) throws Exception {
        //        FeedPolicyAccessor fpa = policyAccessor;
        writer = new ConcurrentSurrogateWriter(writer);
        for (int iter1 = 0; iter1 < pipelineList.length; iter1++) {
            pipelineList[iter1].setOutputFrameWriter(0, writer, recordDesc);
            workerList[iter1] = new PipelineWorker(pipelineList[iter1], iter1, partition);
            threadPoolExecutor.submit(workerList[iter1]);
        }
        //        if (fpa.flowControlEnabled()) {
        //            nextWriter = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, coreOperator, fpa, fta,
        //                    feedManager.getFramePool());
        //        } else {
        //        nextWriter = new SyncFeedRuntimeInputHandler(ctx, coreOperator, fta);
        //        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {

        if (LOGGER.isInfoEnabled()) {
            LOGGER.log(Level.INFO,
                    this.getClass().getSimpleName() + " on " + partition + " received buffer " + buffer.array().length);
        }
        try {
            ByteBuffer next = (buffer.capacity() <= framepool.getMaxFrameSize()) ? getFreeBuffer(buffer.capacity())
                    : null;
            if (next != null) {
                next.put(buffer);
                inbox.put(next);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARN, e.getMessage(), e);
            throw new HyracksDataException(e);
        }
    }

    private ByteBuffer getFreeBuffer(int frameSize) throws HyracksDataException {
        int numFrames = frameSize / initialFrameSize;
        if (numFrames == 1) {
            return framepool.get();
        } else {
            return framepool.get(frameSize);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        if (opened) {
            try {
                for (int iter1=0; iter1 < WORKER_N; iter1++) {
                    inbox.put(POISON_PILL);
                }
//                System.out.println("Close call " + threadPoolExecutor.shutdownNow());
//                List<Runnable> tasks = threadPoolExecutor.shutdownNow();
                threadPoolExecutor.shutdown();
                System.out.println("Executor stops at " + threadPoolExecutor.awaitTermination(3, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
            if (LOGGER.isInfoEnabled()) {
                LOGGER.log(Level.INFO, this.getClass().getSimpleName() + " on " + partition + " is closed.");
            }
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }

    private class PipelineWorker implements Runnable {

        private final AbstractUnaryInputUnaryOutputOperatorNodePushable pipeline;
        private Throwable cause;
        private int counter;
        private final int partition;
        private final int workerId;
        private final String workerName;

        public PipelineWorker(AbstractUnaryInputUnaryOutputOperatorNodePushable pipeline, int id, int partition) {
            this.pipeline = pipeline;
            this.workerId = id;
            this.counter = 0;
            this.partition = partition;
            workerName = "Pipeline_worker_" + partition + "-" + workerId;
        }

        public Throwable getCause() {
            return this.cause;
        }

        private Throwable consume(ByteBuffer frame) {
            while (frame != null) {
                try {
                    pipeline.nextFrame(frame);
                    frame = null;
                } catch (HyracksDataException e) {
                    frame = feedExceptionHandler.handle(e, frame);
                    if (frame == null) {
                        this.cause = e;
                        return e;
                    }
                } catch (Throwable th) {
                    this.cause = th;
                    return th;
                }
            }
            return null;
        }

        @Override
        public void run() {
            boolean running = true;
            Thread.currentThread().setName(workerName);
            System.out.println(workerName + " started.");
            try {
                pipeline.open();
                while (running) {
                    ByteBuffer frame = inbox.poll();
                    if (frame == null) {
                        pipeline.flush();
                        frame = inbox.take();
                    }

                    if (frame == POISON_PILL) {
                        System.out.println(workerName + " poisoned.");
                        running = false;
                    } else {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.log(Level.INFO, workerName + " obtained frame.");
                        }
                        running = consume(frame) == null;
                        counter++;
                        framepool.release(frame);
                    }
                }
                pipeline.close();
            } catch (HyracksDataException | InterruptedException e) {
                this.cause = e;
            }
            System.out.println(workerName + " stopped with " + counter + " processed.");
        }
    }

    private class ConcurrentSurrogateWriter implements IFrameWriter {

        private final IFrameWriter nextWriter;
        private Integer workerCounter;


        public ConcurrentSurrogateWriter(IFrameWriter nextWriter) {
            this.nextWriter = nextWriter;
            workerCounter = 0;
        }

        @Override
        public void open() throws HyracksDataException {
            synchronized (this) {
                if (workerCounter++ < 1) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.log(Level.INFO, "Surrogate nextWriter opened");
                    }
                    nextWriter.open();
                    opened = true;
                }
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            synchronized (this) {
                nextWriter.nextFrame(buffer);
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            synchronized (this) {
                nextWriter.fail();
            }
        }

        @Override
        public void close() throws HyracksDataException {
            synchronized (this) {
                if (--workerCounter < 1) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.log(Level.INFO, "Surrogate nextWriter closed");
                    }
                    nextWriter.close();
                    opened = false;
                }
            }
        }
    }
}
