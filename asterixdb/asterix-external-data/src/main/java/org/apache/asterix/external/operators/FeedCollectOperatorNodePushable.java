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
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.partition.PartitionHolderId;
import org.apache.asterix.active.partition.PartitionHolderManager;
import org.apache.asterix.active.partition.PullablePartitionHolderByRecordRuntime;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.FeedWritableState;
import org.apache.hyracks.dataflow.std.misc.FileBackedState;
import org.apache.hyracks.dataflow.std.misc.MemoryBackedState;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The first operator in a collect job in a feed.
 */
public class FeedCollectOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    public static final Logger LOGGER = LogManager.getLogger();

    private final IHyracksTaskContext ctx;
    private final IRecordDataParser<char[]> parser;
    private final ArrayTupleBuilder tb;
    private final FrameTupleAppender fta;
    private final EntityId feedId;
    private final PartitionHolderManager phm;

    private PullablePartitionHolderByRecordRuntime partitionHolderRuntime;
    private final PartitionHolderId intakePhID;
    private final int localBatchSize;

    private CharArrayRecord charArrayRecord;
    private final ITracer tracer;
    private final long registry;
    private int sPartitionNum;
    private int ePartitionNum;
    private final boolean materialize;
    private FrameSweeper frameSweeper;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicy, int partition, IRecordDataParserFactory<?> parserFactory,
            int globalBatchSize, int nParititons, boolean materialize, int invocationCtr) throws HyracksDataException {
        this.ctx = ctx;
        this.feedId = feedConnectionId.getFeedId();
        // TODO: now we treat all records as bytearray before it arrives collector
        this.parser = (IRecordDataParser<char[]>) parserFactory.createRecordParser(ctx);
        this.charArrayRecord = new CharArrayRecord();
        this.tb = new ArrayTupleBuilder(1);
        this.fta = new FrameTupleAppender(new VSizeFrame(ctx), true);
        this.materialize = materialize;
        phm = (PartitionHolderManager) ((INcApplicationContext) ctx.getJobletContext().getServiceContext()
                .getApplicationContext()).getPartitionHolderMananger();
        this.intakePhID = new PartitionHolderId(feedId, FeedConstants.FEED_INTAKE_PARTITION_HOLDER, partition);
        sPartitionNum = (invocationCtr * (globalBatchSize % nParititons)) % nParititons;
        ePartitionNum = (invocationCtr * (globalBatchSize % nParititons) + globalBatchSize) % nParititons;

        if (globalBatchSize < 0) {
            localBatchSize = -1;
        } else if ((sPartitionNum < ePartitionNum && partition < ePartitionNum && partition >= sPartitionNum)
                || (sPartitionNum > ePartitionNum && (partition < ePartitionNum || partition >= sPartitionNum))) {
            // This condition should be simplified
            localBatchSize = globalBatchSize / nParititons + 1;
        } else {
            localBatchSize = globalBatchSize / nParititons;
        }

        this.tracer = ctx.getJobletContext().getServiceContext().getTracer();
        this.registry = tracer.getRegistry().get(FeedConstants.FEED_TRACER_CATEGORY);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(this + " is created with batch size " + localBatchSize);
        }
    }

    @Override
    public String toString() {
        return "Collector " + intakePhID.getPartition();
    }

    private void doAddRecord(String strRecord, FeedWritableState state) throws HyracksDataException {
        // TODO: trim get rid of trailing spaces. LF add to mark the boundary of records.
        // parse the incoming record
        charArrayRecord.set(strRecord.toCharArray());
        tb.reset();
        parser.parse(charArrayRecord, tb.getDataOutput());
        tb.addFieldEndOffset();
        // append to fta or state buffer
        if (!fta.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            // the original copy is kept in state. create 2 new copies, one for downstream, the other for fta.
            state.appendFrame(fta.getBuffer());
            IFrame writerFrame = new VSizeFrame(ctx);
            FrameUtils.copyAndFlip(fta.getBuffer(), writerFrame.getBuffer());
            try {
                frameSweeper.framePool.put(writerFrame.getBuffer());
            } catch (InterruptedException e) {
                throw new HyracksDataException(e.getMessage());
            }
            fta.reset(new VSizeFrame(ctx), true);
            if (!fta.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw HyracksDataException.create(ErrorCode.TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME, tb.getSize());
            }
        }
    }

    private void loadBatch(FeedWritableState state, IFrameWriter writer) throws Exception {
        long ctid = tracer.durationB("Collector getting frames", registry, null);

        if (partitionHolderRuntime != null) {
            frameSweeper = new FrameSweeper(writer);
            writer.open();
            Thread sweeperThread = new Thread(frameSweeper);
            sweeperThread.start();
            try {
                for (int iter1 = 0; iter1 < localBatchSize; iter1++) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(this + " ready to get a record");
                    }
                    long gtid = tracer.durationB("Collector's getting a record", registry, null);
                    String record = partitionHolderRuntime.nextRecord();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(this + " gets record with size " + record.length());
                    }
                    tracer.durationE(gtid, registry, String.valueOf(record.length()));
                    if (record.length() == 0) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(this + " stopped with " + iter1);
                        }
                        break;
                    } else {
                        doAddRecord(record, state);
                    }
                }
                // add leftover records into buffer
                if (fta.getTupleCount() > 0) {
                    state.appendFrame(fta.getBuffer());
                    IFrame writerFrame = new VSizeFrame(ctx);
                    FrameUtils.copyAndFlip(fta.getBuffer(), writerFrame.getBuffer());
                    frameSweeper.framePool.put(writerFrame.getBuffer());
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw HyracksDataException.create(e);
            } finally {
                frameSweeper.framePool.put(ByteBuffer.allocate(0));
                sweeperThread.join();
                writer.close();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(this + " is closing.");
                }
                tracer.instant("Load batch finishes", registry, ITracer.Scope.t, null);
            }
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(this + " cannot find " + intakePhID + ". Work is done.");
            }
        }
        tracer.durationE(ctid, registry, null);

    }

    @Override
    public void initialize() throws HyracksDataException {
        String threadName = Thread.currentThread().getName();
        long atid = tracer.durationB("Collector Running", registry, null);
        try {
            Thread.currentThread().setName("Collector Thread");
            partitionHolderRuntime = (PullablePartitionHolderByRecordRuntime) phm.getPartitionHolderRuntime(intakePhID);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(this + " connected to " + intakePhID);
            }
            FeedWritableState collectorState = (FeedWritableState) ctx.getStateObject(intakePhID);
            IFrame writerBuffer = new VSizeFrame(ctx);
            // inject failure handler
            // writer = new SyncFeedRuntimeInputHandler(ctx, writer, new FrameTupleAccessor(recordDesc));
            if (collectorState == null) {
                //                collectorState = new MemoryBackedState(ctx.getJobletContext().getJobId(), intakePhID);
                if (materialize) {
                    collectorState = new FileBackedState(ctx.getJobletContext().getJobId(), intakePhID);
                } else {
                    collectorState = new MemoryBackedState(ctx.getJobletContext().getJobId(), intakePhID);
                }
                collectorState.open(ctx);
                ctx.setStateObject(collectorState);
                // do some data gathering work here
                loadBatch(collectorState, writer);
                collectorState.close();
            } else {
                collectorState.writeOut(writer, writerBuffer);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Thread.currentThread().setName(threadName);
            tracer.durationE(atid, registry, null);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(this + " is closed.");
            }
            tracer.instant("Collector closed", registry, ITracer.Scope.t, null);
        }
    }

    private class FrameSweeper implements Runnable {

        protected BlockingQueue<ByteBuffer> framePool;
        protected final IFrameWriter writer;

        public FrameSweeper(IFrameWriter writer) {
            framePool = new LinkedBlockingQueue<>();
            this.writer = writer;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    ByteBuffer frame = framePool.take();
                    if (frame.capacity() == 0) {
                        break;
                    }
                    writer.nextFrame(frame);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
