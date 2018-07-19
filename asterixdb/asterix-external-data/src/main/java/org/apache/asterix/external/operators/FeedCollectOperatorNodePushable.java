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

import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.partition.PartitionHolderId;
import org.apache.asterix.active.partition.PartitionHolderManager;
import org.apache.asterix.active.partition.PullablePartitionHolderByRecordRuntime;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.dataflow.TupleForwarder;
import org.apache.asterix.external.feed.dataflow.SyncFeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
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
    private final PartitionHolderId phid;
    private final int localBatchSize;

    private CharArrayRecord charArrayRecord;
    private final ITracer tracer;
    private final long registry;
    private int sPartitionNum;
    private int ePartitionNum;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicy, int partition, IRecordDataParserFactory<?> parserFactory,
            int globalBatchSize, int nParititons, int invocationCtr) throws HyracksDataException {
        this.ctx = ctx;
        this.feedId = feedConnectionId.getFeedId();
        // TODO: now we treat all records as bytearray before it arrives collector
        this.parser = (IRecordDataParser<char[]>) parserFactory.createRecordParser(ctx);
        this.charArrayRecord = new CharArrayRecord();
        this.tb = new ArrayTupleBuilder(1);
        this.fta = new FrameTupleAppender(new VSizeFrame(ctx), true);
        phm = (PartitionHolderManager) ((INcApplicationContext) ctx.getJobletContext().getServiceContext()
                .getApplicationContext()).getPartitionHolderMananger();
        this.phid = new PartitionHolderId(feedId, FeedConstants.FEED_INTAKE_PARTITION_HOLDER, partition);
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
        return "Collector " + phid.getPartition();
    }

    @Override
    public void initialize() throws HyracksDataException {
        String threadName = Thread.currentThread().getName();
        long atid = tracer.durationB("Collector Running", registry, null);
        try {
            Thread.currentThread().setName("Collector Thread");
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(this + " is initializing");
            }
            FrameTupleAccessor tAccessor = new FrameTupleAccessor(recordDesc);
            writer = new SyncFeedRuntimeInputHandler(ctx, writer, tAccessor);
            partitionHolderRuntime = (PullablePartitionHolderByRecordRuntime) phm.getPartitionHolderRuntime(phid);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(this + " connected to " + phid);
            }
            tracer.instant("Collector connected to ph", registry, ITracer.Scope.t, null);
            writer.open();
            long ctid = tracer.durationB("Collector getting frames", registry, null);
            if (partitionHolderRuntime != null) {
                for (int iter1 = 0; localBatchSize == -1 || iter1 < localBatchSize; iter1++) {
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
                        break;
                    } else {
                        doAddRecord(record);
                    }
                }
                long ftid = tracer.durationB("Collector's last flush", registry, null);
//                tf.flush();
                tracer.durationE(ftid, registry, null);
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(this + " cannot find " + phid + ". Work is done.");
                }
            }
            tracer.durationE(ctid, registry, null);
        } catch (Exception e) {
            e.printStackTrace();
            throw HyracksDataException.create(e);
        } finally {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(this + " is closing.");
            }
            tracer.instant("Collector closes", registry, ITracer.Scope.t, null);
            fta.flush(writer);
            writer.close();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(this + " is closed.");
            }
            tracer.instant("Collector closed", registry, ITracer.Scope.t, null);
            Thread.currentThread().setName(threadName);
            tracer.durationE(atid, registry, null);
        }
    }

    private void doAddRecord(String strRecord) throws HyracksDataException {
        // TODO: trim get rid of trailing spaces. LF add to mark the boundary of records.
        charArrayRecord.set(strRecord.toCharArray());
        tb.reset();
        parser.parse(charArrayRecord, tb.getDataOutput());
        tb.addFieldEndOffset();
        //TODO: maybe get rid of tf?
        FrameUtils.appendToWriter(writer, fta, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
    }
}
