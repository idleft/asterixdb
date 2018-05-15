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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.message.DropDeployedJobMessage;
import org.apache.asterix.active.partition.IPullablePartitionHolderRuntime;
import org.apache.asterix.active.partition.PartitionHolderId;
import org.apache.asterix.active.partition.PartitionHolderManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.dataflow.TupleForwarder;
import org.apache.asterix.external.feed.dataflow.SyncFeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The first operator in a collect job in a feed.
 */
public class FeedCollectOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    Logger LOGGER = LogManager.getLogger();

    private final IHyracksTaskContext ctx;
    private final IRecordDataParser<char[]> parser;
    private final FrameTupleAccessor fta;
    private final ArrayTupleBuilder tb;
    private TupleForwarder tf;
    private final EntityId feedId;
    private final PartitionHolderManager phm;

    private IPullablePartitionHolderRuntime partitionHolderRuntime;
    private final PartitionHolderId phid;
    private final int batchSize = Integer.MAX_VALUE;
    private NodeControllerService ncs;

    private CharArrayRecord record;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicy, int partition, IRecordDataParserFactory<?> parserFactory)
            throws HyracksDataException {
        this.ctx = ctx;
        this.feedId = feedConnectionId.getFeedId();
        // TODO: now we treat all records as bytearray before it arrives collector
        this.parser = (IRecordDataParser<char[]>) parserFactory.createRecordParser(ctx);
        this.fta = new FrameTupleAccessor(null);
        this.record = new CharArrayRecord();
        this.tb = new ArrayTupleBuilder(1);
        phm = (PartitionHolderManager) ((INcApplicationContext) ctx.getJobletContext().getServiceContext()
                .getApplicationContext()).getPartitionHolderMananger();
        this.phid = new PartitionHolderId(feedId, FeedConstants.FEED_INTAKE_PARTITION_HOLDER, partition);
        ncs = (NodeControllerService) ctx.getJobletContext().getServiceContext().getControllerService();
    }

    @Override
    public String toString() {
        return "Collector " + phid;
    }

    @Override
    public void initialize() throws HyracksDataException {
        String threadName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName("Collector Thread");
            FrameTupleAccessor tAccessor = new FrameTupleAccessor(recordDesc);
            writer = new SyncFeedRuntimeInputHandler(ctx, writer, tAccessor);
            tf = new TupleForwarder(ctx, writer);
            partitionHolderRuntime = (IPullablePartitionHolderRuntime)phm.getPartitionHolderRuntime(phid);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(this + " connected to " + phid);
            }
            writer.open();
            if (partitionHolderRuntime != null) {
                for (int iter1 = 0; iter1 < batchSize; iter1++) {
                    ByteBuffer dataframe = partitionHolderRuntime.getHoldFrame();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(this + " gets frame with size " + dataframe.capacity());
                    }
                    if (dataframe.capacity() == 0) {
                        untrackDeployedJob();
                        break;
                    } else {
                        doPushFrame(dataframe);
                    }
                }
                tf.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw HyracksDataException.create(e);
        } finally {
            tf.complete();
            writer.close();
            Thread.currentThread().setName(threadName);
        }
    }

    private void untrackDeployedJob() throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(this + " poisoned.");
        }
        DropDeployedJobMessage msg = new DropDeployedJobMessage(feedId);
        ncs.sendApplicationMessageToCC(ctx.getJobletContext().getJobId().getCcId(),
                JavaSerializationUtils.serialize(msg), null);
    }

    private void doPushFrame(ByteBuffer buffer) throws HyracksDataException {
        fta.reset(buffer);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(this + " gets frame with size " + fta.getTupleCount());
        }
        for (int iter1 = 0; iter1 < fta.getTupleCount(); iter1++) {
            // TODO: trim get rid of trailing spaces. LF add to mark the boundary of records.
            String strRecord = new String(Arrays.copyOfRange(fta.getBuffer().array(), fta.getTupleStartOffset(iter1),
                    fta.getTupleStartOffset(iter1) + fta.getTupleLength(iter1)), StandardCharsets.UTF_8).trim()
                    + ExternalDataConstants.LF;
            record.set(strRecord.toCharArray());
            tb.reset();
            parser.parse(record, tb.getDataOutput());
            tb.addFieldEndOffset();
            //TODO: maybe get rid of tf?
            tf.addTuple(tb);
        }
    }
}
