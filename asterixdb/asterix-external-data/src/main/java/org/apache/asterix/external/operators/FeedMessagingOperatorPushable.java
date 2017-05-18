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

import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

import java.nio.ByteBuffer;
import java.util.Map;

public class FeedMessagingOperatorPushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private final IHyracksTaskContext ctx;
    private final IRecordDescriptorProvider recordDescriptorProvider;
    private final VSizeFrame message;
    private FrameTupleAccessor fta;
    private final ActivityId activityId;
    private final FeedPolicyAccessor fpa;
    private boolean opened;

    public FeedMessagingOperatorPushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescriptorProvider,
            Map<String, String> feedPolicyProperties, ActivityId activityId) throws HyracksDataException {
        this.ctx = ctx;
        this.recordDescriptorProvider = recordDescriptorProvider;
        this.message = new VSizeFrame(ctx);
        this.activityId = activityId;
        // TODO: Enable policy control for each message handler by configuration
        this.fpa = new FeedPolicyAccessor(feedPolicyProperties);
        this.opened = false;
        TaskUtil.putInSharedMap(HyracksConstants.KEY_MESSAGE, message, ctx);
    }

    @Override
    public void open() throws HyracksDataException {
        fta = new FrameTupleAccessor(recordDescriptorProvider.getInputRecordDescriptor(activityId, 0));
        writer.open();
        opened = true;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        FeedUtils.processFeedMessage(buffer, message, fta);
        writer.nextFrame(buffer);
    }

    @Override
    public void fail() throws HyracksDataException {
        if (opened) {
            writer.fail();
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (opened) {
            writer.close();
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }
}
