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

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveMessage;
import org.apache.asterix.active.message.ActiveManagerMessage;
import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.external.feed.api.ISubscribableRuntime;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.message.EndFeedMessage;
import org.apache.asterix.external.feed.runtime.AdapterRuntimeManager;
import org.apache.asterix.external.feed.runtime.CollectionRuntime;
import org.apache.asterix.external.feed.runtime.IngestionRuntime;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * @deprecated
 *             Runtime for the FeedMessageOpertorDescriptor. This operator is responsible for communicating
 *             a feed message to the local feed manager on the host node controller.
 *             For messages, use IMessageBroker interfaces
 * @see FeedMessageOperatorDescriptor
 *      IFeedMessage
 *      IFeedManager
 */
@Deprecated
public class FeedMessageOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(FeedMessageOperatorNodePushable.class.getName());

    private final ActiveManagerMessage message;
    private final EntityId feedId;
    private final ActiveManager feedManager;

    public FeedMessageOperatorNodePushable(IHyracksTaskContext ctx, byte messageType, String runtimeType, int partition,
            EntityId feedId) {
        this.message = new ActiveManagerMessage(messageType, "", new ActiveRuntimeId(feedId, runtimeType, partition));
        this.feedId = feedId;
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedManager = (ActiveManager) runtimeCtx.getActiveManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            writer.open();
            feedManager.submit(message);
            LOGGER.log(Level.INFO, "Feed " + feedId.getEntityName() + " is stopped.");
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }
}
