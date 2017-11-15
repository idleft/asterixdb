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

package org.apache.asterix.app.active;

import java.util.Arrays;
import java.util.EnumSet;

import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventSubscriber;
import org.apache.asterix.active.IRetryPolicyFactory;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.external.feed.watch.FeedActivityDetails;
import org.apache.asterix.external.feed.watch.WaitForStateSubscriber;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.utils.FeedOperations;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedConnEventsListener extends ActiveEntityEventsListener {

    private final Feed feed;
    private final FeedConnection feedConn;
    private final String[] intakeLocations;

    public FeedConnEventsListener(IStatementExecutor statementExecutor, ICcApplicationContext appCtx,
            IHyracksClientConnection hcc, EntityId entityId, Dataset dataset,
            AlgebricksAbsolutePartitionConstraint locations, String runtimeName, IRetryPolicyFactory retryPolicyFactory,
            Feed feed, final FeedConnection feedConn, String[] intakeLocations) throws HyracksDataException {
        super(statementExecutor, appCtx, hcc, entityId, Arrays.asList(dataset), locations, runtimeName,
                retryPolicyFactory);
        this.feed = feed;
        this.feedConn = feedConn;
        this.intakeLocations = intakeLocations;
    }

    @Override
    public synchronized void remove(Dataset dataset) throws HyracksDataException {
        throw new NotImplementedException();
    }

    @Override
    public synchronized void add(Dataset dataset) throws HyracksDataException {
        throw new NotImplementedException();
    }

    public Feed getFeed() {
        return feed;
    }

    @Override
    protected void doStart(MetadataProvider mdProvider) throws HyracksDataException {
        try {
            mdProvider.getConfig().put(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS, Boolean.TRUE.toString());
            mdProvider.getConfig().put(FeedActivityDetails.COLLECT_LOCATIONS,
                    StringUtils.join(intakeLocations, ','));
            JobSpecification connJob = FeedOperations.getConnectionJob(mdProvider, feedConn, statementExecutor, hcc,
                    false);
            WaitForStateSubscriber collectEventSubscriber = new WaitForStateSubscriber(this, EnumSet
                    .of(ActivityState.RUNNING, ActivityState.TEMPORARILY_FAILED, ActivityState.PERMANENTLY_FAILED));
            connJob.setProperty(ActiveNotificationHandler.ACTIVE_ENTITY_PROPERTY_NAME, entityId);
            JobUtils.runJob(hcc, connJob, false);
            collectEventSubscriber.sync();
            if (collectEventSubscriber.getFailure() != null) {
                throw collectEventSubscriber.getFailure();
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    protected Void doStop(MetadataProvider metadataProvider) throws HyracksDataException {
        IActiveEntityEventSubscriber eventSubscriber = new WaitForStateSubscriber(this,
                EnumSet.of(ActivityState.STOPPED, ActivityState.PERMANENTLY_FAILED));
        try {
            // Construct ActiveMessage
            for (int i = 0; i < getLocations().getLocations().length; i++) {
                String intakeLocation = getLocations().getLocations()[i];
                FeedOperations.SendStopMessageToNode(metadataProvider.getApplicationContext(), entityId, intakeLocation,
                        i);
            }
            eventSubscriber.sync();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        return null;
    }

    @Override
    protected void setRunning(MetadataProvider metadataProvider, boolean running) throws HyracksDataException {
        // No op
    }

    @Override
    protected Void doSuspend(MetadataProvider metadataProvider) throws HyracksDataException {
        throw new RuntimeDataException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }

    @Override
    protected void doResume(MetadataProvider metadataProvider) throws HyracksDataException {
        throw new RuntimeDataException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }
}
