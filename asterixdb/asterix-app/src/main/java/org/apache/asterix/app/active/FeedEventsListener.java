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

import java.util.EnumSet;
import java.util.List;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.DeployedJobLifeCycleListener;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventSubscriber;
import org.apache.asterix.active.IRetryPolicyFactory;
import org.apache.asterix.active.partition.DeployedJobUntrackSubscriber;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.external.feed.watch.WaitForStateSubscriber;
import org.apache.asterix.external.operators.FeedIntakeOperatorNodePushable;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.asterix.lang.common.statement.StartFeedStatement;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.utils.FeedOperations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedEventsListener extends ActiveEntityEventsListener {

    private final Feed feed;
    private final List<FeedConnection> feedConnections;
    private DeployedJobSpecId feedConnJobId;
    private final boolean decouple;

    public FeedEventsListener(IStatementExecutor statementExecutor, ICcApplicationContext appCtx,
            IHyracksClientConnection hcc, EntityId entityId, List<Dataset> datasets,
            AlgebricksAbsolutePartitionConstraint locations, String runtimeName, IRetryPolicyFactory retryPolicyFactory,
            Feed feed, final List<FeedConnection> feedConnections) throws HyracksDataException {
        super(statementExecutor, appCtx, hcc, entityId, datasets, locations, runtimeName, retryPolicyFactory);
        this.feed = feed;
        this.feedConnections = feedConnections;
        this.decouple =
                Boolean.valueOf(feed.getConfiguration().getOrDefault(FeedConstants.FEED_PIPELINE_DECOUPLE, "true"));
    }

    @Override
    public synchronized void remove(Dataset dataset) throws HyracksDataException {
        super.remove(dataset);
        feedConnections.removeIf(o -> o.getDataverseName().equals(dataset.getDataverseName())
                && o.getDatasetName().equals(dataset.getDatasetName()));
    }

    public synchronized void addFeedConnection(FeedConnection feedConnection) {
        feedConnections.add(feedConnection);
    }

    public Feed getFeed() {
        return feed;
    }

    public DeployedJobSpecId getFeedConnJobId() {
        return feedConnJobId;
    }

    private void updateStgJobId(EntityId entityId, JobId jobId) {
        ICcApplicationContext ccAppCtx = (ICcApplicationContext) appCtx.getServiceContext().getApplicationContext();
        DeployedJobLifeCycleListener lifeCycleListener =
                ((DeployedJobLifeCycleListener) ccAppCtx.getDeployedJobLifeCycleListener());
        lifeCycleListener.registerStgJobId(entityId, jobId);
    }

    @Override
    public synchronized void start(MetadataProvider metadataProvider)
            throws HyracksDataException, InterruptedException {
        super.start(metadataProvider);
        // Note: The current implementation of the wait for completion flag is problematic due to locking issues:
        // Locks obtained during the start of the feed are not released, and so, the feed can't be stopped
        // and also, read locks over dataverses, datasets, etc, are never released.
        boolean wait = Boolean.parseBoolean(metadataProvider.getConfig().get(StartFeedStatement.WAIT_FOR_COMPLETION));
        if (wait) {
            IActiveEntityEventSubscriber stoppedSubscriber =
                    new WaitForStateSubscriber(this, EnumSet.of(ActivityState.STOPPED));
            stoppedSubscriber.sync();
        }
    }

    @Override
    protected JobId compileAndStartJob(MetadataProvider mdProvider) throws HyracksDataException {
        JobId feedJobId;
        try {
            String[] collectLocations = mdProvider.getClusterLocations().getLocations();
            // Prepare intake job
            Pair<JobSpecification, AlgebricksAbsolutePartitionConstraint> intakeJobInfo =
                    FeedOperations.buildStartFeedJob(mdProvider, feed, feedConnections, collectLocations);
            JobSpecification intakeJob = intakeJobInfo.getLeft();
            DeployedJobUntrackSubscriber deployedJobSubscriber = new DeployedJobUntrackSubscriber(entityId,
                    (DeployedJobLifeCycleListener) appCtx.getDeployedJobLifeCycleListener());
            intakeJob.setProperty(ActiveNotificationHandler.ACTIVE_ENTITY_PROPERTY_NAME, entityId);
            // TODO(Yingyi): currently we do not check IFrameWriter protocol violations for Feed jobs.
            setLocations(intakeJobInfo.getRight());
            boolean wait = Boolean.parseBoolean(mdProvider.getConfig().get(StartFeedStatement.WAIT_FOR_COMPLETION));
            // Prepare connJob
            JobSpecification connectJob = FeedOperations.buildFeedConnectionJob(mdProvider, feedConnections, hcc,
                    statementExecutor, feed, metadataProvider.getClusterLocations().getLocations());

            if (decouple) {
                Pair<JobSpecification, JobSpecification> feedJob = FeedOperations.decoupleStorageJob(connectJob, feed);
                this.feedConnJobId = hcc.deployJobSpec(feedJob.getLeft());
                ((FeedIntakeOperatorDescriptor) intakeJob.getOperatorMap().get(new OperatorDescriptorId(0)))
                        .setConnJobId(feedConnJobId);
                JobId stgJobId = JobUtils.runJob(hcc, feedJob.getRight(), false);
                feedJobId = JobUtils.runJob(hcc, intakeJob, false);
                updateStgJobId(entityId, stgJobId);
            } else {
                // Deploy conn job
                this.feedConnJobId = hcc.deployJobSpec(connectJob);
                ((FeedIntakeOperatorDescriptor) intakeJob.getOperatorMap().get(new OperatorDescriptorId(0)))
                        .setConnJobId(feedConnJobId);
                // Run intake job
                feedJobId = JobUtils.runJob(hcc, intakeJob, false);
            }
            deployedJobSubscriber.waitForUntrack();
        } catch (Exception e) {
            e.printStackTrace();
            throw HyracksDataException.create(e);
        }
        return feedJobId;
    }

    @Override
    protected void setRunning(MetadataProvider metadataProvider, boolean running) {
        // No op
    }

    @Override
    protected void doSuspend(MetadataProvider metadataProvider) throws HyracksDataException {
        throw new RuntimeDataException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }

    @Override
    protected void doResume(MetadataProvider metadataProvider) throws HyracksDataException {
        throw new RuntimeDataException(ErrorCode.OPERATION_NOT_SUPPORTED);
    }

    @Override
    protected ActiveRuntimeId getActiveRuntimeId(int partition) {
        return new ActiveRuntimeId(entityId, FeedIntakeOperatorNodePushable.class.getSimpleName(), partition);
    }
}
