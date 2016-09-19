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
package org.apache.asterix.external.feed.management;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveJob;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.external.feed.api.FeedOperationCounter;
import org.apache.asterix.external.feed.api.IFeedJoint;
import org.apache.asterix.external.feed.api.IFeedLifecycleEventSubscriber;
import org.apache.asterix.external.feed.api.IFeedLifecycleEventSubscriber.FeedLifecycleEvent;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.watch.FeedConnectInfo;
import org.apache.asterix.external.feed.watch.FeedIntakeInfo;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.asterix.external.util.FeedUtils.JobType;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.avro.generic.GenericData;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexInsertUpdateDeleteOperatorDescriptor;
import org.apache.log4j.Logger;


//TODO: Deregister Collection JOB?
public class FeedEventsListener implements IActiveEntityEventsListener {
    private static final Logger LOGGER = Logger.getLogger(FeedEventsListener.class);
    private final Map<EntityId, FeedOperationCounter> partitionCounters;
    private final Map<Long, ActiveJob> jobs;
    private final Map<Long, ActiveJob> intakeJobs;
    private final Map<EntityId, FeedIntakeInfo> intakeInfos;
    private final Map<EntityId, FeedConnectInfo> connectInfos;
    private final List<String> connectedDatasets;
    private EntityId entityId;

    public FeedEventsListener(EntityId entityId) {
        this.entityId = entityId;
        jobs = new HashMap<>();
        connectedDatasets = new ArrayList<>();
        partitionCounters = new HashMap<>();
        intakeInfos = new HashMap<>();
        connectInfos = new HashMap<>();
        intakeJobs = new HashMap<>();
    }

    @Override
    public void notify(ActiveEvent event) {
        try {
            switch (event.getEventKind()) {
                case JOB_START:
                    handleJobStartEvent(event);
                    break;
                case JOB_FINISH:
                    handleJobFinishEvent(event);
                    break;
                case PARTITION_EVENT:
                    handlePartitionStart(event);
                    break;
                default:
                    LOGGER.warn("Unknown Feed Event" + event);
                    break;
            }
        } catch (Exception e) {
            LOGGER.error("Unhandled Exception", e);
        }
    }

    private synchronized void handleJobStartEvent(ActiveEvent message) throws Exception {
        ActiveJob jobInfo = jobs.get(message.getJobId().getId());
        JobType jobType = (JobType) jobInfo.getJobObject();
        switch (jobType) {
            case INTAKE:
                handleIntakeJobStartMessage((FeedIntakeInfo) jobInfo);
                break;
            case FEED_CONNECT:
                handleCollectJobStartMessage((FeedConnectInfo) jobInfo);
                break;
            default:
        }
    }

    private synchronized void handleJobFinishEvent(ActiveEvent message) throws Exception {
        ActiveJob jobInfo = jobs.get(message.getJobId().getId());
        JobType jobType = (JobType) jobInfo.getJobObject();
        switch (jobType) {
            case FEED_CONNECT:
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Collect Job finished for  " + jobInfo);
                }
                handleFeedCollectJobFinishMessage((FeedConnectInfo) jobInfo);
                break;
            case INTAKE:
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Intake Job finished for feed intake " + jobInfo.getJobId());
                }
                handleFeedIntakeJobFinishMessage((FeedIntakeInfo) jobInfo, message);
                break;
            default:
                break;
        }
    }

    private synchronized void handlePartitionStart(ActiveEvent message) {
        ActiveJob jobInfo = jobs.get(message.getJobId().getId());
        JobType jobType = (JobType) jobInfo.getJobObject();
        switch (jobType) {
            case FEED_CONNECT:
                ((FeedConnectInfo) jobInfo).partitionStart();
                break;
            case INTAKE:
                handleIntakePartitionStarts(message, jobInfo);
                break;
            default:
                break;
        }
    }

    private void handleIntakePartitionStarts(ActiveEvent message, ActiveJob jobInfo) {
        if (partitionCounters.get(message.getFeedId()).decrementAndGet() == 0) {
            jobInfo.setState(ActivityState.ACTIVE);
        }
    }

    private static synchronized void handleIntakeJobStartMessage(FeedIntakeInfo intakeJobInfo) throws Exception {
        List<OperatorDescriptorId> intakeOperatorIds = new ArrayList<>();
        Map<OperatorDescriptorId, IOperatorDescriptor> operators = intakeJobInfo.getSpec().getOperatorMap();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                intakeOperatorIds.add(opDesc.getOperatorId());
            }
        }

        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(intakeJobInfo.getJobId());
        List<String> intakeLocations = new ArrayList<>();
        for (OperatorDescriptorId intakeOperatorId : intakeOperatorIds) {
            Map<Integer, String> operatorLocations = info.getOperatorLocations().get(intakeOperatorId);
            int nOperatorInstances = operatorLocations.size();
            for (int i = 0; i < nOperatorInstances; i++) {
                intakeLocations.add(operatorLocations.get(i));
            }
        }
        // intakeLocations is an ordered list; 
        // element at position i corresponds to location of i'th instance of operator
        intakeJobInfo.setIntakeLocation(intakeLocations);
    }

    public synchronized void registerFeedIntakeOperator(EntityId feedId, JobId jobId, JobSpecification jobSpec)
            throws HyracksDataException {
        if (intakeInfos.get(feedId) != null) {
            throw new IllegalStateException("Feed already has an intake job");
        }
        if (intakeJobs.get(jobId.getId()) != null) {
            throw new IllegalStateException("Feed job already registered in intake jobs");
        }
        if (jobs.get(jobId.getId()) != null) {
            throw new IllegalStateException("Feed job already registered in all jobs");
        }
        FeedIntakeInfo intakeJobInfo = new FeedIntakeInfo(jobId, ActivityState.CREATED, feedId, jobSpec);
        intakeInfos.put(feedId, intakeJobInfo);
        jobs.put(jobId.getId(), intakeJobInfo);
        intakeJobs.put(jobId.getId(), intakeJobInfo);

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Registered feed intake [" + jobId + "]" + " for feed " + feedId);
        }
    }

    public synchronized void registerFeedCollectOperator(EntityId feedId, String targetDataset, JobId jobId,
            JobSpecification jobSpec, Map<String, String> feedPolicy) {
        FeedConnectInfo cInfo = null;
        if (connectInfos.containsKey(feedId)) {
            cInfo = connectInfos.get(feedId);
            cInfo.addTargetDataset(targetDataset);
        } else {
            cInfo = new FeedConnectInfo(feedId,jobId, ActivityState.CREATED, jobSpec,
                    feedPolicy);
            cInfo.addTargetDataset(targetDataset);
            jobs.put(jobId.getId(), cInfo);
            connectInfos.put(feedId, cInfo);
        }


        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Registered feed connection [" + jobId + "]" + " for feed " + feedId);
        }
    }

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification spec) {
        Map<String, String> feedPolicy = null;
        try {
            for (IOperatorDescriptor opDesc : spec.getOperatorMap().values()) {
                if (opDesc instanceof FeedCollectOperatorDescriptor) {
                    feedPolicy = ((FeedCollectOperatorDescriptor) opDesc).getFeedPolicyProperties();
                    registerFeedCollectOperator(((FeedCollectOperatorDescriptor) opDesc).getFeedId(),
                            ((FeedCollectOperatorDescriptor) opDesc).getTargetDataset(), jobId, spec, feedPolicy);
                    return;
                } else if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                    registerFeedIntakeOperator(((FeedIntakeOperatorDescriptor) opDesc).getFeedId(), jobId, spec);
                    return;
                }
            }
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }

    private synchronized void handleFeedIntakeJobFinishMessage(FeedIntakeInfo intakeInfo, ActiveEvent message)
            throws Exception {
        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(message.getJobId());
        JobStatus status = info.getStatus();
        EntityId feedId = intakeInfo.getFeedId();
        FeedOperationCounter fc = partitionCounters.get(feedId);
        if (status.equals(JobStatus.FAILURE)) {
            fc.setFailedIngestion(true);
        }
        // deregister feed intake job
        partitionCounters.remove(feedId);
        jobs.remove(feedId);
        intakeInfos.remove(feedId);
        LOGGER.info("Deregistered feed intake job [" + feedId + "]");
    }

    private synchronized void handleFeedCollectJobFinishMessage(FeedConnectInfo cInfo) throws Exception {
        EntityId feedId = cInfo.getEntityId();
        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(cInfo.getJobId());
        JobStatus status = info.getStatus();
        // probably missing place for policy
        connectInfos.remove(feedId);
        jobs.remove(cInfo.getJobId().getId());
    }

    private void setLocations(FeedConnectInfo cInfo) {
        JobSpecification jobSpec = cInfo.getSpec();

        List<OperatorDescriptorId> collectOperatorIds = new ArrayList<>();
        List<OperatorDescriptorId> computeOperatorIds = new ArrayList<>();
        List<OperatorDescriptorId> storageOperatorIds = new ArrayList<>();

        Map<OperatorDescriptorId, IOperatorDescriptor> operators = jobSpec.getOperatorMap();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            IOperatorDescriptor actualOp;
            if (opDesc instanceof FeedMetaOperatorDescriptor) {
                actualOp = ((FeedMetaOperatorDescriptor) opDesc).getCoreOperator();
            } else {
                actualOp = opDesc;
            }

            if (actualOp instanceof AlgebricksMetaOperatorDescriptor) {
                AlgebricksMetaOperatorDescriptor op = (AlgebricksMetaOperatorDescriptor) actualOp;
                IPushRuntimeFactory[] runtimeFactories = op.getPipeline().getRuntimeFactories();
                boolean computeOp = false;
                for (IPushRuntimeFactory rf : runtimeFactories) {
                    if (rf instanceof AssignRuntimeFactory) {
                        IConnectorDescriptor connDesc = jobSpec.getOperatorInputMap().get(op.getOperatorId()).get(0);
                        IOperatorDescriptor sourceOp =
                                jobSpec.getConnectorOperatorMap().get(connDesc.getConnectorId()).getLeft().getLeft();
                        if (sourceOp instanceof FeedCollectOperatorDescriptor) {
                            computeOp = true;
                            break;
                        }
                    }
                }
                if (computeOp) {
                    computeOperatorIds.add(entry.getKey());
                }
            } else if (actualOp instanceof LSMTreeIndexInsertUpdateDeleteOperatorDescriptor) {
                storageOperatorIds.add(entry.getKey());
            } else if (actualOp instanceof FeedCollectOperatorDescriptor) {
                collectOperatorIds.add(entry.getKey());
            }
        }

        try {
            IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
            JobInfo info = hcc.getJobInfo(cInfo.getJobId());
            List<String> collectLocations = new ArrayList<>();
            for (OperatorDescriptorId collectOpId : collectOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(collectOpId);
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    collectLocations.add(operatorLocations.get(i));
                }
            }

            List<String> computeLocations = new ArrayList<>();
            for (OperatorDescriptorId computeOpId : computeOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(computeOpId);
                if (operatorLocations != null) {
                    int nOperatorInstances = operatorLocations.size();
                    for (int i = 0; i < nOperatorInstances; i++) {
                        computeLocations.add(operatorLocations.get(i));
                    }
                } else {
                    computeLocations.clear();
                    computeLocations.addAll(collectLocations);
                }
            }

            List<String> storageLocations = new ArrayList<>();
            for (OperatorDescriptorId storageOpId : storageOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(storageOpId);
                if (operatorLocations == null) {
                    continue;
                }
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    storageLocations.add(operatorLocations.get(i));
                }
            }
            cInfo.setCollectLocations(collectLocations);
            cInfo.setComputeLocations(computeLocations);
            cInfo.setStorageLocations(storageLocations);

        } catch (Exception e) {
            LOGGER.error("Error while setting feed active locations", e);
        }

    }

    public synchronized boolean isFeedConnectionActive(EntityId feedId) {
        boolean active = false;
        FeedConnectInfo cInfo = connectInfos.get(feedId);
        if (cInfo != null) {
            active = cInfo.getState().equals(ActivityState.ACTIVE);
        }
        return active;
    }

    public FeedConnectInfo getFeedConnectJobInfo(EntityId feedId) {
        return connectInfos.get(feedId);
    }

    private void handleCollectJobStartMessage(FeedConnectInfo cInfo) throws ACIDException {
        // set locations of feed sub-operations (intake, compute, store)
        setLocations(cInfo);
        cInfo.setState(ActivityState.ACTIVE);
    }

    public synchronized boolean isConnectedToDataset(String datasetName) {
        return connectedDatasets.contains(datasetName);
    }

    @Override
    public boolean isEntityActive() {
        return !jobs.isEmpty();
    }

    @Override
    public EntityId getEntityId() {
        return entityId;
    }
}
