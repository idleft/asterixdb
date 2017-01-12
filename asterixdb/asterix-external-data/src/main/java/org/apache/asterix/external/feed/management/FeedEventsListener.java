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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.external.feed.api.IActiveLifecycleEventSubscriber;
import org.apache.asterix.external.feed.watch.FeedJob;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.asterix.runtime.util.AppContextInfo;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexInsertUpdateDeleteOperatorDescriptor;

public class FeedEventsListener implements IActiveEntityEventsListener {
    private static final Logger LOGGER = Logger.getLogger(FeedEventsListener.class.getName());
    private final IActiveLifecycleEventSubscriber eventSubscriber;
    private final List<String> connectedDatasets;
    private FeedJob cInfo;
    private EntityId entityId;

    public FeedEventsListener(EntityId entityId) {
        this.entityId = entityId;
        connectedDatasets = new ArrayList<>();
        eventSubscriber = new ActiveLifecycleEventSubscriber();
    }

    @Override
    public void notify(ActiveEvent event) {
        try {
            switch (event.getEventKind()) {
                case JOB_START:
                    handleJobStartEvent();
                    break;
                case JOB_FINISH:
                    handleJobFinishEvent();
                    break;
                case PARTITION_EVENT:
                    handlePartitionStart();
                    break;
                default:
                    LOGGER.log(Level.SEVERE, "Unknown Feed Event" + event);
                    break;
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unhandled Exception", e);
        }
    }

    private void handleJobStartEvent() {
        LOGGER.log(Level.INFO, "Feed Start " + cInfo.getEntityId());
        setLocations(cInfo);
        cInfo.setState(ActivityState.ACTIVE);
        notifyFeedEventSubscriber(IActiveLifecycleEventSubscriber.ActiveLifecycleEvent.FEED_INTAKE_STARTED);
    }

    private synchronized void handleJobFinishEvent() throws Exception {
        LOGGER.log(Level.INFO, "Feed End " + cInfo);
        IHyracksClientConnection hcc = AppContextInfo.INSTANCE.getHcc();
        JobStatus status = hcc.getJobStatus(cInfo.getJobId());
        cInfo.setState(ActivityState.INACTIVE);
        notifyFeedEventSubscriber(status.equals(JobStatus.FAILURE)
                ? IActiveLifecycleEventSubscriber.ActiveLifecycleEvent.FEED_INTAKE_FAILURE
                : IActiveLifecycleEventSubscriber.ActiveLifecycleEvent.FEED_INTAKE_ENDED);
    }

    public void setFeedConnectJobInfo(FeedJob info) {
        this.cInfo = info;
    }

    private void handlePartitionStart() {
        cInfo.setState(ActivityState.ACTIVE);
        notifyFeedEventSubscriber(IActiveLifecycleEventSubscriber.ActiveLifecycleEvent.FEED_COLLECT_STARTED);
    }

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification spec) {
        cInfo.setJobId(jobId);
    }

    public IActiveLifecycleEventSubscriber getEventSubscriber() {
        return this.eventSubscriber;
    }

    private void setLocations(FeedJob cInfo) {
        JobSpecification jobSpec = cInfo.getSpec();

        List<OperatorDescriptorId> computeOperatorIds = new ArrayList<>();
        List<OperatorDescriptorId> storageOperatorIds = new ArrayList<>();
        List<OperatorDescriptorId> intakeOperatorIds = new ArrayList<>();

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
                        IOperatorDescriptor sourceOp = jobSpec.getConnectorOperatorMap().get(connDesc.getConnectorId())
                                .getLeft().getLeft();
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
            } else if (actualOp instanceof FeedIntakeOperatorDescriptor) {
                intakeOperatorIds.add(entry.getKey());
            }

        }

        try {
            IHyracksClientConnection hcc = AppContextInfo.INSTANCE.getHcc();
            JobInfo info = hcc.getJobInfo(cInfo.getJobId());

            // intake operator locations
            List<String> intakeLocations = new ArrayList<>();
            for (OperatorDescriptorId intakeOperatorId : intakeOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(intakeOperatorId);
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    intakeLocations.add(operatorLocations.get(i));
                }
            }
            // compute operator locations
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
                    computeLocations.addAll(intakeLocations);
                }
            }
            // storage operator locations
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


            cInfo.setComputeLocations(computeLocations);
            cInfo.setStorageLocations(storageLocations);
            cInfo.setIntakeLocations(intakeLocations);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error while setting feed active locations", e);
        }
    }

    private synchronized void notifyFeedEventSubscriber(IActiveLifecycleEventSubscriber.ActiveLifecycleEvent event) {
        eventSubscriber.handleEvent(event);
    }

    public synchronized boolean isConnectedToDataset(String datasetName) {
        return connectedDatasets.contains(datasetName);
    }

    @Override
    public boolean isEntityActive() {
        return cInfo.getState() == ActivityState.ACTIVE;
    }

    @Override
    public EntityId getEntityId() {
        return entityId;
    }

    @Override
    public boolean isEntityUsingDataset(String dataverseName, String datasetName) {
        return isConnectedToDataset(datasetName);
    }

    public List<String> getIntakeLocations() {
        return cInfo.getIntakeLocations();
    }
}
