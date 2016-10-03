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
import org.apache.asterix.external.feed.api.IFeedJoint.State;
import org.apache.asterix.external.feed.api.IFeedLifecycleEventSubscriber;
import org.apache.asterix.external.feed.api.IFeedLifecycleEventSubscriber.FeedLifecycleEvent;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.watch.FeedConnectJobInfo;
import org.apache.asterix.external.feed.watch.FeedIntakeInfo;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.asterix.external.util.FeedUtils.JobType;
import org.apache.asterix.runtime.util.AsterixAppContextInfo;
import org.apache.hyracks.algebricks.common.utils.Pair;
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
    private final List<String> intakeLocations;
    private final List<String> connectedDatasets;
    private JobId connectionJobId = null;
    private JobSpecification connectionJobSpec = null;
    private FeedConnectJobInfo connectionJobInfo = null;
    private EntityId entityId;

    public FeedEventsListener(EntityId entityId) {
        this.entityId = entityId;
        connectedDatasets = new ArrayList<>();
        intakeLocations = new ArrayList<>();
    }

    @Override
    public void notify(ActiveEvent event) {
        try {
            switch (event.getEventKind()) {
                case JOB_START:
                    handleJobStartEvent();
                    break;
                case JOB_FINISH:
                    handleJobFinishEvent(event);
                    break;
                case PARTITION_EVENT:
                    handlePartitionStart();
                    break;
                default:
                    LOGGER.warn("Unknown Feed Event" + event);
                    break;
            }
        } catch (Exception e) {
            LOGGER.error("Unhandled Exception", e);
        }
    }

    private void handleJobStartEvent() throws Exception {
        handleCollectJobStartMessage(connectionJobInfo);
    }

    private synchronized void handleJobFinishEvent(ActiveEvent message) throws Exception {
       // null
    }

    private void handlePartitionStart() {
        connectionJobInfo.setState(ActivityState.ACTIVE);
    }

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification spec) {
        this.connectionJobSpec = spec;
        this.connectionJobId = jobId;
        connectionJobInfo = new FeedConnectJobInfo(entityId, jobId, ActivityState.CREATED, spec);
    }

    public synchronized List<String> getConnectionLocations(IFeedJoint feedJoint, final FeedConnectionRequest request)
            throws Exception {
        return this.intakeLocations;
    }

    public JobSpecification getConnectionJobSpec(FeedConnectionId connectionId) {
        return connectionJobSpec;
    }

    private void setLocations(FeedConnectJobInfo cInfo) {
        JobSpecification jobSpec = cInfo.getSpec();

        List<OperatorDescriptorId> collectOperatorIds = new ArrayList<>();
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
            } else if (actualOp instanceof FeedCollectOperatorDescriptor) {
                collectOperatorIds.add(entry.getKey());
            } else if (actualOp instanceof FeedIntakeOperatorDescriptor) {
                intakeOperatorIds.add(entry.getKey());
            }

        }

        try {
            IHyracksClientConnection hcc = AsterixAppContextInfo.INSTANCE.getHcc();
            JobInfo info = hcc.getJobInfo(cInfo.getJobId());

            // collect operator locations
            List<String> collectLocations = new ArrayList<>();
            for (OperatorDescriptorId collectOpId : collectOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(collectOpId);
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    collectLocations.add(operatorLocations.get(i));
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
                    computeLocations.addAll(collectLocations);
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
            // intake operator locations
            List<String> intakeLocations = new ArrayList<>();
            for (OperatorDescriptorId intakeOperatorId : intakeOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(intakeOperatorId);
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    intakeLocations.add(operatorLocations.get(i));
                }
            }

            cInfo.setCollectLocations(collectLocations);
            cInfo.setComputeLocations(computeLocations);
            cInfo.setStorageLocations(storageLocations);
            cInfo.setIngestLocations(intakeLocations);

        } catch (Exception e) {
            LOGGER.error("Error while setting feed active locations", e);
        }

    }

    public FeedConnectJobInfo getFeedConnectJobInfo() {
        return connectionJobInfo;
    }

    private void handleCollectJobStartMessage(FeedConnectJobInfo cInfo) throws ACIDException {
        setLocations(cInfo);
        cInfo.setState(ActivityState.ACTIVE);
    }

    public synchronized boolean isConnectedToDataset(String datasetName) {
        return connectedDatasets.contains(datasetName);
    }

    @Override
    public boolean isEntityActive() {
        return connectionJobId!=null;
    }

    @Override
    public EntityId getEntityId() {
        return entityId;
    }

    @Override
    public boolean isEntityConnectedToDataset(String dataverseName, String datasetName) {
        return isConnectedToDataset(datasetName);
    }
}
