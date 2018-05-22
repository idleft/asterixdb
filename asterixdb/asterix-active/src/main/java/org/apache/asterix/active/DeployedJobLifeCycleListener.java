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
package org.apache.asterix.active;

import org.apache.asterix.active.message.ShutdownPartitionHandlerMessage;
import org.apache.asterix.active.partition.DeployedJobUntrackSubscriber;
import org.apache.asterix.active.partition.PartitionHolderId;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.Job;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DeployedJobLifeCycleListener implements IJobLifecycleListener {

    public static final String TRANSACTION_ID_PARAMETER_NAME = "TxnIdParameter";
    public static final String FEED_INTAKE_PARTITION_HOLDER = "intake_partition_holder";
    public static final String FEED_STORAGE_PARTITION_HOLDER = "storage_partition_holder";
    private static final Logger LOGGER = LogManager.getLogger();
    private final Level logLevel = Level.DEBUG;
    private ICCMessageBroker messageBroker;

    // get service context to send message to nc
    ICCServiceContext ccServiceCtx;
    // this stores the mapping from jobid to the information of running deployed job (a hyracks job)
    private final Map<JobId, EntityId> jobToEntityIdMap;
    private final Map<EntityId, DeployedJobSpecId> keepDeployedJobMap;
    private final Map<EntityId, Integer> entityToLivePartitionCount;
    private final Map<EntityId, Set<String>> entityToNodeIds;
    private final Map<EntityId, Integer> entityIdToInstanceN;
    private final Map<EntityId, List<DeployedJobUntrackSubscriber>> entityIdToSubscribedListener;
    private final Map<JobId, EntityId> entityIdToJobIdMap;

    public DeployedJobLifeCycleListener(ICCServiceContext ccServiceCtx) {
        this.ccServiceCtx = ccServiceCtx;
        this.jobToEntityIdMap = new ConcurrentHashMap<>();
        this.keepDeployedJobMap = new HashMap<>();
        this.entityToLivePartitionCount = new HashMap<>();
        this.entityToNodeIds = new HashMap<>();
        this.entityIdToInstanceN = new HashMap<>();
        this.messageBroker = (ICCMessageBroker) ccServiceCtx.getMessageBroker();
        this.entityIdToSubscribedListener = new HashMap<>();
        this.entityIdToJobIdMap = new HashMap<>();
    }

    public synchronized void registerDeployedJob(JobId jobId, EntityId entityId) {
        LOGGER.log(logLevel, "Job " + jobId + " registered to " + entityId);
        jobToEntityIdMap.put(jobId, entityId);
    }

    public synchronized void registerStgJobId(EntityId entityId, JobId jobId) {
        entityIdToJobIdMap.put(jobId, entityId);
    }

    public synchronized void keepDeployedJob(EntityId entityId, DeployedJobSpecId deployedJobSpecId,
            int liveCollectorPartitionN, Set<String> stgNodes, int instanceNum) {
        LOGGER.log(logLevel, "Keep " + entityId + " with " + liveCollectorPartitionN + " partitions.");
        keepDeployedJobMap.put(entityId, deployedJobSpecId);
        entityToLivePartitionCount.put(entityId, liveCollectorPartitionN);
        entityToNodeIds.put(entityId, stgNodes);
        entityIdToInstanceN.put(entityId, instanceNum);
    }

    public synchronized void subscribe(EntityId entityId, DeployedJobUntrackSubscriber untrackSubscriber) {
        if (entityIdToSubscribedListener.get(entityId) == null) {
            entityIdToSubscribedListener.put(entityId, new ArrayList<>());
        }
        entityIdToSubscribedListener.get(entityId).add(untrackSubscriber);
    }

    public synchronized void shutdownPartitionHolderByPartitionHolderId(PartitionHolderId phId) throws Exception {
        ShutdownPartitionHandlerMessage msg = new ShutdownPartitionHandlerMessage(phId);
        Set<String> nodeIds = entityToNodeIds.get(phId.getEntityId());
        if (nodeIds != null) {
            for (String ncId : nodeIds) {
                LOGGER.log(logLevel, "CC message to shutdown " + phId + " on " + ncId);
                messageBroker.sendApplicationMessageToNC(msg, ncId);
            }
        } else {
            LOGGER.log(logLevel, "No Nodes for " + phId.getEntityId());
        }
    }

    public synchronized void dropDeployedJob(EntityId entityId) throws Exception {
        int newCount = entityToLivePartitionCount.get(entityId) - 1;
        LOGGER.log(Level.INFO, "Drop " + entityId + " to " + newCount + " partitions.");
        if (newCount == 0) {
            keepDeployedJobMap.remove(entityId);
            //            shutdownPartitionHolderByPartitionHolderId(new PartitionHolderId(entityId, FEED_INTAKE_PARTITION_HOLDER, -1));
        } else {
            entityToLivePartitionCount.put(entityId, newCount);
        }
    }

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification spec) throws HyracksException {
        // do nothing, i don't care ;(
    }

    @Override
    public void notifyJobStart(JobId jobId) throws HyracksException {
        // i don't care now
    }

    private void notifySubscriber(EntityId entityId) {
        for (DeployedJobUntrackSubscriber sub : entityIdToSubscribedListener.get(entityId)) {
            sub.notifyUntrack();
        }
    }

    private void untrackDeployedJobForEntity(EntityId entityId) throws Exception {
        Integer currentInstanceId = entityIdToInstanceN.get(entityId);
        if (currentInstanceId == null) {
            throw new HyracksDataException(entityId + " is not being tracked.");
        } else {
            currentInstanceId = currentInstanceId - 1;
            if (currentInstanceId == 0) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(entityId + " stg closing");
                }
                shutdownPartitionHolderByPartitionHolderId(
                        new PartitionHolderId(entityId, FEED_STORAGE_PARTITION_HOLDER, -1));
                if (!entityIdToJobIdMap.containsValue(entityId)) {
                    notifySubscriber(entityId);
                }
            } else {
                entityIdToInstanceN.put(entityId, currentInstanceId);
            }
        }
    }

    @Override
    public synchronized void notifyJobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions)
            throws HyracksException {
        // notify the nc that hosts the feed
        // currently, we ignore all the exceptions and failures
        // this method is synchronized at outside
        try {
            if (jobToEntityIdMap.containsKey(jobId)) {
                EntityId entityId = jobToEntityIdMap.get(jobId);
                jobToEntityIdMap.remove(jobId);
                if (keepDeployedJobMap.containsKey(entityId)) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(jobId + " is interesting. Keep " + entityId + " alive.");
                    }
                    Thread startT = new Thread(() -> {
                        try {
                            DeployedJobSpecId deployedJobId = keepDeployedJobMap.get(entityId);
                            // this cannot be moved as the ccAppCtx is only available at runtime.
                            ICcApplicationContext ccAppCtx =
                                    ((ICcApplicationContext) ccServiceCtx.getApplicationContext());
                            Map<byte[], byte[]> jobParameter = new HashMap<>();
                            TxnId newDeployedJobTxnId = ccAppCtx.getTxnIdFactory().create();
                            jobParameter.put((TRANSACTION_ID_PARAMETER_NAME).getBytes(),
                                    String.valueOf(newDeployedJobTxnId.getId()).getBytes());
                            JobId newJobID = ccAppCtx.getHcc().startJob(deployedJobId, jobParameter);
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Started a new job " + newJobID + " for deployed job " + deployedJobId);
                            }
                            jobToEntityIdMap.put(newJobID, entityId);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                    startT.start();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(jobId + " triggered new job.");
                    }
                } else {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(jobId + " is no longer interesting. Stop keep " + entityId + " alive.");
                    }
                    untrackDeployedJobForEntity(entityId);
                }
            } else if (entityIdToJobIdMap.containsKey(jobId)) {
                notifySubscriber(entityIdToJobIdMap.get(jobId));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e.getMessage());
        }
    }
}
