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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final Map<EntityId, AtomicInteger> entityToLivePartitionCount;
    private final Map<EntityId, Set<String>> entityToNodeIds;
    private final Map<EntityId, AtomicInteger> entityIdToInstanceN;
    private final Map<EntityId, List<DeployedJobUntrackSubscriber>> entityIdToSubscribedListener;
    private final Map<JobId, EntityId> entityIdToJobIdMap;

    public DeployedJobLifeCycleListener(ICCServiceContext ccServiceCtx) {
        this.ccServiceCtx = ccServiceCtx;
        this.jobToEntityIdMap = new ConcurrentHashMap<>();
        this.keepDeployedJobMap = new ConcurrentHashMap<>();
        this.entityToLivePartitionCount = new ConcurrentHashMap<>();
        this.entityToNodeIds = new ConcurrentHashMap<>();
        this.entityIdToInstanceN = new ConcurrentHashMap<>();
        this.messageBroker = (ICCMessageBroker) ccServiceCtx.getMessageBroker();
        this.entityIdToSubscribedListener = new HashMap<>();
        this.entityIdToJobIdMap = new ConcurrentHashMap<>();
    }

    public void registerDeployedJob(JobId jobId, EntityId entityId) {
        LOGGER.log(logLevel, "Job " + jobId + " registered to " + entityId);
        jobToEntityIdMap.put(jobId, entityId);
    }

    public void registerStgJobId(EntityId entityId, JobId jobId) {
        entityIdToJobIdMap.put(jobId, entityId);
    }

    public void keepDeployedJob(EntityId entityId, DeployedJobSpecId deployedJobSpecId, int liveCollectorPartitionN,
            Set<String> stgNodes, int instanceNum) {
        LOGGER.log(logLevel, entityId + " keep deploy job with " + liveCollectorPartitionN + " partitions "
                + instanceNum + " workers.");
        // Keep track of deployed job id
        keepDeployedJobMap.put(entityId, deployedJobSpecId);
        // Storage nodes
        entityToNodeIds.put(entityId, stgNodes);
        // Keep track of intake partition holder and deployed job instances
        entityToLivePartitionCount.put(entityId, new AtomicInteger(liveCollectorPartitionN));
        entityIdToInstanceN.put(entityId, new AtomicInteger(instanceNum));
    }

    public void subscribe(EntityId entityId, DeployedJobUntrackSubscriber untrackSubscriber) {
        entityIdToSubscribedListener.putIfAbsent(entityId, new ArrayList<>());
        entityIdToSubscribedListener.get(entityId).add(untrackSubscriber);
    }

    public void shutdownPartitionHolderByPartitionHolderId(PartitionHolderId phId) throws Exception {
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

    public void untrackIntakePartitionHolder(EntityId entityId) throws Exception {
        synchronized (entityToLivePartitionCount) {
            int newCount = entityToLivePartitionCount.get(entityId).decrementAndGet();
            LOGGER.log(Level.INFO, "Drop " + entityId + " to " + newCount + " partitions.");
            if (newCount == 0) {
                keepDeployedJobMap.remove(entityId);
                //            shutdownPartitionHolderByPartitionHolderId(new PartitionHolderId(entityId, FEED_INTAKE_PARTITION_HOLDER, -1));
            }
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Notify subscribers of " + entityId + " for untrack. "
                    + entityIdToSubscribedListener.getOrDefault(entityId, Collections.EMPTY_LIST).size());
        }
        for (DeployedJobUntrackSubscriber sub : entityIdToSubscribedListener.get(entityId)) {
            sub.notifyUntrack();
        }
        entityIdToSubscribedListener.remove(entityId);
    }

    private void untrackDeployedJobForEntity(EntityId entityId) throws Exception {
        synchronized (entityIdToInstanceN) {
            AtomicInteger currentInstanceN = entityIdToInstanceN.get(entityId);
            if (currentInstanceN == null) {
                throw new HyracksDataException(entityId + " is not being tracked.");
            } else {
                if (currentInstanceN.decrementAndGet() == 0) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(entityId + " has no more live instances. Poisoning stg.");
                    }
                    shutdownPartitionHolderByPartitionHolderId(
                            new PartitionHolderId(entityId, FEED_STORAGE_PARTITION_HOLDER, -1));
                    if (!entityIdToJobIdMap.containsValue(entityId)) {
                        notifySubscriber(entityId);
                    }
                }
            }
        }
    }

    @Override
    public void notifyJobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions) throws HyracksException {
        // notify the nc that hosts the feed
        // currently, we ignore all the exceptions and failures
        // this method is synchronized at outside
        try {
            // make sure newStartT is not deadlock with finish when put new id
            synchronized (jobToEntityIdMap) {
                EntityId entityId = jobToEntityIdMap.get(jobId);
                if (entityId != null) {
                    jobToEntityIdMap.remove(jobId);
                    DeployedJobSpecId deployedJobId = keepDeployedJobMap.get(entityId);
                    if (deployedJobId != null) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(jobId + " is interesting. Keep " + entityId + " alive.");
                        }
                        Thread startT = new Thread(() -> {
                            try {
                                // this cannot be moved as the ccAppCtx is only available at runtime.
                                ICcApplicationContext ccAppCtx =
                                        ((ICcApplicationContext) ccServiceCtx.getApplicationContext());
                                Map<byte[], byte[]> jobParameter = new HashMap<>();
                                TxnId newDeployedJobTxnId = ccAppCtx.getTxnIdFactory().create();
                                jobParameter.put((TRANSACTION_ID_PARAMETER_NAME).getBytes(),
                                        String.valueOf(newDeployedJobTxnId.getId()).getBytes());
                                synchronized (jobToEntityIdMap) {
                                    JobId newJobID = ccAppCtx.getHcc().startJob(deployedJobId, jobParameter);
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug(
                                                "Started a new job " + newJobID + " for deployed job " + deployedJobId);
                                    }
                                    jobToEntityIdMap.put(newJobID, entityId);
                                }
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
                            LOGGER.debug(jobId + " is no longer interesting. Decrease live count for " + entityId);
                        }
                        untrackDeployedJobForEntity(entityId);
                    }
                } else if (entityIdToJobIdMap.containsKey(jobId)) {
                    notifySubscriber(entityIdToJobIdMap.get(jobId));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e.getMessage());
        }
    }
}
