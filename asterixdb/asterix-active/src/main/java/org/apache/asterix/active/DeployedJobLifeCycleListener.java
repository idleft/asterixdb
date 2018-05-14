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

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.transactions.TxnId;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeployedJobLifeCycleListener implements IJobLifecycleListener {

    public static final String TRANSACTION_ID_PARAMETER_NAME = "TxnIdParameter";
    private static final Logger LOGGER = LogManager.getLogger();
    private final Level logLevel = Level.DEBUG;

    // get service context to send message to nc
    ICCServiceContext ccServiceCtx;
    // this stores the mapping from jobid to the information of running deployed job (a hyracks job)
    Map<JobId, EntityId> jobToEntityIdMap;
    Map<EntityId, DeployedJobSpecId> keepDeployedJobMap;
    Map<EntityId, Integer> entityToLivePartitionCount;

    public DeployedJobLifeCycleListener(ICCServiceContext ccServiceCtx) {
        this.ccServiceCtx = ccServiceCtx;
        this.jobToEntityIdMap = new HashMap<>();
        this.keepDeployedJobMap = new HashMap<>();
        this.entityToLivePartitionCount = new HashMap<>();
    }

    public synchronized void registerDeployedJob(JobId jobId, EntityId entityId) {
        LOGGER.log(logLevel, "Job " + jobId + " registered to " + entityId);
        jobToEntityIdMap.put(jobId, entityId);
    }

    public synchronized void keepDeployedJob(EntityId entityId, DeployedJobSpecId deployedJobSpecId,
            int livePartitionCount) {
        LOGGER.log(logLevel, "Keep " + entityId + " with " + livePartitionCount + "partitions.");
        keepDeployedJobMap.put(entityId, deployedJobSpecId);
        entityToLivePartitionCount.put(entityId, livePartitionCount);
    }

    public synchronized void dropDeployedJob(EntityId entityId) {
        int newCount = entityToLivePartitionCount.get(entityId) - 1;
        LOGGER.log(logLevel, "Drop " + entityId + " to " + newCount + " partitions.");
        if (newCount == 0) {
            keepDeployedJobMap.remove(entityId);
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

    private JobId startNewDeployedJob(DeployedJobSpecId deployedJobId) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Start a new job for deployed job " + deployedJobId);
        }
        ICcApplicationContext ccAppCtx = ((ICcApplicationContext) ccServiceCtx.getApplicationContext());
        Map<byte[], byte[]> jobParameter = new HashMap<>();
        TxnId newDeployedJobTxnId = ccAppCtx.getTxnIdFactory().create();
        jobParameter.put((TRANSACTION_ID_PARAMETER_NAME).getBytes(),
                String.valueOf(newDeployedJobTxnId.getId()).getBytes());
        return ccAppCtx.getHcc().startJob(deployedJobId, jobParameter);
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
                    Thread startT = new Thread(() -> {
                        try {
                            jobToEntityIdMap.put(startNewDeployedJob(keepDeployedJobMap.get(entityId)), entityId);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                    startT.start();
                }

            }
        } catch (Exception e) {
            throw new HyracksDataException(e.getMessage());
        }
    }
}
