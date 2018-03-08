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

import org.apache.asterix.active.message.ActiveEntityMessage;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeployedJobLifeCycleListener implements IJobLifecycleListener {

    // get service context to send message to nc
    ICCServiceContext ccServiceCtx;
    // this stores the mapping from jobid to the information of running deployed job (a hyracks job)
    Map<JobId, DeployedJobInstanceInfo> jobToDataframeMapping;

    public DeployedJobLifeCycleListener(ICCServiceContext ccServiceCtx) {
        this.ccServiceCtx = ccServiceCtx;
        this.jobToDataframeMapping = new HashMap<>();
    }

    public void registerDeployedJobWithDataFrame(JobId jobId, ActiveRuntimeId runtimeId, long dataFrameId, String ncId) {
        DeployedJobInstanceInfo info = new DeployedJobInstanceInfo(ncId, dataFrameId, runtimeId);
        jobToDataframeMapping.put(jobId, info);
    }

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification spec) throws HyracksException {
        // do nothing, i don't care ;(
    }

    @Override
    public void notifyJobStart(JobId jobId) throws HyracksException {
        // i don't care now
    }

    @Override
    public void notifyJobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions) throws HyracksException {
        // notify the nc that hosts the feed
        // currently, we ignore all the exceptions and failures
        try {
            if (jobToDataframeMapping.containsKey(jobId)) {
                DeployedJobInstanceInfo info = jobToDataframeMapping.get(jobId);
                ActiveEntityMessage ackMsg = new ActiveEntityMessage(info.runtimeId, info.dataFrameId);
                ICCMessageBroker messageBroker = (ICCMessageBroker) ccServiceCtx.getMessageBroker();
                messageBroker.sendApplicationMessageToNC(ackMsg, info.ncId);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private class DeployedJobInstanceInfo {
        private final String ncId;
        private final long dataFrameId;
        private final ActiveRuntimeId runtimeId;

        public DeployedJobInstanceInfo(String ncid, long dataFrameId, ActiveRuntimeId runtimeId) {
            this.ncId = ncid;
            this.dataFrameId = dataFrameId;
            this.runtimeId = runtimeId;
        }
    }

}
