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
package org.apache.asterix.active.message;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.DeployedJobLifeCycleListener;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

public class KeepDeployedJobMessage implements ICcAddressedMessage {

    private static Logger LOGGER = LogManager.getLogger();

    private static final long serialVersionUID = 1L;
    private ActiveRuntimeId runtimeId;
    private DeployedJobSpecId deployedJobSpecId;

    public KeepDeployedJobMessage(ActiveRuntimeId runtimeId, DeployedJobSpecId deployedJobSpecId) {
        this.runtimeId = runtimeId;
        this.deployedJobSpecId = deployedJobSpecId;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException {
        try {
            ICcApplicationContext ccAppCtx = (ICcApplicationContext) appCtx.getServiceContext().getApplicationContext();
            ((DeployedJobLifeCycleListener) ccAppCtx.getDeployedJobLifeCycleListener()).keepDeployedJob(runtimeId,
                    deployedJobSpecId);
        } catch (Exception e) {
            throw new HyracksDataException(e.getMessage());
        }
    }
}
