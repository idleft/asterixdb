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

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.active.DeployedJobLifeCycleListener;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StartFeedConnWorkersMsg implements ICcAddressedMessage {

    private static Logger LOGGER = LogManager.getLogger();

    public static final String TRANSACTION_ID_PARAMETER_NAME = "TxnIdParameter";
    public static final String DATA_FRAME_PARAMETER_NAME = "DataFrameParameter";
    public static final String DATA_FRAME_FRAME_ID_NAME = "DataFrameID";

    private static final long serialVersionUID = 1L;
    private DeployedJobSpecId deployedJobId;
    private int workerNum;
    private EntityId feedId;
    private int livePartitionN;

    public StartFeedConnWorkersMsg(DeployedJobSpecId deployedJobSpecId, int workerNum, EntityId feedId,
            int livePartitionN) {
        this.deployedJobId = deployedJobSpecId;
        this.workerNum = workerNum;
        this.feedId = feedId;
        this.livePartitionN = livePartitionN;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException {
        try {
            ICcApplicationContext ccAppCtx = (ICcApplicationContext) appCtx.getServiceContext().getApplicationContext();
            DeployedJobLifeCycleListener lifeCycleListener =
                    ((DeployedJobLifeCycleListener) ccAppCtx.getDeployedJobLifeCycleListener());
            lifeCycleListener.keepDeployedJob(feedId, deployedJobId, livePartitionN);
            for (int iter1 = 0; iter1 < workerNum; iter1++) {
                Map<byte[], byte[]> jobParameter = new HashMap<>();
                TxnId newDeployedJobTxnId = ccAppCtx.getTxnIdFactory().create();
                jobParameter.put((TRANSACTION_ID_PARAMETER_NAME).getBytes(),
                        String.valueOf(newDeployedJobTxnId.getId()).getBytes());
                JobId newConnJobId = ccAppCtx.getHcc().startJob(deployedJobId, jobParameter);
                lifeCycleListener.registerDeployedJob(newConnJobId, feedId);
                LOGGER.log(Level.DEBUG, "Start and keep alive " + newConnJobId);
            }

        } catch (Exception e) {
            throw new HyracksDataException(e.getMessage());
        }
    }
}
