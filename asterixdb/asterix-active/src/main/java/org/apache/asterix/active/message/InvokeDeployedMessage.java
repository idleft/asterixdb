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

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.DeployedJobLifeCycleListener;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;

public class InvokeDeployedMessage implements ICcAddressedMessage {

    public static final String TRANSACTION_ID_PARAMETER_NAME = "TxnIdParameter";
    public static final String DATA_FRAME_PARAMETER_NAME = "DataFrameParameter";
    public static final String DATA_FRAME_FRAME_ID_NAME = "DataFrameID";

    private static final long serialVersionUID = 1L;
    private byte[] dataframe;
    private DeployedJobSpecId deployedJobId;
    private final int connDs;
    private final long frameId;
    private final String ncId;
    private final ActiveRuntimeId runtimeId;

    public InvokeDeployedMessage(DeployedJobSpecId deployedJobSpecId, byte[] dataframe, int connDs, long frameId,
            String ncId, ActiveRuntimeId runtimeId) {
        this.dataframe = dataframe;
        this.deployedJobId = deployedJobSpecId;
        this.connDs = connDs;
        this.frameId = frameId;
        this.ncId = ncId;
        this.runtimeId = runtimeId;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException {
        try {
            ICcApplicationContext ccAppCtx = (ICcApplicationContext) appCtx.getServiceContext().getApplicationContext();
            Map<byte[], byte[]> jobParameter = new HashMap<>();
            for (int iter1 = 0; iter1 < connDs; iter1++) {
                TxnId newDeployedJobTxnId = ccAppCtx.getTxnIdFactory().create();
                jobParameter.put((TRANSACTION_ID_PARAMETER_NAME + String.valueOf(iter1)).getBytes(),
                        String.valueOf(newDeployedJobTxnId.getId()).getBytes());
            }
            jobParameter.put(DATA_FRAME_PARAMETER_NAME.getBytes(), dataframe);
            jobParameter.put(DATA_FRAME_FRAME_ID_NAME.getBytes(), String.valueOf(frameId).getBytes());
            JobId runtimeJobId = ccAppCtx.getHcc().startJob(deployedJobId, jobParameter);
            ((DeployedJobLifeCycleListener) ccAppCtx.getDeployedJobLifeCycleListener())
                    .registerDeployedJobWithDataFrame(runtimeJobId, runtimeId, frameId, ncId);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
