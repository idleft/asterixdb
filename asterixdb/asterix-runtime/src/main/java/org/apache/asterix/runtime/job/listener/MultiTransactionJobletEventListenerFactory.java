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
package org.apache.asterix.runtime.job.listener;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.api.IJobEventListenerFactory;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.job.IJobletEventListener;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobParameterByteStore;
import org.apache.hyracks.api.job.JobStatus;

/**
 * This Joblet enable transactions on multiple datasets to take place in the same Hyracks Job
 * It takes a list of Transaction job ids instead of a single job Id
 */
public class MultiTransactionJobletEventListenerFactory implements IJobEventListenerFactory {

    public static final String TRANSACTION_ID_PARAMETER_NAME = "TxnIdParameter";

    private static final long serialVersionUID = 1L;
    private final Map<Integer, TxnId> txnIdMap;
    private final boolean transactionalWrite;

    // TODO: maybe we want to replace multiple insert with one
    public MultiTransactionJobletEventListenerFactory(Map<Integer, TxnId> txnIdMap, boolean transactionalWrite) {
        this.txnIdMap = txnIdMap;
        this.transactionalWrite = transactionalWrite;
    }

    @Override
    public TxnId getTxnId(int datasetId) {
        return txnIdMap.get(datasetId);
    }

    @Override
    public IJobletEventListenerFactory copyFactory() {
        // TODO: this needs to be merged in master
        Map<Integer, TxnId> newTxnIdMap = new HashMap<>();
        newTxnIdMap.putAll(txnIdMap);
        return new MultiTransactionJobletEventListenerFactory(newTxnIdMap, transactionalWrite);
    }

    @Override
    public void updateListenerJobParameters(JobParameterByteStore jobParameterByteStore) {
        int txnIdOffset = 0;
        for (Map.Entry<Integer, TxnId> entry : txnIdMap.entrySet()) {
            String txnIdString =
                    new String(jobParameterByteStore.getParameterByName(TRANSACTION_ID_PARAMETER_NAME + txnIdOffset));
            TxnId newTxnId = new TxnId(Long.valueOf(txnIdString));
            txnIdMap.put(entry.getKey(), newTxnId);
            txnIdOffset++;
        }
    }

    @Override
    public IJobletEventListener createListener(final IHyracksJobletContext jobletContext) {

        return new IJobletEventListener() {
            @Override
            public void jobletFinish(JobStatus jobStatus) {
                try {
                    ITransactionManager txnManager =
                            ((INcApplicationContext) jobletContext.getServiceContext().getApplicationContext())
                                    .getTransactionSubsystem().getTransactionManager();
                    for (TxnId subTxnId : txnIdMap.values()) {
                        ITransactionContext txnContext = txnManager.getTransactionContext(subTxnId);
                        txnContext.setWriteTxn(transactionalWrite);
                        System.out.println("SubTxn " + subTxnId + " finishing.");
                        if (jobStatus != JobStatus.FAILURE) {
                            txnManager.commitTransaction(subTxnId);
                        } else {
                            txnManager.abortTransaction(subTxnId);
                        }
                    }
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

            @Override
            public void jobletStart() {
                try {
                    TransactionOptions options =
                            new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL);
                    for (TxnId subTxnId : txnIdMap.values()) {
                        ((INcApplicationContext) jobletContext.getServiceContext().getApplicationContext())
                                .getTransactionSubsystem().getTransactionManager().beginTransaction(subTxnId, options);
                        System.out.println("SubTxn " + subTxnId + " started");
                    }
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

        };
    }
}
