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
package org.apache.asterix.active.partition;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class PullablePartitionHolderPushable extends AbstractUnaryInputSinkOperatorNodePushable
        implements IPullablePartitionHolderRuntime {

    private static final Logger LOGGER = LogManager.getLogger();
    protected final IHyracksTaskContext ctx;
    protected final PartitionHolderManager partitionHolderMananger;
    /** A unique identifier for the runtime **/
    protected Thread taskThread;
    protected final PartitionHolderId phid;

    public PullablePartitionHolderPushable(IHyracksTaskContext ctx, PartitionHolderId phid) {
        this.ctx = ctx;
        partitionHolderMananger = (PartitionHolderManager) ((INcApplicationContext) ctx.getJobletContext()
                .getServiceContext().getApplicationContext()).getPartitionHolderMananger();
        this.phid = phid;
    }

    @Override
    public PartitionHolderId getPartitionHolderId() {
        return phid;
    }

    @Override
    public String toString() {
        return phid.toString();
    }

    @Override
    public final void initialize() throws HyracksDataException {
        taskThread = Thread.currentThread();
        partitionHolderMananger.registerRuntime(this);
    }

    @Override
    public final void deinitialize() throws HyracksDataException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Partition Holder " + phid + " de-initialized");
        }
        partitionHolderMananger.deregisterRuntime(phid);
    }

    @Override
    public JobId getJobId() {
        return ctx.getJobletContext().getJobId();
    }
}
