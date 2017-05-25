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

package org.apache.asterix.external.operators;

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveRuntime;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.transactions.IAppRuntimeContextProvider;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class FeedSubWorkflowSourceOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private final EntityId feedId;

    public FeedSubWorkflowSourceOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity,
            EntityId feedId) {
        super(spec, inputArity, outputArity);
        this.feedId = feedId;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        ActiveManager activeManager = (ActiveManager) ((INcApplicationContext)ctx.getJobletContext().getServiceContext().getApplicationContext()).getActiveManager();
        ActiveRuntimeId feedIntakeActiveRuntimeId = new ActiveRuntimeId(feedId,
                FeedIntakeOperatorNodePushable.class.getSimpleName(), partition);
        IActiveRuntime feedIntakeActiveRuntime = activeManager.getRuntime(feedIntakeActiveRuntimeId);
        return new FeedSubWorkflowSourceOperatorNodePushable(ctx, partition, nPartitions, feedIntakeActiveRuntime);
    }
}
