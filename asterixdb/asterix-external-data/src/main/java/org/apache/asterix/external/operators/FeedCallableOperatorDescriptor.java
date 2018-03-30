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

import java.nio.ByteBuffer;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

import static org.apache.asterix.active.message.StartFeedConnWorkersMsg.DATA_FRAME_PARAMETER_NAME;

public class FeedCallableOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    public static final int DEFAULT_FRAME_SIZE = GlobalConfig.DEFAULT_FRAME_SIZE;
    private static final long serialVersionUID = 1L;

    public FeedCallableOperatorDescriptor(JobSpecification spec, String sourceFeedNC, ActiveRuntimeId activeRuntimeId) {
        super(spec, 0, 1);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            private byte[] dataByteArray;
            private ByteBuffer buffer;

            @Override
            public void initialize() throws HyracksDataException {
                // get data frame in job parameters
                buffer = ByteBuffer.allocate(DEFAULT_FRAME_SIZE);
                dataByteArray = ctx.getJobParameter(DATA_FRAME_PARAMETER_NAME.getBytes(), 0,
                        DATA_FRAME_PARAMETER_NAME.length());
                buffer.put(dataByteArray);

                writer.open();
                writer.nextFrame(buffer);
                writer.flush();
                writer.close();
            }
        };
    }
}
