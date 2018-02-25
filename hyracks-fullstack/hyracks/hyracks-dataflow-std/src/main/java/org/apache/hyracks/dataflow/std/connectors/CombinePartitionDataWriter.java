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
package org.apache.hyracks.dataflow.std.connectors;

import org.apache.hyracks.api.comm.IPartitionWriterFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class CombinePartitionDataWriter extends PartitionDataWriter {

    private int[] rangeMap;
    private int range;

    public CombinePartitionDataWriter(IHyracksTaskContext ctx, int consumerPartitionCount,
            IPartitionWriterFactory pwFactory, RecordDescriptor recordDescriptor, ITuplePartitionComputer tpc,
            int[] rangeMap, int range) throws HyracksDataException {
        super(ctx, consumerPartitionCount, pwFactory, recordDescriptor, tpc);
        this.rangeMap = rangeMap;
        this.range = range;
    }

    private int remapPartitionIdx(int val) {
        int idx = 0;
        while (idx < rangeMap.length - 1 && val >= rangeMap[idx]) {
            idx++;
        }
        return idx;
    }

    @Override
    protected int getPartitionIdx(int tupleIdx) throws HyracksDataException {
        int idx = tpc.partition(tupleAccessor, tupleIdx, range);
        int mappedIdx = remapPartitionIdx(idx);
        //        System.out.println("Send " + tupleIdx + "  from " + idx + " to " + mappedIdx);
        return mappedIdx;
    }

}
