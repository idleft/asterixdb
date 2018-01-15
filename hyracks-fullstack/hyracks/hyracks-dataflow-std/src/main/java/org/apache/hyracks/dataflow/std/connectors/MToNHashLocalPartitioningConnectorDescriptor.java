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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;

public class MToNHashLocalPartitioningConnectorDescriptor extends MToNPartitioningConnectorDescriptor {
    private static final long serialVersionUID = 1L;
    protected ITuplePartitionComputerFactory tpcf;
    int[] pCounts;
    int[] pOffsets;

    public MToNHashLocalPartitioningConnectorDescriptor(IConnectorDescriptorRegistry spec,
            ITuplePartitionComputerFactory tpcf, int[] pCounts, int[] pOffsets) {
        super(spec, tpcf);
        this.tpcf = tpcf;
        this.pCounts = pCounts;
        this.pOffsets = pOffsets;
    }

    @Override
    public Pair<int[], int[]> getLocalMap() {
        return Pair.of(pOffsets, pCounts);
    }
}
