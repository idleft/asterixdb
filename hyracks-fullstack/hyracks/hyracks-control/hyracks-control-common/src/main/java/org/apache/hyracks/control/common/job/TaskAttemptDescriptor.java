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
package org.apache.hyracks.control.common.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.io.IWritable;

public class TaskAttemptDescriptor implements IWritable, Serializable {
    private static final long serialVersionUID = 1L;

    private TaskAttemptId taId;

    private int nPartitions;

    private int[] nInputPartitions;

    private int[] nInputOffsets;

    private int[] nOutputPartitions;

    private int[] nOutputOffsets;

    private NetworkAddress[][] inputPartitionLocations;

    public static TaskAttemptDescriptor create(DataInput dis) throws IOException {
        TaskAttemptDescriptor taskAttemptDescriptor = new TaskAttemptDescriptor();
        taskAttemptDescriptor.readFields(dis);
        return taskAttemptDescriptor;
    }

    private TaskAttemptDescriptor() {

    }

    public TaskAttemptDescriptor(TaskAttemptId taId, int nPartitions, int[] nInputPartitions, int[] nOutputPartitions,
            int[] nInputOffsets, int[] nOutputOffsets) {
        this.taId = taId;
        this.nPartitions = nPartitions;
        this.nInputPartitions = nInputPartitions;
        this.nOutputPartitions = nOutputPartitions;
        this.nInputOffsets = nInputOffsets;
        this.nOutputOffsets = nOutputOffsets;
    }

    public TaskAttemptId getTaskAttemptId() {
        return taId;
    }

    public int getPartitionCount() {
        return nPartitions;
    }

    public int[] getInputPartitionCounts() {
        return nInputPartitions;
    }

    public int[] getOutputPartitionCounts() {
        return nOutputPartitions;
    }

    public void setInputPartitionLocations(NetworkAddress[][] inputPartitionLocations) {
        this.inputPartitionLocations = inputPartitionLocations;
    }

    public NetworkAddress[][] getInputPartitionLocations() {
        return inputPartitionLocations;
    }

    @Override
    public String toString() {
        return "TaskAttemptDescriptor[taId = " + taId + ", nPartitions = " + nPartitions + ", nInputPartitions = "
                + Arrays.toString(nInputPartitions) + ", nOutputPartitions = " + Arrays.toString(nOutputPartitions)
                + "]";
    }

    private void writeArray(int[] outArray, DataOutput output) throws IOException{
        output.writeInt(outArray == null ? -1 : outArray.length);
        if (outArray != null) {
            for (int i = 0; i < outArray.length; i++) {
                output.writeInt(outArray[i]);
            }
        }
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        taId.writeFields(output);
        output.writeInt(nPartitions);

        writeArray(nInputPartitions, output);
        writeArray(nOutputPartitions, output);

        output.writeInt(inputPartitionLocations == null ? -1 : inputPartitionLocations.length);
        if (inputPartitionLocations != null) {
            for (int i = 0; i < inputPartitionLocations.length; i++) {
                if (inputPartitionLocations[i] != null) {
                    output.writeInt(inputPartitionLocations[i].length);
                    for (int j = 0; j < inputPartitionLocations[i].length; j++) {
                        inputPartitionLocations[i][j].writeFields(output);
                    }
                } else {
                    output.writeInt(-1);
                }
            }
        }

        writeArray(nInputOffsets, output);
        writeArray(nOutputOffsets, output);
    }

    private int[] readArray(DataInput input) throws IOException {
        int[] inArray = null;
        int inputCount = input.readInt();
        if (inputCount >= 0) {
            inArray = new int[inputCount];
            for (int i = 0; i < inArray.length; i++) {
                inArray[i] = input.readInt();
            }
        }
        return inArray;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        taId = TaskAttemptId.create(input);
        nPartitions = input.readInt();

        nInputPartitions = readArray(input);
        nOutputPartitions = readArray(input);

        int addrCount = input.readInt();
        if (addrCount >= 0) {
            inputPartitionLocations = new NetworkAddress[addrCount][];
            for (int i = 0; i < inputPartitionLocations.length; i++) {
                int columns = input.readInt();
                if (columns >= 0) {
                    inputPartitionLocations[i] = new NetworkAddress[columns];
                    for (int j = 0; j < columns; j++) {
                        inputPartitionLocations[i][j] = NetworkAddress.create(input);
                    }
                }
            }
        }

        nInputOffsets = readArray(input);
        nOutputOffsets = readArray(input);
    }

    public int getInputOffset(int i) {
        return nInputOffsets[i];
    }

    public int[] getnOutputOffsets() {
        return nOutputOffsets;
    }
}
