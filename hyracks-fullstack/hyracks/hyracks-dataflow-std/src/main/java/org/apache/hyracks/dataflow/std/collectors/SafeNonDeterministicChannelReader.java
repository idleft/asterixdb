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
package org.apache.hyracks.dataflow.std.collectors;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hyracks.api.channels.IInputChannel;
import org.apache.hyracks.api.channels.IInputChannelMonitor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SafeNonDeterministicChannelReader extends NonDeterministicChannelReader {

    public SafeNonDeterministicChannelReader(int nSenderPartitions, BitSet expectedPartitions) {
        super(nSenderPartitions, expectedPartitions);
    }

    @Override
    public void addPartition(PartitionId pid, IInputChannel channel) {
        channel.registerMonitor(this);
        channel.setAttachment(pid);
        synchronized (this) {
            channels[(pid.getSenderIndex())%nSenderPartitions] = channel;
        }
    }

    @Override
    public synchronized void notifyFailure(IInputChannel channel) {
        PartitionId pid = (PartitionId) channel.getAttachment();
        int senderIndex = pid.getSenderIndex();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Failure: " + pid.getConnectorDescriptorId() + " sender: " + senderIndex + " receiver: "
                    + pid.getReceiverIndex());
        }
        failSenders.set(senderIndex % nSenderPartitions);
        eosSenders.set(senderIndex % nSenderPartitions);
        notifyAll();
    }

    @Override
    public synchronized void notifyDataAvailability(IInputChannel channel, int nFrames) {
        PartitionId pid = (PartitionId) channel.getAttachment();
        int senderIndex = pid.getSenderIndex();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Data available: " + pid.getConnectorDescriptorId() + " sender: " + senderIndex + " receiver: "
                    + pid.getReceiverIndex());
        }
        availableFrameCounts[senderIndex % nSenderPartitions] += nFrames;
        frameAvailability.set(senderIndex % nSenderPartitions);
        notifyAll();
    }

    @Override
    public synchronized void notifyEndOfStream(IInputChannel channel) {
        PartitionId pid = (PartitionId) channel.getAttachment();
        int senderIndex = pid.getSenderIndex();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("EOS: " + pid);
        }
        eosSenders.set(senderIndex % nSenderPartitions);
        notifyAll();
    }
}
