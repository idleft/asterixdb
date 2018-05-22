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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.common.memory.ConcurrentFramePool;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PartitionHolderManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int SHUTDOWN_TIMEOUT_SECS = 60;

    private final ConcurrentMap<PartitionHolderId, IPartitionHolderRuntime> runtimes;
    private final ConcurrentFramePool activeFramePool;
    private final String nodeId;
    private final INCServiceContext serviceCtx;
    private volatile boolean shutdown;

    public PartitionHolderManager(String nodeId, long activeMemoryBudget, int frameSize, INCServiceContext serviceCtx)
            throws HyracksDataException {
        this.nodeId = nodeId;
        this.activeFramePool = new ConcurrentFramePool(nodeId, activeMemoryBudget, frameSize);
        this.runtimes = new ConcurrentHashMap<>();
        this.serviceCtx = serviceCtx;
    }

    public ConcurrentFramePool getFramePool() {
        return activeFramePool;
    }

    public void registerRuntime(IPartitionHolderRuntime runtime) throws HyracksDataException {
        if (runtimes.putIfAbsent(runtime.getPartitionHolderId(), runtime) != null) {
            throw new IllegalStateException(
                    "Partition holder " + runtime.getPartitionHolderId() + " is already registered");
        }
    }

    public void deregisterRuntime(PartitionHolderId id) {
        runtimes.remove(id);
    }

    public Set<PartitionHolderId> getRuntimeIds() {
        return Collections.unmodifiableSet(runtimes.keySet());
    }

    public IPartitionHolderRuntime getPartitionHolderRuntime(PartitionHolderId runtimeId) {
        return runtimes.get(runtimeId);
    }

    public void shutdownByEntity(EntityId entityId) throws HyracksDataException {
        for (Map.Entry<PartitionHolderId, IPartitionHolderRuntime> entry : runtimes.entrySet()) {
            if (entry.getKey().getEntityId().equals(entityId)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(entry.getKey() + " is requested to shutdown.");
                }
                entry.getValue().shutdown();
            }
        }
    }

    public void shutdownByPartionHolderId(PartitionHolderId phid) throws HyracksDataException {
        for (Map.Entry<PartitionHolderId, IPartitionHolderRuntime> entry : runtimes.entrySet()) {
            PartitionHolderId targetPhid = entry.getKey();
            if (targetPhid == phid || (phid.getPartition() == -1 && targetPhid.getEntityId().equals(phid.getEntityId())
                    && targetPhid.getRuntimeName().equals(phid.getRuntimeName()))) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(phid + " is requested to shutdown.");
                }
                entry.getValue().shutdown();
            }
        }
    }

    @Override
    public String toString() {
        return PartitionHolderManager.class.getSimpleName() + "[" + nodeId + "]";
    }
}
