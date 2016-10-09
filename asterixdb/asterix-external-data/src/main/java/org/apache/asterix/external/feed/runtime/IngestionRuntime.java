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
package org.apache.asterix.external.feed.runtime;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class IngestionRuntime implements IActiveRuntime {

    private static final Logger LOGGER = Logger.getLogger(IngestionRuntime.class.getName());

    private final AdapterRuntimeManager adapterRuntimeManager;
    private final ActiveRuntimeId runtimeId;
    private final EntityId feedId;

    public IngestionRuntime(EntityId entityId, ActiveRuntimeId runtimeId, AdapterRuntimeManager adaptorRuntimeManager) {
        this.feedId = entityId;
        this.runtimeId = runtimeId;
        this.adapterRuntimeManager = adaptorRuntimeManager;
    }

    @Override
    public ActiveRuntimeId getRuntimeId() {
        return this.runtimeId;
    }

    public void start() {
        adapterRuntimeManager.start();
        LOGGER.log(Level.INFO, "Feed " + feedId.getEntityName() + " running on partition " + runtimeId);
    }

    @Override
    public void stop() throws InterruptedException {
        adapterRuntimeManager.stop();
        LOGGER.log(Level.INFO, "Feed " + feedId.getEntityName() + " stopped on partition " + runtimeId);
    }

    public EntityId getFeedId() {
        return feedId;
    }
}
