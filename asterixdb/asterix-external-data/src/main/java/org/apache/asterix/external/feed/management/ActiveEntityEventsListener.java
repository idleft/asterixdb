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
package org.apache.asterix.external.feed.management;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.active.IActiveEventSubscriber;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;

public abstract class ActiveEntityEventsListener implements IActiveEntityEventsListener {

    enum RequestState {
        INIT,
        STARTED,
        TIMEDOUT,
        FINISHED
    }

    // members
    protected volatile ActivityState state;
    protected JobId jobId;
    protected final List<IActiveEventSubscriber> subscribers = new ArrayList<>();
    protected final ICcApplicationContext appCtx;
    protected final EntityId entityId;
    protected final List<IDataset> datasets;
    protected final ActiveEvent statsUpdatedEvent;
    protected long statsTimestamp;
    protected String stats;
    protected RequestState statsRequestState;

    public ActiveEntityEventsListener(ICcApplicationContext appCtx, EntityId entityId, List<IDataset> datasets) {
        this.appCtx = appCtx;
        this.entityId = entityId;
        this.datasets = datasets;
        state = ActivityState.STOPPED;
        statsTimestamp = Long.MIN_VALUE;
        statsRequestState = RequestState.INIT;
        statsUpdatedEvent = new ActiveEvent(null, Kind.STATS_UPDATED, entityId);
    }

    @Override
    public synchronized void subscribe(IActiveEventSubscriber subscriber) throws HyracksDataException {
        if (this.state == ActivityState.FAILED) {
            throw new RuntimeDataException(ErrorCode.CANNOT_SUBSCRIBE_TO_FAILED_ACTIVE_ENTITY);
        }
        subscriber.subscribed(this);
        if (!subscriber.isDone()) {
            subscribers.add(subscriber);
        }
    }

    @Override
    public EntityId getEntityId() {
        return entityId;
    }

    @Override
    public ActivityState getState() {
        return state;
    }

    @Override
    public boolean isEntityUsingDataset(IDataset dataset) {
        return datasets.contains(dataset);
    }

    public JobId getJobId() {
        return jobId;
    }

    @Override
    public String getStats() {
        return stats;
    }

    @Override
    public long getStatsTimeStamp() {
        return statsTimestamp;
    }
}
