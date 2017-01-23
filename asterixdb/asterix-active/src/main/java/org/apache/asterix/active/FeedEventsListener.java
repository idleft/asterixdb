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
package org.apache.asterix.active;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.runtime.util.AppContextInfo;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;

public class FeedEventsListener implements IActiveEntityEventsListener {

    // constants
    private static final Logger LOGGER = Logger.getLogger(FeedEventsListener.class.getName());
    // members
    private final EntityId entityId;
    private final List<Dataset> datasets;
    private final int numSources;
    private final List<StateSubscriber> subscribers;
    private volatile byte state;
    private int numRegistered;
    private JobId jobId;

    public FeedEventsListener(EntityId entityId, List<Dataset> datasets, int numSources) {
        this.entityId = entityId;
        this.datasets = datasets;
        this.numSources = numSources;
        subscribers = Collections.synchronizedList(new ArrayList<>());
        state = ActivityState.STOPPED;
    }

    @Override
    public synchronized void notify(ActiveEvent event) {
        try {
            switch (event.getEventKind()) {
                case ActiveEvent.JOB_START:
                    start(event);
                    break;
                case ActiveEvent.JOB_FINISH:
                    finish();
                    break;
                case ActiveEvent.PARTITION_EVENT:
                    partition((ActivePartitionMessage) event.getEventObject());
                    break;
                default:
                    LOGGER.log(Level.SEVERE, "Unknown Feed Event" + event);
                    break;
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unhandled Exception", e);
        }
    }

    private void partition(ActivePartitionMessage message) {
        if (message.getEvent() == ActivePartitionMessage.ACTIVE_RUNTIME_REGISTERED) {
            numRegistered++;
            if (numRegistered == numSources) {
                state = ActivityState.STARTED;
            }
        }
    }

    private void finish() throws Exception {
        IHyracksClientConnection hcc = AppContextInfo.INSTANCE.getHcc();
        JobStatus status = hcc.getJobStatus(jobId);
        state = status.equals(JobStatus.FAILURE) ? ActivityState.FAILED : ActivityState.STOPPED;
    }

    private void start(ActiveEvent event) {
        this.jobId = event.getJobId();
        state = ActivityState.STARTING;
    }

    @Override
    public EntityId getEntityId() {
        return entityId;
    }

    @Override
    public byte getState() {
        return state;
    }

    @Override
    public void sync(byte state) throws HyracksDataException {
        if (state != ActivityState.STARTED && state != ActivityState.STOPPED) {
            throw new HyracksDataException("Can only wait for STARTED or STOPPED state");
        }
        StateSubscriber subscriber;
        synchronized (this) {
            if (this.state == ActivityState.FAILED) {
                throw new HyracksDataException("Feed has failed");
            } else if (this.state == state) {
                return;
            }
            subscriber = subscribe(state);
        }
        try {
            subscriber.complete();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.WARNING, "Thread interrupted while waiting for active entity to become active/inactive",
                    e);
            throw new HyracksDataException(e);
        }
    }

    // Called within synchronized block
    private StateSubscriber subscribe(byte state) {
        StateSubscriber subscriber = new StateSubscriber(state);
        subscribers.add(subscriber);
        return subscriber;
    }

    @Override
    public boolean isEntityUsingDataset(Dataset dataset) {
        return datasets.contains(dataset);
    }

}
