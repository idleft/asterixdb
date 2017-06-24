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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveLifecycleListener;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEventSubscriber;
import org.apache.asterix.active.message.ActiveManagerMessage;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.active.message.StatsRequestMessage;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.asterix.external.operators.FeedIntakeOperatorNodePushable;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobStatus;

public class FeedEventsListener extends ActiveEntityEventsListener {
    // constants
    private static final Logger LOGGER = Logger.getLogger(FeedEventsListener.class.getName());
    // members
    private final AlgebricksAbsolutePartitionConstraint locations;
    private final String[] feedStats;
    private int numRegistered;

    public FeedEventsListener(ICcApplicationContext appCtx, EntityId entityId, List<IDataset> datasets,
            AlgebricksAbsolutePartitionConstraint locations) {
        super(appCtx, entityId, datasets);
        this.locations = locations;
        feedStats = new String[locations.getLocations().length];
    }

    @Override
    public synchronized void notify(ActiveEvent event) {
        try {
            LOGGER.finer("EventListener is notified.");
            ActiveEvent.Kind eventKind = event.getEventKind();
            switch (eventKind) {
                case JOB_CREATED:
                    state = ActivityState.CREATED;
                    break;
                case JOB_STARTED:
                    start(event);
                    break;
                case JOB_FINISHED:
                    finish();
                    break;
                case PARTITION_EVENT:
                    partition((ActivePartitionMessage) event.getEventObject());
                    break;
                default:
                    LOGGER.log(Level.WARNING, "Unhandled feed event notification: " + event);
                    break;
            }
            notifySubscribers(event);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unhandled Exception", e);
        }
    }

    private synchronized void notifySubscribers(ActiveEvent event) {
        notifyAll();
        Iterator<IActiveEventSubscriber> it = subscribers.iterator();
        while (it.hasNext()) {
            IActiveEventSubscriber subscriber = it.next();
            if (subscriber.isDone()) {
                it.remove();
            } else {
                try {
                    subscriber.notify(event);
                } catch (HyracksDataException e) {
                    LOGGER.log(Level.WARNING, "Failed to notify subscriber", e);
                }
                if (subscriber.isDone()) {
                    it.remove();
                }
            }
        }
    }

    private synchronized void partition(ActivePartitionMessage message) {
        if (message.getEvent() == ActivePartitionMessage.ACTIVE_RUNTIME_REGISTERED) {
            numRegistered++;
            if (numRegistered == getLocations().getLocations().length) {
                state = ActivityState.STARTED;
            }
        } else if (message.getEvent() == ActivePartitionMessage.ACTIVE_RUNTIME_STATS) {
            feedStats[message.getActiveRuntimeId().getPartition()] = (String) message.getPayload();
        }
    }

    private void finish() throws Exception {
        IHyracksClientConnection hcc = appCtx.getHcc();
        JobStatus status = hcc.getJobStatus(jobId);
        state = status.equals(JobStatus.FAILURE) ? ActivityState.FAILED : ActivityState.STOPPED;
        ActiveLifecycleListener activeLcListener = (ActiveLifecycleListener) appCtx.getActiveLifecycleListener();
        activeLcListener.getNotificationHandler().removeListener(this);
    }

    private void start(ActiveEvent event) {
        this.jobId = event.getJobId();
        state = ActivityState.STARTING;
    }

    public AlgebricksAbsolutePartitionConstraint getLocations() {
        return locations;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void refreshStats(long timeout) throws HyracksDataException {
        synchronized (this) {
            if (state != ActivityState.STARTED || statsRequestState == RequestState.STARTED) {
                return;
            } else {
                statsRequestState = RequestState.STARTED;
            }
        }
        ICCMessageBroker messageBroker = (ICCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        long reqId = messageBroker.newRequestId();
        List<INcAddressedMessage> requests = new ArrayList<>();
        List<String> ncs = Arrays.asList(locations.getLocations());
        for (int i = 0; i < ncs.size(); i++) {
            requests.add(new StatsRequestMessage(ActiveManagerMessage.REQUEST_STATS,
                    new ActiveRuntimeId(entityId, FeedIntakeOperatorNodePushable.class.getSimpleName(), i), reqId));
        }
        try {
            List<String> response = (List<String>) messageBroker.sendSyncRequestToNCs(reqId, ncs, requests, timeout);
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append('[').append(response.get(0));
            for (int i = 1; i < response.size(); i++) {
                strBuilder.append(", ").append(response.get(i));
            }
            strBuilder.append(']');
            stats = strBuilder.toString();
            statsTimestamp = System.currentTimeMillis();
            notifySubscribers(statsUpdatedEvent);
        } catch (TimeoutException e) {
            // Following two state changes are synchronized
            statsRequestState = RequestState.TIMEDOUT;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        // Same as above
        statsRequestState = RequestState.FINISHED;
    }
}
